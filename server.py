import asyncio
import json
import os
import random
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import httpx
from fastmcp import FastMCP
from websockets.asyncio.client import connect


# ============================================================
# CONFIGURATION
# ============================================================

BINANCE_REST_BASE = "https://fapi.binance.com"
BINANCE_WS_BASE = "wss://fstream.binance.com/market/stream"

ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}
ALLOWED_INTERVALS = {
    "1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"
}
INTERVAL_ORDER = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]

STREAMS = [
    f"{symbol.lower()}@kline_{interval}"
    for symbol in sorted(ALLOWED_SYMBOLS)
    for interval in INTERVAL_ORDER
]
WS_URL = f"{BINANCE_WS_BASE}?streams=" + "/".join(STREAMS)

# REST safety
REST_MIN_INTERVAL_SECONDS = float(os.getenv("REST_MIN_INTERVAL_SECONDS", "1.5"))
REST_WEIGHT_LIMIT = int(os.getenv("REST_WEIGHT_LIMIT", "2400"))
REST_WEIGHT_SAFETY_RATIO = float(os.getenv("REST_WEIGHT_SAFETY_RATIO", "0.75"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
FALLBACK_429_COOLDOWN = int(os.getenv("FALLBACK_429_COOLDOWN", "120"))
FALLBACK_418_COOLDOWN = int(os.getenv("FALLBACK_418_COOLDOWN", "3600"))

# WebSocket safety
WS_MIN_RECONNECT_SECONDS = float(os.getenv("WS_MIN_RECONNECT_SECONDS", "5"))
WS_MAX_RECONNECT_SECONDS = float(os.getenv("WS_MAX_RECONNECT_SECONDS", "300"))
WS_ROTATE_SECONDS = int(
    os.getenv("WS_ROTATE_SECONDS", str(23 * 60 * 60 + 50 * 60))
)
MAX_CACHED_CANDLES = int(os.getenv("MAX_CACHED_CANDLES", "1000"))


# ============================================================
# MCP SERVER
# ============================================================

mcp = FastMCP("Binance Quant Data")


# ============================================================
# RUNTIME STATE
# ============================================================

_candle_cache: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(
    lambda: deque(maxlen=MAX_CACHED_CANDLES)
)
_live_candles: dict[tuple[str, str], dict[str, Any]] = {}

_ws_stream_stats: dict[tuple[str, str], dict[str, Any]] = {
    (symbol, interval): {
        "message_count": 0,
        "closed_candle_count": 0,
        "last_event_at_utc": None,
        "last_open_time_ms": None,
        "last_close_time_ms": None,
        "last_is_closed": None,
        "last_source": None,
    }
    for symbol in sorted(ALLOWED_SYMBOLS)
    for interval in INTERVAL_ORDER
}

_ws_task: asyncio.Task | None = None
_ws_connected = False
_ws_connected_at: float | None = None
_ws_last_message_at: float | None = None
_ws_reconnect_count = 0
_ws_last_error: dict[str, Any] | None = None

_rest_lock = asyncio.Lock()
_rest_last_request_at = 0.0
_rest_blocked_until = 0.0
_rest_last_status: int | None = None
_rest_last_headers: dict[str, str] = {}
_rest_last_error: dict[str, Any] | None = None
_rest_last_success_at: float | None = None


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def epoch_ms() -> int:
    return int(time.time() * 1000)


def format_utc_timestamp(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()


def remaining_seconds(timestamp: float | None) -> int:
    if not timestamp:
        return 0
    return max(0, int(timestamp - time.time()))


def validate_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()
    if symbol not in ALLOWED_SYMBOLS:
        raise ValueError(
            f"Unsupported symbol={symbol}. Allowed={sorted(ALLOWED_SYMBOLS)}"
        )
    return symbol


def validate_interval(interval: str) -> str:
    if interval not in ALLOWED_INTERVALS:
        raise ValueError(
            f"Unsupported interval={interval}. Allowed={INTERVAL_ORDER}"
        )
    return interval


def validate_limit(limit: int) -> int:
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    return limit


def parse_retry_after(headers: httpx.Headers) -> int | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return None


def extract_binance_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        key.lower(): value
        for key, value in headers.items()
        if key.lower().startswith("x-mbx-") or key.lower() == "retry-after"
    }


def get_used_weight(headers: httpx.Headers) -> int | None:
    for key, value in headers.items():
        if key.upper() == "X-MBX-USED-WEIGHT-1M":
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None


def structured_error(
    code: str,
    message: str,
    *,
    http_status: int | None = None,
    retry_after_seconds: int | None = None,
    url: str | None = None,
    headers: dict[str, str] | None = None,
    body: str | None = None,
) -> dict[str, Any]:
    return {
        "status": code,
        "message": message,
        "http_status": http_status,
        "retry_after_seconds": retry_after_seconds,
        "url": url,
        "headers": headers or {},
        "body": body,
        "timestamp_utc": utc_now(),
    }


# ============================================================
# CANDLE VALIDATION
# ============================================================

def validate_candles(candles: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [candle["open_time_ms"] for candle in candles]
    duplicates = len(timestamps) != len(set(timestamps))
    timestamp_ordered = all(
        timestamps[index] < timestamps[index + 1]
        for index in range(len(timestamps) - 1)
    )
    ohlc_valid = all(
        candle["high"] >= max(candle["open"], candle["close"])
        and candle["low"] <= min(candle["open"], candle["close"])
        and candle["high"] >= candle["low"]
        and candle["volume"] >= 0
        for candle in candles
    )
    all_closed = all(candle["is_closed"] for candle in candles)
    return {
        "row_count": len(candles),
        "duplicates": duplicates,
        "timestamp_ordered": timestamp_ordered,
        "ohlc_valid": ohlc_valid,
        "all_closed": all_closed,
    }


# ============================================================
# CACHE HELPERS
# ============================================================

def upsert_closed_candle(key: tuple[str, str], candle: dict[str, Any]) -> None:
    """Insert/replace by open_time_ms and keep the cache sorted."""
    cache = _candle_cache[key]
    by_open_time = {item["open_time_ms"]: item for item in cache}
    by_open_time[candle["open_time_ms"]] = candle
    ordered = sorted(by_open_time.values(), key=lambda item: item["open_time_ms"])
    cache.clear()
    cache.extend(ordered[-MAX_CACHED_CANDLES:])


def cache_candle_source(candles: list[dict[str, Any]]) -> str:
    if not candles:
        return "none"
    sources = {candle.get("source") for candle in candles}
    if sources == {"binance_websocket"}:
        return "websocket_cache"
    if sources == {"binance_rest"}:
        return "binance_rest_backfill"
    return "mixed_websocket_cache_and_rest"


def build_cache_entry(symbol: str, interval: str) -> dict[str, Any]:
    cache = _candle_cache[(symbol, interval)]
    stats = _ws_stream_stats[(symbol, interval)]
    return {
        "symbol": symbol,
        "interval": interval,
        "closed_candle_count": len(cache),
        "latest_closed_open_time_ms": cache[-1]["open_time_ms"] if cache else None,
        "latest_closed_open_time_utc": (
            format_utc_timestamp(cache[-1]["open_time_ms"]) if cache else None
        ),
        "has_live_candle": (symbol, interval) in _live_candles,
        "websocket_stream_received_messages": stats["message_count"],
        "websocket_stream_closed_candles_seen": stats["closed_candle_count"],
        "websocket_last_event_at_utc": stats["last_event_at_utc"],
        "websocket_last_open_time_ms": stats["last_open_time_ms"],
        "websocket_last_open_time_utc": format_utc_timestamp(
            stats["last_open_time_ms"]
        ),
        "websocket_last_close_time_ms": stats["last_close_time_ms"],
        "websocket_last_is_closed": stats["last_is_closed"],
        "websocket_last_source": stats["last_source"],
    }


# ============================================================
# REST RATE-LIMIT / CIRCUIT BREAKER
# ============================================================

async def rest_pacing() -> None:
    global _rest_last_request_at
    now = time.monotonic()
    wait_seconds = REST_MIN_INTERVAL_SECONDS - (now - _rest_last_request_at)
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    _rest_last_request_at = time.monotonic()


async def protected_rest_get(
    path: str,
    params: dict[str, Any] | None = None,
) -> tuple[Any | None, dict[str, Any] | None]:
    global _rest_blocked_until
    global _rest_last_status
    global _rest_last_headers
    global _rest_last_error
    global _rest_last_success_at

    if time.time() < _rest_blocked_until:
        seconds = remaining_seconds(_rest_blocked_until)
        return None, structured_error(
            "REST_LOCAL_COOLDOWN",
            "REST access is locally blocked. No Binance request was sent.",
            retry_after_seconds=seconds,
        )

    url = f"{BINANCE_REST_BASE}{path}"
    last_exception = None

    for attempt in range(3):
        try:
            async with _rest_lock:
                await rest_pacing()
                async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
                    response = await client.get(
                        url,
                        params=params,
                        headers={"User-Agent": "BinanceQuantDataMCP/PhaseA"},
                    )

            _rest_last_status = response.status_code
            _rest_last_headers = extract_binance_headers(response.headers)
            used_weight = get_used_weight(response.headers)

            if used_weight is not None:
                safety_threshold = int(
                    REST_WEIGHT_LIMIT * REST_WEIGHT_SAFETY_RATIO
                )
                if used_weight >= safety_threshold:
                    _rest_blocked_until = time.time() + 60

            if response.status_code == 429:
                retry_after = parse_retry_after(response.headers)
                if retry_after is None:
                    retry_after = FALLBACK_429_COOLDOWN
                _rest_blocked_until = time.time() + retry_after
                error = structured_error(
                    "BINANCE_RATE_LIMITED",
                    "Binance returned HTTP 429. REST requests are stopped until Retry-After expires. No retry was performed.",
                    http_status=429,
                    retry_after_seconds=retry_after,
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )
                _rest_last_error = error
                return None, error

            if response.status_code == 418:
                retry_after = parse_retry_after(response.headers)
                if retry_after is None:
                    retry_after = FALLBACK_418_COOLDOWN
                _rest_blocked_until = time.time() + retry_after
                error = structured_error(
                    "BINANCE_IP_BANNED",
                    "Binance returned HTTP 418. REST requests are stopped until Retry-After expires. No retry was performed.",
                    http_status=418,
                    retry_after_seconds=retry_after,
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )
                _rest_last_error = error
                return None, error

            if response.status_code in (403, 451):
                code = "BINANCE_WAF_BLOCK" if response.status_code == 403 else "BINANCE_GEO_OR_POLICY_BLOCK"
                error = structured_error(
                    code,
                    "Binance rejected the request. No retry was attempted.",
                    http_status=response.status_code,
                    retry_after_seconds=parse_retry_after(response.headers),
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )
                _rest_last_error = error
                return None, error

            if response.status_code >= 500:
                response.raise_for_status()

            response.raise_for_status()
            _rest_last_success_at = time.time()
            _rest_last_error = None
            return response.json(), None

        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
        ) as exc:
            last_exception = exc
        except httpx.HTTPStatusError as exc:
            last_exception = exc
        except Exception as exc:
            last_exception = exc
            break

        backoff_seconds = min(30.0, (2 ** attempt) + random.uniform(0, 1))
        await asyncio.sleep(backoff_seconds)

    error = structured_error(
        "BINANCE_NETWORK_OR_5XX_ERROR",
        "Binance REST request failed after bounded transient-error retries.",
        url=url,
        body=str(last_exception)[:1000] if last_exception else None,
    )
    _rest_last_error = error
    return None, error


# ============================================================
# WEBSOCKET CACHE
# ============================================================

def ws_candle_to_dict(event: dict[str, Any]) -> dict[str, Any]:
    kline = event["k"]
    return {
        "open_time_ms": int(kline["t"]),
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
        "volume": float(kline["v"]),
        "close_time_ms": int(kline["T"]),
        "quote_volume": float(kline["q"]),
        "trade_count": int(kline["n"]),
        "taker_buy_volume": float(kline["V"]),
        "taker_buy_quote_volume": float(kline["Q"]),
        "is_closed": bool(kline["x"]),
        "source": "binance_websocket",
        "event_time_ms": int(event["E"]),
        "received_at_utc": utc_now(),
    }


async def websocket_manager() -> None:
    global _ws_connected
    global _ws_connected_at
    global _ws_last_message_at
    global _ws_reconnect_count
    global _ws_last_error

    reconnect_delay = WS_MIN_RECONNECT_SECONDS

    while True:
        connection_started = time.monotonic()
        try:
            async with connect(
                WS_URL,
                open_timeout=10,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1024,
            ) as websocket:
                _ws_connected = True
                _ws_connected_at = time.time()
                _ws_last_error = None
                reconnect_delay = WS_MIN_RECONNECT_SECONDS

                while time.monotonic() - connection_started < WS_ROTATE_SECONDS:
                    raw = await websocket.recv()
                    _ws_last_message_at = time.time()

                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")

                    payload = json.loads(raw)
                    event = payload.get("data", payload)

                    if event.get("e") != "kline":
                        continue

                    kline = event.get("k", {})
                    symbol = str(kline.get("s", "")).upper()
                    interval = str(kline.get("i", ""))

                    if symbol not in ALLOWED_SYMBOLS or interval not in ALLOWED_INTERVALS:
                        continue

                    key = (symbol, interval)
                    candle = ws_candle_to_dict(event)
                    stats = _ws_stream_stats[key]
                    stats["message_count"] += 1
                    stats["last_event_at_utc"] = utc_now()
                    stats["last_open_time_ms"] = candle["open_time_ms"]
                    stats["last_close_time_ms"] = candle["close_time_ms"]
                    stats["last_is_closed"] = candle["is_closed"]
                    stats["last_source"] = "binance_websocket"

                    _live_candles[key] = candle

                    if candle["is_closed"]:
                        stats["closed_candle_count"] += 1
                        upsert_closed_candle(key, candle)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ws_reconnect_count += 1
            _ws_last_error = {
                "code": "WEBSOCKET_ERROR",
                "message": str(exc)[:1000],
                "timestamp_utc": utc_now(),
                "reconnect_delay_seconds": reconnect_delay,
            }
        finally:
            _ws_connected = False

        await asyncio.sleep(
            reconnect_delay
            + random.uniform(0, min(5.0, reconnect_delay * 0.25))
        )
        reconnect_delay = min(WS_MAX_RECONNECT_SECONDS, reconnect_delay * 2)


async def ensure_websocket_started(
    wait_for_startup: bool = True,
    timeout_seconds: float | None = None,
) -> None:
    """
    Start the long-lived WebSocket manager if necessary.

    Important: the previous implementation created the task and
    immediately returned. On the first tool call this could produce
    a perfectly healthy server with connected=false simply because
    the background task had not been scheduled far enough to complete
    the WebSocket handshake.

    This version optionally waits briefly for either:
    - WebSocket connected
    - a WebSocket error to be recorded
    - the startup timeout to expire

    It never calls REST.
    """
    global _ws_task
    global _ws_task_started_at

    if _ws_task is None or _ws_task.done():
        _ws_task = asyncio.create_task(websocket_manager())
        _ws_task_started_at = time.time()

    if not wait_for_startup:
        return

    timeout = (
        WS_STARTUP_WAIT_SECONDS
        if timeout_seconds is None
        else max(0.0, timeout_seconds)
    )

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if _ws_connected or _ws_last_error is not None:
            return

        if _ws_task is not None and _ws_task.done():
            return

        await asyncio.sleep(0.1)


# ============================================================
# REST BACKFILL
# ============================================================

async def rest_backfill(symbol: str, interval: str, limit: int) -> dict[str, Any]:
    raw, error = await protected_rest_get(
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": min(limit + 2, 100),
        },
    )

    if error:
        return {"status": "DATA_UNAVAILABLE", "error": error}

    current_ms = epoch_ms()
    closed_candles: list[dict[str, Any]] = []

    for row in raw or []:
        close_time_ms = int(row[6])
        if close_time_ms >= current_ms:
            continue
        closed_candles.append(
            {
                "open_time_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time_ms": close_time_ms,
                "quote_volume": float(row[7]),
                "trade_count": int(row[8]),
                "taker_buy_volume": float(row[9]),
                "taker_buy_quote_volume": float(row[10]),
                "is_closed": True,
                "source": "binance_rest",
                "received_at_utc": utc_now(),
            }
        )

    key = (symbol, interval)
    for candle in closed_candles:
        upsert_closed_candle(key, candle)

    return {
        "status": "PASS",
        "source": "binance_rest_backfill",
        "candles": list(_candle_cache[key])[-limit:],
    }


# ============================================================
# MCP TOOLS
# ============================================================

@mcp.tool
async def ping() -> dict[str, Any]:
    """MCP health check. Does NOT call Binance REST."""
    await ensure_websocket_started()
    return {
        "status": "PASS",
        "service": "Binance Quant Data",
        "market_type": "Binance USDⓈ-M Futures",
        "websocket": {
            "connected": _ws_connected,
            "connected_at_utc": (
                format_utc_timestamp(int(_ws_connected_at * 1000))
                if _ws_connected_at else None
            ),
            "last_message_at_utc": (
                format_utc_timestamp(int(_ws_last_message_at * 1000))
                if _ws_last_message_at else None
            ),
            "reconnect_count": _ws_reconnect_count,
            "last_error": _ws_last_error,
            "task_started_at_utc": (
                format_utc_timestamp(int(_ws_task_started_at * 1000))
                if _ws_task_started_at else None
            ),
            "task_done": (
                _ws_task.done()
                if _ws_task is not None else None
            ),
            "stream_count": len(STREAMS),
            "expected_streams": STREAMS,
        },
        "rest": {
            "last_http_status": _rest_last_status,
            "blocked_seconds_remaining": remaining_seconds(_rest_blocked_until),
            "last_error": _rest_last_error,
            "last_headers": _rest_last_headers,
        },
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def rest_status() -> dict[str, Any]:
    """Show REST circuit-breaker state. Does NOT call Binance."""
    return {
        "status": "PASS",
        "last_http_status": _rest_last_status,
        "blocked_seconds_remaining": remaining_seconds(_rest_blocked_until),
        "last_success_at_utc": (
            format_utc_timestamp(int(_rest_last_success_at * 1000))
            if _rest_last_success_at else None
        ),
        "last_error": _rest_last_error,
        "last_headers": _rest_last_headers,
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def websocket_status() -> dict[str, Any]:
    """Show WebSocket connection/cache summary. Does NOT call REST."""
    await ensure_websocket_started()
    cache_status: dict[str, Any] = {}

    for symbol in sorted(ALLOWED_SYMBOLS):
        for interval in INTERVAL_ORDER:
            entry = build_cache_entry(symbol, interval)
            cache_status[f"{symbol}:{interval}"] = {
                "closed_candle_count": entry["closed_candle_count"],
                "latest_open_time_ms": entry["latest_closed_open_time_ms"],
                "has_live_candle": entry["has_live_candle"],
                "websocket_stream_received_messages": entry["websocket_stream_received_messages"],
                "websocket_last_is_closed": entry["websocket_last_is_closed"],
                "websocket_last_event_at_utc": entry["websocket_last_event_at_utc"],
            }

    return {
        "status": "PASS",
        "connected": _ws_connected,
        "reconnect_count": _ws_reconnect_count,
        "last_message_at_utc": (
            format_utc_timestamp(int(_ws_last_message_at * 1000))
            if _ws_last_message_at else None
        ),
        "last_error": _ws_last_error,
        "task_started_at_utc": (
            format_utc_timestamp(int(_ws_task_started_at * 1000))
            if _ws_task_started_at else None
        ),
        "task_done": (
            _ws_task.done()
            if _ws_task is not None else None
        ),
        "configured_stream_count": len(STREAMS),
        "configured_streams": STREAMS,
        "cache": cache_status,
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def websocket_cache_status() -> dict[str, Any]:
    """
    Detailed WebSocket/cache diagnostics.

    Does NOT call Binance REST.

    message_count > 0 means the kline stream for that interval is
    actually delivering events to this process, even if no candle has
    closed yet. closed_candle_count > 0 means a closed candle has been
    cached since this service instance started.
    """
    await ensure_websocket_started()

    streams = {
        f"{symbol}:{interval}": build_cache_entry(symbol, interval)
        for symbol in sorted(ALLOWED_SYMBOLS)
        for interval in INTERVAL_ORDER
    }

    return {
        "status": "PASS",
        "websocket": {
            "connected": _ws_connected,
            "connected_at_utc": (
                format_utc_timestamp(int(_ws_connected_at * 1000))
                if _ws_connected_at else None
            ),
            "last_message_at_utc": (
                format_utc_timestamp(int(_ws_last_message_at * 1000))
                if _ws_last_message_at else None
            ),
            "reconnect_count": _ws_reconnect_count,
            "last_error": _ws_last_error,
            "task_started_at_utc": (
                format_utc_timestamp(int(_ws_task_started_at * 1000))
                if _ws_task_started_at else None
            ),
            "task_done": (
                _ws_task.done()
                if _ws_task is not None else None
            ),
            "configured_stream_count": len(STREAMS),
        },
        "streams": streams,
        "rest_touched": False,
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def websocket_probe() -> dict[str, Any]:
    """
    One-shot WebSocket connectivity probe.

    This opens the exact configured Binance market WebSocket URL, waits
    for the first market-data message, reports the result, then closes
    the probe connection. It does NOT call Binance REST and does not
    modify the long-lived cache.
    """
    started = utc_now()
    started_monotonic = time.monotonic()

    try:
        async with connect(
            WS_URL,
            open_timeout=10,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_queue=1024,
        ) as websocket:
            raw = await asyncio.wait_for(
                websocket.recv(),
                timeout=10,
            )

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            payload = json.loads(raw)
            event = payload.get("data", payload)
            kline = event.get("k", {}) if isinstance(event, dict) else {}

            return {
                "status": "PASS",
                "connected": True,
                "first_event_type": event.get("e") if isinstance(event, dict) else None,
                "symbol": str(kline.get("s", "")).upper() or None,
                "interval": kline.get("i"),
                "event_time_ms": event.get("E") if isinstance(event, dict) else None,
                "elapsed_seconds": round(time.monotonic() - started_monotonic, 3),
                "rest_called": False,
                "started_at_utc": started,
                "timestamp_utc": utc_now(),
            }

    except Exception as exc:
        return {
            "status": "DATA_UNAVAILABLE",
            "connected": False,
            "error": {
                "code": "WEBSOCKET_PROBE_FAILED",
                "message": str(exc)[:1000],
            },
            "elapsed_seconds": round(time.monotonic() - started_monotonic, 3),
            "rest_called": False,
            "started_at_utc": started,
            "timestamp_utc": utc_now(),
        }


@mcp.tool
async def get_cached_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    limit: int = 1,
) -> dict[str, Any]:
    """
    Read ONLY from the WebSocket closed-candle cache.

    This tool NEVER calls REST and never triggers REST fallback.
    """
    symbol = validate_symbol(symbol)
    interval = validate_interval(interval)
    limit = validate_limit(limit)
    await ensure_websocket_started()

    key = (symbol, interval)
    cache = _candle_cache[key]
    candles = list(cache)[-limit:]
    quality = validate_candles(candles)
    enough_rows = len(candles) >= limit

    return {
        "status": "PASS" if enough_rows else "DATA_UNAVAILABLE",
        "source": "websocket_cache",
        "market_type": "Binance USDⓈ-M Futures",
        "symbol": symbol,
        "interval": interval,
        "requested_limit": limit,
        "cache_count": len(cache),
        "row_count": len(candles),
        "data_quality": quality,
        "candles": candles,
        "rest_called": False,
        "rest_touched": False,
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def rest_ping() -> dict[str, Any]:
    """Explicitly test Binance REST connectivity; protected by breaker."""
    data, error = await protected_rest_get("/fapi/v1/time")
    if error:
        return {"status": "DATA_UNAVAILABLE", "error": error}
    return {
        "status": "PASS",
        "source": "Binance",
        "market_type": "USDⓈ-M Futures",
        "server_time_ms": data["serverTime"],
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def get_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    limit: int = 5,
) -> dict[str, Any]:
    """
    Production path:
    1) WebSocket closed-candle cache
    2) One protected REST backfill if cache is insufficient

    Unlike get_cached_klines(), this tool MAY call REST.
    """
    symbol = validate_symbol(symbol)
    interval = validate_interval(interval)
    limit = validate_limit(limit)
    await ensure_websocket_started()

    key = (symbol, interval)
    cache = _candle_cache[key]
    rest_used = False

    if len(cache) < limit:
        result = await rest_backfill(symbol, interval, limit)
        if result["status"] != "PASS":
            return {
                "status": "DATA_UNAVAILABLE",
                "symbol": symbol,
                "interval": interval,
                "requested_limit": limit,
                "error": result["error"],
                "cache_count": len(cache),
                "rest_called": True,
                "timestamp_utc": utc_now(),
            }
        rest_used = True

    candles = list(cache)[-limit:]
    quality = validate_candles(candles)

    return {
        "status": "PASS" if quality["row_count"] == limit else "PARTIAL",
        "source": cache_candle_source(candles),
        "market_type": "Binance USDⓈ-M Futures",
        "symbol": symbol,
        "interval": interval,
        "requested_limit": limit,
        "row_count": len(candles),
        "data_quality": quality,
        "candles": candles,
        "rest_called": rest_used,
        "rest_protection": {
            "last_http_status": _rest_last_status,
            "blocked_seconds_remaining": remaining_seconds(_rest_blocked_until),
        },
        "timestamp_utc": utc_now(),
    }


@mcp.tool
async def get_live_candle(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
) -> dict[str, Any]:
    """Return the current forming WebSocket candle. Does NOT call REST."""
    symbol = validate_symbol(symbol)
    interval = validate_interval(interval)
    await ensure_websocket_started()

    candle = _live_candles.get((symbol, interval))
    if candle is None:
        return {
            "status": "DATA_UNAVAILABLE",
            "message": "No live WebSocket candle received yet.",
            "symbol": symbol,
            "interval": interval,
            "rest_called": False,
            "timestamp_utc": utc_now(),
        }

    return {
        "status": "PASS",
        "source": "binance_websocket",
        "symbol": symbol,
        "interval": interval,
        "candle": candle,
        "rest_called": False,
        "timestamp_utc": utc_now(),
    }


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
        stateless_http=True,
        json_response=True,
    )
