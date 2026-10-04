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

# Binance USDⓈ-M current routed WebSocket market endpoint.
BINANCE_WS_BASE = "wss://fstream.binance.com/market/stream"

ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}

ALLOWED_INTERVALS = {
    "1m",
    "5m",
    "15m",
    "1h",
    "4h",
    "1d",
    "1w",
    "1M",
}

# One combined WebSocket connection.
STREAMS = [
    f"{symbol.lower()}@kline_{interval}"
    for symbol in sorted(ALLOWED_SYMBOLS)
    for interval in [
        "1m",
        "5m",
        "15m",
        "1h",
        "4h",
        "1d",
        "1w",
        "1M",
    ]
]

WS_URL = (
    f"{BINANCE_WS_BASE}?streams="
    + "/".join(STREAMS)
)

# ------------------------------------------------------------
# REST safety
# ------------------------------------------------------------

REST_MIN_INTERVAL_SECONDS = float(
    os.getenv("REST_MIN_INTERVAL_SECONDS", "1.5")
)

# Binance current public request-weight limit is 2400/min.
# We deliberately stop ourselves much earlier.
REST_WEIGHT_LIMIT = int(
    os.getenv("REST_WEIGHT_LIMIT", "2400")
)

REST_WEIGHT_SAFETY_RATIO = float(
    os.getenv("REST_WEIGHT_SAFETY_RATIO", "0.75")
)

HTTP_TIMEOUT_SECONDS = float(
    os.getenv("HTTP_TIMEOUT_SECONDS", "15")
)

# Never retry 429/418 automatically.
FALLBACK_429_COOLDOWN = int(
    os.getenv("FALLBACK_429_COOLDOWN", "120")
)

FALLBACK_418_COOLDOWN = int(
    os.getenv("FALLBACK_418_COOLDOWN", "3600")
)

# ------------------------------------------------------------
# WebSocket safety
# ------------------------------------------------------------

WS_MIN_RECONNECT_SECONDS = float(
    os.getenv("WS_MIN_RECONNECT_SECONDS", "5")
)

WS_MAX_RECONNECT_SECONDS = float(
    os.getenv("WS_MAX_RECONNECT_SECONDS", "300")
)

# Binance says a market-data WS connection is valid for 24h.
# Rotate slightly before the forced disconnect.
WS_ROTATE_SECONDS = int(
    os.getenv(
        "WS_ROTATE_SECONDS",
        str(23 * 60 * 60 + 50 * 60)
    )
)

MAX_CACHED_CANDLES = int(
    os.getenv("MAX_CACHED_CANDLES", "1000")
)


# ============================================================
# MCP SERVER
# ============================================================

mcp = FastMCP("Binance Quant Data")


# ============================================================
# RUNTIME STATE
# ============================================================

_candle_cache: dict[
    tuple[str, str],
    deque[dict[str, Any]]
] = defaultdict(
    lambda: deque(maxlen=MAX_CACHED_CANDLES)
)

_live_candles: dict[
    tuple[str, str],
    dict[str, Any]
] = {}

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


def remaining_seconds(timestamp: float | None) -> int:
    if not timestamp:
        return 0

    return max(
        0,
        int(timestamp - time.time())
    )


def validate_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()

    if symbol not in ALLOWED_SYMBOLS:
        raise ValueError(
            f"Unsupported symbol={symbol}. "
            f"Allowed={sorted(ALLOWED_SYMBOLS)}"
        )

    return symbol


def validate_interval(interval: str) -> str:
    if interval not in ALLOWED_INTERVALS:
        raise ValueError(
            f"Unsupported interval={interval}. "
            f"Allowed={sorted(ALLOWED_INTERVALS)}"
        )

    return interval


def parse_retry_after(
    headers: httpx.Headers,
) -> int | None:

    value = headers.get("Retry-After")

    if value is None:
        return None

    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return None


def extract_binance_headers(
    headers: httpx.Headers,
) -> dict[str, str]:

    return {
        key.lower(): value
        for key, value in headers.items()
        if (
            key.lower().startswith("x-mbx-")
            or key.lower() == "retry-after"
        )
    }


def get_used_weight(
    headers: httpx.Headers,
) -> int | None:

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

def validate_candles(
    candles: list[dict[str, Any]]
) -> dict[str, Any]:

    timestamps = [
        candle["open_time_ms"]
        for candle in candles
    ]

    duplicates = (
        len(timestamps)
        != len(set(timestamps))
    )

    timestamp_ordered = all(
        timestamps[index]
        < timestamps[index + 1]
        for index in range(
            len(timestamps) - 1
        )
    )

    ohlc_valid = all(
        candle["high"]
        >= max(
            candle["open"],
            candle["close"],
        )
        and
        candle["low"]
        <= min(
            candle["open"],
            candle["close"],
        )
        and
        candle["high"]
        >= candle["low"]
        and
        candle["volume"]
        >= 0

        for candle in candles
    )

    all_closed = all(
        candle["is_closed"]
        for candle in candles
    )

    return {
        "row_count": len(candles),
        "duplicates": duplicates,
        "timestamp_ordered": timestamp_ordered,
        "ohlc_valid": ohlc_valid,
        "all_closed": all_closed,
    }


# ============================================================
# REST RATE-LIMIT / CIRCUIT BREAKER
# ============================================================

async def rest_pacing() -> None:

    global _rest_last_request_at

    now = time.monotonic()

    wait_seconds = (
        REST_MIN_INTERVAL_SECONDS
        -
        (
            now
            -
            _rest_last_request_at
        )
    )

    if wait_seconds > 0:

        await asyncio.sleep(
            wait_seconds
        )

    _rest_last_request_at = (
        time.monotonic()
    )


async def protected_rest_get(
    path: str,
    params: dict[str, Any] | None = None,
) -> tuple[
    Any | None,
    dict[str, Any] | None
]:

    global _rest_blocked_until
    global _rest_last_status
    global _rest_last_headers
    global _rest_last_error
    global _rest_last_success_at

    # --------------------------------------------------------
    # LOCAL CIRCUIT BREAKER
    # --------------------------------------------------------

    if time.time() < _rest_blocked_until:

        seconds = remaining_seconds(
            _rest_blocked_until
        )

        return None, structured_error(
            "REST_LOCAL_COOLDOWN",
            (
                "REST access is locally blocked. "
                "No Binance request was sent."
            ),
            retry_after_seconds=seconds,
        )

    url = (
        f"{BINANCE_REST_BASE}"
        f"{path}"
    )

    # --------------------------------------------------------
    # ONLY TRANSIENT ERRORS MAY RETRY
    # --------------------------------------------------------

    last_exception = None

    for attempt in range(3):

        try:

            async with _rest_lock:

                await rest_pacing()

                async with httpx.AsyncClient(
                    timeout=HTTP_TIMEOUT_SECONDS
                ) as client:

                    response = await client.get(
                        url,
                        params=params,
                        headers={
                            "User-Agent":
                                "BinanceQuantDataMCP/PhaseA"
                        },
                    )

            _rest_last_status = (
                response.status_code
            )

            _rest_last_headers = (
                extract_binance_headers(
                    response.headers
                )
            )

            used_weight = (
                get_used_weight(
                    response.headers
                )
            )

            # ------------------------------------------------
            # ADAPTIVE REST STOP
            # ------------------------------------------------

            if used_weight is not None:

                safety_threshold = int(
                    REST_WEIGHT_LIMIT
                    *
                    REST_WEIGHT_SAFETY_RATIO
                )

                if used_weight >= safety_threshold:

                    _rest_blocked_until = (
                        time.time()
                        + 60
                    )

            # ------------------------------------------------
            # 429
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = (
                    parse_retry_after(
                        response.headers
                    )
                )

                if retry_after is None:

                    retry_after = (
                        FALLBACK_429_COOLDOWN
                    )

                _rest_blocked_until = (
                    time.time()
                    + retry_after
                )

                error = structured_error(
                    "BINANCE_RATE_LIMITED",
                    (
                        "Binance returned HTTP 429. "
                        "REST requests are stopped "
                        "until Retry-After expires. "
                        "No retry was performed."
                    ),
                    http_status=429,
                    retry_after_seconds=retry_after,
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )

                _rest_last_error = error

                return None, error

            # ------------------------------------------------
            # 418
            # ------------------------------------------------

            if response.status_code == 418:

                retry_after = (
                    parse_retry_after(
                        response.headers
                    )
                )

                if retry_after is None:

                    retry_after = (
                        FALLBACK_418_COOLDOWN
                    )

                _rest_blocked_until = (
                    time.time()
                    + retry_after
                )

                error = structured_error(
                    "BINANCE_IP_BANNED",
                    (
                        "Binance returned HTTP 418. "
                        "REST requests are stopped "
                        "until Retry-After expires. "
                        "No retry was performed."
                    ),
                    http_status=418,
                    retry_after_seconds=retry_after,
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )

                _rest_last_error = error

                return None, error

            # ------------------------------------------------
            # 403 / 451
            # ------------------------------------------------

            if response.status_code in (
                403,
                451,
            ):

                code = (
                    "BINANCE_WAF_BLOCK"
                    if response.status_code == 403
                    else "BINANCE_GEO_OR_POLICY_BLOCK"
                )

                error = structured_error(
                    code,
                    (
                        "Binance rejected the request. "
                        "No retry was attempted."
                    ),
                    http_status=response.status_code,
                    retry_after_seconds=(
                        parse_retry_after(
                            response.headers
                        )
                    ),
                    url=url,
                    headers=_rest_last_headers,
                    body=response.text[:1000],
                )

                _rest_last_error = error

                return None, error

            # ------------------------------------------------
            # TRANSIENT 5XX
            # ------------------------------------------------

            if response.status_code >= 500:

                response.raise_for_status()

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            response.raise_for_status()

            _rest_last_success_at = (
                time.time()
            )

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

        # Bounded transient-error backoff only.
        backoff_seconds = min(
            30.0,
            (2 ** attempt)
            + random.uniform(
                0,
                1,
            ),
        )

        await asyncio.sleep(
            backoff_seconds
        )

    error = structured_error(
        "BINANCE_NETWORK_OR_5XX_ERROR",
        (
            "Binance REST request failed after "
            "bounded transient-error retries."
        ),
        url=url,
        body=(
            str(last_exception)[:1000]
            if last_exception
            else None
        ),
    )

    _rest_last_error = error

    return None, error


# ============================================================
# WEBSOCKET CACHE
# ============================================================

def ws_candle_to_dict(
    event: dict[str, Any]
) -> dict[str, Any]:

    kline = event["k"]

    return {
        "open_time_ms": int(
            kline["t"]
        ),
        "open": float(
            kline["o"]
        ),
        "high": float(
            kline["h"]
        ),
        "low": float(
            kline["l"]
        ),
        "close": float(
            kline["c"]
        ),
        "volume": float(
            kline["v"]
        ),
        "close_time_ms": int(
            kline["T"]
        ),
        "quote_volume": float(
            kline["q"]
        ),
        "trade_count": int(
            kline["n"]
        ),
        "taker_buy_volume": float(
            kline["V"]
        ),
        "taker_buy_quote_volume": float(
            kline["Q"]
        ),
        "is_closed": bool(
            kline["x"]
        ),
        "source": "binance_websocket",
        "event_time_ms": int(
            event["E"]
        ),
        "received_at_utc": utc_now(),
    }


async def websocket_manager() -> None:

    global _ws_connected
    global _ws_connected_at
    global _ws_last_message_at
    global _ws_reconnect_count
    global _ws_last_error

    reconnect_delay = (
        WS_MIN_RECONNECT_SECONDS
    )

    while True:

        connection_started = (
            time.monotonic()
        )

        try:

            async with connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1024,
            ) as websocket:

                _ws_connected = True

                _ws_connected_at = (
                    time.time()
                )

                _ws_last_error = None

                reconnect_delay = (
                    WS_MIN_RECONNECT_SECONDS
                )

                while (
                    time.monotonic()
                    -
                    connection_started
                    <
                    WS_ROTATE_SECONDS
                ):

                    raw = (
                        await websocket.recv()
                    )

                    _ws_last_message_at = (
                        time.time()
                    )

                    if isinstance(
                        raw,
                        bytes,
                    ):
                        raw = raw.decode(
                            "utf-8"
                        )

                    payload = json.loads(
                        raw
                    )

                    event = payload.get(
                        "data",
                        payload,
                    )

                    if (
                        event.get("e")
                        != "kline"
                    ):
                        continue

                    kline = event.get(
                        "k",
                        {}
                    )

                    symbol = str(
                        kline.get(
                            "s",
                            "",
                        )
                    ).upper()

                    interval = str(
                        kline.get(
                            "i",
                            "",
                        )
                    )

                    if (
                        symbol
                        not in ALLOWED_SYMBOLS
                        or
                        interval
                        not in ALLOWED_INTERVALS
                    ):
                        continue

                    candle = (
                        ws_candle_to_dict(
                            event
                        )
                    )

                    key = (
                        symbol,
                        interval,
                    )

                    # Keep the current forming candle.
                    _live_candles[key] = candle

                    # Only closed candles enter history.
                    if candle[
                        "is_closed"
                    ]:

                        cache = (
                            _candle_cache[
                                key
                            ]
                        )

                        if (
                            cache
                            and
                            cache[-1][
                                "open_time_ms"
                            ]
                            ==
                            candle[
                                "open_time_ms"
                            ]
                        ):

                            cache[-1] = candle

                        else:

                            cache.append(
                                candle
                            )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            _ws_reconnect_count += 1

            _ws_last_error = {
                "code":
                    "WEBSOCKET_ERROR",
                "message":
                    str(exc)[
                        :1000
                    ],
                "timestamp_utc":
                    utc_now(),
                "reconnect_delay_seconds":
                    reconnect_delay,
            }

        finally:

            _ws_connected = False

        # Never reconnect aggressively.
        await asyncio.sleep(
            reconnect_delay
            +
            random.uniform(
                0,
                min(
                    5.0,
                    reconnect_delay * 0.25,
                ),
            )
        )

        reconnect_delay = min(
            WS_MAX_RECONNECT_SECONDS,
            reconnect_delay * 2,
        )


async def ensure_websocket_started() -> None:

    global _ws_task

    if (
        _ws_task is None
        or _ws_task.done()
    ):

        _ws_task = asyncio.create_task(
            websocket_manager()
        )


# ============================================================
# REST BACKFILL
# ============================================================

async def rest_backfill(
    symbol: str,
    interval: str,
    limit: int,
) -> dict[str, Any]:

    key = (
        symbol,
        interval,
    )

    raw, error = (
        await protected_rest_get(
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": min(
                    limit + 2,
                    100,
                ),
            },
        )
    )

    if error:

        return {
            "status":
                "DATA_UNAVAILABLE",
            "error":
                error,
        }

    current_ms = epoch_ms()

    closed_candles = []

    for row in raw or []:

        close_time_ms = int(
            row[6]
        )

        if close_time_ms >= current_ms:
            continue

        closed_candles.append(
            {
                "open_time_ms":
                    int(row[0]),
                "open":
                    float(row[1]),
                "high":
                    float(row[2]),
                "low":
                    float(row[3]),
                "close":
                    float(row[4]),
                "volume":
                    float(row[5]),
                "close_time_ms":
                    close_time_ms,
                "quote_volume":
                    float(row[7]),
                "trade_count":
                    int(row[8]),
                "taker_buy_volume":
                    float(row[9]),
                "taker_buy_quote_volume":
                    float(row[10]),
                "is_closed":
                    True,
                "source":
                    "binance_rest",
                "received_at_utc":
                    utc_now(),
            }
        )

    cache = _candle_cache[
        key
    ]

    for candle in closed_candles:

        if (
            cache
            and
            cache[-1][
                "open_time_ms"
            ]
            ==
            candle[
                "open_time_ms"
            ]
        ):

            cache[-1] = candle

        else:

            cache.append(
                candle
            )

    return {
        "status": "PASS",
        "source":
            "binance_rest_backfill",
        "candles":
            list(cache)[-limit:],
    }


# ============================================================
# MCP TOOLS
# ============================================================

@mcp.tool
async def ping() -> dict[str, Any]:
    """
    MCP health check.

    Does NOT call Binance REST.
    """

    await ensure_websocket_started()

    return {
        "status": "PASS",
        "service":
            "Binance Quant Data",
        "market_type":
            "Binance USDⓈ-M Futures",
        "websocket": {
            "connected":
                _ws_connected,
            "last_message_at_utc":
                (
                    datetime.fromtimestamp(
                        _ws_last_message_at,
                        timezone.utc,
                    ).isoformat()
                    if _ws_last_message_at
                    else None
                ),
            "reconnect_count":
                _ws_reconnect_count,
            "last_error":
                _ws_last_error,
            "stream_count":
                len(STREAMS),
        },
        "rest": {
            "last_http_status":
                _rest_last_status,
            "blocked_seconds_remaining":
                remaining_seconds(
                    _rest_blocked_until
                ),
            "last_error":
                _rest_last_error,
            "last_headers":
                _rest_last_headers,
        },
        "timestamp_utc":
            utc_now(),
    }


@mcp.tool
async def rest_status() -> dict[str, Any]:
    """
    Show REST rate-limit/circuit-breaker state.
    Does NOT call Binance.
    """

    return {
        "status":
            "PASS",
        "last_http_status":
            _rest_last_status,
        "blocked_seconds_remaining":
            remaining_seconds(
                _rest_blocked_until
            ),
        "last_success_at_utc":
            (
                datetime.fromtimestamp(
                    _rest_last_success_at,
                    timezone.utc,
                ).isoformat()
                if _rest_last_success_at
                else None
            ),
        "last_error":
            _rest_last_error,
        "last_headers":
            _rest_last_headers,
        "timestamp_utc":
            utc_now(),
    }


@mcp.tool
async def websocket_status() -> dict[str, Any]:
    """
    Show WebSocket connection and cache status.
    Does NOT call Binance REST.
    """

    await ensure_websocket_started()

    cache_status = {}

    for (
        symbol,
        interval,
    ), cache in _candle_cache.items():

        cache_status[
            f"{symbol}:{interval}"
        ] = {
            "closed_candle_count":
                len(cache),
            "latest_open_time_ms":
                (
                    cache[-1][
                        "open_time_ms"
                    ]
                    if cache
                    else None
                ),
            "has_live_candle":
                (
                    symbol,
                    interval,
                )
                in _live_candles,
        }

    return {
        "status":
            "PASS",
        "connected":
            _ws_connected,
        "reconnect_count":
            _ws_reconnect_count,
        "last_message_at_utc":
            (
                datetime.fromtimestamp(
                    _ws_last_message_at,
                    timezone.utc,
                ).isoformat()
                if _ws_last_message_at
                else None
            ),
        "last_error":
            _ws_last_error,
        "cache":
            cache_status,
        "timestamp_utc":
            utc_now(),
    }


@mcp.tool
async def rest_ping() -> dict[str, Any]:
    """
    Explicitly test Binance REST connectivity.

    Protected by the REST circuit breaker.
    """

    data, error = (
        await protected_rest_get(
            "/fapi/v1/time"
        )
    )

    if error:

        return {
            "status":
                "DATA_UNAVAILABLE",
            "error":
                error,
        }

    return {
        "status":
            "PASS",
        "source":
            "Binance",
        "market_type":
            "USDⓈ-M Futures",
        "server_time_ms":
            data[
                "serverTime"
            ],
        "timestamp_utc":
            utc_now(),
    }


@mcp.tool
async def get_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    limit: int = 5,
) -> dict[str, Any]:

    symbol = validate_symbol(
        symbol
    )

    interval = validate_interval(
        interval
    )

    if limit < 1 or limit > 500:

        raise ValueError(
            "limit must be between 1 and 500"
        )

    await ensure_websocket_started()

    key = (
        symbol,
        interval,
    )

    cache = _candle_cache[
        key
    ]

    # --------------------------------------------------------
    # PRIMARY: WEBSOCKET CACHE
    # --------------------------------------------------------

    if len(cache) < limit:

        # ----------------------------------------------------
        # SECONDARY: ONE PROTECTED REST BACKFILL
        # ----------------------------------------------------

        result = await rest_backfill(
            symbol,
            interval,
            limit,
        )

        if result["status"] != "PASS":

            return {
                "status":
                    "DATA_UNAVAILABLE",
                "symbol":
                    symbol,
                "interval":
                    interval,
                "requested_limit":
                    limit,
                "error":
                    result[
                        "error"
                    ],
                "cache_count":
                    len(cache),
                "timestamp_utc":
                    utc_now(),
            }

    candles = list(cache)[
        -limit:
    ]

    quality = validate_candles(
        candles
    )

    source = (
        "websocket_cache"
        if (
            candles
            and
            candles[-1][
                "source"
            ]
            ==
            "binance_websocket"
        )
        else
        "binance_rest_backfill"
    )

    return {
        "status":
            (
                "PASS"
                if quality[
                    "row_count"
                ] == limit
                else "PARTIAL"
            ),
        "source":
            source,
        "market_type":
            "Binance USDⓈ-M Futures",
        "symbol":
            symbol,
        "interval":
            interval,
        "requested_limit":
            limit,
        "row_count":
            len(candles),
        "data_quality":
            quality,
        "candles":
            candles,
        "rest_protection": {
            "last_http_status":
                _rest_last_status,
            "blocked_seconds_remaining":
                remaining_seconds(
                    _rest_blocked_until
                ),
        },
        "timestamp_utc":
            utc_now(),
    }


@mcp.tool
async def get_live_candle(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
) -> dict[str, Any]:

    symbol = validate_symbol(
        symbol
    )

    interval = validate_interval(
        interval
    )

    await ensure_websocket_started()

    candle = _live_candles.get(
        (
            symbol,
            interval,
        )
    )

    if candle is None:

        return {
            "status":
                "DATA_UNAVAILABLE",
            "message":
                "No live WebSocket candle received yet.",
            "symbol":
                symbol,
            "interval":
                interval,
            "timestamp_utc":
                utc_now(),
        }

    return {
        "status":
            "PASS",
        "source":
            "binance_websocket",
        "symbol":
            symbol,
        "interval":
            interval,
        "candle":
            candle,
        "timestamp_utc":
            utc_now(),
    }


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
        stateless_http=True,
        json_response=True,
    )
