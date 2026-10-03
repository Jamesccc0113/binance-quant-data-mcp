import os
from datetime import datetime, timezone

import httpx
from fastmcp import FastMCP

BINANCE_BASE = "https://fapi.binance.com"

ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}

mcp = FastMCP(
    "Binance Quant Data",
    stateless_http=True,
    json_response=True,
)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def validate_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()

    if symbol not in ALLOWED_SYMBOLS:
        raise ValueError(
            f"Only BTCUSDT and ETHUSDT are allowed. Received: {symbol}"
        )

    return symbol


@mcp.tool
async def ping() -> dict:
    """Test Binance USD-M Futures public API connectivity."""
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{BINANCE_BASE}/fapi/v1/time"
        )
        response.raise_for_status()

        data = response.json()

    return {
        "status": "PASS",
        "source": "Binance",
        "market_type": "USDⓈ-M Futures",
        "server_time_ms": data["serverTime"],
        "fetched_at_utc": now_utc(),
    }


@mcp.tool
async def get_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    limit: int = 5,
) -> dict:
    """Get closed Binance USD-M Futures klines for BTCUSDT or ETHUSDT."""
    symbol = validate_symbol(symbol)

    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": limit + 2,
            },
        )

        response.raise_for_status()
        raw = response.json()

    current_ms = int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    candles = []

    for row in raw:
        close_time_ms = int(row[6])

        # Exclude currently forming candle.
        if close_time_ms >= current_ms:
            continue

        candles.append(
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
            }
        )

    candles = candles[-limit:]

    ordered = all(
        candles[i]["open_time_ms"]
        < candles[i + 1]["open_time_ms"]
        for i in range(len(candles) - 1)
    )

    ohlc_valid = all(
        candle["high"] >= max(
            candle["open"],
            candle["close"],
        )
        and candle["low"] <= min(
            candle["open"],
            candle["close"],
        )
        and candle["high"] >= candle["low"]
        and candle["volume"] >= 0
        for candle in candles
    )

    return {
        "status": "PASS",
        "source": "Binance",
        "market_type": "USDⓈ-M Futures",
        "symbol": symbol,
        "interval": interval,
        "row_count": len(candles),
        "fetched_at_utc": now_utc(),
        "data_quality": {
            "timestamp_ordered": ordered,
            "ohlc_valid": ohlc_valid,
            "all_closed": all(
                candle["is_closed"]
                for candle in candles
            ),
        },
        "candles": candles,
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
    )
