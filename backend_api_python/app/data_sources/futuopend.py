"""
FutuOpenD Data Source Adapter for QuantDinger
Connects to Cloudflare Tunnel → FutuOpenD Proxy for HK Stock data (HSI, HK stocks)
"""

from __future__ import annotations

import os
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

from app.data_sources.base import BaseDataSource
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Timeframe mapping: QuantDinger → FutuOpenD ktype
TF_MAP = {
    "1m": "K_1M",
    "5m": "K_5M",
    "15m": "K_15M",
    "30m": "K_30M",
    "1H": "K_60M",
    "4H": "K_60M",  # Futu doesn't have 4H, use 60M as closest
    "1D": "K_DAY",
    "1W": "K_DAY",  # Futu doesn't have weekly, use daily
}

# Max bars per request (FutuOpenD proxy limit)
MAX_COUNT = 5000

# HKT timezone (UTC+8)
HKT = timezone(timedelta(hours=8))


class FutuOpenDDataSource(BaseDataSource):
    """FutuOpenD proxy data source for HK market."""

    name = "HKStock/FutuOpenD"

    def __init__(self):
        self.base_url = os.getenv(
            "FUTUOPEND_URL",
            "https://diego-katie-adopt-tsunami.trycloudflare.com"  # default Cloudflare Tunnel URL
        ).rstrip("/")
        self.session = requests.Session()
        self.session.timeout = 30

    def _map_symbol(self, symbol: str) -> str:
        """Normalize symbol to FutuOpenD format."""
        s = symbol.strip().upper()
        # Already in Futu format (HK.xxx)
        if s.startswith("HK."):
            return s
        # HSI main contract
        if s in ("HSI", "HSIF", "HSIMAIN", "HSI_MAIN"):
            return "HK.HSImain"
        # HK stock codes: 00001 → HK.00001
        if s.isdigit() and len(s) <= 5:
            return f"HK.{s.zfill(5)}"
        # Assume already correct
        return s

    def _map_timeframe(self, tf: str) -> str:
        """Map QuantDinger timeframe to Futu ktype."""
        return TF_MAP.get(tf, "K_1M")

    def _parse_time_key(self, time_key: str) -> int:
        """
        Convert HKT time_key string to UTC Unix timestamp.
        FutuOpenD returns time_key in HKT: '2026-08-03 09:50:00'
        """
        try:
            dt = datetime.strptime(time_key, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=HKT)
            return int(dt.timestamp())
        except Exception as e:
            logger.warning(f"Failed to parse time_key '{time_key}': {e}")
            return 0

    def _fetch_kline(
        self,
        symbol: str,
        ktype: str,
        count: int,
        start: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch kline from FutuOpenD proxy."""
        url = f"{self.base_url}/kline/{symbol}"
        params = {"ktype": ktype, "count": min(count, MAX_COUNT)}
        if start:
            params["start"] = start

        try:
            resp = self.session.get(url, params=params)
            if resp.status_code != 200:
                logger.warning(f"FutuOpenD HTTP {resp.status_code}: {resp.text[:200]}")
                return []
            data = resp.json()
            return data.get("records", [])
        except requests.exceptions.Timeout:
            logger.warning(f"FutuOpenD timeout: {symbol} {ktype}")
            return []
        except Exception as e:
            logger.warning(f"FutuOpenD error: {e}")
            return []

    def get_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        before_time: Optional[int] = None,
        after_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch K-line data from FutuOpenD proxy.

        Args:
            symbol: Trading symbol (e.g., 'HSI', '00001', 'HK.HSImain')
            timeframe: Candle interval (1m, 5m, 15m, 30m, 1H, 4H, 1D, 1W)
            limit: Number of rows to fetch
            before_time: Fetch rows before this Unix timestamp (seconds)
            after_time: Optional left boundary. Keep only rows with time >= after_time.

        Returns:
            Normalized K-line rows:
            [{"time": int, "open": float, "high": float, "low": float, "close": float, "volume": float}, ...]
        """
        futu_symbol = self._map_symbol(symbol)
        ktype = self._map_timeframe(timeframe)
        lim = max(int(limit or 300), 1)

        # Determine start date for pagination
        start_date = None
        if after_time:
            dt = datetime.fromtimestamp(after_time, tz=HKT)
            start_date = dt.strftime("%Y-%m-%d")

        # Fetch data (single request, up to 5000 bars)
        records = self._fetch_kline(futu_symbol, ktype, lim, start_date)
        if not records:
            return []

        # Convert to QuantDinger normalized format
        klines = []
        for r in records:
            ts = self._parse_time_key(r.get("time_key", ""))
            if ts == 0:
                continue

            # Filter by time boundaries
            if before_time and ts >= before_time:
                continue
            if after_time is not None and ts < after_time:
                continue

            klines.append(self.format_kline(
                timestamp=ts,
                open_price=r.get("open", 0),
                high=r.get("high", 0),
                low=r.get("low", 0),
                close=r.get("close", 0),
                volume=r.get("volume", 0)
            ))

        # Sort by time ascending
        klines.sort(key=lambda x: x["time"])

        # Apply limit (keep most recent)
        if len(klines) > lim:
            klines = klines[-lim:]

        return klines

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get latest ticker from FutuOpenD proxy."""
        futu_symbol = self._map_symbol(symbol)
        try:
            resp = self.session.get(f"{self.base_url}/snapshot/{futu_symbol}")
            if resp.status_code != 200:
                return {"last": 0, "symbol": futu_symbol}
            data = resp.json()
            return {
                "last": data.get("last_price", 0),
                "change": data.get("change", 0),
                "changePercent": data.get("change_rate", 0) * 100,
                "high": data.get("high", 0),
                "low": data.get("low", 0),
                "open": data.get("open", 0),
                "previousClose": data.get("prev_close", 0),
                "volume": data.get("volume", 0),
                "symbol": futu_symbol,
            }
        except Exception as e:
            logger.warning(f"FutuOpenD ticker error: {e}")
            return {"last": 0, "symbol": futu_symbol}


# Factory registration helper
def create_futuopend_source() -> FutuOpenDDataSource:
    return FutuOpenDDataSource()