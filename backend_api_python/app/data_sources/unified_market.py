"""
Unified Market Data Source for QuantDinger
Integrates multiple data providers with intelligent routing and fallback

Sources (priority order):
1. FutuOpenD (via Cloudflare Tunnel) - HSI 1m/5m, QQQ 1m/5m [PRIMARY]
2. iTick Indices - HSI, HIS, SPX, DJI, IXIC, HSTECH... [FALLBACK + EXTENSION]
3. iTick Forex/Metal - XAUUSD, EURUSD, USDJPY, XAGUSD... [NEW MARKETS]
4. iTick Fund US - QQQ, SPY, GLD... [BACKUP for QQQ]
5. iTick Future HK - HSI, MHI, HTI... [BACKUP for HSI futures]
6. Original HKStockDataSource - TwelveData/Tencent/yfinance/AkShare [LAST RESORT]
"""

from __future__ import annotations

import os
import csv
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from threading import Lock

import requests

from app.data_sources.base import BaseDataSource
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ============================================================
# Constants & Config
# ============================================================

# Timeframe mapping: QuantDinger -> Provider
TF_MAP_FUTU = {
    "1m": "K_1M", "5m": "K_5M", "15m": "K_15M", "30m": "K_30M",
    "1H": "K_60M", "4H": "K_60M", "1D": "K_DAY", "1W": "K_DAY",
}

TF_MAP_ITICK_INDICES = {
    "1m": 1, "5m": 2, "15m": 3, "30m": 4,
    "1H": 5, "4H": 6, "1D": 8, "1W": 9,
}

TF_MAP_ITICK_FOREX = {
    "1m": 1, "5m": 2, "15m": 3, "30m": 4,
    "1H": 5, "4H": 6, "1D": 8, "1W": 9,
}

TF_MAP_ITICK_FUTURE = {
    "1m": 1, "5m": 2, "15m": 3, "30m": 4,
    "1H": 5, "4H": 6, "1D": 8, "1W": 9,
}

TF_MAP_ITICK_FUND = {
    "1m": 1, "5m": 2, "15m": 3, "30m": 4,
    "1H": 5, "4H": 6, "1D": 8, "1W": 9,
}

MAX_BARS_PER_REQUEST = 5000  # FutuOpenD & iTick limit
HKT = timezone(timedelta(hours=8))
CSV_DIR = Path("/app/app/data")

# ============================================================
# Symbol Mapping (loaded from CSV)
# ============================================================

class SymbolMapper:
    """Maps QuantDinger symbols to provider-specific symbols."""
    
    def __init__(self):
        self._lock = Lock()
        self._loaded = False
        self._futu_map: Dict[str, str] = {}
        self._itick_indices: Dict[str, Tuple[str, str]] = {}  # symbol -> (code, region)
        self._itick_forex: Dict[str, Tuple[str, str]] = {}
        self._itick_metal: Dict[str, Tuple[str, str]] = {}
        self._itick_fund: Dict[str, Tuple[str, str]] = {}
        self._itick_future_hk: Dict[str, Tuple[str, str]] = {}
    
    def load(self):
        with self._lock:
            if self._loaded:
                return
            self._load_futu_map()
            self._load_itick_indices()
            self._load_itick_forex()
            self._load_itick_metal()
            self._load_itick_fund()
            self._load_itick_future_hk()
            self._loaded = True
    
    def _load_futu_map(self):
        """Hardcoded FutuOpenD symbol mappings (from working tests)."""
        self._futu_map = {
            # HK
            "HSI": "HK.HSImain", "HSIF": "HK.HSImain", "HSIMAIN": "HK.HSImain",
            "MHI": "HK.MHImain", "HTI": "HK.HTImain",
            # US Stocks/ETFs
            "QQQ": "US.QQQ", "SPY": "US.SPY", "IVV": "US.IVV",
            "TSLA": "US.TSLA", "AAPL": "US.AAPL", "NVDA": "US.NVDA",
            "GLD": "US.GLD", "SLV": "US.SLV",
            # Futures (if subscribed)
            "NQ": "US.NQmain", "ES": "US.ESmain", "YM": "US.YMmain",
            "GC": "US.GCmain", "SI": "US.SImain", "CL": "US.CLmain",
        }
    
    def _load_csv(self, filepath: Path, key_col: str, val_cols: List[str]) -> Dict[str, Tuple]:
        """Generic CSV loader."""
        mapping = {}
        if not filepath.exists():
            logger.warning(f"CSV not found: {filepath}")
            return mapping
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row.get(key_col, '').strip().upper()
                    if key:
                        vals = tuple(row.get(c, '').strip() for c in val_cols)
                        mapping[key] = vals
        except Exception as e:
            logger.error(f"Failed to load {filepath}: {e}")
        return mapping
    
    def _load_itick_indices(self):
        self._itick_indices = self._load_csv(
            CSV_DIR / "itick_symbols_indices.csv",
            "symbol", ["code", "region"]
        )
    
    def _load_itick_forex(self):
        self._itick_forex = self._load_csv(
            CSV_DIR / "itick_symbols_forex.csv",
            "symbol", ["code", "region"]
        )
    
    def _load_itick_metal(self):
        self._itick_metal = self._load_csv(
            CSV_DIR / "itick_symbols_metal.csv",
            "symbol", ["code", "region"]
        )
    
    def _load_itick_fund(self):
        self._itick_fund = self._load_csv(
            CSV_DIR / "itick_symbols_fund_us.csv",
            "symbol", ["code", "region", "exchange"]
        )
    
    def _load_itick_future_hk(self):
        self._itick_future_hk = self._load_csv(
            CSV_DIR / "itick_symbols_future_hk.csv",
            "Code", ["Description", "type", "region"]
        )
    
    def get_futu_symbol(self, symbol: str) -> Optional[str]:
        self.load()
        s = symbol.strip().upper()
        # Direct mapping
        if s in self._futu_map:
            return self._futu_map[s]
        # Numeric HK codes
        if s.isdigit() and len(s) <= 5:
            return f"HK.{s.zfill(5)}"
        # Already in Futu format
        if s.startswith("HK.") or s.startswith("US."):
            return s
        return None
    
    def get_itick_indices(self, symbol: str) -> Optional[Tuple[str, str]]:
        self.load()
        return self._itick_indices.get(symbol.strip().upper())
    
    def get_itick_forex(self, symbol: str) -> Optional[Tuple[str, str]]:
        self.load()
        return self._itick_forex.get(symbol.strip().upper())
    
    def get_itick_metal(self, symbol: str) -> Optional[Tuple[str, str]]:
        self.load()
        return self._itick_metal.get(symbol.strip().upper())
    
    def get_itick_fund(self, symbol: str) -> Optional[Tuple[str, str, str]]:
        self.load()
        return self._itick_fund.get(symbol.strip().upper())
    
    def get_itick_future_hk(self, symbol: str) -> Optional[Tuple[str, str]]:
        self.load()
        s = symbol.strip().upper()
        v = self._itick_future_hk.get(s)
        if v:
            return (v[0], v[2])  # (code, region)
        return None


# ============================================================
# Provider Classes
# ============================================================

class FutuOpenDProvider:
    """FutuOpenD via Cloudflare Tunnel."""
    
    def __init__(self):
        self.base_url = os.getenv(
            "FUTUOPEND_URL",
            "https://judges-excited-tribe-yamaha.trycloudflare.com"
        ).rstrip("/")
        self.session = requests.Session()
        self.session.timeout = 30
        self.mapper = SymbolMapper()
    
    def _map_tf(self, tf: str) -> str:
        return TF_MAP_FUTU.get(tf, "K_1M")
    
    def _parse_time(self, time_key: str) -> int:
        try:
            dt = datetime.strptime(time_key, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=HKT)
            return int(dt.timestamp())
        except Exception:
            return 0
    
    def fetch_kline(self, symbol: str, tf: str, limit: int, start: Optional[str] = None) -> List[Dict]:
        futu_symbol = self.mapper.get_futu_symbol(symbol)
        if not futu_symbol:
            return []
        ktype = self._map_tf(tf)
        url = f"{self.base_url}/kline/{futu_symbol}"
        params = {"ktype": ktype, "count": min(limit, MAX_BARS_PER_REQUEST)}
        if start:
            params["start"] = start
        try:
            resp = self.session.get(url, params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data.get("records", [])
        except Exception as e:
            logger.warning(f"FutuOpenD error {symbol}: {e}")
            return []
    
    def fetch_snapshot(self, symbol: str) -> Dict:
        futu_symbol = self.mapper.get_futu_symbol(symbol)
        if not futu_symbol:
            return {}
        try:
            resp = self.session.get(f"{self.base_url}/snapshot/{futu_symbol}")
            if resp.status_code != 200:
                return {}
            return resp.json()
        except Exception:
            return {}


class ITickIndicesProvider:
    """iTick Indices endpoint (/indices/kline)."""
    
    def __init__(self):
        self.base_url = "https://api-free.itick.org/indices/kline"
        self.session = requests.Session()
        self.session.timeout = 30
        self.token = os.getenv("ITICK_TOKEN", "5b33693f59dc423ba17c9d90313bb2ef3da8590e18c04160b81389fd4438ab1a")
        self.mapper = SymbolMapper()
    
    def _map_tf(self, tf: str) -> int:
        return TF_MAP_ITICK_INDICES.get(tf, 1)
    
    def fetch_kline(self, symbol: str, tf: str, limit: int, start_ts: Optional[int] = None) -> List[Dict]:
        mapping = self.mapper.get_itick_indices(symbol)
        if not mapping:
            return []
        code, region = mapping
        ktype = self._map_tf(tf)
        params = {
            "code": code, "region": region, "kType": ktype,
            "limit": min(limit, MAX_BARS_PER_REQUEST)
        }
        if start_ts:
            params["et"] = start_ts * 1000  # iTick uses ms
        headers = {"accept": "application/json", "token": self.token}
        try:
            resp = self.session.get(self.base_url, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            data = resp.json()
            if data.get("code") != 0:
                return []
            return data.get("data", [])
        except Exception as e:
            logger.warning(f"iTick Indices error {symbol}: {e}")
            return []
    
    def parse_record(self, rec: Dict) -> Optional[Dict]:
        try:
            t = rec.get("t", 0)
            if t > 1e12:  # ms
                t = t // 1000
            return self.format_kline(
                timestamp=int(t),
                open_price=rec.get("o", 0),
                high=rec.get("h", 0),
                low=rec.get("l", 0),
                close=rec.get("c", 0),
                volume=rec.get("v", 0)
            )
        except Exception:
            return None


class ITickForexProvider:
    """iTick Forex endpoint (/forex/kline) - covers FX + Metals."""
    
    def __init__(self):
        self.base_url = "https://api-free.itick.org/forex/kline"
        self.session = requests.Session()
        self.session.timeout = 30
        self.token = os.getenv("ITICK_TOKEN", "5b33693f59dc423ba17c9d90313bb2ef3da8590e18c04160b81389fd4438ab1a")
        self.mapper = SymbolMapper()
    
    def _map_tf(self, tf: str) -> int:
        return TF_MAP_ITICK_FOREX.get(tf, 1)
    
    def fetch_kline(self, symbol: str, tf: str, limit: int, start_ts: Optional[int] = None) -> List[Dict]:
        # Try forex first, then metal
        for mapper in [self.mapper.get_itick_forex, self.mapper.get_itick_metal]:
            mapping = mapper(symbol)
            if mapping:
                code, region = mapping
                break
        else:
            return []
        ktype = self._map_tf(tf)
        params = {
            "code": code, "region": region, "kType": ktype,
            "limit": min(limit, MAX_BARS_PER_REQUEST)
        }
        if start_ts:
            params["et"] = start_ts * 1000
        headers = {"accept": "application/json", "token": self.token}
        try:
            resp = self.session.get(self.base_url, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            data = resp.json()
            if data.get("code") != 0:
                return []
            return data.get("data", [])
        except Exception as e:
            logger.warning(f"iTick Forex error {symbol}: {e}")
            return []


class ITickFutureHKProvider:
    """iTick Future HK endpoint (/future/kline region=hk)."""
    
    def __init__(self):
        self.base_url = "https://api-free.itick.org/future/kline"
        self.session = requests.Session()
        self.session.timeout = 30
        self.token = os.getenv("ITICK_TOKEN", "5b33693f59dc423ba17c9d90313bb2ef3da8590e18c04160b81389fd4438ab1a")
        self.mapper = SymbolMapper()
    
    def _map_tf(self, tf: str) -> int:
        return TF_MAP_ITICK_FUTURE.get(tf, 1)
    
    def fetch_kline(self, symbol: str, tf: str, limit: int, start_ts: Optional[int] = None) -> List[Dict]:
        mapping = self.mapper.get_itick_future_hk(symbol)
        if not mapping:
            return []
        code, region = mapping
        ktype = self._map_tf(tf)
        params = {
            "code": code, "region": region, "kType": ktype,
            "limit": min(limit, MAX_BARS_PER_REQUEST)
        }
        if start_ts:
            params["et"] = start_ts * 1000
        headers = {"accept": "application/json", "token": self.token}
        try:
            resp = self.session.get(self.base_url, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            data = resp.json()
            if data.get("code") != 0:
                return []
            return data.get("data", [])
        except Exception as e:
            logger.warning(f"iTick Future HK error {symbol}: {e}")
            return []


# ============================================================
# Unified Data Source (Main Entry)
# ============================================================

class UnifiedMarketDataSource(BaseDataSource):
    """
    Unified market data source with multi-provider fallback.
    
    Priority per market:
    - HKStock: FutuOpenD -> iTick Indices(HSI) -> iTick Future HK -> Original
    - USStock (QQQ): FutuOpenD -> iTick Fund -> Original
    - Indices (SPX, DJI, etc): iTick Indices -> Original
    - Forex/Metal: iTick Forex -> Original
    """
    
    name = "Unified/FutuOpenD+iTick"
    
    def __init__(self):
        super().__init__()
        self.futu = FutuOpenDProvider()
        self.itick_indices = ITickIndicesProvider()
        self.itick_forex = ITickForexProvider()
        self.itick_future_hk = ITickFutureHKProvider()
        self._fallback_source = None  # Lazy init original HKStockDataSource
    
    def _get_fallback(self):
        if self._fallback_source is None:
            from app.data_sources.hk_stock import HKStockDataSource
            self._fallback_source = HKStockDataSource()
        return self._fallback_source
    
    def _select_provider(self, symbol: str, market: str):
        """Select provider chain based on symbol and market."""
        s = symbol.strip().upper()
        
        # HSI Futures - FutuOpenD primary, iTick Indices/FutureHK fallback
        if s in ("HSI", "HSIF", "HSIMAIN", "HIS", "MHI", "HTI", "MCH") or s == "HK.HSImain":
            return [
                ("futu", self.futu),
                ("itick_indices", self.itick_indices),
                ("itick_future_hk", self.itick_future_hk),
                ("fallback", self._get_fallback()),
            ]
        
        # QQQ - FutuOpenD primary, iTick Fund fallback
        if s in ("QQQ", "US.QQQ"):
            return [
                ("futu", self.futu),
                # ("itick_fund", self.itick_fund),  # Not implemented yet
                ("fallback", self._get_fallback()),
            ]
        
        # HK Stocks - FutuOpenD if available, else fallback
        if s.isdigit() and len(s) <= 5:
            return [
                ("futu", self.futu),
                ("fallback", self._get_fallback()),
            ]
        
        # Global Indices - iTick Indices
        if s in ("SPX", "DJI", "IXIC", "NAS100", "HSTECH", "HSCEI", "HSCCI", "VIX", "DXY"):
            return [
                ("itick_indices", self.itick_indices),
                ("fallback", self._get_fallback()),
            ]
        
        # Forex/Metals - iTick Forex
        if s in ("XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "AUDUSD", "XAGUSD", "XPTUSD", "XPDUSD", "XCUUSD"):
            return [
                ("itick_forex", self.itick_forex),
                ("fallback", self._get_fallback()),
            ]
        
        # Default fallback
        return [("fallback", self._get_fallback())]
    
    def _normalize_itick_record(self, rec: Dict) -> Optional[Dict]:
        """Convert iTick record to standard format."""
        try:
            t = rec.get("t", 0)
            if t > 1e12:
                t = t // 1000
            return self.format_kline(
                timestamp=int(t),
                open_price=rec.get("o", 0),
                high=rec.get("h", 0),
                low=rec.get("l", 0),
                close=rec.get("c", 0),
                volume=rec.get("v", 0)
            )
        except Exception:
            return None
    
    def _fetch_with_provider(self, provider_name: str, provider, symbol: str, tf: str, limit: int, after_ts: Optional[int] = None) -> List[Dict]:
        """Fetch and normalize from a specific provider."""
        try:
            if provider_name == "futu":
                # Futu uses date string for pagination
                start_date = None
                if after_ts:
                    dt = datetime.fromtimestamp(after_ts, tz=HKT)
                    start_date = dt.strftime("%Y-%m-%d")
                records = provider.fetch_kline(symbol, tf, limit, start_date)
                klines = []
                for r in records:
                    ts = provider._parse_time(r.get("time_key", ""))
                    if ts == 0:
                        continue
                    if after_ts and ts < after_ts:
                        continue
                    klines.append(self.format_kline(
                        timestamp=ts,
                        open_price=r.get("open", 0),
                        high=r.get("high", 0),
                        low=r.get("low", 0),
                        close=r.get("close", 0),
                        volume=r.get("volume", 0)
                    ))
                return klines
            else:
                # iTick providers use timestamp pagination
                records = provider.fetch_kline(symbol, tf, limit, after_ts)
                klines = []
                for r in records:
                    kl = provider.parse_record(r) or self._normalize_itick_record(r)
                    if kl:
                        if after_ts and kl["time"] < after_ts:
                            continue
                        klines.append(kl)
                return klines
        except Exception as e:
            logger.warning(f"Provider {provider_name} failed for {symbol}: {e}")
            return []
    
    def get_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        before_time: Optional[int] = None,
        after_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch K-line with multi-provider fallback."""
        providers = self._select_provider(symbol, "")
        lim = max(int(limit or 300), 1)
        
        all_klines = []
        remaining = lim
        
        for provider_name, provider in providers:
            if remaining <= 0:
                break
            
            klines = self._fetch_with_provider(provider_name, provider, symbol, timeframe, remaining, after_time)
            
            if klines:
                # Filter by before_time
                if before_time:
                    klines = [k for k in klines if k["time"] < before_time]
                
                # Sort and deduplicate
                klines.sort(key=lambda x: x["time"])
                # Merge with existing (avoid duplicates by timestamp)
                seen = set()
                merged = []
                for k in all_klines + klines:
                    if k["time"] not in seen:
                        seen.add(k["time"])
                        merged.append(k)
                all_klines = merged
                
                logger.info(f"{provider_name}: got {len(klines)} bars for {symbol} {timeframe}")
                remaining = lim - len(all_klines)
        
        # Final sort and limit
        all_klines.sort(key=lambda x: x["time"])
        if len(all_klines) > lim:
            all_klines = all_klines[-lim:]
        
        if not all_klines:
            logger.warning(f"Unified: no data for {symbol} {timeframe}")
        
        return all_klines
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get latest ticker - try FutuOpenD first for HK/US, else fallback."""
        s = symbol.strip().upper()
        
        # Try FutuOpenD for supported symbols
        if s in ("HSI", "HSIF", "HSIMAIN", "QQQ", "US.QQQ") or s.isdigit() or s.startswith(("HK.", "US.")):
            snap = self.futu.fetch_snapshot(symbol)
            if snap:
                return {
                    "last": snap.get("last_price", 0),
                    "change": snap.get("change", 0),
                    "changePercent": 0,
                    "high": snap.get("high", 0),
                    "low": snap.get("low", 0),
                    "open": snap.get("open", 0),
                    "previousClose": snap.get("prev_close", 0),
                    "volume": snap.get("volume", 0),
                    "symbol": symbol,
                }
        
        # Fallback
        return self._get_fallback().get_ticker(symbol)
    
    def fetch_history_range(
        self,
        symbol: str,
        timeframe: str,
        start_date: str,  # "YYYY-MM-DD"
        end_date: str,    # "YYYY-MM-DD"
        limit_per_day: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch 1 year of historical data by iterating daily.
        Used for backtesting data preparation.
        """
        all_klines = []
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        providers = self._select_provider(symbol, "")
        
        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            day_klines = []
            
            for provider_name, provider in providers:
                try:
                    if provider_name == "futu":
                        records = provider.fetch_kline(symbol, timeframe, limit_per_day, date_str)
                        for r in records:
                            ts = provider._parse_time(r.get("time_key", ""))
                            if ts:
                                day_klines.append(self.format_kline(
                                    timestamp=ts,
                                    open_price=r.get("open", 0),
                                    high=r.get("high", 0),
                                    low=r.get("low", 0),
                                    close=r.get("close", 0),
                                    volume=r.get("volume", 0)
                                ))
                    else:
                        # iTick: fetch with et = end of day timestamp
                        end_ts = int(datetime.combine(current, datetime.max.time()).replace(tzinfo=HKT).timestamp())
                        records = provider.fetch_kline(symbol, timeframe, limit_per_day, end_ts)
                        for r in records:
                            kl = provider.parse_record(r)
                            if kl:
                                day_klines.append(kl)
                    
                    if day_klines:
                        break  # Success with this provider
                except Exception as e:
                    logger.warning(f"{provider_name} failed on {date_str}: {e}")
                    continue
            
            if day_klines:
                all_klines.extend(day_klines)
                logger.info(f"{symbol} {date_str}: {len(day_klines)} bars")
            
            current += timedelta(days=1)
            time.sleep(0.1)  # Rate limit
        
        # Deduplicate and sort
        seen = set()
        final = []
        for k in all_klines:
            if k["time"] not in seen:
                seen.add(k["time"])
                final.append(k)
        final.sort(key=lambda x: x["time"])
        
        return final


# Factory helper
def create_unified_source() -> UnifiedMarketDataSource:
    return UnifiedMarketDataSource()