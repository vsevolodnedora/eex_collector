"""
Inspired by https://github.com/gruijter/com.gruijter.powerhour

Fixed:
- Uses logging instead of print (clear, structured tracing)
- Robust HTTP layer with retries + JSON/content-type validation
- Proper handling of 401 Unauthorized (EGSI and weekend data may be paywalled)
- Avoids corrupting data by writing fake 0 prices when price is missing
- Fixes swapped column names in merge (EOD vs EGSI)
- Better timezone handling for "gas day" start (06:00 Europe/Berlin) using zoneinfo
"""

from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import urllib.parse
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------
# Config / constants
# --------------------------------------------------------------------

# EEX migrated its market-data API from the legacy GVSI webservice
# (webservice-eex.gvsi.com/query/json/getDaily/..., decommissioned ~May 2026)
# to api.eex-group.com/pub/market-data/table-data. See _query_table_rows().
DEFAULT_HOST = "api.eex-group.com"
DEFAULT_PORT = 443
DEFAULT_TIMEOUT_MS = 30_000
TABLE_DATA_PATH = "/pub/market-data/table-data"

BERLIN = ZoneInfo("Europe/Berlin")

# Maps the leading token of a biddingZone (e.g. "TTF" in "TTF_EOD") to the
# "area" code expected by the table-data endpoint.
AREA_MAP: Dict[str, str] = {
    "TTF": "TTF",
    "CEGH": "CEGHVTP",
    "CZ": "CZVTP",
    "ETF": "ETF",
    "THE": "THE",
    "PEG": "PEG",
    "ZTP": "ZTP",
    "PVB": "PVB",
    "NBP": "NBP",
}

# Conversion factor (bcm -> TWh). NOTE: dimensionally this turns the raw EUR/MWh
# price into a non-standard unit; it is kept ONLY so new data stays on the same
# scale as the ~20 months of history already collected. Divide stored values by
# this factor to recover the real EUR/MWh TTF price.
GAS_BCM_TO_TWH = 9.7694

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------

logger = logging.getLogger("eex_collector")


def setup_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # Make requests/urllib3 less noisy unless you explicitly bump LOG_LEVEL
    logging.getLogger("urllib3").setLevel(max(level, logging.WARNING))
    logging.getLogger("requests").setLevel(max(level, logging.WARNING))


# --------------------------------------------------------------------
# Exceptions
# --------------------------------------------------------------------

class EEXError(RuntimeError):
    """Base EEX error."""


class EEXUnauthorized(EEXError):
    """HTTP 401 from the endpoint (often means paywalled / missing auth)."""


class EEXBadResponse(EEXError):
    """Non-JSON or malformed JSON response."""


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def parse_iso_dt(value: str) -> datetime:
    """
    Parse ISO datetime. If timezone-aware, convert to UTC naive.
    If naive, assume it's UTC (keeps behavior consistent in CI).
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def to_utc_naive_from_berlin_gasday_start(day_utc_naive: datetime) -> datetime:
    """
    Given a UTC-naive datetime representing the day (00:00 UTC),
    compute the gas day start at 06:00 Europe/Berlin for that calendar date,
    then return UTC-naive datetime.
    """
    # Use the calendar date in Berlin time:
    # We interpret 'day_utc_naive' as a date marker; use its Y-M-D
    local = datetime(day_utc_naive.year, day_utc_naive.month, day_utc_naive.day, 6, 0, 0, tzinfo=BERLIN)
    utc = local.astimezone(timezone.utc).replace(tzinfo=None)
    return utc


# --------------------------------------------------------------------
# EEX Client
# --------------------------------------------------------------------

@dataclass
class EEXOptions:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    verify_ssl: bool = False  # keep original behavior by default


class EEX:
    def __init__(self, biddingZone: Optional[str] = None, opts: Optional[Dict[str, Any]] = None):
        options = opts or {}
        self.opts = EEXOptions(
            host=options.get("host", DEFAULT_HOST),
            port=int(options.get("port", DEFAULT_PORT)),
            timeout_ms=int(options.get("timeout", DEFAULT_TIMEOUT_MS)),
            verify_ssl=bool(options.get("verify_ssl", False)),
        )
        self.biddingZone = biddingZone or options.get("biddingZone")
        self.tmpZone: Optional[str] = None

        self.lastResponseSnippet: Optional[str] = None

        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        if not self.opts.verify_ssl:
            logger.warning("SSL verification is DISABLED (verify_ssl=False).")

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Origin": "https://www.eex.com",
            "Referer": "https://www.eex.com/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Connection": "keep-alive",
        }

    def _url(self, path: str) -> str:
        return f"https://{self.opts.host}:{self.opts.port}{path}"

    def _get_json(self, path: str, timeout_ms: Optional[int] = None) -> Dict[str, Any]:
        url = self._url(path)
        start = time.time()
        r = self.session.get(
            url,
            headers=self._headers(),
            timeout=(timeout_ms or self.opts.timeout_ms) / 1000,
            verify=self.opts.verify_ssl,
            allow_redirects=True,
        )
        elapsed_ms = int((time.time() - start) * 1000)

        body_snippet = (r.text or "")[:300].replace("\n", " ")
        self.lastResponseSnippet = body_snippet

        logger.debug("HTTP GET %s | status=%s | %dms", url, r.status_code, elapsed_ms)

        if r.status_code == 401:
            raise EEXUnauthorized(f"HTTP 401 for {url}. Body starts: {body_snippet!r}")

        if r.status_code != 200:
            raise EEXError(f"HTTP {r.status_code} for {url}. Body starts: {body_snippet!r}")

        ctype = (r.headers.get("content-type") or "").lower()
        if "json" not in ctype:
            raise EEXBadResponse(f"Non-JSON response for {url} (content-type={ctype!r}). Body starts: {body_snippet!r}")

        try:
            return r.json()
        except ValueError as e:
            raise EEXBadResponse(f"Invalid JSON for {url}. Body starts: {body_snippet!r}") from e

    @staticmethod
    def _area_for_zone(zone: str) -> str:
        for key, val in AREA_MAP.items():
            if zone.startswith(key):
                return val
        raise ValueError(f"Unknown biddingZone={zone}. No area mapping.")

    def _query_table_rows(
        self, zone: str, start_date: date, end_date: date, weekend: bool = False
    ) -> List[tuple]:
        """
        Query the table-data endpoint for one product and return a list of
        (delivery_date, price_or_None) tuples (newest delivery first, as served).
        """
        area = self._area_for_zone(zone)
        display = {"CEGHVTP": "CEGH VTP", "CZVTP": "CZ VTP"}.get(area, area)
        is_egsi = "EGSI" in zone

        if is_egsi:
            pricing, product = "I", "EGSI"
            short_code = f"EEX EGSI {display} {'Weekend' if weekend else 'Day'}"
        else:
            pricing = "S"
            product = "WE" if weekend else "DA"
            short_code = f"{area}WE" if weekend else f"{area}DA"

        params = {
            "shortCode": short_code,
            "commodity": "NATGAS",
            "pricing": pricing,
            "area": area,
            "product": product,
            "isRolling": "true",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        path = f"{TABLE_DATA_PATH}?{urllib.parse.urlencode(params)}"
        logger.info("EEX request | zone=%s | weekend=%s | %s..%s | %s",
                    zone, weekend, start_date, end_date, short_code)

        payload = self._get_json(path)
        data = payload.get("data")
        header = payload.get("header") or []
        if not isinstance(data, list):
            raise EEXBadResponse(f"Malformed table-data (no 'data' array) for zone={zone}, short_code={short_code!r}")

        delivery_idx = header.index("deliveryDay") if "deliveryDay" in header else 5
        price_idx = next((header.index(c) for c in ("settlPx", "close", "lastPrice") if c in header), 4)

        rows: List[tuple] = []
        for row in data:
            try:
                delivery = row[delivery_idx]
                price = row[price_idx]
            except (IndexError, TypeError):
                continue
            if not delivery:
                continue
            try:
                d = date.fromisoformat(str(delivery)[:10])
            except ValueError:
                continue
            rows.append((d, None if price is None else float(price)))
        return rows

    def _fetch_table(self, zone: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """
        Fetch one price per delivery day for ``zone`` over the window, deduped and
        with None prices dropped. EOD day-ahead skips weekend delivery days, so for
        EOD zones the weekend product is fetched (best-effort) to fill Saturdays.
        Returns [{"time": <delivery-date midnight>, "price": <raw>, "descr": zone}, ...].
        """
        # Buffer back so weekend/holiday context and late settlements are included.
        start_date = (start - timedelta(days=7)).date()
        end_date = end.date()

        by_day: Dict[date, float] = {}
        # Response is newest-delivery-first; setdefault keeps the most recent settlement.
        for d, price in self._query_table_rows(zone, start_date, end_date, weekend=False):
            if price is not None:
                by_day.setdefault(d, price)

        if "EOD" in zone:
            try:
                for d, price in self._query_table_rows(zone, start_date, end_date, weekend=True):
                    if price is not None:
                        by_day.setdefault(d, price)
            except EEXUnauthorized as e:
                logger.warning("Weekend product unauthorized (ignored) | zone=%s | %s", zone, e)
            except Exception:
                logger.exception("Weekend product fetch failed (ignored) | zone=%s", zone)

        return [
            {"time": datetime(d.year, d.month, d.day), "price": p, "descr": zone}
            for d, p in sorted(by_day.items())
        ]

    def getPrices(self, options: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Returns hourly price info in UTC-naive timestamps:
        [
          {"time": <datetime>, "price": <float>},
          ...
        ]
        """
        opts = options or {}
        zone = opts.get("biddingZone", self.biddingZone)
        if not zone:
            raise ValueError("biddingZone must be set (either in constructor or options).")

        today_utc = datetime.utcnow()
        tomorrow_utc = today_utc + timedelta(days=1)

        start = parse_iso_dt(opts["dateStart"]) if opts.get("dateStart") else today_utc
        end = parse_iso_dt(opts["dateEnd"]) if opts.get("dateEnd") else tomorrow_utc

        start = start.replace(minute=0, second=0, microsecond=0)
        end = end.replace(minute=0, second=0, microsecond=0)

        self.tmpZone = zone

        daily = self._fetch_table(zone, start, end)
        if not daily:
            raise EEXBadResponse(f"No gas price info found for zone={zone}")

        # One gas-day record per delivery day. Gas day = 06:00 Europe/Berlin on the
        # delivery date to 06:00 Europe/Berlin the next day. Deriving the end from the
        # *next* date (not a fixed +24h) makes the span 23/24/25h across DST switches,
        # so the padded hourly grid stays continuous in UTC.
        priceInfo: List[Dict[str, Any]] = []
        for day in daily:
            day_date = day["time"].replace(hour=0, minute=0, second=0, microsecond=0)
            priceInfo.append(
                {
                    "tariffStart": to_utc_naive_from_berlin_gasday_start(day_date),
                    "tariffEnd": to_utc_naive_from_berlin_gasday_start(day_date + timedelta(days=1)),
                    "price": float(day["price"]) * GAS_BCM_TO_TWH,
                    "descr": zone,
                }
            )

        # Keep one extra day before start for padding correctness, then order by time.
        priceInfo = [d for d in priceInfo if d["tariffStart"] >= (start - timedelta(days=1))]
        priceInfo.sort(key=lambda d: d["tariffStart"])

        # Carry-forward fill any missing delivery days (weekends/holidays) so the
        # hourly grid is continuous: a gap [prev.end, next.start) is bridged with the
        # previous day's price.
        filled: List[Dict[str, Any]] = []
        for i, d in enumerate(priceInfo):
            filled.append(d)
            if i + 1 < len(priceInfo):
                gap_start, gap_end = d["tariffEnd"], priceInfo[i + 1]["tariffStart"]
                if gap_end > gap_start:
                    filled.append(
                        {"tariffStart": gap_start, "tariffEnd": gap_end, "price": d["price"], "descr": zone}
                    )

        # Pad: fill all hours in each gas day (DST-aware: span may be 23/24/25h)
        info: List[Dict[str, Any]] = []
        for d in filled:
            t = d["tariffStart"]
            endTime = d["tariffEnd"]
            while t < endTime:
                info.append({"time": t, "price": d["price"]})
                t += timedelta(hours=1)

        # Clip to requested range
        info = [h for h in info if start <= h["time"] < end]
        return info


# --------------------------------------------------------------------
# Data collection
# --------------------------------------------------------------------

def fetch_data_for_zone(
    biddingZone: str,
    look_back_window_days: int = 20,
    chunk_days: int = 2,
) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Fetch data going back in time in chunks.
    For EGSI, a 401 may happen for older history; we stop lookback on EGSI 401.
    For EOD, 401 is treated as fatal (unless it happens only for weekend/settle, which are already best-effort).

    Returns (dataframe, last_error). ``last_error`` is a short string describing the
    most recent failure (or None), so callers can surface *why* collection came back empty.
    """
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # The table-data endpoint returns the whole window in a single response (it
    # serves ~45 days of rolling history), so we fetch in one request per zone
    # rather than many small chunks. That avoids the 429 rate limiting the old
    # chunked approach triggered, and is dramatically faster.
    start = today - timedelta(days=look_back_window_days)
    end = today + timedelta(days=chunk_days)  # small future buffer for the day-ahead

    eex = EEX(biddingZone=biddingZone)
    logger.info("Collect | zone=%s | window=%s..%s", biddingZone, start, end)

    try:
        rows = eex.getPrices({"dateStart": start.isoformat(), "dateEnd": end.isoformat()})
    except EEXUnauthorized as e:
        logger.warning("Unauthorized | zone=%s | %s", biddingZone, e)
        return pd.DataFrame(), f"401 Unauthorized: {e}"
    except Exception as e:
        logger.exception("Failed request | zone=%s | window=%s..%s", biddingZone, start, end)
        return pd.DataFrame(), f"{type(e).__name__}: {e}"

    if not rows:
        logger.warning("Empty result | zone=%s | window=%s..%s", biddingZone, start, end)
        return pd.DataFrame(), "empty response"

    df_all = pd.DataFrame(rows)
    df_all.sort_values(by="time", ascending=True, inplace=True)
    df_all.drop_duplicates(subset=["time"], inplace=True)
    logger.info("Collected OK | zone=%s | points=%d", biddingZone, len(df_all))
    return df_all, None


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def main() -> None:
    setup_logging()

    zones = ["TTF_EOD", "TTF_EGSI"]
    look_back_days = int(os.environ.get("LOOKBACK_DAYS", "20"))

    logger.info("Start | zones=%s | lookback_days=%d | endpoint=https://%s:%d",
                zones, look_back_days, DEFAULT_HOST, DEFAULT_PORT)

    results: Dict[str, pd.DataFrame] = {}

    for z in zones:
        df, last_error = fetch_data_for_zone(z, look_back_window_days=look_back_days)

        # EOD is typically required. EGSI may be unavailable (401).
        if df.empty and "EOD" in z:
            raise RuntimeError(
                f"Failed to fetch ANY data for required zone={z}. "
                f"Last underlying error: {last_error or 'none (empty response)'}. "
                f"Endpoint: https://{DEFAULT_HOST}:{DEFAULT_PORT} — "
                f"check whether EEX changed/paywalled the API or is blocking the runner IP."
            )
        if df.empty and "EGSI" in z:
            logger.warning("No EGSI data available for zone=%s (last_error=%s). Continuing.", z, last_error)

        results[z] = df.copy()

    # Merge result into one dataframe
    # Fix: correct column naming (EOD from TTF_EOD, EGSI from TTF_EGSI)
    df_eod = results.get("TTF_EOD", pd.DataFrame()).rename(columns={"time": "date", "price": "EOD"})
    df_egsi = results.get("TTF_EGSI", pd.DataFrame()).rename(columns={"time": "date", "price": "EGSI"})

    if df_eod.empty and df_egsi.empty:
        raise RuntimeError("No data available from any zone; nothing to write.")

    if df_eod.empty:
        df = df_egsi.copy()
    elif df_egsi.empty:
        df = df_eod.copy()
    else:
        df = pd.merge(df_egsi, df_eod, on="date", how="outer")  # date is hourly, so outer is fine

    df.sort_values("date", inplace=True)

    # Frequency inference can be None; handle safely
    freq = pd.DatetimeIndex(df["date"]).inferred_freq or "unknown"

    out_dir = "./data"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/ttf_{datetime.utcnow().strftime('%Y-%m-%d')}_{freq}.csv"

    df.to_csv(out_path, index=False)
    logger.info("Wrote CSV | rows=%d | path=%s", len(df), out_path)


if __name__ == "__main__":
    main()
