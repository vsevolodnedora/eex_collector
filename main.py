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
import copy
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

DEFAULT_HOST = "webservice-eex.gvsi.com"
DEFAULT_PORT = 443
DEFAULT_TIMEOUT_MS = 30_000

BERLIN = ZoneInfo("Europe/Berlin")

biddingZonesMap: Dict[str, str] = {
    # End of Day (EOD) Prices:
    "TTF_EOD": '"#E.TTF_GND1"',
    "CEGH_VTP_EOD": '"#E.CEGH_GND1"',
    "CZ_VTP_EOD": '"#E.OTE_GSND"',
    "ETF_EOD": '"#E.ETF_GND1"',  # fixed from ETFF typo in your original
    "THE_EOD": '"#E.THE_GND1"',
    "PEG_EOD": '"#E.PEG_GND1"',
    "ZTP_EOD": '"#E.ZTP_GTND"',
    "PVB_EOD": '"#E.PVB_GSND"',
    "NBP_EOD": '"#E.NBP_GPND"',

    # European Gas Spot Index (EGSI) Prices:
    "TTF_EGSI": '"$E.EGSI_TTF_DAY"',
    "CEGH_VTP_EGSI": '"$E.EGSI_CEHG_VTP_DAY"',
    "CZ_VTP_EGSI": '"$E.EGSI_CZ_VTP"',
    "ETF_EGSI": '"$E.EGSI_ETF_DAY"',
    "THE_EGSI": '"$E.EGSI_THE_DAY"',
    "PEG_EGSI": '"$E.EGSI_PEG_DAY"',
    "ZTP_EGSI": '"$E.EGSI_ZTP_DAY"',
    "PVB_EGSI": '"$E.EGSI_PVB_DAY"',
    "NBP_EGSI": '"$E.EGSI_NBP_DAY"',
}

# Conversion factor from bcm to TWh (as in your comment)
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
        self.lastDailyInfo: Optional[List[Dict[str, Any]]] = None

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
            "Referer": "https://www.eex.com",
            "User-Agent": "Mozilla/5.0",
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

    def _items(self, path: str, timeout_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        payload = self._get_json(path, timeout_ms=timeout_ms)
        items = payload.get("results", {}).get("items")
        if not isinstance(items, list):
            raise EEXBadResponse(f"Malformed payload for path={path}: missing results.items")
        return items

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

        # Start earlier to ensure weekend or delayed settlements don't cause gaps
        start2 = start - timedelta(days=1)
        chartstartdate = start2.date().isoformat()
        chartstopdate = end.date().isoformat()

        priceSymbol = biddingZonesMap.get(zone)
        if not priceSymbol:
            raise ValueError(f"Unknown biddingZone={zone}. Missing priceSymbol mapping.")

        query = urllib.parse.urlencode(
            {"priceSymbol": priceSymbol, "chartstartdate": chartstartdate, "chartstopdate": chartstopdate}
        )
        path = f"/query/json/getDaily/close/tradedatetimegmt/?{query}"

        logger.info("EEX request | zone=%s | start=%s | end=%s | path=%s", zone, start, end, path)

        daily = self._makeRequest(path, timeout_ms=90_000)

        if not daily or not any(d.get("time") for d in daily):
            raise EEXBadResponse("No gas price info found in daily response")

        # Build per-day records (skip None price days)
        priceInfo: List[Dict[str, Any]] = []
        for day in daily:
            if day.get("descr") != zone:
                continue

            raw_day = day.get("time")
            raw_price = day.get("price")

            if raw_day is None:
                continue

            if raw_price is None:
                logger.warning("No gas price (None) | zone=%s | day=%s | options=%s", zone, raw_day, opts)
                continue

            # Gas day start: 06:00 Europe/Berlin converted to UTC naive
            tariffStart = to_utc_naive_from_berlin_gasday_start(raw_day.replace(hour=0, minute=0, second=0, microsecond=0))

            priceInfo.append(
                {
                    "tariffStart": tariffStart,
                    "price": float(raw_price) * GAS_BCM_TO_TWH,
                    "descr": day.get("descr") or "None",
                }
            )

        # Filter: keep one extra day before start for padding correctness
        priceInfo = [d for d in priceInfo if d["tariffStart"] >= (start - timedelta(days=1))]

        # Pad: fill all hours in each gas day
        info: List[Dict[str, Any]] = []
        for d in priceInfo:
            t = d["tariffStart"]
            endTime = t + timedelta(days=1)
            while t < endTime:
                info.append({"time": t, "price": d["price"]})
                t += timedelta(hours=1)

        # Clip to requested range
        info = [h for h in info if start <= h["time"] < end]
        return info

    def _makeRequest(self, path: str, timeout_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Returns daily mapped structure:
        [
          {"time": <datetime day-ahead marker>, "price": <close>, "descr": <zone>},
          ...
        ]
        """
        # --- daily data ---
        dailyInfo = self._items(path, timeout_ms=timeout_ms)
        lastDaily = dailyInfo[-1] if dailyInfo else None

        # Compensate for bug in EEX (kept from your original logic)
        if self.lastDailyInfo:
            for idx, dayInfo in enumerate(dailyInfo):
                if (len(dailyInfo) - 4) < idx < (len(dailyInfo) - 1):
                    previousInfo = next(
                        (x for x in self.lastDailyInfo if x.get("tradedatetimegmt") == dayInfo.get("tradedatetimegmt")),
                        None,
                    )
                    if previousInfo and previousInfo.get("close") != dayInfo.get("close"):
                        dailyInfo[idx]["close"] = previousInfo["close"]
        self.lastDailyInfo = dailyInfo.copy()

        # --- weekend info (best-effort; may 401) ---
        weekendInfo: Optional[List[Dict[str, Any]]] = None
        if self.tmpZone and "EOD" in self.tmpZone:
            try:
                new_path = path.replace("ND", "WE").replace("DAY", "WEEKEND")
                weekendInfo = self._items(new_path, timeout_ms=timeout_ms)
            except EEXUnauthorized as e:
                logger.warning("Weekend info unauthorized (ignored) | zone=%s | %s", self.tmpZone, e)
                weekendInfo = None
            except Exception:
                logger.exception("Weekend info request failed (ignored) | zone=%s", self.tmpZone)
                weekendInfo = None

        # --- settledate (best-effort; may 401) ---
        lastSettleDate: Optional[str] = None
        try:
            priceSymbol = biddingZonesMap.get(self.tmpZone or "")
            query = urllib.parse.urlencode({"priceSymbol": priceSymbol})
            settle_path = f"/query/json/getQuotes/settledate/dir/?{query}"
            settle_items = self._items(settle_path, timeout_ms=timeout_ms)
            lastSettleDate = (settle_items[0] if settle_items else {}).get("settledate")
        except EEXUnauthorized as e:
            logger.warning("Settle date unauthorized (will treat last day as not closed) | zone=%s | %s", self.tmpZone, e)
            lastSettleDate = None
        except Exception:
            logger.exception("Settle date request failed (will treat last day as not closed) | zone=%s", self.tmpZone)
            lastSettleDate = None

        lastDailyIsClosed = bool(
            lastDaily
            and lastDaily.get("tradedatetimegmt")
            and lastSettleDate
            and lastDaily["tradedatetimegmt"].split(" ")[0] == lastSettleDate
        )

        # --- Map to day-ahead dates ---
        resp: List[Dict[str, Any]] = []
        for idx, day in enumerate(dailyInfo):
            traded = day.get("tradedatetimegmt")
            if not traded:
                continue

            # tradedatetimegmt format: "MM/DD/YYYY ..."
            day_date = datetime.strptime(traded.split(" ")[0], "%m/%d/%Y")
            day_ahead = (day_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

            mappedDay: Optional[Dict[str, Any]] = {
                "time": day_ahead,
                "price": day.get("close"),
                "descr": self.tmpZone,
            }

            # If last day isn't settled yet, ignore it (safe)
            if idx == len(dailyInfo) - 1 and day_ahead.weekday() != 5 and not lastDailyIsClosed:
                mappedDay = None

            # Weekend info handling (if available)
            if weekendInfo and mappedDay and day_ahead.weekday() == 5:
                weekend = next((w for w in weekendInfo if w.get("tradedatetimegmt") == traded), None)
                if weekend:
                    sat_date = datetime.strptime(weekend["tradedatetimegmt"].split(" ")[0], "%m/%d/%Y") + timedelta(days=1)
                    sat = {"time": sat_date.replace(hour=0, minute=0, second=0, microsecond=0), "price": weekend.get("close"), "descr": self.tmpZone}
                    sun = {**sat, "time": sat["time"] + timedelta(days=1)}
                    mon = {**mappedDay, "time": mappedDay["time"] + timedelta(days=2)}
                    resp.extend([sat, sun, mon])
                    mappedDay = None

            if mappedDay:
                resp.append(mappedDay)

        return resp


# --------------------------------------------------------------------
# Data collection
# --------------------------------------------------------------------

def fetch_data_for_zone(
    biddingZone: str,
    look_back_window_days: int = 20,
    chunk_days: int = 2,
) -> pd.DataFrame:
    """
    Fetch data going back in time in chunks.
    For EGSI, a 401 may happen for older history; we stop lookback on EGSI 401.
    For EOD, 401 is treated as fatal (unless it happens only for weekend/settle, which are already best-effort).
    """
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_saved = copy.deepcopy(today)

    df_all = pd.DataFrame()
    eex = EEX(biddingZone=biddingZone)

    attempts = 0

    while today > (today_saved - timedelta(days=look_back_window_days)):
        start = today - timedelta(days=chunk_days)
        end = today + timedelta(days=chunk_days)

        logger.info("Collect | zone=%s | window=%s..%s", biddingZone, start, end)

        try:
            rows = eex.getPrices({"dateStart": start.isoformat(), "dateEnd": end.isoformat()})
        except EEXUnauthorized as e:
            logger.warning("Unauthorized | zone=%s | window=%s..%s | %s", biddingZone, start, end, e)
            # Stop lookback for both EGSI and EOD when hitting paywall
            logger.warning("Stopping further lookback for %s due to 401 (likely paywall for older data).", biddingZone)
            break
        except Exception:
            logger.exception("Failed request | zone=%s | window=%s..%s", biddingZone, start, end)
            # Skip this chunk and continue (transient)
            today -= timedelta(days=chunk_days)
            continue
        except Exception:
            logger.exception("Failed request | zone=%s | window=%s..%s", biddingZone, start, end)
            # Skip this chunk and continue (transient)
            today -= timedelta(days=chunk_days)
            continue

        if not rows:
            logger.warning("Empty result | zone=%s | window=%s..%s", biddingZone, start, end)
            today -= timedelta(days=chunk_days)
            continue

        df_all = pd.concat([df_all, pd.DataFrame(rows)], ignore_index=True)
        today -= timedelta(days=chunk_days)
        attempts += 1

    if df_all.empty:
        logger.warning("No data collected | zone=%s | attempts=%s", biddingZone, attempts)
        return df_all

    df_all.sort_values(by="time", ascending=True, inplace=True)
    df_all.drop_duplicates(subset=["time"], inplace=True)
    logger.info("Collected OK | zone=%s | points=%d | attempts=%d", biddingZone, len(df_all), attempts)
    return df_all


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def main() -> None:
    setup_logging()

    zones = ["TTF_EOD", "TTF_EGSI"]
    look_back_days = int(os.environ.get("LOOKBACK_DAYS", "20"))

    results: Dict[str, pd.DataFrame] = {}

    for z in zones:
        df = fetch_data_for_zone(z, look_back_window_days=look_back_days)

        # EOD is typically required. EGSI may be unavailable (401).
        if df.empty and "EOD" in z:
            raise RuntimeError(f"Failed to fetch ANY data for required zone={z}")
        if df.empty and "EGSI" in z:
            logger.warning("No EGSI data available for zone=%s (possibly unauthorized / paywalled). Continuing.", z)

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
