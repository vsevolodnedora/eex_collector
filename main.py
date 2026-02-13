"""
Inspired by https://github.com/gruijter/com.gruijter.powerhour
Translated with the help of ChatGPT-o1-preview
"""

from datetime import datetime, timedelta
import pytz
import pandas as pd
import copy
import time
from urllib3.util.retry import Retry
import requests
import urllib3
import urllib.parse
urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)
from requests.adapters import HTTPAdapter
# Each bidding zone has two variants:
# EOD: End of Day settlement prices.
# EGSI: European Gas Spot Index prices.
biddingZones = {
    # TTF - Title Transfer Facility (Netherlands)
    "TTF_EOD": 'TTF_EOD', # End of Day (EOD) Prices: Reflect the price at which the gas contracts settled at the end of the trading day.
    "TTF_EGSI": 'TTF_EGSI', # European Gas Spot Index (EGSI) Prices: Represents spot prices for natural gas, which are prices for immediate delivery.
    # CEGH_VTP: Central European Gas Hub Virtual Trading Point (Austria)
    "CEGH_VTP_EOD": 'CEGH_VTP_EOD',
    "CEGH_VTP_EGSI": 'CEGH_VTP_EGSI',
    # CZ_VTP: Czech Virtual Trading Point
    "CZ_VTP_EOD": 'CZ_VTP_EOD',
    "CZ_VTP_EGSI": 'CZ_VTP_EGSI',
    # ETF: Zeebrugge Hub (Belgium)
    "ETF_EOD": 'ETFF_EOD',
    "ETF_EGSI": 'ETF_EGSI',
    # THE: Trading Hub Europe (Germany)
    "THE_EOD": 'THE_EOD',
    "THE_EGSI": 'THE_EGSI',
    # PEG: Point d'Échange de Gaz (France)
    "PEG_EOD": 'PEG_EOD',
    "PEG_EGSI": 'PEG_EGSI',
    # ZTP: Zeebrugge Trading Point (Belgium)
    "ZTP_EOD": 'ZTP_EOD',
    "ZTP_EGSI": 'ZTP_EGSI',
    # PVB: Punto Virtual de Balance (Spain)
    "PVB_EOD": 'PVB_EOD',
    "PVB_EGSI": 'PVB_EGSI',
    # NBP: National Balancing Point (United Kingdom)
    "NBP_EOD": 'NBP_EOD',
    "NBP_EGSI": 'NBP_EGSI',
}

# The biddingZonesMap provides the priceSymbol required by the EEX API for each bidding zone. For example:
biddingZonesMap = {
    # End of Day (EOD) Prices:
    "TTF_EOD": '"#E.TTF_GND1"',
    "CEGH_VTP_EOD": '"#E.CEGH_GND1"',
    "CZ_VTP_EOD": '"#E.OTE_GSND"',
    "ETF_EOD": '"#E.ETF_GND1"',
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


defaultHost = 'webservice-eex.gvsi.com'  # EOD and EGSI # The default host URL for the EEX API.
defaultPort = 443 # The port number for HTTPS connections.
defaultTimeout = 30000  # in milliseconds # The default timeout for HTTP requests, specified in milliseconds.

class EEX:
    """
    The EEX class encapsulates all the functionality needed to interact with the EEX API.
    It includes methods for initializing the session, fetching bidding zones, retrieving gas prices,
    and making HTTP requests.
    """
    def __init__(self, opts=None):
        options = opts or {}
        self.host = options.get('host', defaultHost)
        self.port = options.get('port', defaultPort)
        self.timeout = options.get('timeout', defaultTimeout)
        self.biddingZone = options.get('biddingZone')
        self.verify_ssl = options.get('verify_ssl', False)  # keep your current behavior by default
        self.lastResponse = None
        self.lastDailyInfo = None
        self.tmpZone = None

        # One session + retries (handles transient 429/5xx)
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _headers(self):
        # For GET, "Accept" matters; request "content-type" usually doesn't.
        return {
            "Accept": "application/json",
            "Origin": "https://www.eex.com",
            "Referer": "https://www.eex.com",
            "User-Agent": "Mozilla/5.0",
            "Connection": "keep-alive",
        }
    def _get_json(self, path: str, timeout_ms: int | None = None) -> dict:
        url = f"https://{self.host}:{self.port}{path}"
        r = self.session.get(
            url,
            headers=self._headers(),
            timeout=(timeout_ms or self.timeout) / 1000,
            verify=self.verify_ssl,
            allow_redirects=True,
        )

        # keep something useful for debugging
        self.lastResponse = (r.text or "")[:2000]

        # status check first
        if r.status_code != 200:
            snippet = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"HTTP {r.status_code} for {r.url}. Body starts: {snippet!r}")

        # content-type check (tolerate charset etc.)
        ctype = (r.headers.get("content-type") or "").lower()
        if "json" not in ctype:
            snippet = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"Non-JSON response for {r.url} (content-type={ctype!r}). Body starts: {snippet!r}")

        try:
            return r.json()
        except ValueError as e:
            snippet = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"Invalid JSON from {r.url}. Body starts: {snippet!r}") from e

    def _items(self, path: str, timeout_ms: int | None = None) -> list[dict]:
        payload = self._get_json(path, timeout_ms)
        items = payload.get("results", {}).get("items")
        if not isinstance(items, list):
            raise RuntimeError(f"Invalid payload shape for {path}: missing results.items")
        return items

    def getPrices(self, options=None):
        """
         Retrieves gas price information based on specified options.
        :param options: A dictionary containing optional parameters:
            'biddingZone': The bidding zone to query.
            'dateStart': The start date for the data retrieval.
            'dateEnd': The end date for the data retrieval.
        :return:
            Price: The settlement or spot price of natural gas, adjusted to euro per 1,000 cubic meters
            (the code multiplies by 9.7694 to convert the unit).
        """

        # Date setup
        today = datetime.utcnow()
        tomorrow = today + timedelta(days=1)

        # Parsing options
        opts = options or {}
        zone = opts.get('biddingZone', self.biddingZone)
        start = datetime.fromisoformat(opts.get('dateStart')) if opts.get('dateStart') else today
        end = datetime.fromisoformat(opts.get('dateEnd')) if opts.get('dateEnd') else tomorrow

        # Adjusting times
        start = start.replace(minute=0, second=0, microsecond=0)
        end = end.replace(minute=0, second=0, microsecond=0)

        # Preparing query parameters
        self.tmpZone = zone
        start2 = start - timedelta(days=1)  # start earlier to make sure weekend is fetched
        chartstartdate = start2.isoformat().split('T')[0]  # 'YYYY-MM-DD'
        chartstopdate = end.isoformat().split('T')[0]  # 'YYYY-MM-DD'
        priceSymbol = biddingZonesMap.get(zone)  # e.g., "#E.TTF_GND1"

        query = urllib.parse.urlencode({
            'priceSymbol': priceSymbol,
            'chartstartdate': chartstartdate,
            'chartstopdate': chartstopdate,
        })
        path = f'/query/json/getDaily/close/tradedatetimegmt/?{query}'

        # Making the API request
        print(f"\tRequest: {path}")
        res = self._makeRequest(path, 90*1000)

        # Processing the response
        if not res or not res[0] or not res[0].get('time'):
            raise Exception('No gas price info found')

        # Extracting and adjusting price information; Make array with concise info per day in euro / 1000 m3 gas
        priceInfo = []
        for day in res:
            if day['descr'] == zone:
                # Time adjustments
                dayStart = day['time'].replace(hour=0, minute=0, second=0, microsecond=0)
                timezone = pytz.timezone('Europe/Berlin')
                dayStartLocalized = timezone.localize(dayStart)
                timeZoneOffset = dayStartLocalized.utcoffset().total_seconds() * 1000
                dayStart -= timedelta(milliseconds=timeZoneOffset)
                tariffStart = dayStart + timedelta(hours=6)


                if day['price'] is None:
                    print(f"WARNING: No gas price info found for options={options}")
                # Collecting price information
                ''' 
                https://beyondfossilfuels.org/wp-content/uploads/2023/03/BFF-FreedomFromFossilFuels-2023-LowRes-2.pdf
                Base units for our report are billion cubic metres (bcm) for fossil gas and million tonnes
                (Mt) for hard coal. For coherence with other similar reports, we have applied uniform
                conversion factors for both commodities: 9.7694 TWh/bcm for fossil gas and 8.141 TWh/
                Mt for hard coal. 
                '''
                priceInfo.append({
                    'tariffStart': tariffStart,
                    'price': float(day['price'] if not day['price'] is None else 0) * 9.7694,
                    'descr': day['descr'] if not day['descr'] is None else "None",
                })

        # Filtering and adjusting the data
        priceInfo = [day for day in priceInfo if day['tariffStart'] >= (start - timedelta(days=1))]

        # Pad info to fill all hours in a day -- Filling all hours in a day
        info = []
        for day in priceInfo:
            startTime = day['tariffStart']
            endTime = startTime + timedelta(days=1)
            time = startTime
            while time < endTime:
                info.append(
                    {'time': time,
                     'price': day['price']
                     }
                )
                time += timedelta(hours=1)
        # Remove out-of-bounds data
        info = [hourInfo for hourInfo in info if start <= hourInfo['time'] < end]
        return info


    def _makeRequest(self, path, timeout=None):
        # --- daily info ---
        dailyInfo = self._items(path, timeout)
        lastDaily = dailyInfo[-1] if dailyInfo else None

        # Compensate for bug in EEX (your logic preserved)
        if self.lastDailyInfo:
            for idx, dayInfo in enumerate(dailyInfo):
                if (len(dailyInfo) - 4) < idx < (len(dailyInfo) - 1):
                    previousInfo = next(
                        (x for x in self.lastDailyInfo
                         if x.get("tradedatetimegmt") == dayInfo.get("tradedatetimegmt")),
                        None
                    )
                    if previousInfo and previousInfo.get("close") != dayInfo.get("close"):
                        dailyInfo[idx]["close"] = previousInfo["close"]
        self.lastDailyInfo = dailyInfo.copy()

        # --- weekend info (best-effort) ---
        weekendInfo = None
        if self.tmpZone and "EOD" in self.tmpZone:
            try:
                new_path = path.replace("ND", "WE").replace("DAY", "WEEKEND")
                weekendInfo = self._items(new_path, timeout)
            except Exception as e:
                print(f"WARNING: weekend request failed ({e}). Continuing without weekend data.")
                weekendInfo = None

        # --- settledate (best-effort; if fails, treat last day as NOT closed) ---
        lastSettleDate = None
        try:
            priceSymbol = biddingZonesMap.get(self.tmpZone)
            query = urllib.parse.urlencode({"priceSymbol": priceSymbol})
            settle_path = f"/query/json/getQuotes/settledate/dir/?{query}"
            settle_items = self._items(settle_path, timeout)
            lastSettleDate = (settle_items[0] if settle_items else {}).get("settledate")
        except Exception as e:
            print(f"WARNING: settledate request failed ({e}). Will treat last day as not closed.")
            lastSettleDate = None

        lastDailyIsClosed = bool(
            lastDaily
            and lastDaily.get("tradedatetimegmt")
            and lastSettleDate
            and lastDaily["tradedatetimegmt"].split(" ")[0] == lastSettleDate
        )

        # --- map to your simplified response ---
        resp = []
        for idx, day in enumerate(dailyInfo):
            t = datetime.strptime(day["tradedatetimegmt"].split(" ")[0], "%m/%d/%Y") + timedelta(days=1)
            mappedDay = {"time": t, "price": day.get("close"), "descr": self.tmpZone}

            # If we can't confirm settlement, drop last day (safe default)
            if idx == len(dailyInfo) - 1 and t.weekday() != 5 and not lastDailyIsClosed:
                mappedDay = None

            if weekendInfo and mappedDay and t.weekday() == 5:
                weekend = next((w for w in weekendInfo if w.get("tradedatetimegmt") == day.get("tradedatetimegmt")), None)
                if weekend:
                    satTime = datetime.strptime(weekend["tradedatetimegmt"].split(" ")[0], "%m/%d/%Y") + timedelta(days=1)
                    sat = {"time": satTime, "price": weekend.get("close"), "descr": self.tmpZone}
                    sun = {**sat, "time": satTime + timedelta(days=1)}
                    mon = {**mappedDay, "time": t + timedelta(days=2)}
                    resp.extend([sat, sun, mon])
                    mappedDay = None

            if mappedDay:
                resp.append(mappedDay)

        return resp


def fetch_data_for_zone(biddingZone:str,look_back_window_days = 20):

    today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    today_saved = copy.deepcopy(today)

    df_ = pd.DataFrame()

    eex = EEX(dict(biddingZone = biddingZone))
    n = 0
    while today > (today_saved - timedelta(days=look_back_window_days)):

        yesterday = today - timedelta(days=2)
        tomorrow = today + timedelta(days=2)

        print(f"Getting EEX prices for zone={biddingZone} for time period {yesterday} - {tomorrow}")
        result = eex.getPrices(
            {'dateStart': yesterday.isoformat(), 'dateEnd': tomorrow.isoformat()}
        )
        df_i = pd.DataFrame(result)
        df_ = pd.concat([df_, df_i])
        today -= timedelta(days=2)

        n+=1

    print(f"Successfully collected data for {biddingZone} N={n} times")

    df_.sort_values(by='time', ascending=True, inplace=True)
    df_.drop_duplicates(inplace=True)
    return df_

def main():
    res = {}

    for biddingZone in ['TTF_EOD','TTF_EGSI']:

        df_ = fetch_data_for_zone(biddingZone)
        if df_.empty:
            print(f'Error. Failed to fetch ANY data for zone = {biddingZone}. Trying again...')
            time.sleep(60)
            df_ = fetch_data_for_zone(biddingZone)
            if df_.empty:
                raise IOError(f"Error. Repeatedly failed to fetch ANY data for zone = {biddingZone}.")

        res[biddingZone] = copy.deepcopy(df_)

    # merge the result into one dataframe

    df = pd.merge(
        left=res['TTF_EGSI'].rename(columns={'time': 'date', 'price': 'EOD'}),
        right=res['TTF_EOD'].rename(columns={'time': 'date', 'price': 'EGSI'}),
        on='date',
        how='outer'
    )
    frequency = pd.DatetimeIndex(df['date']).inferred_freq
    df.to_csv(f"./data/ttf_{datetime.today().strftime('%Y-%m-%d')}_{frequency}.csv",index=False)


if __name__ == '__main__':
    main()


