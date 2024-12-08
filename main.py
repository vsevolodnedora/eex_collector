"""
Inspired by https://github.com/gruijter/com.gruijter.powerhour
Translated with the help of ChatGPT-o1-preview
"""

from datetime import datetime, timedelta
import pytz
import pandas as pd
import copy
import time

import requests
import urllib3
import urllib.parse
urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

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
        """
        self.host: The API host to connect to.
        self.port: The port to use for the connection.
        self.timeout: The timeout for HTTP requests.
        self.biddingZone: The default bidding zone for price queries.
        self.biddingZones: A dictionary to store bidding zones (to be defined).
        self.biddingZonesMap: A mapping from bidding zones to price symbols (to be defined).
        self.lastResponse: Stores the last HTTP response received.
        self.lastDailyInfo: Stores the last set of daily information retrieved.
        :param opts:
        """
        options = opts or {}
        self.host = options.get('host', defaultHost)
        self.port = options.get('port', defaultPort)
        self.timeout = options.get('timeout', defaultTimeout)
        self.biddingZone = options.get('biddingZone')
        # self.biddingZones = {}  # Define your bidding zones here
        # self.biddingZonesMap = {}  # Define your bidding zones map here
        self.lastResponse = None
        self.lastDailyInfo = None

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
        try:
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
                        'price': day['price'] * 9.7694,
                        'descr': day['descr'],
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
        except Exception as error:
            raise error

    def _makeRequest(self, path, timeout=None):
        try:
            headers = {
                'content-type': 'application/json',
                'Origin': 'https://www.eex.com',
                'Referer': 'https://www.eex.com',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Connection': 'keep-alive',
            }
            url = f"https://{self.host}:{self.port}{path}"
            response = requests.get(
                url,
                headers=headers,
                timeout=(timeout or self.timeout)/1000,
                verify=False # rejectUnauthorized: false -- Disables SSL verification to bypass any certificate issues
            )

            self.lastResponse = response.text or response.status_code
            content_type = response.headers.get('content-type')

            if response.status_code != 200:
                self.lastResponse = response.status_code
                print(f"For url {response.url}")
                raise Exception(f'HTTP request failed. Status Code: {response.status_code}')

            if headers['content-type'] != content_type:
                body = response.text[:20]
                raise Exception(f"Expected {headers['content-type']} but received {content_type}: {body}")

            if 'json' in content_type:
                respJSON = response.json()
                if not respJSON.get('results') or not respJSON['results'].get('items'):
                    raise Exception('Invalid info')
                dailyInfo = respJSON['results']['items']
                lastDaily = dailyInfo[-1] if dailyInfo else None

                # Compensate for bug in EEX
                if self.lastDailyInfo:
                    for idx, dayInfo in enumerate(dailyInfo):
                        if (len(dailyInfo) - 4) < idx < (len(dailyInfo) - 1):
                            previousInfo = next(
                                (lastDayInfo for lastDayInfo in self.lastDailyInfo
                                 if lastDayInfo['tradedatetimegmt'] == dayInfo['tradedatetimegmt']), None)
                            if previousInfo and previousInfo['close'] != dayInfo['close']:
                                dailyInfo[idx]['close'] = previousInfo['close']
                self.lastDailyInfo = dailyInfo.copy()

                # Fetch weekend info ONLY FOR EOD (not EGSI)
                weekendInfo = None
                if 'EOD' in self.tmpZone:
                    new_path = path.replace('ND', 'WE').replace('DAY', 'WEEKEND')
                    weekend_response = requests.get(
                        f"https://{self.host}:{self.port}{new_path}",
                        headers=headers,
                        timeout=(timeout or self.timeout)/1000,
                        verify=False
                    )
                    weekendJSON = weekend_response.json()
                    weekendInfo = weekendJSON.get('results', {}).get('items')

                # Fetch today's settle
                priceSymbol = biddingZonesMap.get(self.tmpZone)
                query = urllib.parse.urlencode({'priceSymbol': priceSymbol})
                settle_path = f"/query/json/getQuotes/settledate/dir/?{query}"
                settle_response = requests.get(
                    f"https://{self.host}:{self.port}{settle_path}",
                    headers=headers,
                    timeout=(timeout or self.timeout)/1000,
                    verify=False
                )
                settleJSON = settle_response.json()
                settle_items = settleJSON.get('results', {}).get('items', [{}])
                lastSettleDate = settle_items[0].get('settledate')

                # Check if lastDaily is settled (closed)
                lastDailyIsClosed = lastDaily and lastDaily.get('tradedatetimegmt') and (
                        lastDaily['tradedatetimegmt'].split(' ')[0] == lastSettleDate
                )

                # Create array with daily values
                resp = []
                for idx, day in enumerate(dailyInfo):
                    time = datetime.strptime(day['tradedatetimegmt'].split(' ')[0], '%m/%d/%Y')
                    time += timedelta(days=1)  # Day ahead
                    mappedDay = {
                        'time': time,
                        'price': day['close'],
                        'descr': self.tmpZone,
                    }

                    # Check if last daily entry is closed
                    if (idx == len(dailyInfo) - 1 and time.weekday() != 5 and not lastDailyIsClosed):
                        mappedDay = None  # Ignore last day info because it is not closed yet

                    # Check if last entry is weekend info and is closed
                    if weekendInfo and mappedDay and time.weekday() == 5:
                        weekend = next(
                            (dayW for dayW in weekendInfo if dayW['tradedatetimegmt'] == day['tradedatetimegmt']),
                            None
                        )
                        satTime = datetime.strptime(weekend['tradedatetimegmt'].split(' ')[0], '%m/%d/%Y')
                        satTime += timedelta(days=1)
                        sat = {
                            'time': satTime,
                            'price': weekend['close'],
                            'descr': self.tmpZone,
                        }
                        sun = sat.copy()
                        sun['time'] += timedelta(days=1)
                        mon = mappedDay.copy()
                        mon['time'] += timedelta(days=2)
                        resp.extend([sat, sun, mon])
                        mappedDay = None  # Skip adding normal weekday info

                    # Add normal weekday info
                    if mappedDay:
                        resp.append(mappedDay)
                return resp
            else:
                raise Exception(f"No JSON received: {content_type}")
        except Exception as error:
            raise error


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

        try:
            print(f"Getting EEX prices for zone={biddingZone} for time period {yesterday} - {tomorrow}")
            result = eex.getPrices(
                {'dateStart': yesterday.isoformat(), 'dateEnd': tomorrow.isoformat()}
            )
            df_i = pd.DataFrame(result)
            df_ = pd.concat([df_, df_i])
            today -= timedelta(days=2)
        except Exception as e:
            print(f"Failed with {e}")
            continue
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


