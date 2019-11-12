from datetime import datetime, timedelta
import attr
import pandas as pd
import pytz
import requests
from time import sleep
from itertools import groupby


RESOLUTION_IN_MINUTES = 1

SITE_DATA = {'site_id': 0,
             'start_datetime':datetime.strptime('2016-01-01', '%Y-%m-%d'),
             'end_datetime':datetime.strptime('2017-01-01', '%Y-%m-%d'),
             'latitude':28.538336,
             'longitude':-81.379234,
             'pv':True,
             }

# 2016
# 0: {'name': 'Orlando', 'lat':28.538336, 'lon':-81.379234},
# 1: {'name': 'London', 'lat':51.507351, 'lon':-0.127758},


def fetch_historic_weather_data(site_data, api_key:str='4b6c6722e6c612fd5f789cee71aa7135', file_path:str='./input')-> pd.DataFrame:
    """
    Fetch Historic Weather data for the given site

    id: Job Id
    site_id: Site identifier
    start_datetime: Start date from which the historic weather data needs to be fetched
    end_datetime: End date for the historic weather data fetch
    """

    if site_data.get('end_datetime') == None:
        site_data['end_datetime'] = datetime.now()

    weather_connector = DarkSkyWeatherConnector(site_id=site_data.get('site_id'),
                                                latitude=site_data.get('latitude'),
                                                longitude=site_data.get('longitude'),
                                                start_date=site_data.get('start_datetime'),
                                                end_date=site_data.get('end_datetime'),
                                                exclude_existing_data=False,
                                                include_solar=site_data.get('pv'),
                                                api_key=api_key,
                                                )

    weather_data = weather_connector.extract_weather_in_range()
    weather_data.to_csv(f'{file_path}/site{site_data.get("site_id")}_darksky_weather.csv')
    return weather_data


@attr.s
class DarkSkyWeatherConnector(object):
    """
    Class used for pulling weather data from DarkSky's API.  `self.extract_weather_in_range` will pull hourly weather
    data from `self.start_date` to `self.end_date` for `self.latitude` and `self.longitude`, parse returned json for
    self.desired_columns, and construct and return a pd.DataFrame.

    Their documentation is concise and comprehensive:
        https://darksky.net/dev/docs

    The basic http request for either historical data or forecasted data is:
        https://api.darksky.net/forecast/[key]/[latitude],[longitude],[time]
    eg
        https://api.darksky.net/forecast/0123456789abcdef9876543210fedcba/42.3601,-71.0589,255657600

    By default, we use the following params with the above request:
        exclude: 'currently,minutely,daily,alerts,flags'
        solar: 1

    Setting exclude to 'currently,minutely,daily,alerts,flags' strips out data that we do not use; therefore, reducing
    the amount of data that we transfer.

    We presently do not use minutely data because it is only for the current hour, and when we pull weather data it
    is for historical simulations.

    By specifying solar = 1, we get the following additional data:
        'azimuth', 'altitude', 'dni', 'ghi', 'dhi', 'etr'

    We currently only use dni, but should continue integrating other data in the future.
    """

    site_id = attr.ib()
    latitude = attr.ib()
    longitude = attr.ib()
    start_date = attr.ib()
    end_date = attr.ib()
    exclude_existing_data = attr.ib(default=False)

    api_key = attr.ib(default="4b6c6722e6c612fd5f789cee71aa7135")
    base_url = attr.ib(default="https://api.darksky.net/forecast")
    # Weather data that will be parsed from api request and stored in returned pd.DataFrame
    desired_columns = attr.ib(default=['time', 'dni', 'ghi', 'dhi', 'apparentTemperature', 'temperature', 'precipIntensity',
                                       'humidity', 'dewPoint', 'pressure', 'cloudCover', 'windSpeed', 'windGust', 'windBearing','visibility'])
    # will remove data points from returned json (reduces I/O burden)
    to_exclude = attr.ib(default='minutely,daily,alerts,flags')
    # If true, api request will return 'azimuth', 'altitude', 'dni', 'ghi', 'dhi', 'etr'
    include_solar = attr.ib(default=True)

    current_weather_data_dict = attr.ib(init=False, default=None)
    weather_df = attr.ib(init=True, default=pd.DataFrame())


    def fetch_current_weather_data(self):
        """
        Pull current weather data from the weather data provider DarkSky
        :return:
        """

        url = f"{self.base_url}/{self.api_key}/{self.latitude},{self.longitude}"

        # Exclude everything except "currently"
        to_exclude = 'minutely,hourly,daily,alerts,flags'
        params = {"exclude": to_exclude, "solar": 1 if self.include_solar else 0}

        self.current_weather_data_dict = requests.get(url, params=params).json()['currently']


    def make_request_for_date(self, date):
        """
        Makes api request for specified date and returns json
        :param date: <datetime.datetime or datetime.date> If datetime, request will ignore time and pull for 00:00
        :return:
        """
        str_date = date.strftime('%Y-%m-%dT00:00:00')
        url = f"{self.base_url}/{self.api_key}/{self.latitude},{self.longitude},{str_date}"
        params = {"exclude": self.to_exclude, "solar": 1 if self.include_solar else 0}
        return requests.get(url, params=params).json()


    def construct_daily_df(self, date):
        """
        Calls `self.make_request_for_date` and converts returned json into a pd.DataFrame
        :param date: <datetime.datetime or datetime.date> If datetime, request will ignore time and pull for 00:00
        :return:
        """
        raw_json = self.make_request_for_date(date=date)

        #Get currently data
        self.current_weather_data_dict = raw_json['currently']
        flat_daily_df = []
        for hd in raw_json['hourly']['data']:
            hd['time'] = datetime.fromtimestamp(hd['time'], pytz.timezone(raw_json['timezone']))
            flat_daily_df.append({k:v for k,v in hd.items() if k!='solar' and k in self.desired_columns})
            flat_daily_df[-1].update(hd.get('solar',{}))


        weather_df = pd.DataFrame(flat_daily_df).set_index('time')
        weather_df[['dni', 'dhi', 'ghi']] = weather_df[['dni', 'dhi', 'ghi']].fillna(value=0)

        return weather_df[[c for c in self.desired_columns if c!='time']]


    def extract_weather_in_range(self):
        """
        extract_weather_in_range will pull hourly weather data for every day from start_date to
        end_date (inclusive)

        Steps:
            1. Creates a date_range based on `self.start_date` and `self.end_date`
            2. Makes api requests and constructs a pd.DataFrame for date_range
        :return:
        """

        if self.exclude_existing_data:
            # Exclude weather data fetch for those days, for which we have 24 hours of data in the DB.
            exclusion_list = self.get_exclusion_list()
            date_range = [x.date() for x in pd.date_range(start=self.start_date.date(), end=self.end_date.date(), freq='D').to_pydatetime()]
            date_range = [x for x in date_range if x not in exclusion_list]
            if len(date_range) == 0:
                return None
        else:
            # Fetch all data irrespective of whether they exist in the DF or not. Existing data gets overwritten
            date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')

        return pd.concat([self.construct_daily_df(date) for date in date_range])


    def get_exclusion_list(self, nhours:int=24) -> list:
        """
        returns a list of dates for which we have 24 hours of data in the DF
        """

        weather_data_qs = self.weather_df[self.site_id]
        dates_list = [weather_data.timestamp.date() for weather_data in weather_data_qs]

        # Dates for which we have 24 hours of data
        exclusion_list = [key for key, group in groupby(dates_list) if len(list(group)) >= nhours]

        return exclusion_list

# wd = fetch_historic_weather_data(SITE_DATA)