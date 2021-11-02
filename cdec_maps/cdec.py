import param
import pandas as pd
from dask import dataframe as dd
from .cdec_cache import cache_to_file

DURATION_MAP = {'(event)': 'E', '(daily)': 'D',
                '(monthly)': 'M', '(hourly)': 'H'}
DURATION_MAP_INVERTED = {DURATION_MAP[k]: k for k in DURATION_MAP.keys()}


def get_duration_code(duration):
    return DURATION_MAP[duration]


def to_date_format(str):
    try:
        return pd.to_datetime(str).strftime('%Y-%m-%d')
    except:
        return ''


class Reader(param.Parameterized):
    cdec_base_url = param.String(default="http://cdec.water.ca.gov",
                                 allow_None=False, regex='http://.*')

    def __init__(self, cdec_base_url="http://cdec.water.ca.gov"):
        self.cdec_base_url = cdec_base_url

    def _read_single_table(self, url):
        df = pd.read_html(url)
        return df[0]

    @cache_to_file(expires='1D')
    def read_daily_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/dailyStations.html")

    @cache_to_file(expires='1D')
    def read_realtime_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/realStations.html")

    @cache_to_file(expires='1D')
    def read_sensor_list(self):
        return self._read_single_table(self.cdec_base_url + "/misc/senslist.html")

    @cache_to_file(list=True, expires='1D')
    def read_station_meta_info(self, station_id):
        tables = pd.read_html(self.cdec_base_url + '/dynamicapp/staMeta?station_id=%s' % station_id)
        # table[0] should be station meta info

        def _pair_table_columns(df, column_index):
            return df.iloc[:, column_index].set_index(column_index[1]).set_axis(['Value'], axis=1)
        tables[0] = pd.concat([_pair_table_columns(tables[0], [0, 1]),
                              _pair_table_columns(tables[0], [2, 3])])
        if 'Zero Datum' in tables[1].columns:  # For now remove
            datum_table = tables.pop(1)
        else:
            datum_table = pd.DataFrame(
                [], ['Zero Datum', 'Adj To NGVD', 'Peak of Record', 'Monitor Stage', 'Flood Stage', 'Guidance Plots'])
        # table[1] should be station sensor info
        tables[1] = tables[1].set_axis(
            ['Sensor Description', 'Sensor Number', 'Duration', 'Plot', 'Data Collection', 'Data Available'], axis=1)
        # table[2] should be station comments
        if len(tables) > 2:
            tables[2] = tables[2].set_axis(['Date', 'Comment'], axis=1)
        else:
            tables.append(pd.DataFrame([], columns=['Date', 'Comment']))
        tables.append(datum_table)
        return tables

    def _read_station_data(self, station_id, sensor_number, duration_code, start, end):
        data_url = f'{self.cdec_base_url}/dynamicapp/req/CSVDataServletPST?Stations={station_id}&SensorNums={sensor_number}&dur_code={duration_code}&Start={start}&End={end}'
        type_map = {'STATION_ID': 'category', 'DURATION': 'category', 'SENSOR_NUMBER': 'category', 'SENSOR_TYPE': 'category',
               'VALUE': 'float', 'DATA_FLAG': 'category', 'UNITS': 'category'}
        df = pd.read_csv(data_url, dtype=type_map, na_values=[
                         '---', 'ART', 'BRT'], parse_dates=True, index_col='DATE TIME')
        df['OBS DATE'] = pd.to_datetime(df['OBS DATE'])
        return df

    def _to_datetime(self, dstr):
        if dstr == '':
            return pd.Timestamp.now()
        else:
            return pd.to_datetime(dstr)

    def to_year(self, dstr):
        return self._to_datetime(dstr).year

    def _sort_times(self, start, end):
        stime = self._to_datetime(start)
        etime = self._to_datetime(end)
        if stime < etime:
            return to_date_format(stime), to_date_format(etime)
        else:
            return  to_date_format(etime), to_date_format(stime)

    def _undecorated_read_station_data(self, station_id, sensor_number, duration_code, start, end):
        '''
        Using dask read CDEC via multiple threads which is quite fast and scales as much as CDEC services will allow
        '''
        # make sure start and end are in the right order, start < order
        start, end = self._sort_times(start, end)
        start_year = self.to_year(start)
        end_year = self.to_year(end) + 1
        url = self.cdec_base_url + \
            '/dynamicapp/req/CSVDataServletPST?Stations={station_id}&SensorNums={sensor_number}&dur_code={duration_code}&Start=01-01-{start}&End=12-31-{end}+23:59'
        list_urls = [url.format(station_id=station_id, sensor_number=sensor_number, duration_code=duration_code,
                                start=syear, end=syear) for syear in range(start_year, end_year)]
        dtype_map = {'STATION_ID': 'category', 'DURATION': 'category', 'SENSOR_NUMBER': 'category', 'SENSOR_TYPE': 'category',
                'VALUE': 'float', 'DATA_FLAG': 'category', 'UNITS': 'category'}
        ddf = dd.read_csv(list_urls, blocksize=None, dtype=dtype_map,
                            na_values={'VALUE': ['---', 'ART', 'BRT']})
        # parse_dates=['DATE TIME','OBS DATE'] # doesn't work so will have to read in as strings and convert later
        # dd.visualize(): shows parallel tasks which are executed below
        df = ddf.compute()
        df.index = pd.to_datetime(df['DATE TIME'])
        df['OBS DATE'] = pd.to_datetime(df['OBS DATE'])
        df = df.drop(columns=['DATE TIME'])
        return df

    @cache_to_file(expires='1H')
    def read_station_data(self, station_id, sensor_number, duration_code, start, end):
        '''
        Using dask read CDEC via multiple threads which is quite fast and scales as much as CDEC services will allow
        '''
        start, end = self._sort_times(start, end)
        df = self._undecorated_read_station_data(
            station_id, sensor_number, duration_code, start, end)
        return df[(df.index >= start) & (df.index <= end)] # more robust then df.loc[pd.to_datetime(start):pd.to_datetime(end)]

    ###
    def read_entire_station_data_for(self, station_id, sensor_number, duration_code):
        dflist = self.read_station_meta_info(station_id)
        df_sensors = dflist[1]
        sensor_row = df_sensors[(df_sensors['Sensor Number'] == int(sensor_number)) & (
            df_sensors['Duration'] == DURATION_MAP_INVERTED[duration_code])].iloc[0]
        sdate, edate = tuple([s.strip()
                            for s in sensor_row['Data Available'].split('to')])
        df = self._undecorated_read_station_data(station_id, sensor_number, duration_code,
                                    to_date_format(sdate), to_date_format(edate))
        return df

################# MODULE LEVEL methods ###################################
