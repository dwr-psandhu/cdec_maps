import param
import pandas as pd
from dask import dataframe as dd
import diskcache as dc

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


cache_dir = 'cdec_cache'
cache = dc.Cache(cache_dir)


class Reader(param.Parameterized):
    cdec_base_url = param.String(default="http://cdec.water.ca.gov",
                                 allow_None=False, regex='http://.*')

    def __init__(self, cdec_base_url="http://cdec.water.ca.gov"):
        self.cdec_base_url = cdec_base_url

    def _read_single_table(self, url):
        df = pd.read_html(url)
        return df[0]

    @cache.memoize(name='daily_stations', ignore=[0])
    def read_daily_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/dailyStations.html")

    @cache.memoize(name='realtime_stations', ignore=[0])
    def read_realtime_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/realStations.html")

    @cache.memoize(name='sensor_list', ignore=[0])
    def read_sensor_list(self):
        return self._read_single_table(self.cdec_base_url + "/misc/senslist.html")

    @cache.memoize(name='all_stations', ignore=[0])
    def read_all_stations(self):
        daily_stations = self.read_daily_stations()
        realtime_stations = self.read_realtime_stations()
        return daily_stations.merge(realtime_stations, how='outer')

    @cache.memoize(name='all_stations_meta_info', ignore=[0])
    def read_all_stations_meta_info(self):
        all_stations = self.read_all_stations()
        meta_info_list = [self.read_station_meta_info(station_id)[1].assign(
            ID=station_id).set_index('ID') for station_id in all_stations.ID]
        return pd.concat(meta_info_list).astype(dtype={'Sensor Number': 'int'})

    @cache.memoize(name='station_meta_info', ignore=[0])
    def read_station_meta_info(self, station_id):
        try:
            url = self.cdec_base_url + '/dynamicapp/staMeta?station_id=%s' % station_id
            tables = pd.read_html(url)
        except:
            return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
        # table[0] should be station meta info

        def _pair_table_columns(df, column_index):
            return df.iloc[:, column_index].set_index(column_index[0]).set_axis(['Value'], axis=1)
        tables[0] = pd.concat([_pair_table_columns(tables[0], [0, 1]),
                              _pair_table_columns(tables[0], [2, 3])])
        if len(tables) < 2 or len(tables[1].columns) < 6:  # missing or comments table only
            tables.insert(1, pd.DataFrame([], columns=['Sensor Description',
                          'Sensor Number', 'Duration', 'Plot', 'Data Collection', 'Data Available']))
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

    def _xread_station_data(self, station_id, sensor_number, duration_code, start, end):
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
            return to_date_format(etime), to_date_format(stime)

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

    def _read_station_data(self, station_id, sensor_number, duration_code, start, end):
        '''
        Using dask read CDEC via multiple threads which is quite fast and scales as much as CDEC services will allow
        '''
        df = self._undecorated_read_station_data(
            station_id, sensor_number, duration_code, start, end)
        # more robust then df.loc[pd.to_datetime(start):pd.to_datetime(end)]
        return df[(df.index >= start) & (df.index <= end)]

    def read_station_data(self, station_id, sensor_number, duration_code, start, end):
        """
        get station data for indicated start and end times
        0. Look up station data in cache
        I. If not in cache, fetch data from start to end, store in cache and 
        II. if start earlier than data in cache, fetch station data for start to start date of data in cache
        III. if end later than data in cache, fetch station data for end date of data in cache to end
        IV. combine first with data in cache, II and III fetches and store in cache
        V. return 
        """
        key = ('station_data', station_id, sensor_number, duration_code, None)
        if key in cache:
            dfc = cache[key]
            start, end = self._sort_times(start, end)
            updated = False
            if dfc.empty:
                df = self._read_station_data(station_id, sensor_number, duration_code, start, end)
                cache[key] = df
            else:
                if self._to_datetime(start) < dfc.index[0]:
                    df0 = self._read_station_data(
                        station_id, sensor_number, duration_code, start, dfc.index[0])
                    dfc = dfc.combine_first(df0)
                    updated = True
                if self._to_datetime(end) > dfc.index[-1]:
                    df1 = self._read_station_data(
                        station_id, sensor_number, duration_code, dfc.index[-1], end)
                    dfc = dfc.combine_first(df1)
                    updated = True
                if updated:
                    cache[key] = dfc
                df = dfc
        else:
            df = self._read_station_data(station_id, sensor_number, duration_code, start, end)
            cache[key] = df
        return df[(df.index >= start) & (df.index <= end)]
    ###

    def read_entire_station_data(self, station_id, sensor_number, duration_code):
        dflist = self.read_station_meta_info(station_id)
        df_sensors = dflist[1]
        sensor_row = df_sensors[(df_sensors['Sensor Number'] == int(sensor_number)) & (
            df_sensors['Duration'] == DURATION_MAP_INVERTED[duration_code])].iloc[0]
        sdate, edate = tuple([s.strip()
                            for s in sensor_row['Data Available'].split('to')])
        sdate, edate = self._sort_times(to_date_format(sdate), to_date_format(edate))
        # first read station data that might return cached value
        df = self._read_station_data(station_id, sensor_number, duration_code, sdate, edate)
        actual_edate = to_date_format(df.last_valid_index())
        if actual_edate != edate:  # read the extra data and store in cache
            dfext = self._read_station_data.__wrapped__(station_id, sensor_number, duration_code,
                actual_edate, edate)
            df = df.combine_first(dfext)
            key = self._read_station_data.__cache_key__(station_id, sensor_number, duration_code,
                actual_edate, edate)
            cache[key] = df
        return df

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

import tqdm 

def read_station_meta_info(stations):
    r = Reader()
    for sid in tqdm.tqdm(stations.ID):
        try:
            r.read_station_meta_info(sid)
        except:
            print(sid)

def merge_sensors_with_id(stations, sid):
    r=Reader()
    try:
        _, sensors, _, _ = r.read_station_meta_info(sid)
        sensors['ID'] = sid
        return sensors.merge(stations, on='ID')
    except:
        return pd.DataFrame()

@cache.memoize(name='stations_with_meta')
def read_stations_with_meta():
    r = Reader()
    stations=r.read_all_stations()
    stations_with_meta = pd.concat([merge_sensors_with_id(stations, sid) for sid in tqdm.tqdm(stations.ID)], axis=0)
    stations_with_meta = stations_with_meta.reset_index()
    stations_with_meta = stations_with_meta.astype({'Sensor Number': int})
    stations_with_meta = stations_with_meta.merge(r.read_sensor_list(),left_on='Sensor Number', right_on='Sensor No')
    stations_with_meta = stations_with_meta.drop(columns=['Sensor No'])
    return stations_with_meta