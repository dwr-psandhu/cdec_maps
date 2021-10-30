import param
import pandas as pd
from dask import dataframe as dd


class CDEC(param.Parameterized):
    cdec_base_url = param.String(default="http://cdec.water.ca.gov",
                                 allow_None=False, regex='http://.*')

    def __init__(self, cdec_base_url="http://cdec.water.ca.gov"):
        self.cdec_base_url = cdec_base_url

    def _read_single_table(self, url):
        df = pd.read_html(url)
        return df[0]

    def read_daily_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/dailyStations.html")

    def read_realtime_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/realStations.html")

    def read_sensor_list(self):
        return self._read_single_table(self.cdec_base_url + "/misc/senslist.html")

    def read_station_meta_info(self, station_id):
        tables = pd.read_html(self.cdec_base_url + '/dynamicapp/staMeta?station_id=%s' % station_id)
        # table[0] should be station meta info
        def _pair_table_columns(df, column_index):
            return df.iloc[:, column_index].set_index(column_index[1]).set_axis(['Value'], axis=1)
        tables[0] = pd.concat([_pair_table_columns(tables[0], [0, 1]),
                              _pair_table_columns(tables[0], [2, 3])])
        # table[1] should be station sensor info
        tables[1] = tables[1].set_axis(
            ['Sensor Description', 'Sensor Number', 'Duration', 'Plot', 'Data Collection', 'Data Available'], axis=1)
        # table[2] should be station comments
        tables[2] = tables[2].set_axis(['Date', 'Comment'], axis=1)
        return tables

    def read_station_data(self, station_id, sensor_number, duration_code, start, end):
        data_url = f'{self.cdec_base_url}/dynamicapp/req/CSVDataServletPST?Stations={station_id}&SensorNums={sensor_number}&dur_code={duration_code}&Start={start}&End={end}'
        type_map = {'STATION_ID': 'category', 'DURATION': 'category', 'SENSOR_NUMBER': 'category', 'SENSOR_TYPE': 'category',
               'VALUE': 'float', 'DATA_FLAG': 'category', 'UNITS': 'category'}
        return pd.read_csv(data_url, dtype=type_map, na_values=['---', 'ART', 'BRT'], parse_dates=True, index_col='DATE TIME')
#
