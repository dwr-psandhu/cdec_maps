import param
import pandas as pd
from dask import dataframe as dd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import functools  # Add import for lru_cache


DURATION_MAP = {"(event)": "E", "(daily)": "D", "(monthly)": "M", "(hourly)": "H"}
DURATION_MAP_INVERTED = {DURATION_MAP[k]: k for k in DURATION_MAP.keys()}

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def ensure_dir_exists(dir):
    import os

    if not os.path.exists(dir):
        os.makedirs(dir)


def get_duration_code(duration):
    return DURATION_MAP[duration]


def to_date_format(str):
    try:
        return pd.to_datetime(str).strftime("%Y-%m-%d")
    except:
        return ""


def to_datetime(dstr):
    if dstr == "":
        return pd.Timestamp.now()
    else:
        return pd.to_datetime(dstr)


def to_year(dstr):
    return to_datetime(dstr).year


def sort_times(start, end):
    stime = to_datetime(start)
    etime = to_datetime(end)
    if stime < etime:
        return to_date_format(stime), to_date_format(etime)
    else:
        return to_date_format(etime), to_date_format(stime)


class Reader(param.Parameterized):
    cdec_base_url = param.String(
        default="http://cdec.water.ca.gov", allow_None=False, regex="http://.*"
    )
    dbase_dir = param.String(default="cdec_db", allow_None=False)

    def __init__(self, cdec_base_url="http://cdec.water.ca.gov", dbase_dir="cdec_db"):
        self.cdec_base_url = cdec_base_url
        self.dbase_dir = dbase_dir
        ensure_dir_exists(self.dbase_dir)

        # Initialize a session with connection pooling
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )

        # Configure connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Number of connection pools
            pool_maxsize=20,  # Connections per pool
        )

        # Mount the adapter to both http and https
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def __del__(self):
        """Clean up session when object is destroyed"""
        if hasattr(self, "session"):
            self.session.close()

    @functools.lru_cache(maxsize=128)
    def _read_single_table(self, url):
        """Use session for HTTP requests instead of direct calls"""
        response = self.session.get(url)
        response.raise_for_status()
        df = pd.read_html(response.text)
        return df[0]

    @functools.lru_cache(maxsize=128)
    def read_daily_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/dailyStations.html")

    @functools.lru_cache(maxsize=128)
    def read_realtime_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/realStations.html")

    @functools.lru_cache(maxsize=128)
    def read_sensor_list(self):
        return self._read_single_table(self.cdec_base_url + "/misc/senslist.html")

    @functools.lru_cache(maxsize=128)
    def read_all_stations(self):
        daily_stations = self.read_daily_stations()
        realtime_stations = self.read_realtime_stations()
        return daily_stations.merge(realtime_stations, how="outer")

    def read_all_stations_meta_info(self):
        all_stations = self.read_all_stations()
        sensor_list = self.read_sensor_list()
        meta_info_list = [
            self.read_station_meta_info(station_id)[1].assign(ID=station_id)
            for station_id in tqdm.tqdm(all_stations.ID)
        ]
        station_meta_infos = pd.concat(meta_info_list).astype(
            dtype={"Sensor Number": "int"}
        )
        station_meta_infos = station_meta_infos.merge(
            sensor_list, left_on="Sensor Number", right_on="Sensor No"
        )
        station_meta_infos = station_meta_infos.merge(all_stations, on="ID")
        return station_meta_infos

    @functools.lru_cache(maxsize=1024)
    def read_station_meta_info(self, station_id):
        try:
            url = self.cdec_base_url + "/dynamicapp/staMeta?station_id=%s" % station_id
            # Update to use the session
            response = self.session.get(url)
            response.raise_for_status()
            tables = pd.read_html(response.text, match="Sensor Description|Station ")
        except Exception as e:
            logger.error(f"Failed to read station meta info for {station_id}: {e}")
            return [
                pd.DataFrame(),
                pd.DataFrame(),
            ]
        # table[0] should be station meta info
        logger.debug(f"Read {len(tables)} tables")
        try:

            def _pair_table_columns(df, column_index):
                return (
                    df.iloc[:, column_index]
                    .set_index(column_index[0])
                    .set_axis(["Value"], axis=1)
                )

            station_info_table = pd.concat(
                [
                    _pair_table_columns(tables[0], [0, 1]),
                    _pair_table_columns(tables[0], [2, 3]),
                ]
            )
        except Exception as e:
            logger.error(f"Failed to read station meta info for {station_id}: {e}")
            return [
                pd.DataFrame(),
                pd.DataFrame(),
            ]
        try:
            sensor_table = pd.DataFrame(
                [],
                [
                    "Sensor Description",
                    "Sensor Number",
                    "Duration",
                    "Plot",
                    "Data Collection",
                    "Data Available",
                ],
            )
            sensor_table = tables[1]
            sensor_table = sensor_table.set_axis(
                [
                    "Sensor Description",
                    "Sensor Number",
                    "Duration",
                    "Plot",
                    "Data Collection",
                    "Data Available",
                ],
                axis=1,
            )
        except Exception as e:
            logger.error(f"Failed to read sensor meta info for {station_id}: {e}")
            sensor_table = pd.DataFrame()

        return [
            station_info_table,
            sensor_table,
        ]

    def read_station_data_using_dask(
        self, station_id, sensor_number, duration_code, start, end
    ):
        """
        Using dask with our connection pool
        """
        # make sure start and end are in the right order, start < order
        start, end = sort_times(start, end)
        start_year = to_year(start)
        end_year = to_year(end) + 1
        url_template = (
            self.cdec_base_url
            + "/dynamicapp/req/CSVDataServletPST?Stations={station_id}&SensorNums={sensor_number}&dur_code={duration_code}&Start=01-01-{start}&End=12-31-{end}+23:59"
        )

        # Prepare URLs
        list_urls = [
            url_template.format(
                station_id=station_id,
                sensor_number=sensor_number,
                duration_code=duration_code,
                start=syear,
                end=syear,
            )
            for syear in range(start_year, end_year)
        ]

        import io
        import dask

        # Define a function to fetch a single URL
        def fetch_url(url):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                content = response.text
                dtype_map = {
                    "STATION_ID": "category",
                    "DURATION": "category",
                    "SENSOR_NUMBER": "category",
                    "SENSOR_TYPE": "category",
                    "VALUE": "float",
                    "DATA_FLAG": "category",
                    "UNITS": "category",
                }
                return pd.read_csv(
                    io.StringIO(content),
                    dtype=dtype_map,
                    na_values={"VALUE": ["---", "ART", "BRT"]},
                )
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                return pd.DataFrame()

        # Create delayed tasks
        delayed_tasks = [dask.delayed(fetch_url)(url) for url in list_urls]

        # Compute all tasks
        try:
            results = dask.compute(*delayed_tasks)
            # Filter out empty dataframes
            results = [df for df in results if not df.empty]

            if not results:
                raise ValueError(
                    f"Failed to retrieve any data for {station_id} {sensor_number} {duration_code}"
                )

            df = pd.concat(results, ignore_index=True)

        except Exception as e:
            logger.error(
                f"Failed to read data for {station_id} {sensor_number} {duration_code}: {e}"
            )
            raise e

        # drop duplicate date times, keeping last
        df = df.drop_duplicates(subset="DATE TIME", keep="last")
        df.index = pd.to_datetime(df["DATE TIME"])
        df["OBS DATE"] = pd.to_datetime(df["OBS DATE"])
        df = df.drop(columns=["DATE TIME"])
        return df, (start_year, end_year)

    def build_db_path(self, station_id, sensor_number, duration_code):
        return f"{self.dbase_dir}/{station_id}__{sensor_number}__{duration_code}.prq"

    def remove_from_db(self, station_id, sensor_number, duration_code):
        import os

        try:
            os.remove(self.build_db_path(station_id, sensor_number, duration_code))
        except:
            pass

    def load_from_db(self, station_id, sensor_number, duration_code):
        return pd.read_parquet(
            self.build_db_path(station_id, sensor_number, duration_code)
        )

    def save_to_db(self, df, station_id, sensor_number, duration_code):
        df.to_parquet(self.build_db_path(station_id, sensor_number, duration_code))

    def read_station_data(self, station_id, sensor_number, duration_code, start, end):
        """ """
        stime = pd.to_datetime(start)
        etime = pd.to_datetime(end)
        try:
            df = self.load_from_db(station_id, sensor_number, duration_code)
        except Exception as e:
            df = None
        if df is not None:
            # get the date range from stime to begining of df and from end of df to etime
            # if there is a gap then read from CDEC
            _needs_saving = False
            if stime < df.index.min():
                df1, _ = self.read_station_data_using_dask(
                    station_id,
                    sensor_number,
                    duration_code,
                    start,
                    df.index.min(),
                )
                df = df1.combine_first(df)
                _needs_saving = True
            if etime > df.index.max():
                df2, _ = self.read_station_data_using_dask(
                    station_id,
                    sensor_number,
                    duration_code,
                    df.index.max(),
                    end,
                )
                df = df2.combine_first(df)
                _needs_saving = True
            if _needs_saving:
                self.save_to_db(df, station_id, sensor_number, duration_code)
        else:
            df, _ = self.read_station_data_using_dask(
                station_id, sensor_number, duration_code, start, end
            )
            self.save_to_db(df, station_id, sensor_number, duration_code)
        # trim the data down to start to end
        return df.loc[stime:etime]  # doesn't work for non-unique index
        # return df[(df.index >= stime) & (df.index <= etime)]

    #
    def read_entire_station_data_for(self, station_id, sensor_number, duration_code):
        dflist = self.read_station_meta_info(station_id)
        df_sensors = dflist[1]
        sensor_row = df_sensors[
            (df_sensors["Sensor Number"] == int(sensor_number))
            & (df_sensors["Duration"] == DURATION_MAP_INVERTED[duration_code])
        ].iloc[0]
        sdate, edate = tuple(
            [s.strip() for s in sensor_row["Data Available"].split("to")]
        )
        df = self._undecorated_read_station_data(
            station_id,
            sensor_number,
            duration_code,
            to_date_format(sdate),
            to_date_format(edate),
        )
        return df

    def save_all_stations_info(self):
        """Get all the stations and their metadata and save to a csv files locally"""
        daily_stations = self.read_daily_stations()
        daily_stations.to_csv(f"{self.dbase_dir}/cdec_daily_stations.csv")
        realtime_stations = self.read_realtime_stations()
        realtime_stations.to_csv(f"{self.dbase_dir}/cdec_realtime_stations.csv")
        stations = self.read_all_stations()
        stations.to_csv(f"{self.dbase_dir}/cdec_all_stations.csv")
        sensor_list = self.read_sensor_list()
        sensor_list.to_csv(f"{self.dbase_dir}/cdec_sensor_list.csv")
        stations_meta_info = self.read_all_stations_meta_info()
        stations_meta_info.to_csv(f"{self.dbase_dir}/cdec_stations_meta_info.csv")
        return stations, sensor_list, stations_meta_info

    @functools.lru_cache(maxsize=8)
    def read_saved_stations_info(self):
        """Read all the stations and their metadata from local csv files"""
        daily_stations = pd.read_csv(f"{self.dbase_dir}/cdec_daily_stations.csv")
        realtime_stations = pd.read_csv(f"{self.dbase_dir}/cdec_realtime_stations.csv")
        stations = pd.read_csv(f"{self.dbase_dir}/cdec_all_stations.csv")
        sensor_list = pd.read_csv(f"{self.dbase_dir}/cdec_sensor_list.csv")
        stations_meta_info = pd.read_csv(
            f"{self.dbase_dir}/cdec_stations_meta_info.csv"
        )
        return stations, sensor_list, stations_meta_info


################# MODULE LEVEL methods ###################################

import tqdm


@functools.lru_cache(maxsize=128)
def read_station_meta_info(stations):
    r = Reader()
    for sid in tqdm.tqdm(stations.ID):
        try:
            r.read_station_meta_info(sid)
        except:
            print(sid)


@functools.lru_cache(maxsize=1024)
def merge_sensors_with_id(stations, sid):
    r = Reader()
    try:
        _, sensors = r.read_station_meta_info(sid)
        sensors["ID"] = sid
        return sensors.merge(stations, on="ID")
    except:
        return pd.DataFrame()


@functools.lru_cache(maxsize=8)
def read_stations_with_meta():
    r = Reader()
    stations = r.read_all_stations()
    stations_with_meta = pd.concat(
        [merge_sensors_with_id(stations, sid) for sid in tqdm.tqdm(stations.ID)], axis=0
    )
    stations_with_meta = stations_with_meta.reset_index()
    stations_with_meta = stations_with_meta.astype({"Sensor Number": int})
    stations_with_meta = stations_with_meta.merge(
        r.read_sensor_list(), left_on="Sensor Number", right_on="Sensor No"
    )
    stations_with_meta = stations_with_meta.drop(columns=["Sensor No"])
    return stations_with_meta


def clear():
    """Clear all LRU caches to free memory"""
    # Clear class method caches
    Reader._read_single_table.cache_clear()
    Reader.read_daily_stations.cache_clear()
    Reader.read_realtime_stations.cache_clear()
    Reader.read_sensor_list.cache_clear()
    Reader.read_all_stations.cache_clear()
    Reader.read_station_meta_info.cache_clear()
    Reader.read_saved_stations_info.cache_clear()

    # Clear module level function caches
    read_station_meta_info.cache_clear()
    merge_sensors_with_id.cache_clear()
    read_stations_with_meta.cache_clear()

    logging.info("All LRU caches cleared")
