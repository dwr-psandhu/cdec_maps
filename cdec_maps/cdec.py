import param
import pandas as pd
from dask import dataframe as dd
import diskcache as dc

DURATION_MAP = {"(event)": "E", "(daily)": "D", "(monthly)": "M", "(hourly)": "H"}
DURATION_MAP_INVERTED = {DURATION_MAP[k]: k for k in DURATION_MAP.keys()}


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


cache_dir = "cdec_cache"
cache = dc.Cache(cache_dir)


class Reader(param.Parameterized):
    cdec_base_url = param.String(
        default="http://cdec.water.ca.gov", allow_None=False, regex="http://.*"
    )
    dbase_dir = param.String(default="cdec_db", allow_None=False)

    def __init__(self, cdec_base_url="http://cdec.water.ca.gov", dbase_dir="cdec_db"):
        self.cdec_base_url = cdec_base_url
        self.dbase_dir = dbase_dir

    def _read_single_table(self, url):
        df = pd.read_html(url)
        return df[0]

    @cache.memoize(name="daily_stations", ignore=[0])
    def read_daily_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/dailyStations.html")

    @cache.memoize(name="realtime_stations", ignore=[0])
    def read_realtime_stations(self):
        return self._read_single_table(self.cdec_base_url + "/misc/realStations.html")

    @cache.memoize(name="sensor_list", ignore=[0])
    def read_sensor_list(self):
        return self._read_single_table(self.cdec_base_url + "/misc/senslist.html")

    @cache.memoize(name="all_stations", ignore=[0])
    def read_all_stations(self):
        daily_stations = self.read_daily_stations()
        realtime_stations = self.read_realtime_stations()
        return daily_stations.merge(realtime_stations, how="outer")

    @cache.memoize(name="all_stations_meta_info", ignore=[0])
    def read_all_stations_meta_info(self):
        all_stations = self.read_all_stations()
        meta_info_list = [
            self.read_station_meta_info(station_id)[1]
            .assign(ID=station_id)
            .set_index("ID")
            for station_id in all_stations.ID
        ]
        return pd.concat(meta_info_list).astype(dtype={"Sensor Number": "int"})

    @cache.memoize(name="station_meta_info", ignore=[0])
    def read_station_meta_info(self, station_id):
        try:
            url = self.cdec_base_url + "/dynamicapp/staMeta?station_id=%s" % station_id
            tables = pd.read_html(url)
        except:
            return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
        # table[0] should be station meta info

        def _pair_table_columns(df, column_index):
            return (
                df.iloc[:, column_index]
                .set_index(column_index[0])
                .set_axis(["Value"], axis=1)
            )

        tables[0] = pd.concat(
            [
                _pair_table_columns(tables[0], [0, 1]),
                _pair_table_columns(tables[0], [2, 3]),
            ]
        )
        if (
            len(tables) < 2 or len(tables[1].columns) < 6
        ):  # missing or comments table only
            tables.insert(
                1,
                pd.DataFrame(
                    [],
                    columns=[
                        "Sensor Description",
                        "Sensor Number",
                        "Duration",
                        "Plot",
                        "Data Collection",
                        "Data Available",
                    ],
                ),
            )
        if "Zero Datum" in tables[1].columns:  # For now remove
            datum_table = tables.pop(1)
        else:
            datum_table = pd.DataFrame(
                [],
                [
                    "Zero Datum",
                    "Adj To NGVD",
                    "Peak of Record",
                    "Monitor Stage",
                    "Flood Stage",
                    "Guidance Plots",
                ],
            )
        # table[1] should be station sensor info
        tables[1] = tables[1].set_axis(
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
        # table[2] should be station comments
        if len(tables) > 2:
            tables[2] = tables[2].set_axis(["Date", "Comment"], axis=1)
        else:
            tables.append(pd.DataFrame([], columns=["Date", "Comment"]))
        tables.append(datum_table)
        return tables

    def read_station_data_using_dask(
        self, station_id, sensor_number, duration_code, start, end
    ):
        """
        Using dask read CDEC via multiple threads which is quite fast and scales as much as CDEC services will allow
        """
        # make sure start and end are in the right order, start < order
        start, end = sort_times(start, end)
        start_year = to_year(start)
        end_year = to_year(end) + 1
        url = (
            self.cdec_base_url
            + "/dynamicapp/req/CSVDataServletPST?Stations={station_id}&SensorNums={sensor_number}&dur_code={duration_code}&Start=01-01-{start}&End=12-31-{end}+23:59"
        )
        list_urls = [
            url.format(
                station_id=station_id,
                sensor_number=sensor_number,
                duration_code=duration_code,
                start=syear,
                end=syear,
            )
            for syear in range(start_year, end_year)
        ]
        dtype_map = {
            "STATION_ID": "category",
            "DURATION": "category",
            "SENSOR_NUMBER": "category",
            "SENSOR_TYPE": "category",
            "VALUE": "float",
            "DATA_FLAG": "category",
            "UNITS": "category",
        }
        ddf = dd.read_csv(
            list_urls,
            blocksize=None,
            dtype=dtype_map,
            na_values={"VALUE": ["---", "ART", "BRT"]},
            # on_bad_lines="warn",
        )
        # parse_dates=['DATE TIME','OBS DATE'] # doesn't work so will have to read in as strings and convert later
        # dd.visualize(): shows parallel tasks which are executed below
        try:
            df = ddf.compute(scheduler="synchronous")
        except Exception as e:
            print(
                f"Failed to read data for {station_id} {sensor_number} {duration_code}"
            )
            raise e
        # drop duplicate date times, keeping last, TODO: Is this the right thing to do?
        df = df.drop_duplicates(subset="DATE TIME", keep="last")
        df.index = pd.to_datetime(df["DATE TIME"])
        df["OBS DATE"] = pd.to_datetime(df["OBS DATE"])
        df = df.drop(columns=["DATE TIME"])
        return df, (start_year, end_year)

    def build_db_path(self, station_id, sensor_number, duration_code):
        return f"{self.dbase_dir}/{station_id}__{sensor_number}__{duration_code}.prq"

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


def read_station_meta_info(stations):
    r = Reader()
    for sid in tqdm.tqdm(stations.ID):
        try:
            r.read_station_meta_info(sid)
        except:
            print(sid)


def merge_sensors_with_id(stations, sid):
    r = Reader()
    try:
        _, sensors, _, _ = r.read_station_meta_info(sid)
        sensors["ID"] = sid
        return sensors.merge(stations, on="ID")
    except:
        return pd.DataFrame()


@cache.memoize(name="stations_with_meta")
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
