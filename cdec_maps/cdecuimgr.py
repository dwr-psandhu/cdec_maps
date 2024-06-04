# %%
# organize imports by category
from datetime import datetime, timedelta
from functools import lru_cache
import warnings

warnings.filterwarnings("ignore")
#
import pandas as pd
import geopandas as gpd
import holoviews as hv
import cartopy.crs as ccrs

hv.extension("bokeh")
# viz and ui
import param
import panel as pn

pn.extension()
#
from vtools.functions.filter import cosine_lanczos

from pydelmod.dataui import DataUI, full_stack
from pydelmod.tsdataui import TimeSeriesDataUIManager
from . import cdec


class CDECDataUIManager(TimeSeriesDataUIManager):

    sensor_selections = param.ListSelector(
        objects=["RIV STG"],
        default=["RIV STG"],
        doc="Select one or more sensors to display",
    )

    def __init__(self, dfcat, reader, **kwargs):
        """
        geolocations is a geodataframe with station_id, and geometry columns
        This is merged with the data catalog to get the station locations.
        """
        self.time_range = kwargs.pop("time_range", None)
        self.reader = reader
        self.station_id_column = kwargs.pop(
            "station_id_column", "ID"
        )  # The column in the data catalog that contains the station id
        self.dfcat = dfcat
        all_sensor_types = self.dfcat["Sensor"].unique()
        self.param.sensor_selections.objects = all_sensor_types
        self.sensor_selections = list(all_sensor_types)
        super().__init__(filename_column="Source", **kwargs)

    def get_widgets(self):
        control_widgets = super().get_widgets()
        control_widgets.append(pn.WidgetBox(self.param.sensor_selections))
        return control_widgets

    # data related methods
    def get_data_catalog(self):
        return self.dfcat

    def build_station_name(self, r):
        return r["ID"]

    def _get_station_ids(self, df):
        return list((df.apply(self.build_station_name, axis=1).astype(str).unique()))

    def get_time_range(self, dfcat):
        """
        Calculate time range from the data catalog
        """
        if self.time_range is None:  # last 30 days is default
            self.time_range = (
                datetime.now() - timedelta(days=30),
                datetime.now(),
            )
        return self.time_range

    def _get_table_column_width_map(self):
        """only columns to be displayed in the table should be included in the map"""
        column_width_map = {
            "ID": "5%",
            "Station": "20%",
            "County": "10%",
            "River Basin": "10%",
            "Start Date": "10%",
            "End Date": "10%",
            "Sensor": "10%",
            "Sensor Number": "5%",
            "Duration": "5%",
            "Units": "5%",
            "Description": "10%",
        }
        return column_width_map

    def get_table_filters(self):
        table_filters = {
            "ID": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "Station": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "County": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "River Basin": {
                "type": "input",
                "func": "like",
                "placeholder": "Enter match",
            },
            "Sensor": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "Sensor Number": {
                "type": "input",
                "func": "like",
                "placeholder": "Enter match",
            },
            "Units": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "Duration": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "Description": {
                "type": "input",
                "func": "like",
                "placeholder": "Enter match",
            },
        }
        return table_filters

    def _append_value(self, new_value, value):
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    def _append_to_title_map(self, title_map, unit, r):
        if unit in title_map:
            value = title_map[unit]
        else:
            value = ["", "", "", ""]
        value[0] = self._append_value(r["Sensor"], value[0])
        value[1] = self._append_value(r["ID"], value[1])
        value[2] = self._append_value(r["Duration"], value[2])
        value[3] = self._append_value(r["Description"], value[3])
        title_map[unit] = value

    def _create_title(self, v):
        title = f"{v[1]} @ {v[2]} ({v[3]}::{v[0]})"
        return title

    def _create_crv(self, df, r, unit, file_index=None):
        file_index_label = f"{file_index}:" if file_index is not None else ""
        crvlabel = f'{file_index_label}{r["ID"]}/{r["Sensor"]}'
        ylabel = f'{r["Sensor"]} ({unit})'
        title = f'{r["Sensor"]} @ {r["ID"]} ({r["Duration"]}/{r["Description"]})'
        irreg = False  # TODO: set this based on some metadata, but for now all is regular time series
        if irreg:
            crv = hv.Scatter(df.iloc[:, [0]], label=crvlabel).redim(value=crvlabel)
        else:
            crv = hv.Curve(df.iloc[:, [0]], label=crvlabel).redim(value=crvlabel)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            title=title,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    def _get_data_for_time_range(self, row, time_range):
        irreg = False  # TODO: set this based on some metadata, but for now all is regular time series
        unit = row["Units"]
        station_id = row["ID"]
        sensor_desc = row["Sensor"]
        sensor_number = row["Sensor Number"]
        duration_code = cdec.get_duration_code(row["Duration"])
        start = time_range[0].strftime("%Y-%m-%d")
        end = time_range[1].strftime("%Y-%m-%d")
        df = self.reader.read_station_data(
            station_id, sensor_number, duration_code, start, end
        )
        df = df[["VALUE"]]
        df.columns = [f"{station_id}/{sensor_desc}"]
        df = df[slice(df.first_valid_index(), df.last_valid_index())]
        ptype = "instantaneous" if duration_code == "E" else "period-averaged"
        # infer freq type if tidal filtering is needed
        if self.do_tidal_filter:
            # if duration code is given use that else infer from the data
            if duration_code == "E":
                freq_str = pd.infer_freq(df.index)
                if freq_str:
                    df = df.resample(freq_str).mean()
                else:
                    raise ValueError(
                        f"Cannot infer frequency for: {station_id}/{sensor_desc}"
                    )
            else:
                df = df.resample(duration_code).mean()
        return df, unit, ptype

    # methods below if geolocation data is available
    def get_tooltips(self):
        return [
            ("Station ID", "@ID"),
            ("Station", "@Station"),
            ("Sensor", "@Description"),
            ("Units", "@Units"),
            ("Duration", "@Duration"),
        ]

    def get_map_color_columns(self):
        """return the columns that can be used to color the map"""
        return ["Sensor", "Duration", "Units"]

    def get_map_marker_columns(self):
        """return the columns that can be used to color the map"""
        return ["Sensor", "Duration", "Units"]

    def get_name_to_color(self):
        """return a dictionary mapping column names to color names"""
        return hv.Cycle("Category10").values

    def get_name_to_marker(self):
        """return a dictionary mapping column names to marker names"""
        from bokeh.core.enums import MarkerType

        return list(MarkerType)


def show_cdec_ui():
    """
    Show CDEC data UI
    """
    import pandas as pd
    import geopandas as gpd
    from cartopy import crs as ccrs
    from cdec_maps import cdec

    reader = cdec.Reader()
    stations, sensor_list, stations_meta_info = reader.read_saved_stations_info()
    # remove stations outside of a lat lon box
    stations_meta_info = stations_meta_info[
        (stations_meta_info["Latitude"] >= 30)
        & (stations_meta_info["Latitude"] <= 45)
        & (stations_meta_info["Longitude"] >= -125)
        & (stations_meta_info["Longitude"] <= -110)
    ]
    #
    displayed_sensor_list = sensor_list[
        "Sensor"
    ].unique()  # ["RIV STG", "TEMP", "FLOW"]
    stations_meta_info = stations_meta_info[
        stations_meta_info["Sensor"].isin(displayed_sensor_list)
    ]
    # Assuming stations_meta_info is your dataframe
    stations_meta_info[["Start Date", "End Date"]] = stations_meta_info[
        "Data Available"
    ].str.split(" to ", expand=True)
    # Replace 'present' with current date and convert 'End Date' to datetime
    stations_meta_info["End Date"] = stations_meta_info["End Date"].replace(
        "present", pd.to_datetime("today").strftime("%m/%d/%Y")
    )
    # Convert 'Start Date' to datetime
    stations_meta_info["Start Date"] = pd.to_datetime(
        stations_meta_info["Start Date"], errors="coerce"
    )
    stations_meta_info["End Date"] = pd.to_datetime(
        stations_meta_info["End Date"], errors="coerce"
    )
    stations_meta_info["Start Date"].min(), stations_meta_info["End Date"].max()
    # add source column
    stations_meta_info["Source"] = "CDEC"
    if all(column in stations.columns for column in ["Latitude", "Longitude"]):
        geodf = gpd.GeoDataFrame(
            stations_meta_info,
            geometry=gpd.points_from_xy(
                stations_meta_info["Latitude"],
                stations_meta_info["Longitude"],
                crs="EPSG:4326",
            ),
        )
    crs_cartopy = ccrs.PlateCarree()
    time_range = (
        datetime.now() - timedelta(days=30),
        datetime.now(),
    )
    uimgr = CDECDataUIManager(geodf, reader, time_range=time_range)
    ui = DataUI(uimgr, crs=crs_cartopy, station_id_column="ID")
    return ui.create_view().servable()
