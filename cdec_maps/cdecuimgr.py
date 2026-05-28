# %%
# organize imports by category
from datetime import datetime, timedelta
from functools import lru_cache
import logging
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

#
logger = logging.getLogger(__name__)
#
from dvue.catalog import DataReferenceReader, DataReference, DataCatalog
from dvue.dataui import DataUI, full_stack
from dvue.tsdataui import TimeSeriesDataUIManager, TimeSeriesPlotAction
from dvue.dataui import DWR_DISCLAIMER_TEXT
from . import cdec

__all__ = ["CDECDataReferenceReader", "CDECDataReference", "CDECDataUIManager", "show_cdec_ui"]


class CDECDataReferenceReader(DataReferenceReader):
    """Reads a single CDEC time series given station/sensor/duration attributes.

    A single instance is shared across all DataReference objects in the catalog
    (flyweight pattern).  When ``time_range`` is present in the attributes
    passed by DataReference.getData(), only that window is requested from CDEC.

    Standalone: ``reader`` is optional.  When omitted a default
    :class:`cdec.Reader` is created lazily on the first :meth:`load` call so
    that the reader — and any :class:`CDECDataReference` wired to it — can be
    used outside a :class:`CDECDataUIManager` context (e.g. in a mixed
    catalog).

    Parameters
    ----------
    source_or_reader : str or cdec.Reader or None
        When a :class:`cdec.Reader` instance is passed, it is used directly.
        When a string is passed (registry construction via
        :class:`~dvue.registry.ReaderRegistry`), it is treated as a source
        path and ignored — a default :class:`cdec.Reader` is created lazily
        on the first :meth:`load` call.  When *None*, the default reader is
        also created lazily.
    """

    def __init__(self, source_or_reader=None) -> None:
        # Support both:
        # - CDECDataReferenceReader(cdec_reader_obj)  — direct construction
        # - CDECDataReferenceReader("source/path")    — registry construction (source ignored)
        if isinstance(source_or_reader, str):
            self._reader = None  # Registry construction; create default reader lazily
        else:
            self._reader = source_or_reader  # cdec.Reader instance or None

    def _get_reader(self) -> "cdec.Reader":
        """Return the reader, creating a default instance if none was supplied."""
        if self._reader is None:
            self._reader = cdec.Reader()
        return self._reader

    def load(self, **attributes) -> pd.DataFrame:
        station_id = attributes["ID"]
        sensor_desc = attributes["Sensor"]
        sensor_number = attributes["Sensor Number"]
        duration_code = attributes["duration_code"]
        unit = attributes["Units"]
        time_range = attributes.get("time_range")
        if time_range is not None:
            start = pd.Timestamp(time_range[0]).strftime("%Y-%m-%d")
            end = pd.Timestamp(time_range[1]).strftime("%Y-%m-%d")
        else:
            start = "1900-01-01"
            end = pd.Timestamp.now().strftime("%Y-%m-%d")
        df = self._get_reader().read_station_data(
            station_id, sensor_number, duration_code, start, end
        )
        df = df[["VALUE"]]
        df.columns = [f"{station_id}/{sensor_desc}"]
        df = df[slice(df.first_valid_index(), df.last_valid_index())]
        ptype = "instantaneous" if duration_code == "E" else "period-averaged"
        df.attrs["unit"] = unit
        df.attrs["ptype"] = ptype
        return df

    def __repr__(self) -> str:
        return f"CDECDataReferenceReader(reader={self._reader!r})"

    @classmethod
    def scan(cls, path: str) -> list:
        """Scan the CDEC station catalog and return :class:`CDECDataReference` objects.

        Called by :class:`~dvue.registry.ReaderRegistry` when a ``cdec:``
        source spec is passed to ``dvue ui``.  Examples::

            dvue ui cdec:                              # default URL
            dvue ui cdec:https://cdec.water.ca.gov    # explicit URL

        Parameters
        ----------
        path : str
            CDEC base URL, e.g. ``"https://cdec.water.ca.gov"``.  An empty
            string selects the default URL.

        Cache-first behaviour
        ---------------------
        Uses local CSV files written by :meth:`cdec.Reader.save_all_stations_info`
        (``cdec_db/`` in the current directory).  Falls back to a live network
        fetch if the local files are not found — that fetch can take several
        minutes.
        """
        import geopandas as gpd
        from . import cdec as cdec_module

        base_url = (path or "").strip() or "https://cdec.water.ca.gov"
        cdec_reader = cdec_module.Reader(cdec_base_url=base_url)
        shared_dvue_reader = cls(cdec_reader)

        # ------------------------------------------------------------------
        # Load catalog — prefer local cache, fall back to live CDEC fetch
        # ------------------------------------------------------------------
        stations_meta_info = None
        try:
            _, _, stations_meta_info = cdec_reader.read_saved_stations_info()
            logger.info(
                "CDECDataReferenceReader.scan: loaded %d rows from local cache",
                len(stations_meta_info),
            )
        except Exception as exc:
            logger.warning(
                "CDECDataReferenceReader.scan: local cache not found (%s). "
                "Fetching full station catalog from CDEC — this may take several minutes.",
                exc,
            )
            stations_meta_info = cdec_reader.read_all_stations_meta_info()
            logger.info(
                "CDECDataReferenceReader.scan: fetched %d rows from CDEC",
                len(stations_meta_info),
            )

        stations_meta_info = stations_meta_info.copy()

        # ------------------------------------------------------------------
        # Bounding box filter (California / western US)
        # ------------------------------------------------------------------
        if "Latitude" in stations_meta_info.columns and "Longitude" in stations_meta_info.columns:
            stations_meta_info = stations_meta_info[
                (stations_meta_info["Latitude"] >= 30)
                & (stations_meta_info["Latitude"] <= 45)
                & (stations_meta_info["Longitude"] >= -125)
                & (stations_meta_info["Longitude"] <= -110)
            ].copy()

        # ------------------------------------------------------------------
        # Normalize types
        # ------------------------------------------------------------------
        if "Sensor Number" in stations_meta_info.columns:
            stations_meta_info["Sensor Number"] = stations_meta_info["Sensor Number"].astype(str)

        # ------------------------------------------------------------------
        # Parse time extents from "Data Available" (e.g. "01/01/2000 to present")
        # ------------------------------------------------------------------
        if "Data Available" in stations_meta_info.columns:
            avail = stations_meta_info["Data Available"].str.split(" to ", expand=True)
            if avail.shape[1] >= 2:
                stations_meta_info["_extent_start"] = pd.to_datetime(
                    avail[0].str.strip(), errors="coerce"
                )
                end_raw = avail[1].str.strip().replace(
                    "present", pd.Timestamp.now().strftime("%m/%d/%Y")
                )
                stations_meta_info["_extent_end"] = pd.to_datetime(end_raw, errors="coerce")

        # ------------------------------------------------------------------
        # Attach geometry from Latitude / Longitude
        # ------------------------------------------------------------------
        if "Latitude" in stations_meta_info.columns and "Longitude" in stations_meta_info.columns:
            stations_meta_info = gpd.GeoDataFrame(
                stations_meta_info,
                geometry=gpd.points_from_xy(
                    stations_meta_info["Longitude"],
                    stations_meta_info["Latitude"],
                    crs="EPSG:4326",
                ),
            )

        # ------------------------------------------------------------------
        # Deduplicate and build DataReference objects
        # ------------------------------------------------------------------
        dedup = stations_meta_info.drop_duplicates(subset=["ID", "Sensor Number", "Duration"])
        refs = []
        skipped = 0
        for _, row in dedup.iterrows():
            try:
                ref = CDECDataReference.from_catalog_row(row, reader=shared_dvue_reader)
                # Set source so RegistryUIManager can track which endpoint this came from
                ref.source = base_url
                ref._attributes["source"] = base_url
                # Pre-normalise to station/variable for RegistryUIManager display
                ref.set_attribute("station", str(row.get("ID", "")))
                ref.set_attribute("variable", str(row.get("Sensor", "")))
                # Time extents for RegistryUIManager.on_file_added time_range expansion
                if "_extent_start" in row.index and pd.notna(row["_extent_start"]):
                    ref.set_attribute("time_extent_start", str(row["_extent_start"]))
                if "_extent_end" in row.index and pd.notna(row["_extent_end"]):
                    ref.set_attribute("time_extent_end", str(row["_extent_end"]))
                refs.append(ref)
            except Exception as exc:
                skipped += 1
                if skipped <= 5:
                    logger.debug("CDECDataReferenceReader.scan: skipped row — %s", exc)
        if skipped:
            logger.warning(
                "CDECDataReferenceReader.scan: skipped %d rows (first 5 logged at DEBUG)",
                skipped,
            )
        logger.info("CDECDataReferenceReader.scan: returning %d refs", len(refs))
        return refs


class CDECDataReference(DataReference):
    """CDEC-specific :class:`~dvue.catalog.DataReference`.

    References are standalone: each instance carries enough attributes
    (``ID``, ``Sensor Number``, ``duration_code``, ``Units``, …) for
    :class:`CDECDataReferenceReader` to fetch data without any external
    manager context.  This allows CDEC references to participate in
    mixed-catalog workflows alongside references from other sources.

    Use :meth:`from_catalog_row` to build instances from a GeoDataFrame row.
    """

    _REQUIRED = ("ID", "Sensor Number", "duration_code")

    def __init__(self, reader=None, name: str = "", cache: bool = True, **attributes):
        missing = [k for k in self._REQUIRED if not attributes.get(k)]
        if missing:
            raise ValueError(
                f"CDECDataReference requires attributes: {missing}"
            )
        if reader is None:
            reader = CDECDataReferenceReader()
        super().__init__(reader=reader, name=name, cache=cache, **attributes)

    @classmethod
    def from_catalog_row(cls, row, reader=None):
        """Build a standalone :class:`CDECDataReference` from a catalog row.

        Parameters
        ----------
        row : pd.Series
            A row from the station metadata GeoDataFrame.  Must contain at
            minimum ``ID``, ``Sensor Number``, ``Duration``, and ``Units``.
        reader : CDECDataReferenceReader, optional
            Shared flyweight reader.  A new default reader is created when
            omitted.
        """
        duration_code = cdec.get_duration_code(row["Duration"])
        name = f"{row['ID']}/{row['Sensor Number']}/{duration_code}"
        attrs = {k: v for k, v in row.items() if k != "geometry"}
        if "geometry" in row.index and row["geometry"] is not None:
            attrs["geometry"] = row["geometry"]
        attrs["duration_code"] = duration_code
        return cls(reader=reader, name=name, cache=True, **attrs)

    # ------------------------------------------------------------------
    # Typed attribute accessors
    # ------------------------------------------------------------------

    @property
    def station_id(self) -> str:
        return self.get_attribute("ID")

    @property
    def sensor_number(self) -> str:
        return self.get_attribute("Sensor Number")

    @property
    def duration_code(self) -> str:
        return self.get_attribute("duration_code")

    @property
    def unit(self) -> str:
        return self.get_attribute("Units")

    @property
    def sensor(self) -> str:
        return self.get_attribute("Sensor")

    @property
    def geometry(self):
        return self.get_attribute("geometry")


class CDECTimeSeriesPlotAction(TimeSeriesPlotAction):
    """CDEC-specific plot action: builds curve labels and titles from CDEC metadata."""

    def _append_value(self, new_value, value):
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    def append_to_title_map(self, title_map, unit, r):
        value = title_map.get(unit, ["", "", "", ""])
        value[0] = self._append_value(r["Sensor"], value[0])
        value[1] = self._append_value(r["ID"], value[1])
        value[2] = self._append_value(r["Duration"], value[2])
        value[3] = self._append_value(r["Description"], value[3])
        title_map[unit] = value

    def create_title(self, v):
        return f"{v[1]} @ {v[2]} ({v[3]}::{v[0]})"


class CDECDataUIManager(TimeSeriesDataUIManager):
    show_math_ref_editor = param.Boolean(default=True)
    show_clear_cache = param.Boolean(default=False)
    show_reset_session_button = param.Boolean(default=True)
    session_cookie_name = param.String(default="cdec_user_id")
    disclaimer_text = DWR_DISCLAIMER_TEXT
    sensor_selections = param.ListSelector(
        objects=["RIV STG"],
        default=["RIV STG"],
        doc="Select one or more sensors to display",
    )

    bypass_cache = param.Boolean(
        default=False,
        doc="Bypass cache for reading data. Still builds cache but rebuilds from CDEC.",
    )

    do_tidal_filter = param.Boolean(
        default=False,
        doc="Apply cosine-Lanczos tidal filter to data before plotting.",
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
        # Build the dvue catalog
        self._dvue_reader = CDECDataReferenceReader(reader)
        geo_crs = (
            str(self.dfcat.crs)
            if hasattr(self.dfcat, "crs") and self.dfcat.crs is not None
            else None
        )
        self._dvue_catalog = self._build_dvue_catalog(geo_crs)
        super().__init__(**kwargs)
        self.color_cycle_column = "ID"
        self.dashed_line_cycle_column = "Duration"
        self.marker_cycle_column = "Sensor"

    def get_widgets(self):
        from tqdm.dask import TqdmCallback
        import tqdm

        cb = TqdmCallback(desc="cdec", tqdm_class=tqdm.tqdm)
        cb.register()
        control_widgets = super().get_widgets()
        # control_widgets.append(pn.WidgetBox())
        # control_widgets.append(pn.Row(self.param.bypass_cache))
        return control_widgets

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def _build_dvue_catalog(self, crs=None) -> DataCatalog:
        import time
        t0 = time.perf_counter()
        # Deduplicate by the catalog primary key before building.
        # The raw dfcat can have duplicate (ID, Sensor Number, Duration) rows
        # (e.g. from multiple download passes) which would cause catalog.add()
        # to raise ValueError and leave the catalog empty.
        dedup = self.dfcat.drop_duplicates(subset=["ID", "Sensor Number", "Duration"])
        n_raw, n_dedup = len(self.dfcat), len(dedup)
        if n_raw != n_dedup:
            logger.warning(
                "_build_dvue_catalog: dropped %d duplicate rows (%d → %d)",
                n_raw - n_dedup, n_raw, n_dedup,
            )
        logger.info("_build_dvue_catalog: adding %d refs to catalog …", n_dedup)
        catalog = DataCatalog(
            primary_key=["ID", "Sensor Number", "duration_code"],
            crs=crs,
        )
        skipped = 0
        for _, row in dedup.iterrows():
            try:
                ref = CDECDataReference.from_catalog_row(row, reader=self._dvue_reader)
                # Bypass catalog.add()'s O(n) pk-collision loop by inserting
                # directly into the internal dict — bulk-load optimisation.
                # Duplicates are already removed by the drop_duplicates() call
                # above, so the collision check in add() is redundant here.
                # CDEC refs always have an explicit name from from_catalog_row,
                # and source="" so _source_index stays empty.
                if ref.name:
                    catalog._references[ref.name] = ref
                else:
                    logger.warning("_build_dvue_catalog: ref has no name, skipping row")
                    skipped += 1
            except Exception as exc:
                logger.warning("_build_dvue_catalog: skipping ref — %s", exc)
                skipped += 1
        logger.info(
            "_build_dvue_catalog: catalog ready — %d refs, %d skipped  (%.1fs)",
            len(catalog._references), skipped, time.perf_counter() - t0,
        )
        return catalog

    # data related methods
    def get_data_reference(self, row):
        if "name" in row.index:
            ref = self._dvue_catalog.get(row["name"])
        else:
            # Fallback for rows without a name column (e.g. legacy callers)
            duration_code = cdec.get_duration_code(row["Duration"])
            ref_name = f"{row['ID']}/{row['Sensor Number']}/{duration_code}"
            ref = self._dvue_catalog.get(ref_name)
        if self.bypass_cache:
            self.reader.remove_from_db(
                ref.station_id, ref.sensor_number, ref.duration_code
            )
            ref.invalidate_cache()
        return ref

    def get_data(self, df, time_range=None):
        """Yield data for each row, applying tidal filter when requested."""
        row_series = [row for _, row in df.iterrows()]
        for data, row in zip(super().get_data(df, time_range=time_range), row_series):
            if self.do_tidal_filter and data is not None and not data.empty:
                duration_code = cdec.get_duration_code(row["Duration"])
                if duration_code == "E":
                    freq_str = pd.infer_freq(data.index)
                    data = data.resample(freq_str or "15min").mean()
                else:
                    data = data.resample(duration_code).mean()
            yield data

    def build_station_name(self, r):
        return r["ID"]

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
            "County": "8%",
            "River Basin": "7%",
            "Operator": "10%",
            "Start Date": "8%",
            "End Date": "8%",
            "Sensor": "9%",
            "Sensor Number": "5%",
            "Duration": "5%",
            "Units": "5%",
            "Description": "10%",
        }
        return column_width_map

    def get_table_filters(self):
        if False:
            table_filters = {}
        else:
            table_filters = {
                "ID": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "Station": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "County": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "River Basin": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Operator": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Sensor": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Sensor Number": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Units": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Duration": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
                "Description": {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                },
            }
        return table_filters

    def is_irregular(self, r):
        return False

    def _make_plot_action(self):
        return CDECTimeSeriesPlotAction(curve_creator=self.create_curve)

    def get_data_actions(self):
        return super().get_data_actions()

    def create_curve(self, df, r, unit, file_index=None):
        file_index_label = f"{file_index}:" if file_index is not None else ""
        crvlabel = f'{file_index_label}{r["ID"]}/{r["Sensor"]}/ {r["Duration"]}'
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

    def get_version(self):
        try:
            from cdec_maps._version import version
            return version
        except Exception:
            return "unknown"

    def get_about_text(self):
        return """
        # CDEC Data UI Manager
         
        This is a UI manager for CDEC data. It provides a user interface to interact with CDEC data.
        
        ## Features
        - Select sensors to display
        - Filter data by time range
        - View data in a table and map
        ## How to use
        1. Select sensors to display from the dropdown menu.
        2. Filter data by time range using the date picker.
        3. View data in a table and map.
        
        
        """


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
    # convert Sensor Number to string
    stations_meta_info["Sensor Number"] = stations_meta_info["Sensor Number"].astype(
        str
    )
    if all(column in stations.columns for column in ["Latitude", "Longitude"]):
        geodf = gpd.GeoDataFrame(
            stations_meta_info,
            geometry=gpd.points_from_xy(
                stations_meta_info["Longitude"],
                stations_meta_info["Latitude"],
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
    return ui.create_view(), ui
