from . import cdec, maps
import pandas as pd
from cdec_maps import cdec, maps
import dask
from holoviews import opts
import holoviews as hv
import hvplot.pandas
import param
import panel as pn
pn.extension()


class CDECPlotterAllSingleStation(param.Parameterized):
    """Plots all data for single selected CDEC station

     * Displays all the sensors in a single data frame
     * Displayes all the curves for each sensor in stacked plot
    """
    selected = param.List(
        default=[0], doc='Selected node indices to display in plot')
    date_range = param.DateRange(default=(
        pd.Timestamp.now()-pd.Timedelta(500, 'D'), pd.Timestamp.now().ceil('1D')),
                                bounds=(pd.Timestamp('2000-01-01'),pd.Timestamp.now().ceil('1D')))

    def __init__(self, stations, stations_meta, **kwargs):
        super().__init__(**kwargs)
        self.stations = stations
        self.stations_meta = stations_meta
        self.points_map = self.stations.hvplot.points(geo=True, tiles='CartoLight',  # c='WELL_TYPE',
                                                      frame_height=400, frame_width=300,
                                                      fill_alpha=0.9, line_alpha=0.4,
                                                      hover_cols=['index', 'ID', 'Station'])
        self.points_map = self.points_map.opts(opts.Points(tools=['tap', 'hover'], size=5,
                                                           nonselection_color='red', nonselection_alpha=0.3,
                                                           active_tools=['wheel_zoom']))
        self.map_pane = pn.Row(self.points_map)
        # create a selection and add it to a dynamic map calling back show_ts
        self.select_stream = hv.streams.Selection1D(
            source=self.points_map, index=[0])
        self.select_stream.add_subscriber(self.set_selected)
        self.reader = cdec.Reader()
        self.meta_pane = pn.Row()
        self.ts_pane = pn.Row()


    def set_selected(self, index):
        if index is None or len(index) == 0:
            pass  # keep the previous selections
        else:
            self.selected = index

    @dask.delayed
    def get_sensor_data(self, stn_id, sensor_number, duration_code, start, end):
        return self.reader.read_station_data(stn_id, sensor_number, duration_code, start, end)

    def get_all_sensor_data(self, stn_id):
        return [self.get_sensor_data(stn_id,
                                     meta_row['Sensor Number'],
                                     cdec.get_duration_code(
                                         meta_row['Duration']),
                                     start=self.date_range[0].strftime(
                                         '%Y-%m-%d'),
                                     end=self.date_range[1].strftime('%Y-%m-%d'))
                for id, meta_row in self.stations_meta.loc[stn_id].iterrows()]

    def _get_selected_data_row(self):
        index = self.selected
        if index is None or len(index) == 0:
            index = self.selected
        # Use only the first index in the array
        first_index = index[0]
        return self.stations.iloc[first_index, :]

    def get_selected_data(self):
        with pn.param.set_values(self.map_pane, loading=True):
            dfselected = self._get_selected_data_row()
            stn_id = dfselected['ID']
            stn_name = dfselected['Station']
            data_array = dask.compute(*self.get_all_sensor_data(stn_id))
            return data_array, stn_id, stn_name

    @param.depends('selected')
    def show_meta(self):
        dfselected = self._get_selected_data_row()
        self.data_frame = pn.widgets.DataFrame(
            self.stations_meta.loc[dfselected['ID'], :].drop(columns='geometry'))
        return self.data_frame

    @param.depends('selected', 'date_range')
    def show_ts(self):
        data_array, stn_id, stn_name = self.get_selected_data()
        # get data for each row and make a curve
        crv_list = []
        for index, (id, meta_row) in enumerate(self.stations_meta.loc[stn_id].iterrows()):
            data = data_array[index]
            crv_list.append(hv.Curve(data.loc[:, 'VALUE']).redim(VALUE=meta_row['Plot'])
                            .opts(title=f'Sensor: Description: {meta_row["Sensor Description"]} {meta_row["Duration"]}'))
        layout = hv.Layout(crv_list).cols(1).opts(opts.Curve(width=900))
        return layout.opts(title=f'{stn_id}: {stn_name}')

    def get_panel(self):
        #
        slider = pn.Param(self.param.date_range, widgets={'date_range': pn.widgets.DateRangeSlider})
        self.meta_pane = pn.Row(self.show_meta)
        self.ts_pane = pn.Row(self.show_ts)
        return pn.Column(pn.Row(self.map_pane, slider), self.meta_pane, self.ts_pane)

