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
from .map_selection_display import MapSelectionViewer

class CDECPlotterAllSingleStation(MapSelectionViewer):
    """Plots all data for single selected CDEC station

     * Displays all the sensors in a single data frame
     * Displayes all the curves for each sensor in stacked plot
    """
    date_range = param.DateRange(default=(
        pd.Timestamp.now()-pd.Timedelta(500, 'D'), pd.Timestamp.now().ceil('1D')),
                                bounds=(pd.Timestamp('2000-01-01'),pd.Timestamp.now().ceil('1D')))

    def __init__(self, stations, stations_meta, **kwargs):
        super().__init__(stations, **kwargs)
        self.stations_meta = stations_meta
        self.reader = cdec.Reader()
        self.meta_pane = pn.Row()
        self.ts_pane = pn.Row()

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

    def get_selected_data(self):
        with pn.param.set_values(self.map_pane, loading=True):
            dfselected = self._get_selected_data_row()
            stn_id = dfselected['ID']
            stn_name = dfselected['Station']
            data_array = dask.compute(*self.get_all_sensor_data(stn_id))
            return data_array, stn_id, stn_name

    @param.depends('selected')
    def show_meta(self):
        selected_stations = self.stations.iloc[self.selected,:]
        dfselected = self.stations_meta.join(selected_stations[['ID']].set_index('ID'),on='ID',how='inner')
        self.data_frame = pn.widgets.DataFrame(dfselected)
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
        return pn.Column(pn.Row(self.points_map, slider), self.meta_pane, self.ts_pane)

