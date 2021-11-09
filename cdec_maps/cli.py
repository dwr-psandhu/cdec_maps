from argparse import ArgumentParser
from cdec_maps import __version__, panels, cdec, maps
import panel as pn
pn.extension()

def show_all_sensors(args):
    reader = cdec.Reader()
    stations = reader.read_all_stations()
    delta_stations = maps.station_within_delta(maps.convert_to_gpd(stations))
    stations_meta_info=reader.read_all_stations_meta_info()
    delta_station_meta_info = stations_meta_info.join(delta_stations.set_index('ID'), how='inner')
    plotter = panels.CDECPlotterAllSingleStation(delta_stations, delta_station_meta_info)
    pn.serve(plotter.get_panel())

def cli(args=None):
    p = ArgumentParser(
        description="CDEC Maps and Dashboards",
        conflict_handler='resolve'
    )
    p.set_defaults(func=lambda args: p.print_help())
    p.add_argument(
        '-V', '--version',
        action='version',
        help='Show the conda-prefix-replacement version number and exit.',
        version="cdec_maps %s" % __version__,
    )
    sub_p = p.add_subparsers(help='sub-command help')
    # add show all sensors command
    show_all_sensor_map = sub_p.add_parser('show-all-sensors', help='show all sensor data for selected station from map')
    show_all_sensor_map.add_argument('--polygon-file', type=str, required=False, help='path to geojson file containing polygon to limit stations displayed')
    show_all_sensor_map.set_defaults(func=show_all_sensors)

    # Now call the appropriate response.
    pargs = p.parse_args(args)
    pargs.func(pargs)
    return 

if __name__ == '__main__':
    import sys
    cli(sys.argv[1:])
