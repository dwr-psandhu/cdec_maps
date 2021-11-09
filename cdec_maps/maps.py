import pandas as pd
import geopandas as gpd
from . import cdec

def convert_to_gpd(stations):
    stations = gpd.GeoDataFrame(
        stations, geometry=gpd.points_from_xy(stations.Longitude, stations.Latitude))
    stations = stations.set_crs(epsg=4326)
    return stations


def station_within_delta(stations):
    import pkgutil
    import io
    delta_boundary = gpd.read_file(io.BytesIO(pkgutil.get_data(__package__,'Delta_Simplified.geojson')))
    stations_delta = stations[stations.within(delta_boundary.geometry[0])]
    return stations_delta

