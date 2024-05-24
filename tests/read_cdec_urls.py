# %%
from cdec_maps import cdec
import hvplot.pandas

# %%

reader = cdec.Reader()
# %%
daily_stations = reader.read_daily_stations()
daily_stations.sample()
# %%
realtime_stations = reader.read_realtime_stations()
realtime_stations.sample()
# %%
sensor_list = reader.read_sensor_list()
# %%
tlist = reader.read_station_meta_info("FPT")
len(tlist)
# %%
data = reader.read_station_data("FPT", 20, "D", "2021-09-01", "2022-10-21")

# %%
data
# %%
data.loc[:, ["VALUE"]].hvplot()
# %%
