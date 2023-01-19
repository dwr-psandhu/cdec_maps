# Cache CDEC stations information
from . import cdec
import diskcache as dc
import pandas as pd


def cache_all_station_meta_info():
    for sid in tqdm.tqdm(stations.ID):
        try:
            store_station_meta_in_cache(cache, sid)
        except:
            print(sid)


# In[11]:


#store_stations_in_cache(cache)


# In[12]:


#FSW failed stations.ID[262], tables are too many
#POH failed stations.ID[475]
#HOP failed
#GDV
#HIB
#MVV
#NRN
#OCS


# In[15]:


stations = load_stations_from_cache(cache)


# In[19]:


meta, sensors, comments, datum = load_station_meta_from_cache(cache, 'FPT')


# In[20]:


sensors


# In[21]:


stations[stations.ID=='FPT']


# In[24]:


sensors['ID'] = 'FPT'


# In[26]:


sensors.merge(stations, on='ID')


# In[37]:


def merge_sensors_with_id(cache, sid):
    try:
        _, sensors, _, _ = load_station_meta_from_cache(cache, sid)
        sensors['ID'] = sid
        return sensors.merge(stations, on='ID')
    except:
        return pd.DataFrame()


# In[42]:


station_with_meta = pd.concat([merge_sensors_with_id(cache, sid) for sid in stations.ID], axis=0)
stations_with_meta = station_with_meta.reset_index()


# In[43]:


cache['station_with_meta'] = station_with_meta


# In[48]:


station_with_meta = station_with_meta.astype({'Sensor Number': int})


# In[50]:


station_with_meta = station_with_meta.merge(cache['sensor_list'],left_on='Sensor Number', right_on='Sensor No')


# In[53]:


station_with_meta = station_with_meta.drop(columns=['Sensor No'])


# In[59]:


station_with_meta[station_with_meta['Sensor Description'].str.match('PRECIP')]['Sensor Number'].unique()


# In[61]:


stations_with_precip_accumulated = station_with_meta.query('`Sensor Number`==2')


# In[62]:


import hvplot.pandas


# In[64]:


stations_with_precip_accumulated


# In[66]:


import warnings
warnings.filterwarnings('ignore')


# In[68]:


stations_with_precip_accumulated.hvplot.points('Longitude','Latitude',geo=True,tiles='CartoLight',hover_cols='all')


# In[73]:


scurrent = stations_with_precip_accumulated[stations_with_precip_accumulated['Data Available'].str.contains('present')]


# In[87]:


scurrent.groupby(['ID','Duration']).count()


# In[91]:


scurrent.query('`Duration`=="(daily)"')


# In[ ]:


def load_data_from_cache(cache, sid, sensor_number, duration_code):
    return cache[f'{sid}_{sensor_number}_{duration_code}']


# In[123]:


def read_station_data_and_cache(cache, sid, sensor_number, duration_code):
    r = cdec.Reader()
    key = f'{sid}_{sensor_number}_{duration_code}'
    if key in cache:
        data = cache[key]
        sdate = data.last_valid_index().strftime('%Y-%m-%d')
        edate = pd.Timestamp.now().strftime('%Y-%m-%d')
        if sdate == edate:
            return cache[key] # return cache, (no update cache)
        else: # update extra days
            data_ext = r.read_station_data(sid, sensor_number, duration_code, sdate, edate)
            data = data.combine_first(data_ext)
    else:
        data = r.read_entire_station_data_for(sid, sensor_number, duration_code)
    cache[key] = data # update cache
    return data


# In[127]:


data = read_station_data_and_cache(cache, 'SWX', 2, 'D')


# In[136]:


precip_daily = scurrent.query('`Duration`=="(daily)"')


# In[138]:


for i, r in tqdm.tqdm(precip_daily.iloc[149:].iterrows(), total=len(precip_daily.iloc[149:])):
    read_station_data_and_cache(cache, r['ID'], r['Sensor Number'], cdec.DURATION_MAP[r['Duration']])


# In[ ]:




