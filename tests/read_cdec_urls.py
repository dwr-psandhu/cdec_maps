#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[3]:


from cdec_maps import cdec


# In[4]:


c=cdec.CDEC()


# In[5]:


c.read_daily_stations()


# In[6]:


c.read_realtime_stations()


# In[8]:


c.read_sensor_list()


# In[47]:


tlist = c.read_station_meta_info('FPT')


# In[48]:


tlist[0]


# In[50]:


tlist[1]


# In[52]:


tlist[2]


# In[53]:


c.read_station_data('FPT',20,'D','2021-10-01','2021-10-21')


# In[ ]:




