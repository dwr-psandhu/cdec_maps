
from cdec_maps import cdec, cdec_cache

def test_read_all():
    c=cdec.Reader()
    df = c.read_daily_stations()

    assert not df.empty
    assert len(df) > 10

    df = c.read_realtime_stations()
    assert not df.empty
    assert len(df) > 10

    df=c.read_sensor_list()
    assert not df.empty
    assert len(df) > 10

    tlist = c.read_station_meta_info('FPT')
    df = tlist[0]
    assert not df.empty
    assert len(df) > 5

    df = tlist[1]
    assert not df.empty
    assert len(df) > 10
    df = tlist[2]
    assert not df.empty
    assert len(df) > 5

    df = c.read_station_data('FPT',20,'D','2021-10-01','2021-10-21')
    assert not df.empty
    assert len(df) > 10

def test_lis(): # test for stations with 4 tables in station meta info, including a datum table
    c=cdec.Reader()
    dflist = c.read_station_meta_info('LIS')
    assert len(dflist) == 4

def test_mck(): # test for MCK failed as no comments table but has a datum table
    c = cdec.Reader()
    dflist = c.read_station_meta_info('MCK')
    assert len(dflist) == 4

def test_change_cache_dir():
    c=cdec.Reader(cache_dir='cdec_cache2')
    df = c.read_daily_stations()
    assert not df.empty
    assert len(df) > 10

def test_undefined_station():
    c=cdec.Reader()
    dflist = c.read_station_meta_info('BAW')
    assert len(dflist) == 4
    assert dflist[0].empty

def test_station_no_sensors():
    c=cdec.Reader()
    dflist = c.read_station_meta_info('NSW')
    assert len(dflist) == 4
    assert dflist[1].empty

def test_station():
    c=cdec.Reader()
    dflist = c.read_station_meta_info('LSV')