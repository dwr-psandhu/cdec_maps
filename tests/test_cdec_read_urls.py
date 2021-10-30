
from cdec_maps import cdec

def test_read_all():
    c=cdec.CDEC()
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




