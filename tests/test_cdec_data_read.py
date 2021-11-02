from cdec_maps import cdec
import pytest
import pandas as pd


def test_read_simple():
    c = cdec.Reader()
    df = c._read_station_data('FPT', '1', 'H', '2020-01-01', '2020-02-01')
    assert not df.empty
    assert len(df) == 745
    assert df['VALUE'].iloc[0] == pytest.approx(104.31)
    assert df['VALUE'].index[0] == pd.Timestamp('2020-01-01')


def test_read_simple_dask():
    c = cdec.Reader()
    df = c.read_station_data('FPT', '1', 'H', '2020-01-01', '2020-02-01')
    assert not df.empty
    assert len(df) == 745
    assert df['VALUE'].iloc[0] == pytest.approx(104.31)
    assert df['VALUE'].index[0] == pd.Timestamp('2020-01-01')


def test_start_end_order():
    c = cdec.Reader()
    df = c.read_station_data('FPT', '1', 'H', '2020-02-01', '2020-01-01')
    assert not df.empty
    assert len(df) == 745
    assert df['VALUE'].iloc[0] == pytest.approx(104.31)
    assert df['VALUE'].index[0] == pd.Timestamp('2020-01-01')


def test_read_station_data_kwargs():
    '''Test by calling positional args with keywords syntax'''
    c = cdec.Reader()
    df = c.read_station_data(station_id='FPT', duration_code='H',
                             sensor_number='1', end='2020-01-01', start='2020-02-01')
    assert not df.empty
    assert len(df) == 745
    assert df['VALUE'].iloc[0] == pytest.approx(104.31)
    assert df['VALUE'].index[0] == pd.Timestamp('2020-01-01')
