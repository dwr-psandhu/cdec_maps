"""CDEC Caching using pickled files
    * Should handle updates by first looking at pickled files and then appending newer data (past year or more to it)
    """
import functools
import os
import glob
import pandas as pd


def cache_to_file(list=False, expires='1D', cache_dir='cdec_cache', data=True):
    """Caches Dataframe returned by function to filename (name of function + '.pkl')

    Cache expires after "expires" Timedelta at which point the function is called and refreshes the cache before returning.
    Args:
        list (boolean, optional): Indicates that the function returns a list (or other iterable) of DataFrame(s)
        expires (str, optional): [Cache expires after the Timedelta str]. Defaults to '1D'
        cache_dir (str, optional): Directory in which to store the pickled dataframe.
        data (boolean, optional): Indicates that the data frames are appendable and can be updated with additional data rather than replacement
    """
    def expired(mtime):
        return pd.Timestamp.now() - pd.Timestamp.fromtimestamp(mtime) > pd.to_timedelta(expires)

    def ensure_dir(cache_dir):
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

    def decorator(func):

        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            def needs_refresh(fname):
                return not os.path.exists(fname) or expired(os.path.getmtime(fname))
            #

            def read_cache(fname):
                df = pd.read_pickle(fname)
                return df
            #

            def write_cache(df, fname):
                df.to_pickle(fname)
            #
            ensure_dir(cache_dir)
            if list:
                cached_files = glob.glob(f"{cache_dir}/{func.__name__}.*.pkl")
                if not cached_files or any([needs_refresh(fname) for fname in cached_files]):
                    result = func(*args, **kwargs)
                    for i, r in enumerate(result):
                        write_cache(r, f"{cache_dir}/{func.__name__}.{i}.pkl")
                else:
                    result = [read_cache(fname) for fname in cached_files]
            else:
                if func.__name__ == 'read_station_data':
                    cache_file = f"{cache_dir}/{args[1]}_{args[2]}_{args[3]}.pkl"
                else:
                    cache_file = f"{cache_dir}/{func.__name__}.pkl"
                if needs_refresh(cache_file):
                    if func.__name__ == 'read_station_data':  # then cache and fetch differently
                        # station_id, sensor_number, duration_code
                        result = args[0].read_entire_station_data_for(args[1], args[2], args[3])
                    else:
                        result = func(*args, **kwargs)
                    write_cache(result, cache_file)
                else:
                    result = read_cache(cache_file)
                    if func.__name__ == 'read_station_data':  # need update strategy here
                        sdate = result.index[-1].strftime('%Y-%m-%d')
                        dflatest = args[0]._undecorated_read_station_data(
                            args[1], args[2], args[3], sdate, '')
                        result = dflatest.combine_first(result)  # updates with latest fetched
                if func.__name__ == 'read_station_data':  # then subset to desired time window
                    start, end = args[0]._sort_times(args[4], args[5])
                    result = result.loc[pd.to_datetime(start):pd.to_datetime(end)]
            return result
        return wrapper_decorator
    return decorator
