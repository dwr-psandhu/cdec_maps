"""CDEC Caching using pickled files
    * Should handle updates by first looking at pickled files and then appending newer data (past year or more to it)
    """
import functools
import os
import pandas as pd


def cache_to_file(expires='1D', cache_dir='cdec_cache'):
    """Caches Dataframe returned by function to filename (name of function + '.pkl')

    Cache expires after "expires" Timedelta at which point the function is called and refreshes the cache before returning.
    Args:
        expires (str, optional): [Cache expires after the Timedelta str]. Defaults to '1D'
        cache_dir (str, optional): Directory in which to store the pickled dataframe.
    """
    def needs_refresh(mtime):
        return pd.Timestamp.now() - pd.Timestamp.fromtimestamp(mtime) > pd.to_timedelta(expires)

    def ensure_dir(cache_dir):
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

    def decorator(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            ensure_dir(cache_dir)
            filename = func.__name__ + '.pkl'
            full_filename = f"{cache_dir}/{filename}"
            if os.path.exists(full_filename) and not needs_refresh(os.path.getmtime(full_filename)):
                df = pd.read_pickle(full_filename)
            else:
                df = func(*args, **kwargs)
                df.to_pickle(full_filename)
            return df
        return wrapper_decorator
    return decorator
