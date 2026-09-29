git config --global --add safe.directory /home/data/cdec-maps
pip install git+https://github.com/CADWRDeltaModeling/vtools3
pip install --force-reinstall git+https://github.com/CADWRDeltaModeling/dvue.git#egg=dvue --no-deps
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 pip install --no-deps .
#pip install panel==1.5.0b3 # FIXME: remove this line when panel 1.5.0 is released
#python cdec_maps/cdec_cache_build.py &
# CARTO_API_KEY must be set in the environment (get a key at https://carto.com/basemaps/apikey)
# or basemap tiles will show CARTO's "API key required" watermark.
python cdecui.py --address 0.0.0.0 --port 80 --no-show
