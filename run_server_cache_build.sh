#pip install panel==1.5.0b3 # FIXME: remove this line when panel 1.5.0 is released
echo "pip installing this directory: $(pwd)"
pip install --no-deps -e .
echo "starting cdec_cache_build.py"
python cdec_maps/cdec_cache_build.py & # FIXME: move this to a background process as it locks up the cache db
echo "starting panel serve cdecmap_builder.py"
panel serve cdecmap_builder.py --address 0.0.0.0 --port 80 --allow-websocket-origin="*"
