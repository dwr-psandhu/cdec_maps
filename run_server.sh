#pip install --no-deps -e .
python cdec_maps/cdec_cache_build.py &
panel serve cdec_maps/cdec_maps_servable.py --address 0.0.0.0 --port 80 --allow-websocket-origin="*"
