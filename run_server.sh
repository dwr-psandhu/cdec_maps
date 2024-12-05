pip install --no-deps -e .
#pip install panel==1.5.0b3 # FIXME: remove this line when panel 1.5.0 is released
#python cdec_maps/cdec_cache_build.py &
panel serve cdec.py --address 0.0.0.0 --port 80 --allow-websocket-origin="*"
