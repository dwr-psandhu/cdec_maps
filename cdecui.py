"""CDEC Map Explorer — pn.serve entry-point with session persistence.

Launch with:
    python cdecui.py [--port 80] [--no-show]
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Manager factory — called once per unique user session
# ---------------------------------------------------------------------------

def build_cdec_manager():
    """Build and return a fresh :class:`~cdec_maps.cdecuimgr.CDECDataUIManager`.

    All heavy I/O (reading station metadata) happens here so that it only
    runs once per user (the in-memory registry serves returning users without
    re-running this function).
    """
    import pandas as pd
    import geopandas as gpd
    from cdec_maps import cdec
    from cdec_maps.cdecuimgr import CDECDataUIManager

    t0 = time.perf_counter()
    logger.info("build_cdec_manager: reading saved station info …")
    reader = cdec.Reader()
    stations, sensor_list, stations_meta_info = reader.read_saved_stations_info()
    logger.info(
        "build_cdec_manager: loaded %d station-sensor rows, %d sensor types  (%.1fs)",
        len(stations_meta_info), len(sensor_list), time.perf_counter() - t0,
    )

    # Restrict to California / adjacent states lat-lon box
    stations_meta_info = stations_meta_info[
        (stations_meta_info["Latitude"] >= 30)
        & (stations_meta_info["Latitude"] <= 45)
        & (stations_meta_info["Longitude"] >= -125)
        & (stations_meta_info["Longitude"] <= -110)
    ]

    displayed_sensor_list = sensor_list["Sensor"].unique()
    stations_meta_info = stations_meta_info[
        stations_meta_info["Sensor"].isin(displayed_sensor_list)
    ]
    logger.info(
        "build_cdec_manager: %d rows after geo+sensor filter  (%.1fs)",
        len(stations_meta_info), time.perf_counter() - t0,
    )

    # Parse date-availability column into typed Start Date / End Date columns
    stations_meta_info[["Start Date", "End Date"]] = stations_meta_info[
        "Data Available"
    ].str.split(" to ", expand=True)
    stations_meta_info["End Date"] = stations_meta_info["End Date"].replace(
        "present", pd.to_datetime("today").strftime("%m/%d/%Y")
    )
    stations_meta_info["Start Date"] = pd.to_datetime(
        stations_meta_info["Start Date"], errors="coerce"
    )
    stations_meta_info["End Date"] = pd.to_datetime(
        stations_meta_info["End Date"], errors="coerce"
    )

    stations_meta_info["Source"] = "CDEC"
    stations_meta_info["Sensor Number"] = stations_meta_info["Sensor Number"].astype(str)

    if all(col in stations.columns for col in ["Latitude", "Longitude"]):
        geodf = gpd.GeoDataFrame(
            stations_meta_info,
            geometry=gpd.points_from_xy(
                stations_meta_info["Longitude"],
                stations_meta_info["Latitude"],
                crs="EPSG:4326",
            ),
        )
    else:
        geodf = stations_meta_info

    time_range = (
        datetime.now() - timedelta(days=30),
        datetime.now(),
    )
    logger.info(
        "build_cdec_manager: constructing CDECDataUIManager with %d rows …",
        len(geodf),
    )
    mgr = CDECDataUIManager(geodf, reader, time_range=time_range)
    logger.info(
        "build_cdec_manager: done — catalog has %d refs  (total %.1fs)",
        len(mgr._dvue_catalog._references), time.perf_counter() - t0,
    )
    return mgr


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main():
    import cartopy.crs as ccrs
    import panel as pn
    from dvue.dataui import DataUI
    from dvue.session_persistence import (
        install_session_handler,
        SessionManager,
    )

    parser = argparse.ArgumentParser(
        description="CDEC Map Explorer — Panel server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--port", type=int, default=80,
        help="TCP port to serve on",
    )
    parser.add_argument(
        "--address", default="0.0.0.0",
        help="Network address to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--no-show", action="store_true",
        help="Do not open a browser window automatically",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("Starting CDEC Map Explorer on %s:%s", args.address, args.port)

    pn.extension(
        "gridstack", "tabulator", "codeeditor",
        notifications=True, design="native",
    )

    # Pre-import heavy UI modules before pn.serve() so their module-level
    # hv/pn extension() calls happen outside any live Bokeh session.
    from cdec_maps.cdecuimgr import CDECDataUIManager  # noqa: F401

    install_session_handler(cookie_name="cdec_user_id")

    session_mgr = SessionManager(
        cookie_name="cdec_user_id",
        persist=False,
    )

    crs_cartopy = ccrs.PlateCarree()

    def make_app():
        user_id = session_mgr.current_user_id
        reg_key = session_mgr.make_reg_key(user_id, "cdec")
        entry = session_mgr.get_entry(reg_key)

        # Serve the outer VanillaTemplate immediately with a loading spinner.
        # The heavy build runs inside pn.state.onload so the browser gets the
        # template shell before any blocking I/O happens.
        # VanillaTemplate renders main items full-width; FastListTemplate wraps
        # them in Bootstrap cards which breaks GridStack height.
        main_panel = pn.Column(
            pn.indicators.LoadingSpinner(value=True, color="primary", size=50),
            sizing_mode="stretch_both",
        )
        sidebar_panel = pn.Column(
            pn.indicators.LoadingSpinner(value=True, color="primary", size=50),
        )

        if entry:
            # Registry hit: reuse the already-built template and DataUI;
            # only re-register per-Document hooks (they bind to curdoc).
            logger.info("make_app: registry hit for user %s — reusing session", user_id[:8] if user_id else "anon")
            template = entry["template"]
            ui = entry["ui"]
            stored_main = entry.get("main_panel")
            if stored_main is not None:
                stored_main.loading = True

            def _reattach():
                try:
                    ui.setup_location_sync()
                    ui.setup_url_sync()
                finally:
                    if stored_main is not None:
                        stored_main.loading = False

            pn.state.onload(_reattach)
            template.servable()
            return

        logger.info("make_app: new session for user %s — building manager …", user_id[:8] if user_id else "anon")
        # header_row and modal_pane are created now so they are embedded in
        # the Jinja HTML at page-load time.  Items appended inside
        # pn.state.onload have no DOM placeholder, so we must pre-render
        # these containers and swap their .objects dynamically.
        header_row = pn.Row(sizing_mode="fixed")
        modal_pane = pn.Column(sizing_mode="stretch_width")
        template = pn.template.VanillaTemplate(
            title="CDEC Map Explorer",
            sidebar=[sidebar_panel],
            main=[main_panel],
            sidebar_width=450,
            header=[header_row],
        )
        # Append modal_pane before .servable() so the Jinja template embeds
        # its root.  Never call template.modal.clear() after this point.
        template.modal.append(modal_pane)

        def _load_app():
            try:
                mgr = build_cdec_manager()
                ui = DataUI(mgr, crs=crs_cartopy, station_id_column="ID")
                ui_template = ui.create_view(title="CDEC Map Explorer")

                # Move content from the inner FastListTemplate into our
                # VanillaTemplate.  Clear the inner template so Panel doesn't
                # try to serve the same Bokeh models from two documents.
                sidebar_items = list(ui_template.sidebar)
                main_items = list(ui_template.main)
                ui_template.sidebar.clear()
                ui_template.main.clear()
                ui_template.modal.clear()

                sidebar_panel.objects = sidebar_items
                main_panel.objects = main_items

                # Build header buttons.  Callbacks swap modal_pane.objects
                # rather than clearing template.modal (which would remove the
                # pre-rendered slot and leave the modal permanently empty).
                about_text = ui.get_about_text()
                modal_pane.objects = [about_text]

                about_btn = pn.widgets.Button(
                    name="About App", button_type="primary", icon="info-circle"
                )
                def _about_click(event):
                    modal_pane.objects = [about_text]
                    template.open_modal()
                about_btn.on_click(_about_click)

                disclaimer_content = mgr.get_sidebar_disclaimer()
                if disclaimer_content is not None:
                    disclaimer_btn = pn.widgets.Button(
                        name="Disclaimer", button_type="light", icon="alert-circle"
                    )
                    def _disclaimer_click(event):
                        modal_pane.objects = [disclaimer_content]
                        template.open_modal()
                    disclaimer_btn.on_click(_disclaimer_click)
                    header_row.append(disclaimer_btn)

                header_row.append(about_btn)

                if user_id:
                    session_mgr.set_entry(reg_key, {
                        "template": template, "ui": ui,
                        "main_panel": main_panel, "header_row": header_row,
                        "modal_pane": modal_pane,
                    })

            except Exception:
                import traceback
                logger.error("_load_app failed:\n%s", traceback.format_exc())
                main_panel.objects = [
                    pn.pane.Markdown("## Error loading application\n\nCheck the server logs.")
                ]

        pn.state.onload(_load_app)
        template.servable()

    pn.serve(
        {"cdec-map-explorer": make_app},
        port=args.port,
        address=args.address,
        allow_websocket_origin=["*"],
        show=not args.no_show,
        keep_alive=30000,
        # Bounded (not Bokeh's 15s default, but not unbounded either): every
        # cookie-less GET / (Azure Always-On ping, health check) spins up a
        # full heavy CDECDataUIManager session via SessionManager that never
        # reconnects. A huge value here let those orphaned sessions
        # accumulate forever, leaking memory until the container was
        # OOM-restarted -- killing every real user's connection at once.
        unused_session_lifetime_milliseconds=1_800_000,
        # Bokeh's default (300s) expires the reconnect token after a few
        # minutes idle; the browser's automatic reconnect then fails
        # permanently with "Token is expired" until the page is reloaded.
        session_token_expiration=2_592_000,
    )


if __name__ == "__main__":
    main()
