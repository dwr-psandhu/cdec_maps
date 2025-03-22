import datetime as dt
import sys
import panel as pn

pn.extension(
    "gridstack", "tabulator", "codeeditor", notifications=True, design="native"
)
from pydelmod.dvue import fullscreen

main_panel = pn.Column(
    pn.indicators.LoadingSpinner(
        value=True, color="primary", size=50, name="Loading..."
    )
)
sidebar_panel = pn.Column(
    pn.indicators.LoadingSpinner(
        value=True, color="primary", size=50, name="Loading..."
    )
)
template = pn.template.VanillaTemplate(
    title="CDEC Map Explorer",
    sidebar=[sidebar_panel],
    main=[main_panel],
    sidebar_width=650,
    header_color="blue",
    # logo="",
)


def load_explorer():
    from cdec_maps import cdecuimgr

    te = cdecuimgr.show_cdec_ui()

    # Clear existing content first
    main_panel.clear()
    sidebar_panel.clear()

    for obj in te.sidebar.objects:
        sidebar_panel.append(obj)

    # Add objects individually to ensure proper reactivity
    for obj in te.main.objects:
        main_panel.append(pn.panel(obj))

    # Add the disclaimer text to the modal
    template.modal.append(
        """
        CDEC Map Explorer: A view onto CDEC data. 
        
        Disclaimer:
        Information provided by this app is sourced from the California Data Exchange Center (CDEC).
        Please don't rely on this data and refere to original CDEC site for verification.
        We make not guarantees about the accuracy of this data."""
    )


pn.state.onload(load_explorer)

template.servable(title="CDEC Map Explorer")
