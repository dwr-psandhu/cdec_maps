"""DVue plugin registration for cdec_maps.

This module is imported by dvue at startup via the ``dvue.plugins`` entry point
group. It registers the CDEC reader with the ReaderRegistry.

To use with dvue, ensure cdec_maps is installed with the entry point::

    [project.entry-points."dvue.plugins"]
    cdec_maps = "cdec_maps.readers:register_readers"

Once installed, the CDEC reader is auto-discovered on startup::

    dvue ui --list-plugins
"""


def register_readers():
    """Register cdec_maps readers with dvue.

    Called automatically by dvue at startup via entry points.
    Registers:
    - CDEC API reader (ref_type="cdec", no file extensions — API-based source)
    """
    from dvue.registry import ReaderRegistry
    from cdec_maps.cdecuimgr import CDECDataReferenceReader

    ReaderRegistry.register(
        "cdec",
        CDECDataReferenceReader,
    )
