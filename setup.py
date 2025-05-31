from setuptools import setup, find_packages

# setup.py is kept for backward compatibility
# All configuration has been moved to pyproject.toml

requirements = [
    "ipykernel",
    "pyarrow",
    "pandas",
    "dask",
    "numba",
    "aiohttp",
    "requests",
    "decorator",
    "lxml",
    "html5lib",
    "beautifulsoup4",
    "cartopy",
    "geopandas",
    "holoviews",
    "hvplot",
    "matplotlib",
    "bokeh",
    "geoviews",
    "spatialpandas",
    "panel"
]

setup(
    name='cdec-maps',
    # Let setuptools_scm handle versioning
    use_scm_version=True,
    description="CDEC Maps and Dashboards",
    license="MIT",
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    url='https://github.com/dwr-psandhu/cdec-maps',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'cdec_maps=cdec_maps.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='cdec-maps',
    classifiers=[
        'Programming Language :: Python :: 3.10'
    ]
)
