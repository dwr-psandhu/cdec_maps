from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='cdec-maps',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="CDEC Maps and Dashboards",
    license="MIT",
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    url='https://github.com/dwr-psandhu/cdec-maps',
    packages=['cdec_maps'],
    entry_points={
        'console_scripts': [
            'cdec_maps=cdec_maps.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='cdec-maps',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
