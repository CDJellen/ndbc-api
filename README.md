<div align="center">
    <h2>NDBC API</h2>
</div>


[![Coverage Status](https://coveralls.io/repos/github/CDJellen/ndbc-api/badge.svg?branch=main)](https://coveralls.io/github/CDJellen/ndbc-api?branch=main)
[![PyPI](https://img.shields.io/pypi/v/ndbc-api)](https://pypi.org/project/ndbc-api/#history)
[![PyPI - Status](https://img.shields.io/pypi/status/ndbc-api)](https://pypi.org/project/ndbc-api/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ndbc-api)](https://pypi.org/project/ndbc-api/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white&style=flat-square)](https://www.linkedin.com/in/cdjellen/)
[![GitHub](https://img.shields.io/github/license/cdjellen/ndbc-api)](https://github.com/cdjellen/ndbc-api/blob/main/LICENSE)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/ndbc-api)](https://pypi.org/project/ndbc-api/)


<div align="center">
    <h3>A Python API for the National Data Buoy Center</h3>
</div>


The National Oceanic and Atmospheric Association's National Data Buoy Center maintains marine monitoring and observation stations around the world[^1]. These stations report atmospheric, oceanographic, and other meterological data at regular intervals to the NDBC. Measurements are made available over HTTP through the NDBC's data service.

The ndbc-api is a python library that makes this data more widely accessible.

The ndbc-api is primarily built to parse whitespace-delimited oceanographic and atmospheric data distributed as text files for available time ranges, on a station-by-station basis[^2]. Measurements are typically distributed as `utf-8` encoded, station-by-station, fixed-period text files. More information on the measurements and methodology are available [on the NDBC website](https://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf)[^3].

Please see [the included example notebook](/notebooks/overview.ipynb) for a more detailed walkthrough of the API's capabilities.

[^1]: https://www.ndbc.noaa.gov/
[^2]: https://www.ndbc.noaa.gov/obs.shtml
[^3]: https://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf



#### Installation
The `ndbc-api` can be installed via PIP:

```sh
pip install ndbc-api
```

Conda users can install the `ndbc-api` via the `conda-forge` channel:

```sh
conda install -c conda-forge ndbc-api
```

Finally, to install the `ndbc-api` from source, clone the repository and run the following command:

```sh
python setup.py install
```

#### Requirements
The `ndbc-api` has been tested on Python 3.6, 3.7, 3.8, 3.9, and 3.10. Python 2 support is not currently planned, but could be implemented based on the needs of the atmospheric research community.

The API uses synchronous HTTP requests to compile data matching the user-supplied parameters. The `ndbc-api` package depends on:
* requests>=2.10.0
* pandas
* bs4
* html5lib>=1.1
* xarray
* scipy

##### Development
If you would like to contribute to the growth and maintenance of the `ndbc-api`, please feel free to open a PR with tests covering your changes. The tests leverage `pytest` and depend on the above requirements, as well as:
* coveralls
* httpretty
* pytest
* pytest-cov
* pyyaml
* pyarrow

Breaking changes will be considered, especially in the current `alpha` state of the package on `PyPi`.  As the API further matures, breaking changes will only be considered with new major versions (e.g. `N.0.0`).

#### Example

The `ndbc-api` exposes public methods through the `NdbcApi` class.

```python3
from ndbc_api import NdbcApi

api = NdbcApi()
```

The `NdbcApi` provides a unified access point for NDBC data. All methods for obtaining data, metadata, and locating stations are available using the `api` object. The `get_data` method is the primary method for accessing NDBC data, and is used to retrieve measurements from a given station over a specified time range. This method can request data from the NDBC HTTP Data Service or the THREDDS data service, and return the data as a `pandas.DataFrame`, `xarray.Dataset` or python `dict` object.

Data made available by the NDBC falls into two broad categories.

1. Station metadata
2. Station measurements

The `api` supports a range of public methods for accessing data from the above categories.

##### Station metadata

The `api` has five key public methods for accessing NDBC metadata.

1. The `stations` method, which returns all NDBC stations.
2. The `nearest_station` method, which returns the station ID of the nearest station.
3. The `station` method, which returns station metadata from a given station ID.
4. The `available_realtime` method, which returns hyperlinks and measurement names for realtime measurements captured by a given station.
5. The `available_historical` method, which returns hyperlinks and measurement names for historical measurements captured by a given station.

###### `stations`

```python3
# get all stations and some metadata as a Pandas DataFrame
stations_df = api.stations()
# parse the response as a dictionary
stations_dict = api.stations(as_df=False)
```

###### `nearest_station`

```python3
# specify desired latitude and longitude
lat = '38.88N'
lon = '76.43W'

# find the station ID of the nearest NDBC station
nearest = api.nearest_station(lat=lat, lon=lon)
print(nearest_station)
```

```python3
'tplm2'
```

###### `radial_search`

```python3
# specify desired latitude, longitude, radius, and units
lat = '38.88N'
lon = '76.43W'
radius = 100
units = 'km'

# find the station IDs of all NDBC stations within the radius
nearby_stations_df = api.radial_search(lat=lat, lon=lon, radius=radius, units=units)
```

```python3
'tplm2'
```

###### `station`

```python3
# get station metadata
tplm2_meta = api.station(station_id='tplm2')
# parse the response as a Pandas DataFrame
tplm2_df = api.station(station_id='tplm2', as_df=True)
```

###### `available_realtime`

```python3
# get all available realtime measurements, periods, and hyperlinks
tplm2_realtime = api.available_realtime(station_id='tplm2')
# parse the response as a Pandas DataFrame
tplm2_realtime_df = api.available_realtime(station_id='tplm2', as_df=True)
```

###### `available_historical`

```python3
# get all available historical measurements, periods, and hyperlinks
tplm2_historical = api.available_historical(station_id='tplm2')
# parse the response as a Pandas DataFrame
tplm2_historical_df = api.available_historical(station_id='tplm2', as_df=True)
```

##### Station measurements

The `api` has two public methods which support accessing supported NDBC station measurements.

1. The `get_modes` method, which returns a list of supported `mode`s, corresponding to the data formats provided by the NDBC data service. For example, the `adcp` mode represents "Acoustic Doppler Current Profiler" measurements, providing information about ocean currents at different depths, while `cwind` represents "Continuous winds" data, offering high-frequency wind speed and direction measurements.

Note that not all stations provide the same set of measurements. The `available_realtime` and `available_historical` methods can be called on a station-by station basis to ensure a station has the desired data available, before building and executing requests with `get_data`. 

2. The `get_data` method, which returns measurements of a given type for a given station.

###### `get_modes`

```python3
# get the list of supported meterological measurement modes
modes = api.get_modes()
print(modes)
```

```python3
[
    'adcp',
    'cwind',
    'ocean',
    'spec',
    'stdmet',
    'supl',
    'swden',
    'swdir',
    'swdir2',
    'swr1',
    'swr2'
]
```

The mode values above map directly to the identifiers used buy the NDBC. Desriptions for each mode are presented below:
* `adcp`: Acoustic Doppler Current Profiler measurements, providing information about ocean currents at different depths.
* `cwind`: Continuous winds data, offering high-frequency wind speed and direction measurements.
* `ocean`: Oceanographic data, including water temperature, salinity, and wave measurements.
* `spec`: Spectral wave data, providing detailed information about wave energy and direction.
* `stdmet`: Standard meteorological data, including air temperature, pressure, wind speed, and visibility.
* `supl`: Supplemental measurements, which can vary depending on the specific buoy and its sensors.
* `swden`: Spectral wave density data, providing information about the distribution of wave energy across different frequencies.
* `swdir`: Spectral wave direction data, indicating the primary direction of wave energy.
* `swdir2`: Secondary spectral wave direction data, capturing additional wave direction information.
* `swr1`: First-order spectral wave data, providing basic wave height and period information.
* `swr2`: Second-order spectral wave data, offering more detailed wave measurements.

###### `get_data`

```python3
# get all continuous wind (`cwind`) measurements for station tplm2
cwind_df = api.get_data(
    station_id='tplm2',
    mode='cwind',
    start_time='2020-01-01',
    end_time='2022-09-15',
)
# return data as a dictionary
cwind_dict = api.get_data(
    station_id='tplm2',
    mode='cwind',
    start_time='2020-01-01',
    end_time='2022-09-15',
    as_df=False
)
# get only the wind speed measurements
wspd_df = api.get_data(
    station_id='tplm2',
    mode='cwind',
    start_time='2020-01-01',
    end_time='2022-09-15',
    as_df=True,
    cols=['WSPD']
)
# get all standard meterological (`stdmet`) measurements for stations tplm2 and apam2
stdmet_df = api.get_data(
    station_ids=['tplm2', 'apam2'],
    mode='stdmet',
    start_time='2022-01-01',
    end_time='2023-01-01',
)
# get all (available) continuous wind and standard meterological measurements for stations tplm2 and apam2
# for station apam2, this is unavailable and will log an error but not affect the rest of the results.
stdmet_df = api.get_data(
    station_ids=['tplm2', 'apam2'],
    modes=['stdmet', 'cwind'],
    start_time='2022-01-01',
    end_time='2023-01-01',
)
```

#### More Information
Please see [the included example notebook](/notebooks/overview.ipynb) for a more detailed walkthrough of the API's capabilities.

#### Questions
If you have questions regarding the library please post them into
the [GitHub discussion forum](https://github.com/cdjellen/ndbc-api/discussions).
