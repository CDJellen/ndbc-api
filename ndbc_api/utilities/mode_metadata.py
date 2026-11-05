"""Authoritative NDBC measurement mode and variable metadata registry.

Provides physical units, human-readable descriptions, and missing-value
sentinel documentation for all HTTP and OpenDAP measurement modalities
supported by `ndbc-api`, derived from the NOAA NDBC Web Data Guide
(https://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf).
"""
from copy import deepcopy
from typing import Any, Dict, Optional

from ..exceptions import RequestException

MODE_METADATA: Dict[str, Dict[str, Any]] = {
    'stdmet': {
        'mode': 'stdmet',
        'name': 'Standard Meteorological Data',
        'description': (
            'Standard meteorological and wave summary measurements reported '
            'hourly or sub-hourly.'
        ),
        'http_supported': True,
        'opendap_supported': True,
        'columns': {
            'WDIR': {
                'name': 'Wind Direction',
                'units': 'degT',
                'description': 'Wind direction (degrees clockwise from true North).',
                'missing_sentinel': [999, 'MM'],
            },
            'WSPD': {
                'name': 'Wind Speed',
                'units': 'm/s',
                'description': 'Wind speed averaged over 8 minutes (buoys) or 2 minutes (land).',
                'missing_sentinel': [99.0, 'MM'],
            },
            'GST': {
                'name': 'Wind Gust',
                'units': 'm/s',
                'description': 'Peak 5- or 8-second gust speed during the averaging period.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'WVHT': {
                'name': 'Significant Wave Height',
                'units': 'm',
                'description': 'Average of the highest one-third of all wave heights during the 20-minute sampling period.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'DPD': {
                'name': 'Dominant Wave Period',
                'units': 's',
                'description': 'Period with the maximum wave energy.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'APD': {
                'name': 'Average Wave Period',
                'units': 's',
                'description': 'Average wave period of all waves during the 20-minute period.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'MWD': {
                'name': 'Mean Wave Direction',
                'units': 'degT',
                'description': 'Direction from which waves at the dominant period (DPD) are coming.',
                'missing_sentinel': [999, 'MM'],
            },
            'PRES': {
                'name': 'Sea Level Pressure',
                'units': 'hPa',
                'description': 'Sea level atmospheric pressure.',
                'missing_sentinel': [9999.0, 'MM'],
            },
            'ATMP': {
                'name': 'Air Temperature',
                'units': 'degC',
                'description': 'Air temperature.',
                'missing_sentinel': [999.0, 'MM'],
            },
            'WTMP': {
                'name': 'Sea Surface Temperature',
                'units': 'degC',
                'description': 'Sea surface water temperature.',
                'missing_sentinel': [999.0, 'MM'],
            },
            'DEWP': {
                'name': 'Dewpoint Temperature',
                'units': 'degC',
                'description': 'Dewpoint temperature taken at the same height as air temperature.',
                'missing_sentinel': [999.0, 'MM'],
            },
            'VIS': {
                'name': 'Station Visibility',
                'units': 'nmi',
                'description': 'Station visibility in nautical miles.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'PTDY': {
                'name': 'Pressure Tendency',
                'units': 'hPa',
                'description': 'Direction and amount of pressure change over a 3-hour period.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'TIDE': {
                'name': 'Water Level (Tide)',
                'units': 'ft',
                'description': 'Water level in feet above or below Mean Lower Low Water (MLLW).',
                'missing_sentinel': [99.0, 'MM'],
            },
        },
    },
    'cwind': {
        'mode': 'cwind',
        'name': 'Continuous Winds Data',
        'description': '10-minute average continuous wind speed, direction, and peak gust.',
        'http_supported': True,
        'opendap_supported': True,
        'columns': {
            'WDIR': {
                'name': 'Wind Direction',
                'units': 'degT',
                'description': '10-minute average wind direction (degrees clockwise from true North).',
                'missing_sentinel': [999, 'MM'],
            },
            'WSPD': {
                'name': 'Wind Speed',
                'units': 'm/s',
                'description': '10-minute average wind speed.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'GDR': {
                'name': 'Gust Direction',
                'units': 'degT',
                'description': 'Direction of peak 5-second wind gust during the 60-minute period.',
                'missing_sentinel': [999, 'MM'],
            },
            'GST': {
                'name': 'Wind Gust',
                'units': 'm/s',
                'description': 'Peak 5-second wind gust during the 60-minute period.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'GTIME': {
                'name': 'Gust Time',
                'units': 'hhmm',
                'description': 'Hour and minute (UTC) of the peak gust.',
                'missing_sentinel': [9999, 'MM'],
            },
        },
    },
    'ocean': {
        'mode': 'ocean',
        'name': 'Oceanographic Data',
        'description': 'Subsurface oceanographic measurements including temperature, conductivity, salinity, dissolved oxygen, and chlorophyll.',
        'http_supported': True,
        'opendap_supported': True,
        'columns': {
            'DEPTH': {
                'name': 'Measurement Depth',
                'units': 'm',
                'description': 'Depth below sea surface at which measurements were taken.',
                'missing_sentinel': [99.0, 999, 'MM'],
            },
            'OTMP': {
                'name': 'Ocean Temperature',
                'units': 'degC',
                'description': 'Subsurface ocean temperature.',
                'missing_sentinel': [99.0, 999.0, 'MM'],
            },
            'COND': {
                'name': 'Conductivity',
                'units': 'mS/cm',
                'description': 'Electrical conductivity of seawater.',
                'missing_sentinel': [99.0, 999.0, 'MM'],
            },
            'SAL': {
                'name': 'Salinity',
                'units': 'psu',
                'description': 'Practical salinity computed from conductivity, temperature, and depth.',
                'missing_sentinel': [99.0, 99.9, 'MM'],
            },
            'O2%': {
                'name': 'Dissolved Oxygen Saturation',
                'units': '%',
                'description': 'Dissolved oxygen as a percentage of saturation.',
                'missing_sentinel': [99.0, 99.9, 999.0, 'MM'],
            },
            'O2PPM': {
                'name': 'Dissolved Oxygen Concentration',
                'units': 'ppm',
                'description': 'Dissolved oxygen concentration in parts per million.',
                'missing_sentinel': [99.0, 99.9, 'MM'],
            },
            'CLCON': {
                'name': 'Chlorophyll Concentration',
                'units': 'ug/L',
                'description': 'Chlorophyll concentration in micrograms per liter.',
                'missing_sentinel': [99.0, 99.9, 'MM'],
            },
            'TURB': {
                'name': 'Turbidity',
                'units': 'FTU',
                'description': 'Turbidity in Formazin Turbidity Units.',
                'missing_sentinel': [99.0, 99.9, 999.0, 'MM'],
            },
            'PH': {
                'name': 'pH',
                'units': '1',
                'description': 'Acidity/alkalinity of seawater.',
                'missing_sentinel': [99.0, 99.9, 'MM'],
            },
            'EH': {
                'name': 'Redox Potential (Eh)',
                'units': 'mV',
                'description': 'Oxidation-reduction potential in millivolts.',
                'missing_sentinel': [999.0, 9999.0, 'MM'],
            },
        },
    },
    'spec': {
        'mode': 'spec',
        'name': 'Spectral Wave Summary Data',
        'description': 'Separated swell and wind-wave height, period, direction, and wave steepness summary.',
        'http_supported': True,
        'opendap_supported': False,
        'columns': {
            'WVHT': {
                'name': 'Significant Wave Height',
                'units': 'm',
                'description': 'Combined significant wave height.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'SwH': {
                'name': 'Swell Height',
                'units': 'm',
                'description': 'Significant height of swell wave components.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'SwP': {
                'name': 'Swell Period',
                'units': 's',
                'description': 'Peak period of swell wave components.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'WWH': {
                'name': 'Wind Wave Height',
                'units': 'm',
                'description': 'Significant height of wind-wave components.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'WWP': {
                'name': 'Wind Wave Period',
                'units': 's',
                'description': 'Peak period of wind-wave components.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'SwD': {
                'name': 'Swell Direction',
                'units': 'cardinal',
                'description': 'Compass direction from which swell waves are coming.',
                'missing_sentinel': ['MM', 'N/A'],
            },
            'WWD': {
                'name': 'Wind Wave Direction',
                'units': 'cardinal',
                'description': 'Compass direction from which wind waves are coming.',
                'missing_sentinel': ['MM', 'N/A'],
            },
            'STEEPNESS': {
                'name': 'Wave Steepness',
                'units': 'category',
                'description': 'Qualitative wave steepness classification (SWELL, AVERAGE, STEEP, VERY_STEEP).',
                'missing_sentinel': ['MM', 'N/A'],
            },
            'APD': {
                'name': 'Average Wave Period',
                'units': 's',
                'description': 'Average wave period of all waves during the 20-minute period.',
                'missing_sentinel': [99.0, 'MM', 'N/A'],
            },
            'MWD': {
                'name': 'Mean Wave Direction',
                'units': 'degT',
                'description': 'Mean direction from which waves at the dominant period are coming.',
                'missing_sentinel': [999, 'MM', 'N/A'],
            },
        },
    },
    'supl': {
        'mode': 'supl',
        'name': 'Supplemental Measurements Data',
        'description': 'Lowest 1-minute atmospheric pressure and highest 1-minute wind speed hourly extremes.',
        'http_supported': True,
        'opendap_supported': False,
        'columns': {
            'PRES': {
                'name': 'Lowest 1-Minute Sea Level Pressure',
                'units': 'hPa',
                'description': 'Lowest 1-minute sea level pressure recorded during the hour.',
                'missing_sentinel': [9999.0, 'MM'],
            },
            'PRESS': {
                'name': 'Lowest 1-Minute Sea Level Pressure',
                'units': 'hPa',
                'description': 'Lowest 1-minute sea level pressure recorded during the hour (legacy header).',
                'missing_sentinel': [9999.0, 'MM'],
            },
            'PTIME': {
                'name': 'Lowest Pressure Time',
                'units': 'hhmm',
                'description': 'Time (UTC) of lowest 1-minute pressure.',
                'missing_sentinel': [9999, 'MM'],
            },
            'WSPD': {
                'name': 'Highest 1-Minute Wind Speed',
                'units': 'm/s',
                'description': 'Highest 1-minute wind speed recorded during the hour.',
                'missing_sentinel': [99.0, 'MM'],
            },
            'WDIR': {
                'name': 'Highest Wind Speed Direction',
                'units': 'degT',
                'description': 'Direction of the highest 1-minute wind speed.',
                'missing_sentinel': [999, 'MM'],
            },
            'WTIME': {
                'name': 'Highest Wind Speed Time',
                'units': 'hhmm',
                'description': 'Time (UTC) of highest 1-minute wind speed.',
                'missing_sentinel': [9999, 'MM'],
            },
        },
    },
    'adcp': {
        'mode': 'adcp',
        'name': 'Acoustic Doppler Current Profiler (ADCP) Data',
        'description': 'Ocean current depth, direction, and speed profiles across up to 38 depth bins (DEP01..DEP38, DIR01..DIR38, SPD01..SPD38).',
        'http_supported': True,
        'opendap_supported': True,
        'columns': {
            **{
                f'DEP{i:02d}': {
                    'name': f'Depth Bin {i}',
                    'units': 'm',
                    'description': f'Depth (meters) of ADCP bin {i}.',
                    'missing_sentinel': ['MM'],
                }
                for i in range(1, 39)
            },
            **{
                f'DIR{i:02d}': {
                    'name': f'Current Direction Bin {i}',
                    'units': 'degT',
                    'description': f'Direction ocean current is flowing toward in bin {i} (degrees clockwise from true North).',
                    'missing_sentinel': ['MM'],
                }
                for i in range(1, 39)
            },
            **{
                f'SPD{i:02d}': {
                    'name': f'Current Speed Bin {i}',
                    'units': 'cm/s',
                    'description': f'Current speed (cm/s) in bin {i}.',
                    'missing_sentinel': ['MM'],
                }
                for i in range(1, 39)
            },
        },
    },
    'swden': {
        'mode': 'swden',
        'name': 'Spectral Wave Density Data',
        'description': 'Non-directional spectral wave energy density (m^2/Hz) across 47 frequency bins (.0200 Hz to .4850 Hz).',
        'http_supported': True,
        'opendap_supported': True,
        'units': 'm^2/Hz',
        'columns': {},
    },
    'swdir': {
        'mode': 'swdir',
        'name': 'Spectral Wave Mean Direction (alpha1)',
        'description': 'Mean wave direction (degrees from true North) at each spectral frequency bin (.0200 Hz to .4850 Hz).',
        'http_supported': True,
        'opendap_supported': False,
        'units': 'degT',
        'columns': {},
    },
    'swdir2': {
        'mode': 'swdir2',
        'name': 'Spectral Wave Principal Direction (alpha2)',
        'description': 'Principal wave direction (degrees from true North) at each spectral frequency bin (.0200 Hz to .4850 Hz).',
        'http_supported': True,
        'opendap_supported': False,
        'units': 'degT',
        'columns': {},
    },
    'swr1': {
        'mode': 'swr1',
        'name': 'First Normalized Polar Coordinate (r1)',
        'description': 'First normalized directional Fourier coefficient r1 at each spectral frequency bin.',
        'http_supported': True,
        'opendap_supported': False,
        'units': '1',
        'columns': {},
    },
    'swr2': {
        'mode': 'swr2',
        'name': 'Second Normalized Polar Coordinate (r2)',
        'description': 'Second normalized directional Fourier coefficient r2 at each spectral frequency bin.',
        'http_supported': True,
        'opendap_supported': False,
        'units': '1',
        'columns': {},
    },
    'pwind': {
        'mode': 'pwind',
        'name': 'Peak Wind Data (OpenDAP)',
        'description': 'Peak wind speed and direction via THREDDS/OpenDAP.',
        'http_supported': False,
        'opendap_supported': True,
        'columns': {},
    },
    'wlevel': {
        'mode': 'wlevel',
        'name': 'Water Level Data (OpenDAP)',
        'description': 'Six-minute water level (tide) measurements via THREDDS/OpenDAP.',
        'http_supported': False,
        'opendap_supported': True,
        'columns': {},
    },
    'hfradar': {
        'mode': 'hfradar',
        'name': 'High-Frequency Radar Surface Currents (OpenDAP)',
        'description': 'Gridded surface ocean current velocity vectors from national HF Radar networks via THREDDS/OpenDAP.',
        'http_supported': False,
        'opendap_supported': True,
        'columns': {},
    },
}

# Build flat column -> unit lookup for DataFrame.attrs['units'] enrichment
COLUMN_UNITS: Dict[str, str] = {}
for _mode_info in MODE_METADATA.values():
    for _col_name, _col_meta in _mode_info.get('columns', {}).items():
        if _col_name not in COLUMN_UNITS and 'units' in _col_meta:
            COLUMN_UNITS[_col_name] = _col_meta['units']


def describe_mode(mode: Optional[str] = None) -> Dict[str, Any]:
    """Return structured metadata, physical units, and descriptions for NDBC modes.

    Args:
        mode: Optional mode name (e.g. `'stdmet'`, `'cwind'`, `'ocean'`).
            If `None`, returns a dictionary mapping all modes to their metadata.

    Returns:
        A `dict` describing the requested mode (or all modes if `mode` is `None`).

    Raises:
        RequestException: If `mode` is not a recognized NDBC mode.
    """
    if mode is None:
        return deepcopy(MODE_METADATA)
    if mode not in MODE_METADATA:
        raise RequestException(f'Mode {mode} is not available.')
    return deepcopy(MODE_METADATA[mode])

