# conda install -c conda-forge cdsapi

import cdsapi
import calendar

c = cdsapi.Client()

for year in range(1950, 2023):
    for month in range(1, 13):
        c.retrieve(
            'reanalysis-era5-land',
            {
                'format': 'netcdf.zip',
                'variable': 'runoff',
                'year': year,
                'month': str(month).zfill(2),
                'day': [str(x).zfill(2) for x in range(1, calendar.monthrange(year, month)[1] + 1)],
                'time': [f'{x:02d}:00' for x in range(0, 24)],
            },
            target=f'{year}_{str(month).zfill(2)}_era5land_hourly.netcdf.zip'
        )

for year in range(1940, 2023):
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': 'runoff',
            'year': year,
            'month': [str(x).zfill(2) for x in range(1, 13)],
            'day': [str(x).zfill(2) for x in range(1, 32)],
            'time': [f'{x:02d}:00' for x in range(0, 24)],
        },
        target=f'{year}_era5_hourly.nc'
    )