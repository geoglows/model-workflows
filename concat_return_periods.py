import xarray as xr
import glob

ds = xr.open_mfdataset(sorted(glob.glob('/Volumes/EB406_T7_2/geoglows2/return-periods/*.nc')), combine='nested', concat_dim='rivid')

return_periods = xr.concat([ds['rp2'], ds['rp5'], ds['rp10'], ds['rp25'], ds['rp50'], ds['rp100'],], dim='return_period', coords='minimal')
return_periods = return_periods.assign_coords({'return_period': ([2, 5, 10, 25, 50, 100])})
return_periods

(
    xr
    .Dataset(
        coords={
            'rivid': (ds.rivid),
            'return_period': ([2, 5, 10, 25, 50, 100]),
        },
        data_vars={
            'rp_flow': return_periods,
            'max_flow': ds.qout_max,
        },
        attrs={
            'author': 'Riley Hales, PhD',
            'description': 'Return periods and maximum simulated flow between 1940 and 2022 for GEOGloWS V2',
            'institution': 'Group on Earth Observations Global Water Sustainability Initiative',
            'license': 'CC BY 4.0',
        }
    )
    .chunk({'return_period': -1, 'rivid': 100000})
    .to_zarr('/Volumes/EB406_T7_2/geoglows2/return-periods.zarr', mode='w')
)