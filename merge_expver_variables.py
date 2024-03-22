import glob

import numpy as np
import xarray as xr

for file in glob.glob('./2023era5/*.nc'):
    with xr.open_dataset(file) as ds:
        # find the time steps where the runoff is not nan when expver=1
        a = ds.ro.sel(latitude=0, longitude=0, expver=1)
        expver1_timesteps = a.time[~np.isnan(a)]

        # find the time steps where the runoff is not nan when expver=5
        b = ds.ro.sel(latitude=0, longitude=0, expver=5)
        expver5_timesteps = b.time[~np.isnan(b)]

        # assert that the two timesteps combined are the same as the original
        assert len(ds.time) == len(expver1_timesteps) + len(expver5_timesteps)

        new_file_name = file.replace('.nc', '_noexpver_compressed.nc')

        # combine the two
        (
            xr
            .concat(
                [
                    ds.sel(expver=1, time=expver1_timesteps.values).drop_vars('expver'),
                    ds.sel(expver=5, time=expver5_timesteps.values).drop_vars('expver')
                ],
                dim='time'
            )
            .to_netcdf(
                new_file_name,
                encoding={
                    'ro': {'zlib': True, 'complevel': 9}
                }
            )
        )
