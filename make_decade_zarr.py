import glob
import logging
import os
import sys
from multiprocessing import Pool

import xarray as xr

ds_attrs = {
    'author': 'Riley Hales, PhD',
    'history': 'Created 2023-10-26',
    'institution': 'GEOGloWS',
    'references': 'https://geoglows.ecmwf.int/',
    'source': 'GEOGloWS Hydrologic Model Version 2',
    'title': 'GEOGloWS V2 Retrospective Simulation',
}

time_attrs = {
    'units': 'seconds since 1970-01-01',
    'calendar': 'standard',
    'axis': 'T',
    'standard_name': 'time'
}

vars_to_drop = ['Qout_err', 'lat', 'lon', 'crs', 'time_bnds']
outputs_path = '/mnt/outputs/'


def make_decade_zarr(decade):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        filename=f'/home/ubuntu/decadezarr_{decade}.log',
    )
    all_vpu_nc_for_decade = os.path.join(outputs_path, '*', f'Qout_*_{str(decade)[:3]}*0101_{str(decade)[:3]}*1231.nc')
    all_vpu_nc_for_decade = sorted(glob.glob(all_vpu_nc_for_decade))
    logging.info(f'Decade {decade}')
    logging.info(f'Found {len(all_vpu_nc_for_decade)} files')
    logging.info(all_vpu_nc_for_decade)
    with xr.open_mfdataset(all_vpu_nc_for_decade,
                           concat_dim='rivid',
                           combine='nested', ) as ds:
        ds.attrs = ds_attrs
        chunk_sizes = {
            'time': ds.variables['time'].shape[0],
            'rivid': 1500,
        }
        logging.info(f'Writing to /data/retro_{decade}.zarr')
        (
            ds
            .drop_vars(vars_to_drop)
            .chunk(chunk_sizes)
            .to_zarr(
                f'/data/retro_{decade}.zarr',
                zarr_version=2,
            )
        )


if __name__ == '__main__':
    decades = list(range(1940, 2030, 10))
    with Pool(len(decades)) as p:
        p.map(make_decade_zarr, decades)
