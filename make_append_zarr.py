import glob
import logging
import os
import sys

import numpy as np
import pandas as pd

import dask
import xarray as xr

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        stream=sys.stdout,
    )

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
    zarr_path = '/data/retro.zarr'
    vpu_dirs_path = '/mnt/outputs/*'
    vpus = sorted([d for d in glob.glob(vpu_dirs_path) if os.path.isdir(d)])

    with dask.config.set(**{
        'array.slicing.split_large_chunks': True,
        'array.chunk-size': '100MB',
    }):
        logging.info('reading first file')
        first_vpu = vpus.pop(0)
        logging.info(f'first vpu dir is {first_vpu}')
        with xr.open_mfdataset(os.path.join(first_vpu, 'Qout*.nc*')) as ds:
            # number of elements along each dimension to write to a sub file
            chunks = {
                'time': ds['time'].size,
                'rivid': 'auto',
            }
            times = pd.to_datetime(ds['time'].values).values.astype('datetime64[s]').astype(np.int64)
            ds['time'] = xr.DataArray(times, dims='time', attrs=time_attrs)
            ds.attrs = ds_attrs
            logging.info('writing first file')
            (
                ds
                .drop_vars(vars_to_drop)
                .chunk(chunks)
                .to_zarr(zarr_path, mode='w')
            )

        for vpu in vpus:
            logging.info(f'reading file {vpu}')
            with xr.open_mfdataset(os.path.join(vpu, 'Qout*.nc*')) as ds:
                chunks = {
                    'time': ds['time'].size,
                    'rivid': 'auto',
                }
                logging.info('setting correct time values')
                times = pd.to_datetime(ds['time'].values).values.astype('datetime64[s]').astype(np.int64)
                ds['time'] = xr.DataArray(times, dims='time', attrs=time_attrs)

                ds.attrs = ds_attrs
                logging.info(f'appending files from {vpu}')
                (
                    ds
                    .drop_vars(vars_to_drop)
                    .chunk(chunks)
                    .assign_attrs(ds_attrs)
                    .to_zarr(zarr_path, append_dim='rivid', mode='a')
                )
