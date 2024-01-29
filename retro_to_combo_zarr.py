import logging
import os
import sys

import dask
import xarray as xr
from dask.diagnostics import ProgressBar
from numcodecs import Blosc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    stream=sys.stdout,
)

global_attributes = {
    'author': 'Riley Hales, PhD',
    'title': f'GEOGloWS v2 Retrospective Discharge',
    'institution': 'Group on Earth Observations Global Water Sustainability Program',
    'source': 'GEOGloWS Hydrologic Model v2',
    'history': 'Created 2024-01-29',
    'references': 'https://geoglows.org/',
}

# set up a progress bar
progress = ProgressBar()
progress.register()

with dask.config.set(**{
    'array.slicing.split_large_chunks': True,
    'array.chunk-size': '5MB',
}):
    for decade in range(1940, 2030, 10):
        logging.info(f'opening dataset {decade}')
        # filter the dataset to only include the current year
        output_file = f'/mnt/geoglows_v2_retrospective_{decade}.zarr'
        if os.path.exists(output_file):
            logging.info(f'Skipping {output_file}')
            continue

        with xr.open_mfdataset(f'/mnt/outputs/*/Qout_*_{str(decade)[:3]}*0101*.nc',
                               concat_dim='rivid',
                               combine='nested', ) as ds:
            logging.info('dropping variables')
            ds = ds.drop('crs').drop('Qout_err').drop('lat').drop('lon').drop('time_bnds')
            ds.attrs = global_attributes
            chunk_sizes = {
                'time': ds.variables['time'].shape[0],
                'rivid': "auto",
            }
            zarr_compressor = Blosc(cname='zstd', clevel=9, shuffle=Blosc.BITSHUFFLE)
            zarr_enc = {
                x: {'compressor': zarr_compressor} for x in ds.variables
            }
            logging.info('chunking')
            ds = ds.chunk(chunk_sizes)
            logging.info(f'Writing to {output_file}')
            with dask.config.set(scheduler='threads'):
                delayed_task = (
                    ds
                    .to_zarr(
                        output_file,
                        zarr_version=2,
                        encoding=zarr_enc,
                        compute=False,
                    )
                )

                # compute the task
                dask.compute(delayed_task)
                logging.info('Done')

    # combine the all-vpu-1-year-files into a single larger zarr file
    logging.info('opening yearly zarr files')
    with xr.open_mfdataset('/mnt/geoglows_v2_retrospective_*.zarr',
                           concat_dim='time',
                           combine='nested',
                           parallel=True,
                           engine='zarr', ) as ds:
        ds.attrs = global_attributes
        chunk_sizes = {
            'time': 29220,  # hard pin at 80 years (1940-2019 inclusive)
            'rivid': "auto",
        }
        zarr_compressor = Blosc(cname='zstd', clevel=9, shuffle=Blosc.BITSHUFFLE)
        zarr_enc = {
            x: {'compressor': zarr_compressor} for x in ds.variables
        }
        logging.info('chunking')
        ds = ds.chunk(chunk_sizes)
        logging.info('Writing to /mnt/geoglows_v2_retrospective.zarr')
        with dask.config.set(scheduler='threads'):
            delayed_task = (
                ds
                .to_zarr(
                    '/mnt/geoglows_v2_retrospective.zarr',
                    zarr_version=2,
                    encoding=zarr_enc,
                    compute=False,
                )
            )

            # compute the task
            dask.compute(delayed_task)
            logging.info('Done')
