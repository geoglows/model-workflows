import logging
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
    'institution': 'Brigham Young University',
    'source': 'GEOGloWS v2',
    'history': 'Created 2023-10-13',
    'references': 'https://geoglows.ecmwf.int/',
}

with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    logging.info('opening dataset')
    with xr.open_mfdataset('/Volumes/EB406_T7_2/geoglows2/v2_retrospective_outputs/*/Qout_*.nc4',
                           concat_dim='rivid',
                           combine='nested', ) as ds:
        logging.info('dropping variables')
        ds = ds.drop('crs').drop('Qout_err').drop('lat').drop('lon').drop('time_bnds')
        ds.attrs = {
            'author': 'Riley Hales, PhD',
            'title': f'GEOGloWS v2 Retrospective Discharge',
            'institution': 'Brigham Young University',
            'source': 'GEOGloWS v2',
            'history': 'Created 2023-10-13',
            'references': 'https://geoglows.ecmwf.int/',
        }
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
        output_file = f'/Volumes/DrHalesT7/geoglows_v2_retrospective.zarr'
        logging.info(f'Writing to {output_file}')
        with dask.config.set(scheduler='threads'):
            delayed_task = ds.to_zarr(
                output_file,
                zarr_version=2,
                encoding=zarr_enc,
                compute=False,
            )

            # set up a progress bar
            progress = ProgressBar()
            progress.register()

            # compute the task
            dask.compute(delayed_task)
        logging.info('Done')
