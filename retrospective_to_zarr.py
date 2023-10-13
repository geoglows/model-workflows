import glob
import logging
import os
import sys
from numcodecs import Blosc

import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    stream=sys.stdout,
)

vpu_dirs = [d for d in sorted(glob.glob('/Volumes/EB406_T7_2/geoglows2/v2_retrospective_outputs/*')) if os.path.isdir(d)]

for vpu_dir in vpu_dirs:
    vpu_number = os.path.basename(vpu_dir)
    logging.info(f'Processing VPU {vpu_number}')

    global_attributes = {
        'author': 'Riley Hales, PhD',
        'title': f'GEOGloWS v2 Retrospective Discharge',
        'institution': 'Brigham Young University',
        'source': 'GEOGloWS v2',
        'history': 'Created 2023-10-13',
        'references': 'https://geoglows.ecmwf.int/',
    }

    with xr.open_mfdataset(glob.glob(os.path.join(vpu_dir, 'Qout*.nc4'))) as ds:
        ds = ds.drop('crs').drop('Qout_err').drop('lat').drop('lon').drop('time_bnds')
        ds.attrs = global_attributes
        # chunk sizes is interpreted as number of items in the chunk, not the number of chunks
        chunk_sizes = {
            'time': ds.variables['time'].shape[0],
            'rivid': "auto",
        }
        zarr_compressor = Blosc(cname='zstd', clevel=9, shuffle=Blosc.BITSHUFFLE)
        zarr_enc = {
            x: {'compressor': zarr_compressor} for x in ds.variables
        }
        ds = ds.chunk(chunk_sizes)
        output_dir = f'/Volumes/DrHalesT7/retroouputs/'
        os.makedirs(output_dir, exist_ok=True)
        combined_output_file_name = os.path.join(output_dir, f'qout_geoglows_v2.zarr')

        if os.path.exists(combined_output_file_name):
            logging.info(f'Skipping {combined_output_file_name}')

        (
            ds
            .to_zarr(
                combined_output_file_name,
                zarr_version=2,
                encoding=zarr_enc,
            )
        )

