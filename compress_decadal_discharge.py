import glob
import logging
import os
import sys

import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    stream=sys.stdout,
)

compression_options = {
    'zlib': True,
    'complevel': 9,
    'shuffle': True,
}

for vpu_dir in [d for d in sorted(glob.glob('/Volumes/EB406_T7_2/geoglows2/retrospective_outputs/*')) if os.path.isdir(d)]:
    vpu_number = os.path.basename(vpu_dir)
    logging.info(f'Processing VPU {vpu_number}')

    global_attributes = {
        'author': 'Riley Hales, PhD',
        'title': f'GEOGloWS v2 Retrospective Discharge VPU {vpu_number}',
        'institution': 'Brigham Young University',
        'source': 'GEOGloWS v2',
        'history': 'Created 2023-08-23',
        'references': 'https://geoglows.ecmwf.int/',
    }

    for decade in range(1940, 2020, 10):
        logging.info(f'Processing decade {decade}')
        qout_files = glob.glob(os.path.join(vpu_dir, f'Qout_*{str(decade)[:3]}*.nc'))
        start_date = str(decade) + '0101'
        end_date = str(decade + 9) + '1231'
        out_file_path = os.path.join(vpu_dir, f'Qout_{vpu_number}_{start_date}_{end_date}.nc4')

        if os.path.exists(out_file_path):
            logging.info(f'Skipping {out_file_path}')
            continue

        # open the dataset and save it to a netcdf 4 format file with high compression
        with xr.open_mfdataset(qout_files) as ds:
            ds.attrs = global_attributes
            (
                ds
                .to_netcdf(
                    out_file_path,
                    format='NETCDF4',
                    encoding={
                        'Qout': compression_options,
                        'rivid': compression_options,
                        'lat': compression_options,
                        'lon': compression_options,
                        'time': compression_options,
                        'Qout_err': compression_options,
                    },
                )
            )

            # remove the original files
            for f in qout_files:
                os.remove(f)
