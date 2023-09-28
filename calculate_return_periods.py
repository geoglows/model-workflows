import glob
import os

import numpy as np
import pandas as pd
import xarray as xr
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    stream=sys.stdout,
)


def gumbel1(rp: int, xbar: np.array or float, std: np.array or float):
    """
    Calculate the Gumbel Type 1 distribution for a given return period

    Args:
        rp: Return period
        xbar: Mean of the distribution
        std: Standard deviation of the distribution

    Returns:
        The value of the distribution for the given return period
    """
    return np.round(-np.log(-np.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std), 3)


for vpu_dir in [d for d in sorted(glob.glob('/Volumes/EB406_T7_2/geoglows2/outputs/*')) if os.path.isdir(d)]:
    vpu_number = os.path.basename(vpu_dir)

    if os.path.exists(f'/Volumes/EB406_T7_2/geoglows2/return_periods/{vpu_number}/returnperiods_{vpu_number}.nc'):
        logging.info(f'Skipping VPU {vpu_number}')
        continue

    logging.info(f'Processing VPU {vpu_number}')
    df = pd.DataFrame(columns=['rivid', 'qout_max', 'rp2', 'rp5', 'rp10', 'rp25', 'rp50', 'rp100'])

    with xr.open_mfdataset(os.path.join(vpu_dir, 'Qout*.nc')) as ds:
        # get the size of the rivid dimension and read the Qout variable in 5 chunks
        num_chunks = 15
        chunk_size = int(np.ceil(len(ds.rivid) / num_chunks))
        for chunk in range(num_chunks):
            rivids = ds.rivid.isel(rivid=slice(chunk * chunk_size, (chunk + 1) * chunk_size)).values
            qout = (
                ds
                .Qout
                .isel(rivid=slice(chunk * chunk_size, (chunk + 1) * chunk_size))
                .to_dataframe()
                .reset_index()
                .pivot(index='time', columns='rivid', values='Qout')
            )
            qout = qout.groupby(qout.index.year).max()

            qout_mean = np.nanmean(qout, axis=0)
            qout_std = np.nanstd(qout, axis=0)

            df = pd.concat([
                df,
                pd.DataFrame({
                    'rivid': rivids,
                    'qout_max': np.nanmax(qout, axis=0),
                    'rp2': gumbel1(2, qout_mean, qout_std),
                    'rp5': gumbel1(5, qout_mean, qout_std),
                    'rp10': gumbel1(10, qout_mean, qout_std),
                    'rp25': gumbel1(25, qout_mean, qout_std),
                    'rp50': gumbel1(50, qout_mean, qout_std),
                    'rp100': gumbel1(100, qout_mean, qout_std),
                })
            ], axis=0)

    logging.info('Writing NetCDF')
    os.makedirs(f'/Volumes/EB406_T7_2/geoglows2/return_periods/{vpu_number}', exist_ok=True)
    # save to netcdf
    (
        xr
        .Dataset(
            coords={
                'rivid': df.rivid.values,
            },
            data_vars={
                'qout_max': ('rivid', df.qout_max.values),
                'rp2': ('rivid', df.rp2.values.astype(float)),
                'rp5': ('rivid', df.rp5.values.astype(float)),
                'rp10': ('rivid', df.rp10.values.astype(float)),
                'rp25': ('rivid', df.rp25.values.astype(float)),
                'rp50': ('rivid', df.rp50.values.astype(float)),
                'rp100': ('rivid', df.rp100.values.astype(float)),
            },
            attrs={
                'description': 'Calculated using annual maximum flows and the Gumbel Type 1 distribution',
                'author': 'Riley Hales, PhD',
            },
        )
        .to_netcdf(os.path.join(f'/Volumes/EB406_T7_2/geoglows2/return_periods/{vpu_number}/returnperiods_{vpu_number}.nc'))
    )
