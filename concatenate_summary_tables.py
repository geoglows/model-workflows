import argparse
import logging
import sys

import glob
import os

import pandas as pd

if __name__ == '__main__':
    """
    Combines the map_style_tables from each VPU into 1 CSV file per time step with rows from all VPUs
    
    Arguments:
    --date: Date string of forecast in YYYYMMDD format
    --outputsdir: Path to the parent directory containing subdirectories for each VPU
    --savedir: Directory for saving map_style_tables (should be called map_style_tables)
    
    Usage:
    python concatenate_map_style_tables.py
            --date 20230927
            --outputsdir /path/to/geoglows2/outputs
            --savedir /path/to/geoglowsv2/map_style_tables
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True,
                        help='Date string of forecast in YYYYMMDD format')
    parser.add_argument('--outputsdir', type=str, required=True,
                        help='Path to the parent directory containing subdirectories for each VPU')
    parser.add_argument('--savedir', type=str, required=True,
                        help='Directory for saving map_style_tables with subdirectories for each forecast date')

    args = parser.parse_args()
    date_string = args.date
    outputsdir = args.outputsdir
    savedir = args.output

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        stream=sys.stdout)

    logging.debug(f'Arg --date: {date_string}')
    logging.debug(f'Arg --outputsdir: {outputsdir}')
    logging.debug(f'Arg --savedir: {savedir}')

    # select all outputs/VPUNUMBER/DATE/map_style_table*.parquet files
    logging.info('Concatenating parquet map_style_tables from each VPU')
    global_map_style_df = pd.concat([
        pd.read_parquet(x) for x in glob.glob(os.path.join(outputsdir, '*', date_string, 'map_style_table*.parquet'))
    ])

    # replace nans with 0
    logging.info('Preparing concatenated DF')
    global_map_style_df.fillna(0, inplace=True)
    global_map_style_df.set_index('timestamp', inplace=True)

    # for each unique date in the timestamp column, create a new dataframe and write it to csv
    savedir = os.path.join(str(savedir), date_string)
    os.makedirs(savedir, exist_ok=True)

    for idx, date in enumerate(global_map_style_df.index.unique()):
        file_save_path = os.path.join(savedir, f'mapstyletable_{date.strftime("%Y-%m-%d-%H")}.csv')
        logging.info(f'Writing map_style_table for {date}')
        logging.debug(f'Saving to {file_save_path}')
        (
            global_map_style_df
            .loc[date]
            .to_csv(
                file_save_path,
                index=False
            )
        )
