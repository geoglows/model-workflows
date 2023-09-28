import glob
import os

for watershed_output in sorted(glob.glob('/mnt/outputs/*')):
    # identify the watershed being checked
    watershed_id = os.path.basename(watershed_output)

    # look for files in the output directory
    qout_file = glob.glob(os.path.join(watershed_output, 'Qout*.nc'))
    qout_size = sum([os.path.getsize(x) for x in qout_file])

    inflow_files = glob.glob(os.path.join(f'/mnt/inflows/{watershed_id}/*.nc'))
    inflow_size = sum([os.path.getsize(x) for x in inflow_files])
    # calculate percent completed
    pct_complete = (qout_size / inflow_size) * 100

    if pct_complete >= 100:
        print(f'{watershed_id}: COMPLETE {round(pct_complete, 1)}%')
    else:
        print(f'{watershed_id}: {round(pct_complete, 1)}%')

