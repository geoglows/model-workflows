import glob
import os

for watershed_output in sorted(glob.glob('/rapidio/output/*')):
    # identify the watershed being checked
    watershed_id = os.path.basename(watershed_output)

    # look for files in the output directory
    qout_file = glob.glob(os.path.join(watershed_output, 'Qout*.nc'))
    if len(qout_file) == 0:
        print(f'{watershed_id}: No Qout file found')
        continue
    m3_file = glob.glob(os.path.join(watershed_output, 'm3_riv*.nc'))
    if len(m3_file) == 0:
        print(f'{watershed_id}: No m3 file found')
        continue

    # select the first (only) file in the list
    qout_file = qout_file[0]
    m3_file = m3_file[0]

    # check the file size of each file
    qout_file_size = os.path.getsize(qout_file)
    m3_file_size = os.path.getsize(m3_file)

    # calculate percent completed
    pct_complete = qout_file_size / m3_file_size * 100

    if pct_complete >= 100:
        print(f'{watershed_id}: COMPLETE {round(pct_complete, 1)}%')
    else:
        print(f'{watershed_id}: {round(pct_complete, 1)}%')

