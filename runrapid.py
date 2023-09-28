import datetime
import glob
import os
import subprocess
import sys
from multiprocessing import Pool


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_directory(path_to_rapid_executable: str,
                                     namelist_dir: str, ) -> None:
    watershed_id = os.path.basename(namelist_dir)
    with open(f'/mnt/logs/{watershed_id}', 'w') as f:
        try:
            for namelist in sorted(glob.glob(os.path.join(namelist_dir, '*namelist*'))):
                f.write(f'{timestamp()}: Running RAPID for {namelist}')
                subprocess.call(
                    [path_to_rapid_executable, '--namelist', namelist, '--ksp_type', 'preonly'],
                    stdout=f,
                    stderr=f,
                )
                f.write(f'{timestamp()}: Finished RAPID for {namelist}')
        except Exception as e:
            print(e)
            f.write(e)
            f.write(f'Failed to run RAPID for {namelist_dir}')

    return


if __name__ == '__main__':
    """
    Run RAPID for all namelist files in a specified directory

    Usage:
        python run_rapid.py <path_to_rapid_executable> <namelists_dir>

    Directory structure:
        <namelist_directory>
            rapid_namelist_<watershed_1_id>
            rapid_namelist_<watershed_2_id>
            rapid_namelist_<watershed_3_id>
            ...

    Returns:
        None
    """
    path_to_rapid_exec = sys.argv[1]
    namelists_dirs = sys.argv[2]

    namelists_dirs = [d for d in glob.glob(os.path.join(namelists_dirs, '*')) if os.path.isdir(d)]

    print(f'Found {len(namelists_dirs)} input directories')

    with Pool(min([os.cpu_count(), len(namelists_dirs), 3])) as p:
        p.starmap(run_rapid_for_namelist_directory,
                  [(path_to_rapid_exec, d) for d in namelists_dirs])
