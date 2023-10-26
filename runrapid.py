import argparse
import datetime
import glob
import os
import subprocess
from multiprocessing import Pool


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_directory(path_to_rapid_executable: str,
                                     namelist_dir: str, ) -> None:
    watershed_id = os.path.basename(namelist_dir)
    with open(f'/mnt/logs/{watershed_id}.log', 'w') as f:
        for namelist in sorted(glob.glob(os.path.join(namelist_dir, '*namelist*'))):
            try:
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
                f.write(f'Failed to run RAPID for {namelist}')

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
    parser = argparse.ArgumentParser()
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )
    parser.add_argument('--namelistsdir', type=str, required=False,
                        default='/mnt/namelists',
                        help='Path to directory containing subdirectories of namelist files', )

    args = parser.parse_args()
    path_to_rapid_exec = args.rapidexec
    namelists_dirs = args.namelistsdir

    namelists_dirs = [d for d in glob.glob(os.path.join(namelists_dirs, '*')) if os.path.isdir(d)]

    cpu_count = min([os.cpu_count(), len(namelists_dirs)])
    print(f'Found {len(namelists_dirs)} input directories')
    print(f'Have {os.cpu_count()} cpus')
    print(f'Using {cpu_count} cpus')

    with Pool(cpu_count) as p:
        p.starmap(run_rapid_for_namelist_directory, [(path_to_rapid_exec, d) for d in namelists_dirs])
