import glob
import os
import datetime
import multiprocessing

import xarray as xr
import netCDF4 as nc
import s3fs
import pandas as pd

return_periods_dir = '/Users/ricky/Downloads/rps'
rps = glob.glob(return_periods_dir +'/*.nc')
output_dir = '/Volumes/EB406_T7_3/half_initialization'

# Create an S3FileSystem instance to get comid files from geoglows bucket
s3 = s3fs.S3FileSystem(anon=True)
vpu_folders = s3.ls('geoglows-v2/configs/')
comids = {str: str}
for vpu in vpu_folders:
    comids[vpu.split('/')[-1]] = [f for f in s3.ls(vpu) if '/comid' in f][0]

def main():
    processes = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=processes) as pool:
        pool.map(helper, [sublist for sublist in split_into_sublists(rps,processes)])
    print('Finished')

def helper(rps: list) -> None:
    for rp in rps:
        ds = xr.open_dataset(rp)
        vpu = rp.split('_')[-1][:3]
        with s3.open(comids[vpu], 'r') as f:
            df = pd.read_csv(f)

        out_file = os.path.join(output_dir, f"Qinit_{vpu}_half_rp2.nc")

        with nc.Dataset(out_file, "w", format="NETCDF3_CLASSIC") as inflow_nc:
            # create dimensions
            inflow_nc.createDimension('time', 1)
            inflow_nc.createDimension('rivid', df.TDXHydroLinkNo.shape[0])

            qout_var = inflow_nc.createVariable('Qout', 'f8', ('time', 'rivid'))
            qout_var[:] = ds.rp2.values / 2
            qout_var.long_name = 'instantaneous river water discharge downstream of each river reach'
            qout_var.units = 'm3 s-1'
            qout_var.coordinates = 'lon lat'
            qout_var.grid_mapping = 'crs'
            qout_var.cell_methods = "time: point"

            # rivid
            rivid_var = inflow_nc.createVariable('rivid', 'i4', ('rivid',))
            rivid_var[:] = ds.rivid.values
            rivid_var.long_name = 'unique identifier for each river reach'
            rivid_var.units = '1'
            rivid_var.cf_role = 'timeseries_id'

            # time
            time_var = inflow_nc.createVariable('time', 'i4', ('time',))
            time_var[:] = [0]
            time_var.long_name = 'time'
            time_var.standard_name = 'time'
            time_var.units = f'seconds since 1939-12-31'  # Must be seconds
            time_var.axis = 'T'
            time_var.calendar = 'gregorian'

            # longitude
            lon_var = inflow_nc.createVariable('lon', 'f8', ('rivid',))
            lon_var[:] = df['lon'].values
            lon_var.long_name = 'longitude of a point related to each river reach'
            lon_var.standard_name = 'longitude'
            lon_var.units = 'degrees_east'
            lon_var.axis = 'X'

            # latitude
            lat_var = inflow_nc.createVariable('lat', 'f8', ('rivid',))
            lat_var[:] = df['lat'].values
            lat_var.long_name = 'latitude of a point related to each river reach'
            lat_var.standard_name = 'latitude'
            lat_var.units = 'degrees_north'
            lat_var.axis = 'Y'

            # crs
            crs_var = inflow_nc.createVariable('crs', 'i4')
            crs_var.grid_mapping_name = 'latitude_longitude'
            crs_var.epsg_code = 'EPSG:4326'  # WGS 84
            crs_var.semi_major_axis = 6378137.0
            crs_var.inverse_flattening = 298.257223563

            # add global attributes
            inflow_nc.Conventions = 'CF-1.6'
            inflow_nc.history = 'date_created: {0}'.format(datetime.datetime.utcnow())
            inflow_nc.featureType = 'timeSeries'

        ds.close()

def split_into_sublists(lst, n):
    # Calculate the size of each sublist
    sublist_size = len(lst) // n

    # Use list comprehension to create sublists
    sublists = [lst[i * sublist_size:(i + 1) * sublist_size] for i in range(n - 1)]
    
    # Add the remaining elements to the last sublist
    sublists.append(lst[(n - 1) * sublist_size:])
    
    return sublists

if __name__ == '__main__':
    main()
