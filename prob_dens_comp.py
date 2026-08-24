#!/usr/bin/env python
# coding: utf-8
#
# Compute volume weighted particle probability densities 
#
# gulfstream_GO8p7 TRACMASS run
# Tracking for 4 years
# Time unit: seconds from the starting date 01-15-XXXX 00:00:00
# 120 iterartions per monthly gcm time step
# Seeding time step 12, scaled with VT (max transport per particle is 0.005 Sv)
# Output at cell crossings

import numpy as np
import pandas as pd 
import os
import dask.config  
from scipy import stats
import warnings
warnings.filterwarnings("ignore")

#os.environ["OMP_NUM_THREADS"] = "8" 

# Define horizontal bins (1/4 degree bins used)
bin_lon = np.arange(-100, 30, 1/4)
bin_lat = np.arange(5, 70, 1/4) 

data_path_part = 'YOUR_TRAJ_DATA_PATH'

files = [os.path.join(data_path_part, f) for f in os.listdir(data_path_part) if f.startswith('traj_') and f.endswith('.csv')]
files = sorted(files)
#print(files)

column_names = ['id', 'x', 'y', 'z', 'vt', 'time', 'wall','temp','salt']
cols_needed = ['id','x','y','vt']

# Loop over files and extract counts 
COUNTSvt = np.zeros((len(bin_lon) - 1, len(bin_lat) - 1, len(files)))
idx=0
year=1990

with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    for file_name in files:
        print(year)
    
        df = pd.read_csv(file_name, dtype={'id': int}, usecols=cols_needed) 
    
        plon=df['x'].values
        plat=df['y'].values
        pvt=df['vt'].values

        probVT = stats.binned_statistic_2d(plon,plat,pvt,statistic='sum',bins=[bin_lon, bin_lat])
        countsVT, xedges, yedges, _ = probVT
    
        COUNTSvt[:, :, idx] = countsVT
    
        idx = idx+1
        year = year+1

# Save particle probability densities 
yrs=np.arange(1990,2017+1,1)
np.savez('prob_dens_VTw_1990-2017.npz', COUNTSvt=COUNTSvt, xedges=xedges, yedges=yedges, yrs=yrs)
