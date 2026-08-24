#!/usr/bin/env python
# coding: utf-8
#
# Determine depth bin of particles crossing 45N
#
# gulfstream_GO8p7 TRACMASS run
# Tracking for 4 years
# Time unit: seconds from the starting date 01-15-XXXX 00:00:00
# 120 iterartions per monthly gcm time step
# Seeding time step 12, scaled with VT (max transport per particle is 0.005 Sv)
# Output at cell crossings

import xarray as xr
import numpy as np
import pandas as pd 
import os
import dask.config  
import warnings
warnings.filterwarnings("ignore")

#os.environ["OMP_NUM_THREADS"] = "8" 

# Depth bins 50m
dep_bins = [0,50,100,150,200,250,300,350,400,450,500,550,600,650,700,750,800,850,900,950,1000,1050,1100]
exp_Dbins = np.arange(1, len(dep_bins))

data_path_part = 'YOUR_TRAJ_DATA_PATH'

column_names = ['id', 'x', 'y', 'z', 'vt', 'time', 'wall','temp','salt']
cols_needed = ['id','y','z','vt','time']

# .csv files in folder
files = [os.path.join(data_path_part, f) for f in os.listdir(data_path_part) if f.startswith('traj_') and f.endswith('.csv')]
files = sorted(files)
#print(files)

dbin_rel   = np.zeros((len(files), len(dep_bins)-1))  
dbin_cross = np.zeros((len(files), len(dep_bins)-1))   

n = 0
year = 1990

with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    for file_name in files:
        print(year)
        
        df = pd.read_csv(file_name, dtype={'id': int}, usecols=cols_needed) 
        
        # set invalid entries to nan, and drop nan entries
        df['z'] = pd.to_numeric(df['z'], errors='coerce')
        df = df.dropna(subset=['z'])

        ###### particles crossing 45N ######
        max_y = df.groupby('id')['y'].max()
        spg_ids = max_y[max_y >= 45].index
        spg_df = df[df['id'].isin(spg_ids)]
        
        # 1) RELEASE properties
        release_spg = spg_df.groupby('id').first().reset_index()
        
        # binning
        release_spg['depth_bin'] = np.digitize(release_spg['z'], bins=dep_bins)
        
        # sum volume transports
        vt_binD_rel = release_spg.groupby('depth_bin')['vt'].sum().reindex(exp_Dbins, fill_value=0)

        dbin_rel[n,:] = vt_binD_rel.values

        # 2) CROSSING properties
        cross_spg = spg_df[spg_df['y'] >= 45].sort_values(['id','time']).groupby('id').first().reset_index() #vectorized version

        # binning
        cross_spg['depth_bin'] = np.digitize(cross_spg['z'], bins=dep_bins)

        # sum volume transports
        vt_binD_cross = cross_spg.groupby('depth_bin')['vt'].sum().reindex(exp_Dbins, fill_value=0)

        dbin_cross[n,:] = vt_binD_cross.values

        n += 1
        year += 1

## Save vt per bin
yrs=np.arange(1990,2017+1,1)
dep_rel=dbin_rel.T/1e+6/12
dep_cross=dbin_cross.T/1e+6/12
np.savez('dep_init_cross45N_50m_bin.npz', yrs=yrs, dep_rel=dep_rel, dep_cross=dep_cross)

