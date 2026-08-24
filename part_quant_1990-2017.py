#!/usr/bin/env python
# coding: utf-8
#
# Produce throughput/recirc. time series
#
# gulfstream_GO8p7 TRACMASS run
# Tracking for 4 years
# Time unit: seconds from the starting date 01-15-XXXX 00:00:00
# 120 iterartions per monthly gcm time step
# Seeding time step 12, scaled with VT (max transport per particle is 0.005 Sv)
# Output at cell crossings

import pandas as pd
import os
import dask.config
import warnings
warnings.filterwarnings("ignore")

#os.environ["OMP_NUM_THREADS"] = "8" 

#############################################
# Read in TRACMASS output and compute volume transports of recirculation vs throughput per year
data_path_part = 'YOUR_TRAJ_DATA_PATH' #directory of stored data

column_names = ['id', 'x', 'y', 'z', 'vt', 'time', 'wall','temp','salt'] 
cols_needed = ['id','y','vt']

files = [os.path.join(data_path_part, f) for f in os.listdir(data_path_part) if f.startswith('traj_') and f.endswith('.csv')]
files = sorted(files)

vtSPG_data = [] #initialize lists to store the volume transports 
vtSTG_data = []

year = 1990 #start year

###########################################
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    for file_name in files:

        print(year)

        df = pd.read_csv(file_name, dtype={'id': int}, usecols=cols_needed)

        # Quantification - recirc. vs. throughput 
        id_ex = df['id'].unique()   #unique particle IDs

        id_stg = [] # Initialize lists for particle classification
        id_spg = []

        vtSPG=0
        vtSTG=0

        # Group data by each particle ID
        for particle_id, particle_data in df.groupby('id'):
            latmax = particle_data['y'].max()    #max latitude for particle
    
            # Classify based on maximum latitude
            if latmax < 45:
                id_stg.append(particle_id)  #stays in STG
                vtSTG = vtSTG + particle_data['vt'].iloc[0] #total for the year   
            else:
                id_spg.append(particle_id)  #exported to SPG (throughput)
                vtSPG = vtSPG + particle_data['vt'].iloc[0] #total for the year
    
        # Store results in dictionaries with year information
        vtSPG_data.append({'year': year, 'vtSPG': vtSPG})
        vtSTG_data.append({'year': year, 'vtSTG': vtSTG})
        
        year = year+1

###############################################

## Convert lists of dictionaries to DataFrames
df_vtSPG = pd.DataFrame(vtSPG_data)
df_vtSTG = pd.DataFrame(vtSTG_data)

## Save DataFrames as CSV files
df_vtSPG.to_csv('vtSPG_an_1990-2017_data.csv', index=False)
df_vtSTG.to_csv('vtSTG_an_1990-2017_data.csv', index=False)

print("Data saved to vtSPG_an_1990-2017_data.csv and vtSTG_an_1990-2017_data.csv")
