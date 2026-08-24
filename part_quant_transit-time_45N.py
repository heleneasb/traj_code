#!/usr/bin/env python
# coding: utf-8
#
# Compute transit times to 45N
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
import warnings
warnings.filterwarnings("ignore")

###########################################################
# Read in TRACMASS output and compute trasit times to 45N
data_path_part = '/Data/gfi/scratch/has078/Tracmass_working_dir/output/gulfstream_GO8p7/' #directory of stored data

column_names = ['id', 'x', 'y', 'z', 'vt', 'time', 'wall','temp','salt']
cols_needed = ['id','y','time']

files = [os.path.join(data_path_part, f) for f in os.listdir(data_path_part) if f.startswith('traj_') and f.endswith('.csv')]
files = sorted(files) 

bins = np.arange(0, 4.25, 0.25) #bins for histogram
cumulative_counts = np.zeros(len(bins) - 1)  #initialize counts for each bin
fil = 0
numb_part = 0

year = 1990 #start year

##########################################################
for file_name in files:
    print(fil)

    df = pd.read_csv(file_name, dtype={'id': int})
    df = df[cols_needed]

    # Seeding time per particle (first time entry)
    seeding_times = (df
        .groupby('id', as_index=False)
        .first()[['id', 'time']]
        .rename(columns={'time': 'seed_time'})
    )

    # Crossing criterion 
    df_crit = df[df['y'] >= 45]

    # First crossing per particle
    crossing_times = (df_crit
        .groupby('id', as_index=False)
        .first()[['id', 'time']]
        .rename(columns={'time': 'cross_time'})
    )

    # Merge seeding and crossing times 
    merged = crossing_times.merge(seeding_times, on='id', how='left')

    # Transit time in years (crossing time minus seeding time)
    transit_times_yrs = ((merged['cross_time'] - merged['seed_time'])/ (60 * 60 * 24 * 365) ) #time in seconds --> time in years

    counts, _ = np.histogram(transit_times_yrs, bins=bins)
    cumulative_counts += counts

    # Total seeded particles (all ids)
    numb_part += df['id'].nunique()

    fil = fil+1
    del df
#################################################################

# Save counts for histogram
np.savez('histogram_1990-2017_data.npz', bins=bins, counts=cumulative_counts, numb_parts_tot=numb_part)

print("Data saved to histogram_1990-2017_data.npz")
