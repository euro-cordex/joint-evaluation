#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import sys
from icecream import ic

study = sys.argv[1]
url = "dreq_EUR_joint_evaluation.csv"
dreq = pd.read_csv(url)
dreq.query('priority.str.contains(@study)', inplace=True)
dreq = dreq[['out_name', 'frequency']].rename(
    columns = {'out_name': 'variable_id'}
)
#
#  Plot variable availability as heatmap
#
data = pd.read_csv('catalog.csv', usecols=['variable_id', 'frequency', 'source_id', 'mip_era'])
data = data.merge(dreq, on=['variable_id', 'frequency'], how='right')
# Avoid showing different subdaily frequencies
#data['frequency'] = data['frequency'].replace('.hr', 'xhr', regex = True)
data.drop_duplicates(inplace = True)
# matrix with models as rows and variables as columns
matrix = data.pivot_table(index=['mip_era', 'source_id'], columns=['frequency', 'variable_id'], aggfunc='size', fill_value=0)
matrix = matrix.replace(0, np.nan)
ic(matrix)
# Plot as heatmap (make sure to show all ticks and labels)
plt.figure(figsize=(14,12))
plt.title(f'Variable availability for {study} study')
ax = sns.heatmap(matrix, cmap='YlGnBu_r', annot=False, cbar=False, linewidths=1, linecolor='lightgray')
ax.set_xticks(0.5+np.arange(len(matrix.columns)))
xticklabels = [f'{v}\n({f})' for f,v in matrix.columns]
xticklabels = (pd.Series(xticklabels)
  .replace(r'(.*) \(fx\)', r'\1 (fx)   ', regex=True)
  .replace(r'(.*) \(xhr\)', r'\1 (xhr)  ', regex=True)
).to_list()
ax.set_xticklabels(xticklabels)
ax.set_xlabel("variable (freq.)")
ax.set_yticks(0.5+np.arange(len(matrix.index)))
yticklabels = [f'{s}' for e,s in matrix.index]
ax.set_yticklabels(yticklabels, rotation=0)
ax.set_ylabel("source_id")
ax.set_aspect('equal')
plt.savefig(f'variable_availability__{study}.png', bbox_inches='tight')
