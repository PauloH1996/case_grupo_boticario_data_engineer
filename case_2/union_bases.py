import os
import pandas as pd

local_files = 'base_dados/'

df = []

for file in os.listdir(local_files):
    if file.endswith('.xlsx'):
        print(f'Carregando arquivo {file}...')
        df.append(pd.read_excel(os.path.join(local_files, file)))

df_master = pd.concat(df, axis=0)
df_master.to_csv('base_dados/union_bases.csv', index=False)