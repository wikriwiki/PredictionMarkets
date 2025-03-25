import pandas as pd
from tqdm import tqdm
from datetime import datetime,timedelta

df_available = pd.read_csv('data/available.csv')

data_list = []

for i in tqdm(range(len(df_available))):
    id = df_available['condition_id'][i]
    start_date = datetime.strptime(df_available['start_date'][i],"%Y-%m-%d %H:%M:%S%z")
    end_date = datetime.strptime(df_available['end_date'][i],"%Y-%m-%d %H:%M:%S%z")

    period = end_date - start_date
    if period.days >= 5:
        data_list.append({'condition_id':id,'period':period.days})

df_period = pd.DataFrame(data_list,columns=['condition_id','period'])
df_period.to_csv('data/period.csv',index=False)

print(df_period.describe())