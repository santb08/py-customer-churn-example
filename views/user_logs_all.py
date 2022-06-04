import pandas as pd
from datetime import datetime

user_logs = pd.read_csv('decompressed/user_logs_v2.csv')

"""
create or replace view as kkbox.churn.user_logs_all(
	select distinct *,
		date(date,'YYYYMMDD') as log_date,
		TO_TIMESTAMP_NTZ(log_date) as ts
	from kkbox.churn.user_logs
);
"""

columns = [
  'msno',
  'date',
  'num_25',
  'num_50',
  'num_75',
  'num_985',
  'num_100',
  'num_unq',
  'total_secs'
]

print(user_logs) # [18396362 rows x 9 columns]
print(user_logs.head(10))
user_logs_all = user_logs.drop_duplicates(subset=columns) # [18396362 rows x 10 columns] - Distinct in SQL is not doing anything

user_logs_all['log_date'] = pd.to_datetime(user_logs_all['date'], format='%Y%m%d')
user_logs_all['ts'] = user_logs_all['log_date'].apply(lambda x: datetime.timestamp(x))

print(user_logs_all)


# Save user_logs_all
user_logs_all.to_csv('views/user_logs_all.csv')