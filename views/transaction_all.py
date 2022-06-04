import pandas as pd
from datetime import datetime

transactions = pd.read_csv('decompressed/transactions_v2.csv')

print('RAW\n', transactions) # [1431009 rows x 9 columns]

"""
create or replace view as kkbox.churn.transactions_all(
	select distinct
		msno,
		date(transaction_date,'YYYYMMDD') as transaction_dt,
		date(membership_expire_date,'YYYYMMDD') as expiration_dt,
		TO_TIMESTAMP_NTZ(date_from_parts(date_part(year,expiration_dt),date_part(month,expiration_dt),1)) as ts, --predict churn in the beginning of the  month when account is expiring
		payment_method_id,
		payment_plan_days,
		plan_list_price,
		actual_amount_paid,
		is_auto_renew,
		is_cancel
	from kkbox.churn.transactions
"""

# define
# lambda x: datetime.strptime(str(x), '%Y%m%d').date()
def parse_date(x):
  date_format = '%Y%m%d'
  return datetime.strptime(str(x), date_format).date()


transactions['transaction_dt'] = transactions['transaction_date'].apply(parse_date)
transactions['expiration_dt'] = transactions['membership_expire_date'].apply(parse_date)
transactions['ts'] = transactions['expiration_dt'].apply(
  lambda expiration_date: datetime(expiration_date.year, expiration_date.month, 1).timestamp()
)

transactions_all = transactions.drop_duplicates(
  subset=['msno', 'transaction_dt', 'expiration_dt', 'payment_method_id', 'payment_plan_days', 'plan_list_price', 'actual_amount_paid', 'is_auto_renew', 'is_cancel'],
)

# Get columns
expected_columns = [
  'msno',
  'transaction_dt',
  'expiration_dt',
  'ts',
  'payment_method_id',
  'payment_plan_days',
  'plan_list_price',
  'actual_amount_paid',
  'is_auto_renew',
  'is_cancel'
]
transactions_all = transactions_all[expected_columns]
print(transactions_all) # [1197050 rows x 9 columns]


# Save to csv
transactions_all.to_csv('transactions_all.csv', index=False)