import pandas as pd
from datetime import datetime

transactions_all = pd.read_csv('views/transactions_all.csv')
transactions_cancelled = pd.read_csv('views/transactions_cancelled.csv')

"""
with t as (
  select A.*,
  coalesce(B.most_recent_transaction_cancel, 0) as most_recent_transaction_cancel
  from kkbox.churn.transactions_all  as A
  left join kkbox.churn.transactions_canceled  as B
  on A.msno = B.msno
  and A.transaction_dt = B.transaction_dt
  where A.is_cancel = 0
  order by A.msno, A.transaction_dt
)
select
  msno, ts,
  max(payment_method_id) as payment_method_id,
  max(payment_plan_days) as payment_plan_days,
  sum(plan_list_price) as plan_list_price,
  sum(actual_amount_paid) as actual_amount_paid,
  max(is_auto_renew) as is_auto_renew,
  max(expiration_dt) as expiration_dt,
  max(transaction_dt) as transaction_dt,
  max(most_recent_transaction_cancel) as most_recent_transaction_cancel
  from t
  group by msno, ts
  --squash sub-montly subs all into one month, use last value for exp/trans dt to calculate churn
"""

t_table = transactions_all.merge(transactions_cancelled, how='left', on=['msno', 'transaction_dt'])
t_table['most_recent_transaction_cancel'] = t_table['most_recent_transaction_cancel'].fillna(0)
t_table = t_table[t_table['is_cancel'] == 0]
t_table = t_table.sort_values(by=['msno', 'transaction_dt']) # Odered by msno, transaction_dt

# Rename columns
t_table = t_table.rename(columns={'ts_x': 'ts'})
columns = [
  'msno',
  'ts',
]


t_table = t_table.groupby(['msno', 'ts']).agg({
  'payment_method_id': 'max',
  'payment_plan_days': 'max',
  'plan_list_price': 'sum',
  'actual_amount_paid': 'sum',
  'is_auto_renew': 'max',
  'expiration_dt': 'max',
  'transaction_dt': 'max',
  'most_recent_transaction_cancel': 'max',
})

print(t_table.head(30))
t_table.to_csv('views/transactions_final.csv')

