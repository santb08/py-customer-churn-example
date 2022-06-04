import pandas as pd
from datetime import datetime

transactions_all = pd.read_csv('views/transactions_all.csv') # [1431009 rows x 10 columns]

"""
create or replace view as kkbox.churn.transactions_canceled(
	with t as (
	  select
			A.msno,
			A.transaction_dt,
			max(A.ts) as ts,
			max(B.transaction_dt) as most_recent_transaction_dt
	  from kkbox.churn.transactions_all as A
	  join kkbox.churn.transactions_all as B
	  where B.msno = A.msno
	  and B.transaction_dt <= A.ts
		and B.transaction_dt > A.transaction_dt
	  group by A.msno, A.transaction_dt
	  order by A.msno, A.transaction_dt
	)
	select
		t.msno,
		t.transaction_dt,
		max(t.ts) as ts,
		max(t.most_recent_transaction_dt) as most_recent_transaction_dt,
		max(c.is_cancel) as most_recent_transaction_cancel
	from t
	join kkbox.churn.transactions_all as C
	where C.msno = t.msno
	and C.transaction_dt = t.most_recent_transaction_dt
	group by t.msno, t.transaction_dt
);
"""

def parse_date(date_str):
  return datetime.strptime(str(date_str), '%Y-%m-%d')

def to_timestamp(date_str):
  date = datetime.strptime(str(date_str), '%Y-%m-%d')
  return date.timestamp()


A_table = transactions_all
B_table = transactions_all


# Inner join A and B on msno
t_table = A_table.merge(B_table, how='inner', on=['msno'], suffixes=('_A', '_B')) # [3452453 rows x 19 columns]
t_table = t_table.rename(columns={ 'ts_A': 'ts' })


# Filter out transactions that are not the most recent transaction
t_table = t_table[t_table['transaction_dt_B'].apply(to_timestamp) < t_table['ts_B']] # [3368872 rows x 19 columns]
t_table = t_table[
  t_table['transaction_dt_B'].apply(to_timestamp) > t_table['transaction_dt_A'].apply(to_timestamp)
]


# Find biggest ts for each msno
t_table['most_recent_transaction_dt'] = t_table['transaction_dt_A']


# Group by msno and transaction_dt
t_table = t_table.groupby(['msno', 'transaction_dt_A']).agg({
  'ts': 'max',
  'most_recent_transaction_dt': 'max',
}).reset_index()


# Order by msno and transaction_dt
t_table = t_table.sort_values(['msno', 'transaction_dt_A'])


# Wanted columns
t_table_columns = [
  'msno',
  'transaction_dt',
  'ts',
  'most_recent_transaction_dt',
]

t_table = t_table.rename(columns = { 'transaction_dt_A': 'transaction_dt'})
t_table = t_table[t_table_columns]

# Last join
C_table = transactions_all


# JOIN C on msno
# WHERE C.transaction_dt = t.most_recent_transaction_dt
t_table['transaction_dt'] = t_table['most_recent_transaction_dt']

t_table = pd.merge(
  t_table,
  C_table,
  how='inner',
  on=['msno', 'transaction_dt'],
  suffixes=('_t', '_C')
)
print(t_table.columns)

# GROUP BY msno, transaction_dt
t_table = t_table.groupby(['msno', 'transaction_dt']).agg({
  'ts_t': 'max',
  'most_recent_transaction_dt': 'max',
  'is_cancel': 'max',
}).reset_index()

t_table = t_table.rename(columns={
  'ts_t': 'ts',
  'is_cancel': 'most_recent_transaction_cancel'
})

columns = [
  'msno',
  'transaction_dt',
  'ts',
  'most_recent_transaction_dt',
  'most_recent_transaction_cancel'
]

t_table = t_table[columns]
print(t_table.head(20))

t_table.to_csv('views/transactions_cancelled.csv', index=False)