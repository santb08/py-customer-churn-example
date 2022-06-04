import pandas as pd
from datetime import datetime

# user_logs_all = pd.read_csv('views/user_logs_all.csv')
# transactions_final = pd.read_csv('debugging.csv')
transactions_final = pd.read_csv('views/transactions_final.csv')

# print(transactions_final) # [1374440 rows x 10 columns]
"""
begin;

create or replace view as kkbox.churn.churn_model_definition(
	with t as (
		select
			msno,
			ts,
			lag(transaction_dt) over (partition by msno order by transaction_dt desc) as next_transaction,
			first_value(transaction_dt) over (partition by msno order by transaction_dt desc) as most_recent_transaction,
			datediff(days,transaction_dt,expiration_dt) as days_since_last_transaction,
			datediff(days,expiration_dt,date('2017-04-01')) as days_expired, --second date here is just "today's date". For this problem, the most recent month is 4/2017
			case
				when (next_transaction is NULL and days_expired>30) then True --no next transaction, if you are more than 30 days expired then you are churn
				when (next_transaction is not Null and datediff(days,expiration_dt,date(next_transaction,'YYYY-MM-DD'))>30) then True --there is a next transaction, but you waited more than 30 days to renew, so this is churn
				else False
			end as is_churn,
			--case
			--       when (is_churn=True and next_transaction is not NULL and most_recent_transaction > expiration_dt) then True --there was churn, but then another transaction after the expiration date.
			--       when (is_churn=False) then NULL --no churn, so not relevant
			--       else False --churn and no return
			--end as churn_and_return,
			case
				when ts < '2017-01-01' and ts >= '2016-06-01' then 'TRAIN'
				when ts < '2017-02-01' and ts >= '2017-01-01' then 'VALI'
				when ts < '2017-03-01' and ts >= '2017-02-01' then 'TEST'
				else 'PREDICT_ME'
			end as SPLIT,
		datediff(days,(select max(log_date) from kkbox.churn.user_logs_all where msno=A.msno and log_date<=A.ts),ts) as days_since_last_log
		from kkbox.churn.transactions_final as A
	)
	select * from t
	where ts >= '2016-06-01'
	and ts <= '2017-04-01'
);

commit;
"""
def to_timestamp(dt):
    return datetime.strptime(dt, '%Y-%m-%d').timestamp()

# Select transactions in the range
ts_range = ('2016-06-01', '2017-04-01')
ts_range = (to_timestamp(ts_range[0]), to_timestamp(ts_range[1])) # To timestamp
t_table = transactions_final[transactions_final['ts'] >= ts_range[0]]
t_table = t_table[t_table['ts'] <= ts_range[1]] # [1026880 rows x 10 columns]

# next_transaction is the next transaction date for each user identified by msno
t_table['next_transaction'] = t_table.groupby('msno')['transaction_dt'].shift(-1)

# most_recent_transaction
# Get the most recent transaction date for each user identified by msno
t_table['most_recent_transaction'] = t_table.groupby('msno')['transaction_dt'].transform('max')

# days_since_last_transaction
# days_since_last_transaction = expiration_dt - transaction_dt
# expiration_dt is a string so we need to convert it to a datetime object
# transaction_dt is a string so we need to convert it to a datetime object
t_table['days_since_last_transaction'] = t_table.apply(lambda row: (datetime.strptime(row['expiration_dt'], '%Y-%m-%d') - datetime.strptime(row['transaction_dt'], '%Y-%m-%d')).days, axis=1)


# days_expired
# days_expired = expiration_dt - today's date
today_date = datetime.strptime('2017-04-01', '%Y-%m-%d')
t_table['days_expired'] = t_table.apply(lambda row: (datetime.strptime(row['expiration_dt'], '%Y-%m-%d') - today_date).days, axis=1)


# is_churn
# is_churn = True IF days_expired > 30 AND !next_transaction
# is_churn = True if next_transaction IS NOT NULL AND datediff(days,expiration_dt,date(next_transaction,'YYYY-MM-DD'))>30
# is_churn = False otherwise
t_table['is_churn'] = t_table.apply(
  lambda row:
  True if row['days_expired'] > 30 and pd.isna(row['next_transaction'])  else
  True if row['next_transaction'] is not None and pd.notna(row['next_transaction']) and (
    datetime.strptime(str(row['expiration_dt']), '%Y-%m-%d') -
    datetime.strptime(str(row['next_transaction']), '%Y-%m-%d')
  ).days > 30 else
  False, axis=1)

# SPLIT
# SPLIT = TRAIN if ts < '2017-01-01' and ts >= '2016-06-01'
# SPLIT = VALI if ts < '2017-02-01' and ts >= '2017-01-01'
# SPLIT = TEST if ts < '2017-03-01' and ts >= '2017-02-01'
# SPLIT = PREDICT_ME otherwise
ranges = [
  (to_timestamp('2016-06-01'), to_timestamp('2017-01-01')),
  (to_timestamp('2017-01-01'), to_timestamp('2017-02-01')),
  (to_timestamp('2017-02-01'), to_timestamp('2017-03-01')),
]

t_table['SPLIT'] = t_table.apply(
  lambda row:
  'TRAIN' if row['ts'] < ranges[0][1] and row['ts'] >= ranges[0][0] else
  'VALI' if row['ts'] < ranges[1][1] and row['ts'] >= ranges[1][0] else
  'TEST' if row['ts'] < ranges[2][1] and row['ts'] >= ranges[2][0] else
  'PREDICT_ME', axis=1
)


# datediff(days,(select max(log_date) from kkbox.churn.user_logs_all where msno=A.msno and log_date<=A.ts),ts) as days_since_last_log
# I don't think that this is necessary, but I'm leaving it here for now

t_table.to_csv('prediction.csv')