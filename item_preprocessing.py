import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

df_item = pd.read_csv('E:/item_1631.csv')
df_item = df_item.drop(['buyer','seller','item_level'], axis=1)
df_item = df_item.sort_values('trade_time')

# 4월 로그 제거
df_item['trade_time'] = pd.to_datetime(df_item['trade_time'])
df_item = df_item[df_item['trade_time'].dt.strftime('%Y-%m-%d') != '2023-04-01']

trade_day = []

for i, val in enumerate(df_item['trade_time']):
    trade_day.append(val.strftime("%Y-%m-%d"))
    
df_item['trade_day'] = trade_day

grouped_item = df_item.groupby('item_name')

# 그룹별 DataFrame 생성
grouped_item_df = {}
for name, group in grouped_item:
    grouped_item_df[name] = group

# 날짜
start_date = datetime(2023, 3, 16)
end_date = datetime(2023, 3, 31)  

date_range = end_date - start_date

date_list = [start_date + timedelta(days=i) for i in range(date_range.days + 1)]

day_list = []
for date in date_list:
    day_list.append(date.strftime("%Y-%m-%d"))


def check_nan(column, n):
    cnt = 0
    for val in column.isna():
        if val:
            cnt += 1
        else:
            cnt = 0
        if cnt >= n:
            return True
    return False

# 3일 연속으로 로그가 없는 경우가 1번이라도 있으면 비인기아이템으로 분류
unpopular_item = []
popular_item_df = {}
    
for name, group in grouped_item_df.items():
    group_sum = group.groupby('trade_day').agg({'price' : 'sum'}).reset_index()
    
    price_new = []
    for i in day_list:
        if i in group_sum['trade_day'].tolist():
            idx = group_sum[group_sum['trade_day'] == i].index[0]
            price_new.append(group_sum.loc[idx]['price'])
        else:
            price_new.append(np.nan)
    
    group_new = pd.DataFrame()
    group_new['trade_day'] = day_list
    group_new['price'] = price_new
    
    if check_nan(group_new['price'], 3):
        unpopular_item.append(name)
    
    if name not in unpopular_item:
        popular_item_df[name] = group

df_unpopular = pd.DataFrame()
df_unpopular['item'] = unpopular_item
df_unpopular.to_csv('E:/unpopular_1631.csv', index=False, encoding='utf8')

# 전 3일의 quantile 0.75 가격 구하기
item_daily_price = pd.DataFrame()

for name, group in popular_item_df.items():

    group['trade_day'] = pd.to_datetime(group['trade_day'], format = '%Y-%m-%d')
    
    dates = pd.date_range(start = '2023-03-19', end = '2023-03-31')
    
    result = []
    
    for date in dates:

        data = (group['trade_day'] >= date - timedelta(days=3)) & (group['trade_day'] < date)
        price_list = group.loc[data]['price'].tolist()
        result.append(np.quantile(price_list, q=0.75))
        
    price_result = [name] + result
    df_price_result = pd.DataFrame([price_result], columns=['item_name']+[i for i in range(19,32)])
    item_daily_price = pd.concat([item_daily_price, df_price_result])


# 전 3일의 quantile 0.90 가격 구하기
item_daily_price_90 = pd.DataFrame()

for name, group in popular_item_df.items():

    group['trade_day'] = pd.to_datetime(group['trade_day'], format = '%Y-%m-%d')
    
    dates = pd.date_range(start = '2023-03-19', end = '2023-03-31')
    
    result = []
    
    for date in dates:

        data = (group['trade_day'] >= date - timedelta(days=3)) & (group['trade_day'] < date)
        price_list = group.loc[data]['price'].tolist()
        result.append(np.quantile(price_list, q=0.90))
        
    price_result = [name] + result
    df_price_result = pd.DataFrame([price_result], columns=['item_name']+[i for i in range(19,32)])
    item_daily_price_90 = pd.concat([item_daily_price_90, df_price_result])

item_daily_price.to_csv('E:/item_price_q75_1931.csv', index=False, encoding='utf8')
item_daily_price_90.to_csv('E:/item_price_q90_1931.csv', index=False, encoding='utf8')