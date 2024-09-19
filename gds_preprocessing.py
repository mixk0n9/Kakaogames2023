import os
import pandas as pd
import numpy as np
import pickle

df_gds = pd.DataFrame()

path = 'E:/gds03_1/base_ymd=2023-03-31/'
file_list = os.listdir(path)

gzip_file = path + file_list[0]
df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t')
df_gds = pd.concat([df_gds, df_gds_daily])

# 유저별로 해당하는 날짜 데이터 불러오기
with open('./user_list.pkl', 'rb') as f:
    user_list = pickle.load(f)

user_30 = user_list['user_30']
user_29 = user_list['user_29']
user_28 = user_list['user_28']
user_27 = user_list['user_27']
user_26 = user_list['user_26']
user_25 = user_list['user_25']

df_gds_30 = pd.DataFrame()

for i in range(24, 31):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_30)]

        df_gds_30 = pd.concat([df_gds_30, df_gds_daily])

df_gds_29 = pd.DataFrame()

for i in range(23, 30):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_29)]

        df_gds_29 = pd.concat([df_gds_29, df_gds_daily])

df_gds_28 = pd.DataFrame()

for i in range(22, 29):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_28)]

        df_gds_28 = pd.concat([df_gds_28, df_gds_daily])

df_gds_27 = pd.DataFrame()

for i in range(21, 28):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_27)]

        df_gds_27 = pd.concat([df_gds_27, df_gds_daily])

df_gds_26 = pd.DataFrame()

for i in range(20, 27):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_26)]

        df_gds_26 = pd.concat([df_gds_26, df_gds_daily])

df_gds_25 = pd.DataFrame()

for i in range(19, 26):
    path = 'E:/gds03_1/base_ymd=2023-03-' + str(i).zfill(2) + '/'
    file_list = os.listdir(path)
    
    for filename in file_list:
        gzip_file = path + filename
        df_gds_daily = pd.read_csv(gzip_file, compression='gzip', header=None, sep='\t').drop([3,4,5], axis=1)
        
        df_gds_daily = df_gds_daily[(df_gds_daily[2] == 'FreeCash') | (df_gds_daily[2] == 'BuyCash')]
        df_gds_daily = df_gds_daily[df_gds_daily[0].isin(user_25)]

        df_gds_25 = pd.concat([df_gds_25, df_gds_daily])

df_gds_30.columns=['user','time','cash','change','hold']
df_gds_30.to_csv('E:/cash_2430.csv', index=False, encoding='utf8')

df_gds_29.columns=['user','time','cash','change','hold']
df_gds_29.to_csv('E:/cash_2329.csv', index=False, encoding='utf8')

df_gds_28.columns=['user','time','cash','change','hold']
df_gds_28.to_csv('E:/cash_2228.csv', index=False, encoding='utf8')

df_gds_27.columns=['user','time','cash','change','hold']
df_gds_27.to_csv('E:/cash_2127.csv', index=False, encoding='utf8')

df_gds_26.columns=['user','time','cash','change','hold']
df_gds_26.to_csv('E:/cash_2026.csv', index=False, encoding='utf8')

df_gds_25.columns=['user','time','cash','change','hold']
df_gds_25.to_csv('E:/cash_1925.csv', index=False, encoding='utf8')

# 유저 그룹별로 변수 생성
def find_first_row(df):
    start = df.head(1)['time'].values[0]
    df_start_time = df[df['time'] == start]
    change_list = df_start_time['change'].tolist()
    
    if all(x >= 0 for x in change_list):
        df_start_time_sort = df_start_time.sort_values('hold', ascending=True)
        
    elif all(x < 0 for x in change_list):
        df_start_time_sort = df_start_time.sort_values('hold', ascending=False)
        
    else:
        df_start_time_plus = df_start_time[df_start_time['change'] > 0]
        df_start_time_sort = df_start_time_plus.sort_values('hold', ascending=True)

    first_change = df_start_time_sort.head(1)['change'].values[0]
    first_hold = df_start_time_sort.head(1)['hold'].values[0]
    
    return first_change, first_hold



def merge_free_buy(df):
    
    df = df.sort_values('time')
    df.reset_index(inplace=True, drop=True)
    
    first_hold = 0

    if 'FreeCash' in df['cash'].values:
        df_free = df[df['cash'] == 'FreeCash']
        free_first_change, free_first_hold = find_first_row(df_free)
        first_hold += free_first_hold
        
    if 'BuyCash' in df['cash'].values:
        df_buy = df[df['cash'] == 'BuyCash']
        buy_first_change, buy_first_hold = find_first_row(df_buy)
        first_hold += buy_first_hold
        
    hold = []
    hold.append(first_hold)
    
    now = first_hold
    for i in range(1, len(df)):
        
        now += df.iloc[i]['change']
        hold.append(now)
   
    df['hold'] = hold
    
    return df

def time_slicing(df, start_time, end_time, freq, result):
    grouped_cash = df.groupby('user')

    for name, group in grouped_cash:

        group['time'] = pd.to_datetime(group['time'])
        group = group.sort_values('time')
        group.reset_index(inplace=True, drop=True)
    
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
        time_range = pd.date_range(start=start_time, end=end_time, freq=freq)
    
        # 시작점 없으면 뒤 데이터로 생성
        if start_time not in group['time'].values:
        
            buy_change = 0
            if 'BuyCash' in group['cash'].values:
                
                df_buy = group[group['cash'] == 'BuyCash']
                buy_first_change, buy_first_hold = find_first_row(df_buy)
                buy_change += buy_first_change
            
            free_change = 0    
            if 'FreeCash' in group['cash'].values:

                df_free = group[group['cash'] == 'FreeCash']
                free_first_change, free_first_hold = find_first_row(df_free)
                free_change += free_first_change
            
            first_hold = find_first_row(group)[1]
            start_hold = first_hold - free_change - buy_change
            group.loc[-1] = [name, start_time] + list(group.iloc[0, 2:3]) + [0, start_hold]
            group = group.sort_values('time')
    
        df_merge = merge_free_buy(group)
        
        cash_hold = []
        previous_value = None
        for time in time_range:
        
            filtered_data = df_merge[df_merge['time'] <= time]
        
            if len(filtered_data) > 0:
                previous_value = filtered_data['hold'].iloc[-1]
        
            cash_hold.append(previous_value)
        
        cash_hold = [name] + cash_hold
        df_cash_hold = pd.DataFrame([cash_hold], columns=['user']+[i for i in range(336)])
        result = pd.concat([result, df_cash_hold])
    return result

def add_abs_min(row):
    negative_values = row[cash_columns][row[cash_columns] < 0]
    if not negative_values.empty:
        row[cash_columns] += abs(negative_values.min())
    return row

result30 = pd.DataFrame(columns=['user']+[i for i in range(336)])
result30 = time_slicing(df_gds_30, '2023-03-24 00:00:00', '2023-03-30 23:30:00', '30T', result30)
result30.to_csv('E:/cash_2430_30min.csv', index=False, encoding='utf8')
result29 = pd.DataFrame(columns=['user']+[i for i in range(336)])
result29 = time_slicing(df_gds_29, '2023-03-23 00:00:00', '2023-03-29 23:30:00', '30T', result29)
result29.to_csv('E:/cash_2329_30min.csv', index=False, encoding='utf8')
cash_columns = result29.columns[1:]
result29 = result29.apply(add_abs_min, axis=1)
result29.to_csv('E:/cash_2329_30min.csv', index=False, encoding='utf8')

result28 = pd.DataFrame(columns=['user']+[i for i in range(336)])
result28 = time_slicing(df_gds_28, '2023-03-22 00:00:00', '2023-03-28 23:30:00', '30T', result28)
result28.to_csv('E:/cash_2228_30min.csv', index=False, encoding='utf8')