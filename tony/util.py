import numpy as np
import pandas as pd
import requests


def cal_near_stations(df, nears):
    idx_to_station = {k:v for k,v in df.Station_ID.iteritems()}
    near_stations = []
    for k, row in df.iterrows():
        result =  np.sqrt((111*(df.longitude-row[1]))**2 + (74*(df.latitude-row[2]))**2).nsmallest(4)
        near_idx = np.sqrt((111*(df.longitude-row[1]))**2 + (74*(df.latitude-row[2]))**2).nsmallest(4).index[1:(1+len(nears))].tolist()
        near_station = [idx_to_station[idx] for idx in near_idx]
        near_stations.append(near_station)
        print(k, row[0])
        print(result, "\n")
    return near_stations

def df_resample(df):
    dfs = []
    for stationId in df.stationId.unique():
        df_ = df[df['stationId']==stationId]
        df_ = df_.resample('1H', on='utc_time').sum()
        df_ = df_.reset_index()
        df_['stationId'] = stationId
        dfs.append(df_)
    return pd.concat(dfs, ignore_index=True)


def fetch_aq(city):
    end_time = pd.datetime.now().strftime('%Y-%m-%d-%H')
    r = requests.get("https://biendata.com/competition/airquality/" + city + "/2018-03-31-0/" + end_time + "/2k0d1d8")
    df = pd.DataFrame([i.decode('utf8').split(',') for i in r.content.splitlines()])
    df.columns = df.iloc[0]
    df = df.reindex(df.index.drop(0))
    del df['id']
    df.columns = ['stationId', 'utc_time', 'PM2.5', 'PM10', 'NO2', 'CO', 'O3', 'SO2']
    df.utc_time = pd.to_datetime(df.utc_time)
    df = df.replace('', np.nan, regex=True)
    for col in ['PM2.5', 'PM10', 'NO2', 'CO', 'O3', 'SO2']:
        df[col] = df[col].astype(float)
    return df

def fetch_meo(city):
    # wget https://biendata.com/competition/meteorology/ld_grid/2018-03-30-0/2018-05-01-23/2k0d1d8 -O ld_meo_new.csv
    # wget https://biendata.com/competition/meteorology/bj_grid/2018-03-30-0/2018-05-01-23/2k0d1d8 -O bj_meo_new.csv
    df = pd.read_csv("../input/"+ city + "_meo_grid_new.csv")
    del df['id']
    del df['weather']
    df_grid_station = pd.read_csv("../input/"+ city + "_grid_weather_station.csv")
    df = pd.merge(df, df_grid_station, how='left', left_on='station_id', right_on='stationName')
    del df['stationName']
    df.columns = ['stationName', 'utc_time', 'temperature', 'pressure', 'humidity', 'wind_direction', 'wind_speed/kph', 'longitude', 'latitude']
    return df

def get_aq(city):
    if city=="bj":
        df = pd.read_csv("../input/bj_aq_historical.csv")
        df = df[df.utc_time< '2018-03-31 07:00:00']
    else:
        df = pd.read_csv("../input/ld_aq_historical.csv", index_col=['Unnamed: 0'])
        df.columns = ['utc_time', 'stationId','PM2.5', 'PM10', 'NO2']
    
    df.utc_time = pd.to_datetime(df.utc_time)
    df_new = fetch_aq(city)
    df = pd.concat([df, df_new], ignore_index=True)
    return df

def get_meo(city):
    df = pd.read_csv("../input/"+ city + "_meo_grid_historical.csv")
    df_new = fetch_meo(city)
    
    df = pd.concat([df, df_new], ignore_index=True)
    df.utc_time = pd.to_datetime(df.utc_time)
    return df