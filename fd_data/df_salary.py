import datetime
import numpy as np
import pandas as pd
import os

from password import password

def process_df(df):
    df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date())
    df['fanduel_id'] = df['Id'].apply(lambda x: x.partition("-")[-1]).astype(int)


    for pos in ['P', 'OF', 'SS', '2B', '3B', 'C', '1B','UTIL']:
        df[pos] = np.where(df['Roster Position'].str.contains(pos), 1, 0)

    df['home'] = (df['Game'].apply(lambda x: x.split("@")[-1])==df['Team']).astype(int)
    df.drop(['Id','Roster Position','Position','First Name','Last Name','Game'], axis=1, inplace=True)

    cols = {"Team":"team","Opponent":"opp", "Nickname":"name"}
    df.rename(cols, axis=1, inplace=True)
    df['FPPG'] = df['FPPG'].round(2)

    cols = ['fanduel_id', 'name', 'FPPG', 'Played', 'Salary', 'team', 'opp', 'Injury Indicator', 'Injury Details', 'Tier', 'Probable Pitcher', 'Batting Order', 'date', 'P', 'OF', 'SS', '2B', '3B', 'C', '1B', 'UTIL', 'home']
    df = df[cols]
    return df

if __name__ == '__main__':
    pw = password()
    con = pw.engine

    files = [x for x in os.listdir("daily") if x.endswith('salary.csv')]
    df = pd.DataFrame()
    for file in files:
        temp = pd.read_csv(f"daily/{file}")
        temp['date'] = file.partition("_")[0]
        df = pd.concat((df,temp)).reset_index(drop=True)

    df = process_df(df)
    df.to_sql("fd_salary", con, if_exists='append', index=False, chunksize=1000)