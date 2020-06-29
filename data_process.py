import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count

def create_lag(df,  cols, windows =[2,5] , agg_func = ['median', 'max', 'min','std','mean'], apply_func =[np.argmax, np.argmin], prevlag=1 ):
    #df= df.copy()  
    df_by_windows = []
    for window in windows:
        rolled = df[cols].shift(prevlag).rolling(window=window)
        agg= rolled.agg(agg_func)
        agg.columns = [f'{col[0]}_lag_{window}_{col[1]}' for col in agg.columns]
        #apply custom functions
        agg=agg.join([rolled.apply(f,raw=True).add_suffix(f'_lag_{window}_{f.__name__}') for f in apply_func ])
        df_by_windows.append(agg)
    df= df.join(df_by_windows)
    return df

def generate_lag_features(df,n_treads=4, group_cols = ['assetCode'],  cols=['close'], windows =[2,5] , agg_func = ['median', 'max', 'min','std','mean'], apply_func =[np.argmax, np.argmin], prevlag=1 ):
    
    all_df = []
    
    df_codes = df.groupby(group_cols)
    args = ( cols, windows, agg_func, apply_func, prevlag )
    df_codes = [(group[1][group_cols+cols].copy(),)+args for group in df.groupby(group_cols)]
    
    pool = Pool(n_treads)
    all_df = pool.starmap(create_lag,df_codes )
    
    new_df = pd.concat(all_df)  
    new_df.drop(cols+group_cols,axis=1,inplace=True)
    pool.close()
    return new_df


def make_lstm_template(n_rows, time_steps = 10, stride =1 ):
    template = np.zeros((n_rows,time_steps),dtype=int)
    for i  in range(n_rows):
            row = np.arange(i,max(-1,i-time_steps*stride),-stride)
            for j, n in enumerate(row):
                template[i,j] = n
    return template
   
def to_lstm( x, template):       
        return list(to_lstm_array(x,template))
   

def to_lstm_array( x, template):
          return (x[template[:x.shape[0]]])  
    
    
def make_lstm( df, time_steps=10, stride=1, cols=["diff_cost",'close', 'vol'] ):
        t = make_lstm_template(n_rows=df.shape[0],time_steps=time_steps,  stride=stride)   
        new_df= []
        for asset, data in df.groupby("assetCode")[cols]:
            a= data.copy() 
            a["lstm"]= to_lstm(a[cols].values,t)
            new_df.append(a) 
        new_df = pd.concat(new_df)
        df["lstm"] = new_df['lstm']
       
        return df

    
def make_shifts(df, cols = ['close','open'], shifts = [1,5,10,30,60]):
    gbo= df.groupby('assetCode')
    for col in cols:
        for shift in  shifts:
            sh =gbo[col].shift(shift)
            df[col+'_shift_'+str(shift)+ "_return"] = sh/df[col] -1    
    return df
    
def make_target(df,  shift = 10):
    gbo= df.groupby('assetCode')
    sh =gbo.close.shift(-shift)
    next_open = gbo.open.shift(-1)
    df['target'] = sh/next_open  -1    
    return df['target']

def make_target_for_longs(df, future_window):
    W=future_window
    df_new =generate_lag_features(df=df,agg_func=['max'],cols=['high'],windows=[W],apply_func=[],prevlag=0)
    col = df_new.columns.values[0]
    d =df.join(df_new)
    sh = d.groupby('assetCode')[col].shift(-W)
    next_open = d.groupby('assetCode')['open'].shift(-1)
    d["future_max"] = sh 
    d['target'] = (d.future_max/next_open -1)
    return d['target']

def make_target_for_shorts(df, future_window):
    W=future_window
    df_new = generate_lag_features(df=df,agg_func=['min'],cols=['low'],windows=[W],apply_func=[],prevlag=0)
    col = df_new.columns.values[0]
    d =df.join(df_new)
    sh = d.groupby('assetCode')[col].shift(-W)
    next_open = d.groupby('assetCode')['open'].shift(-1)
    d["future_max"] = sh 
    d['target'] = (d.future_max/next_open -1)
    return d['target']      