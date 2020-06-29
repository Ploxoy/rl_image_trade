from finam.export import Exporter, Market, Timeframe
import datetime
import time
import pandas as pd
import os

def get_asset_data(assetCode):
    exporter= Exporter()
    ind = exporter.lookup(code= assetCode)
    assetId = ind.index.values[0]
    assetName, assetCode, market = ind.values[0]
    return  assetId, assetName, market


def get_data_by_code(assetCode, start_date=datetime.date(2008, 1, 1), end_date = None, timeframe=Timeframe.DAILY                  ):
    '''gets finam historical data only bue assetCode'''
    
    
    
    ts=2
    assetId, assetName, market = get_asset_data(assetCode)
    print("assetId:{}, assetName:{}, market:{}".format(assetId, assetName, market))
    exporter= Exporter()
    if timeframe >= Timeframe.DAILY:
        print('download all')
        data= exporter.download(assetId, market=Market(market), start_date=start_date, end_date= end_date,  timeframe=timeframe)
        data.columns = [col.replace("<","").replace(">","").lower() for col in data.columns]
        return data
    
    elif timeframe > Timeframe.TICKS:
        print("timeframe is {}, download by days".format(timeframe) )
        dates =  exporter.download(assetId, market=Market(market), start_date=start_date, end_date= end_date,  timeframe=Timeframe.DAILY).index
        years = dates.year.unique()
        downloaded_list = [] 
        counter = 0
        for year in years:
            y_dates =dates[dates.year==year]
            date_start = datetime.date (y_dates[0].year,y_dates[0].month,y_dates[0].day) 
            date_end = datetime.date (y_dates[-1].year,y_dates[-1].month,y_dates[-1].day) 
            print(date_start, date_end)
            downloaded_list.append(exporter.download(assetId, market=Market(market), start_date=date_start, end_date= date_end,  timeframe=timeframe))
            counter+=1
            if counter==3:
                print('pause {} sec'.format(ts))
                time.sleep(ts)
                counter =0
        data = pd.concat(downloaded_list)
        data.columns = [col.replace("<","").replace(">","").lower() for col in data.columns]
        return data
        
    elif timeframe == Timeframe.TICKS:
        print("timeframe is {}, download by days".format(timeframe))
        dates =  exporter.download(assetId, market=Market(market), start_date=start_date, end_date= end_date,  timeframe=Timeframe.DAILY).index
        time.sleep(ts)
        downloaded_list = [] 
        counter = 0
        for d in dates:
            date = (datetime.date(d.year,d.month,d.day)) 
            print (date)
            downloaded_list.append(exporter.download(assetId, market=Market(market), start_date=date, end_date= date,  timeframe=timeframe))
            counter+=1
            if counter==3:
                print('pause {} sec'.format(ts))
                time.sleep(ts)
                counter =0
        data = pd.concat(downloaded_list)
        #data.columns = [col.replace("<","").replace(">","").lower() for col in data.columns]
        return data
            

def update_data(data, asset="RTS" , timeframe=Timeframe.HOURLY ):
    "updates last day of dataframe"
    last_date =data.index.values[-1]
    last_date = pd.to_datetime(last_date).date()
    data = data[pd.to_datetime(data.index.values).date <last_date]
    new_daydata = get_data_by_code(assetCode=asset,start_date=last_date,timeframe=timeframe ) 
    data = pd.concat([data, new_daydata])
    return data

def get_data(asset = "RTS", start_date =datetime.date(2000, 1, 1), data_dir = './Data/' , timeframe=Timeframe.DAILY, force_reload = False,auto_save = True ):
    
        f_name = f'{asset} {start_date} {str(timeframe)}.csv'
        if os.path.isfile(data_dir+f_name) and not(force_reload):
                print (f' data file {f_name} exist')
                data = pd.read_csv(data_dir+f_name, index_col ='index'                     )
                data.index = pd.to_datetime(data.index.values)
                data.index.name = 'index'
                data = update_data(data,asset=asset , timeframe=timeframe )
                
        else:
            print(f'data file does not exist, start new download')
            data = get_data_by_code( assetCode=asset,start_date=start_date,timeframe=timeframe )
        
        if auto_save : data.to_csv(data_dir+f_name)
        return data
    