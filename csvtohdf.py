import csv
import numpy as np
import pandas as pd
import json
from pathlib import Path
import os
import sys
import time
import matplotlib.pyplot as plt
import h5py
import logging
from datetime import datetime, timedelta
from geodecoder import reverse_geocode
from managedb import Manage

logging.basicConfig(filename='runtime.log', format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
        level=logging.INFO)
loc_db = 'addresses.db'
dfs = []
measurements = {}
def parse_data(file_path,dest_path,filename):
    contents = None
    try:
        with open(file_path, newline='') as fp:
            site, freq_start, step, lat, lon, data = csv.reader(fp,delimiter=' ')
        data = {
                "site": site[0],
                "freq_start": freq_start[0],
                "step": step[0],
                "lat-long": (np.float64(lat[0]),np.float64(lon[0])),
                "data": data
                }
        return data

    except IOError as e:
        print(e)

def get_data(**kwargs):

    site = kwargs['site']
    date = kwargs['data'].pop(0)
    rssi = kwargs['data'][0].split(',')
    lat = kwargs['lat-long'][0]
    lon = kwargs['lat-long'][1]
    time = rssi.pop(0)

    dt = date + " " +  time
    date_time = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    try:
        rssi = np.array(rssi, dtype = np.float64)
        base_freq = np.float16(kwargs['freq_start'])
        step = float(kwargs['step'])/1000.0 # Khz
        step = round(step,2)
        index_freqs = np.arange(base_freq,base_freq + (step * len(rssi) ),step)
        index_freqs = np.round(index_freqs, decimals=2)
        time_stamp = pd.Timestamp(date_time)
        rssi = np.round(rssi, decimals=1)
    except:
        return None

    return [site, time_stamp, rssi, base_freq, step, (float(lat), float(lon)), date_time]

def save_data(fname,dest_path,**kwargs):
    
    try:
        try:
            mul_index =zip ( *[ts, index_freq] )
            tups = list(mul_index)
            index = pd.MultiIndex.from_tuples(tups, names=["timestamp","FREQ"])
            df = pd.DataFrame(data = rssi, index=index, columns=["RSSI"])
            dfs.append(df)
            # json_record = df.to_json(orient="columns")
            # parsed = json.loads(json_record)
            # json_s = json.dumps(parsed, indent=4)
            # print(json_s)

            context = {
                    "bucket":"spectrum_test",
                    "org": "ASTI",
                    "token": "3r3tKvsRrs934WQ4LXQ_HmtFhNy01gKVbpvACShUxw5wbS5N4TKAZBa7gFiv8laJEyhC9BS4Op4gdcfrkGT_Eg==",
                    "url": "http://localhost:8086"
                }
            #pushData(df, **context)
            #readData(**context)
        except ValueError as e:
            print(e)
            
    except ValueError as e:
        print("Skipping existing file..",fname)

def saveCsvData(data, sitename, start, step, date, geom):
    lat, lon = geom 

    token =  "3r3tKvsRrs934WQ4LXQ_HmtFhNy01gKVbpvACShUxw5wbS5N4TKAZBa7gFiv8laJEyhC9BS4Op4gdcfrkGT_Eg=="
    
    with h5py.File('data.h5','a') as h5file:
        print(str(date.date()))
        for key in data:
            time = datetime.strftime(key, '%H:%M:%S')
            group = os.path.join(str(date.date()),token, sitename)
            try:
                g = h5file.create_group(group)
                g.attrs["lat"] = lat
                g.attrs["lon"] = lon
                g.attrs["start_freq"] = start
                g.attrs["step"] = step
            except ValueError as e:
                print(e)
                g = h5file[group]
            try:
                g.create_dataset(time,data=data[key])
            except:
                pass

def get_location(geom):
    lat = geom[0]
    lon = geom[1]
    c, loc = reverse_geocode(lat, lon)
    if c < 0:
        pass
    elif c == 1:
        return loc
    elif c == 0:
        db = Manage(loc_db)
        db.save_location(lat, lon, loc)
        return loc
    
     
if __name__ == "__main__":
    CWD = Path.cwd() 
    HOME_DIR = CWD.home()
    # DATA_FOLDER = HOME_DIR.joinpath("ED_Data")
    DATA_FOLDER = CWD.joinpath("brocoli_data/ED_Data")
    DEST_PATH = HOME_DIR.joinpath("server","data.h5")
    for root, dirs, files in os.walk(str(DATA_FOLDER)):
        for filename in files:
            if filename.endswith(".csv"):
                ret = parse_data(DATA_FOLDER.joinpath(filename),str(DEST_PATH),filename)
                data = get_data(**ret)
                try:
                    key = data.pop()
                    key = key.strftime("%Y-%m-%d")
                    key = str(key)
                    measurements[key] = measurements.get(key, []) + [data]
                except:
                    pass
    dfs = []
    for val in measurements.values():
        df = pd.DataFrame(data= val, columns=['SITE', 'TIMESTAMP', 'RSSI', 'START', 'STEP', 
            'geom'])
        if df.empty:
            continue
        df["address"] = df['geom'].apply(get_location)
        dfs.append(df)
    
    m_df = pd.concat(dfs,ignore_index=True, sort=False)
    del dfs
    dates = []
    for key in measurements.keys():
        dates.append(datetime.strptime(key, "%Y-%m-%d"))
    locs = m_df['geom'].unique()

    for loc in locs:
        df_byloc = m_df[m_df['geom'] == loc]
        for date in dates:
            df_bydate = df_byloc['TIMESTAMP'].apply(lambda ts: ts.year == date.year and ts.month == date.month
                    and ts.day == date.day)
            new_df = df_byloc[df_bydate]
            if new_df.empty:
                continue

            m = dict(zip(new_df.TIMESTAMP, new_df.RSSI))
            sitename = new_df.iloc[0]['SITE']
            start = new_df['START'].reset_index(drop=True)[0]
            step = new_df['STEP'].reset_index(drop=True)[0]
            rssi = new_df['RSSI'].reset_index(drop=True)[0]
            vals = pd.DataFrame.from_dict(dict(zip(new_df.RSSI.index, new_df.RSSI.values)))
            new_index = np.arange(start, start + (step * rssi.size) ,step)
            vals.index = new_index
            vals.columns = new_df.TIMESTAMP

#            plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')
#            plt.plot(vals[vals.columns[0]])
#            plt.legend([str(vals.columns[0])])
#            plt.xlabel('Frequency (MHz)')
#            plt.ylabel('Power (dBm)')
#            plt.show()

            saveCsvData(m, sitename, start, step, date, loc)
            #print(vals.to_csv(header=False, index=False))
            #/stations/date
            #/contrib/date
            #print(vals.to_numpy())
            #input()




