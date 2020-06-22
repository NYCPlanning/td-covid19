import pandas as pd
import numpy as np
import datetime
import pytz


pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/SERVICE/'


# GTFS Schedule
calendar=pd.read_csv(path+'google_transit/calendar.txt')
trips=pd.read_csv(path+'google_transit/trips.txt')
stoptimes=pd.read_csv(path+'google_transit/stop_times.txt')
stoptimes['hour']=[str(x)[0:2] for x in stoptimes['arrival_time']]
tripfirsttime=stoptimes.drop_duplicates('trip_id',keep='first').reset_index(drop=True)
triplasttime=stoptimes.drop_duplicates('trip_id',keep='last').reset_index(drop=True)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=tripfirsttime[np.isin(tripfirsttime['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=triplasttime[np.isin(triplasttime['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=stoptimes[np.isin(stoptimes['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)







# GTFS Realtime
k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='first').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='last').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='first').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='last').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200213.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200409.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200416.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200423.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200430.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200507.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200514.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200521.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200528.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200604.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200608.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200611.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200618.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)





k=pd.read_csv(path+'gtfsrt/tp_20200213.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200409.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200416.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200423.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200430.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200507.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200514.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200521.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200528.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200604.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200608.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200611.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200618.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)
