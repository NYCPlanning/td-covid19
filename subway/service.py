import pandas as pd
import numpy as np
import datetime
import pytz



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/SERVICE/'



calendar=pd.read_csv(path+'google_transit/calendar.txt')
trips=pd.read_csv(path+'google_transit/trips.txt')
stoptimes=pd.read_csv(path+'google_transit/stop_times.txt')

for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    print(str(i)+': '+str(len(tripid)))

i='wednesday'
k=stoptimes.drop_duplicates('trip_id',keep='first')
k=k[np.isin(k['trip_id'],tripid)]
k['hour']=[str(x)[0:2] for x in k['arrival_time']]
k=k.groupby('hour',as_index=False).agg({'trip_id':'count'})
print(k)

i='thursday'
k=stoptimes.drop_duplicates('trip_id',keep='first')
k=k[np.isin(k['trip_id'],tripid)]
k['hour']=[str(x)[0:2] for x in k['arrival_time']]
k=k.groupby('hour',as_index=False).agg({'trip_id':'count'})
print(k)


k=stoptimes[stoptimes['trip_id']=='BFA19GEN-A083-Weekday-00_156200_A..N09R']
k=trips[trips['trip_id']=='BFA19GEN-A083-Weekday-00_156200_A..N09R']
