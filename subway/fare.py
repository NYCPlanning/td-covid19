import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/FARE/'
#path='/home/mayijun/TURNSTILE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float})



## Download data
#dl=datetime.datetime(2020,4,4)
#for i in range(0,150):
#    dl=dl-datetime.timedelta(days=7)
#    url='http://web.mta.info/developers/data/nyct/fares/fares_'+datetime.datetime.strftime(dl,'%y%m%d')+'.csv'
#    req=urllib.request.urlopen(url)
#    file = open(path+'DATA/'+datetime.datetime.strftime(dl,'%y%m%d')+'.csv', "wb")
#    shutil.copyfileobj(req,file)
#    file.close()



# Compile data
tp=pd.DataFrame()
for i in sorted(os.listdir(path+'DATA')):
    wk=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=1,nrows=1,header=None).loc[0,1]
    dt=pd.read_csv(path+'DATA/'+str(i),dtype=float,converters={'STATION':str,'REMOTE':str},skiprows=2,header=0)
    dt['week']=wk
    tp=pd.concat([tp,dt],ignore_index=True)


tp['id']=tp['UNIT']+'|'+tp['C/A']+'|'+tp['SCP']
tp['unit']=tp['UNIT'].copy()
tp['firstdate']=tp['DATE'].copy()
tp['firsttime']=tp['TIME'].copy()
tp['firstdesc']=tp['DESC'].copy()
tp['firstentries']=pd.to_numeric(tp['ENTRIES'])
tp['firstexits']=pd.to_numeric(tp['EXITS                                                               '])
tp=tp[['id','unit','firstdate','firsttime','firstdesc','firstentries','firstexits']].reset_index(drop=True)

# Entries
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    try:
        tpunit=tp[tp['unit']==i].reset_index(drop=True)
        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
        rttp=datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S').astimezone(pytz.timezone('America/New_York')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
            rttp+=4*3600
            rtlist+=[rttp]
        rtlist=[datetime.datetime.fromtimestamp(x).astimezone(pytz.timezone('America/New_York')) for x in rtlist]
        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
        rtunit['unit']=rtunit['Remote'].copy()
        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpentry).reset_index(drop=True)
        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
    except:
        print(str(i))
df=df[['id','unit','firstdate','time','entries','flagtime','flagentry']].reset_index(drop=True)
dfunitentry=df.groupby(['unit','firstdate','time'],as_index=False).agg({'entries':'sum','flagtime':'sum','flagentry':'sum','id':'count'}).reset_index(drop=True)
dfunitentry.to_csv(path+'dfunitentry.csv',index=False)
dfdateentry=dfunitentry[(dfunitentry['flagtime']==0)&(dfunitentry['flagentry']==0)].reset_index(drop=True)
dfdateentry=dfdateentry.groupby('firstdate',as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
dfflagtimeentry=dfunitentry.groupby('firstdate',as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
dfflagentry=dfunitentry.groupby('firstdate',as_index=False).agg({'flagentry':'sum'}).reset_index(drop=True)
dfdateentry=pd.merge(dfdateentry,dfflagtimeentry,how='left',on='firstdate')
dfdateentry=pd.merge(dfdateentry,dfflagentry,how='left',on='firstdate')
dfdateentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry=dfdateentry.sort_values('firstdate').reset_index(drop=True)
dfdateentry.to_csv(path+'dfdateentry.csv',index=False)
