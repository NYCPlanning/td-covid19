import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/TURNSTILE/'
path='/home/mayijun/TURNSTILE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)



## Download data
#dl=datetime.datetime(2020,4,4)
#for i in range(0,300):
#    dl=dl-datetime.timedelta(days=7)
#    url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt'
#    req=urllib.request.urlopen(url)
#    file = open(path+'DATA/'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt', "wb")
#    shutil.copyfileobj(req,file)
#    file.close()



# Clean Entries based on Unit-C/A-SCP
#ucsentry=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
#ucsentry=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
#ucsentry=tpunit[tpunit['id']=='R158|N335|01-00-00'].reset_index(drop=True)
#ucsentry=tpunit[tpunit['id']=='R208|R529|00-00-01'].reset_index(drop=True)
#ucsentry=tpunit[tpunit['id']=='R023|N507|00-06-01'].reset_index(drop=True)
def unitcascpentry(ucsentry):
    global rtunit
    ucsentry=ucsentry.sort_values(['firstdate','firsttime']).reset_index(drop=True)
    ucsid=ucsentry.loc[0,'id']
    ucsentry=pd.merge(rtunit,ucsentry,how='left',on=['unit','firstdate','firsttime'])
    ucsentry['nextdate']=np.roll(ucsentry['firstdate'],-1)
    ucsentry['nexttime']=np.roll(ucsentry['firsttime'],-1)
    ucsentry['nextdesc']=np.roll(ucsentry['firstdesc'],-1)
    ucsentry['nextentries']=np.roll(ucsentry['firstentries'],-1)
    ucsentry['time']=ucsentry['firsttime']+'-'+ucsentry['nexttime']
    ucsentry['entries']=ucsentry['nextentries']-ucsentry['firstentries']
    ucsentry=ucsentry[:-1].reset_index(drop=True)
    ucsentry=ucsentry[['id','unit','firstdate','time','entries']].reset_index(drop=True)
    ucsentry['id']=ucsid
    ucsentry['flagtime']=np.where(pd.isna(ucsentry['entries']),1,0)
    ucsentry['flagentry']=np.where((ucsentry['entries']<0)|(ucsentry['entries']>5000),1,0)
    ucsentry['entries']=ucsentry['entries'].fillna(0)
    return ucsentry

# Clean Exits based on Unit-C/A-SCP
def unitcascpexit(ucsexit):
    global rtunit
    ucsexit=ucsexit.sort_values(['firstdate','firsttime']).reset_index(drop=True)
    ucsid=ucsexit.loc[0,'id']
    ucsexit=pd.merge(rtunit,ucsexit,how='left',on=['unit','firstdate','firsttime'])
    ucsexit['nextdate']=np.roll(ucsexit['firstdate'],-1)
    ucsexit['nexttime']=np.roll(ucsexit['firsttime'],-1)
    ucsexit['nextdesc']=np.roll(ucsexit['firstdesc'],-1)
    ucsexit['nextexits']=np.roll(ucsexit['firstexits'],-1)
    ucsexit['time']=ucsexit['firsttime']+'-'+ucsexit['nexttime']
    ucsexit['exits']=ucsexit['nextexits']-ucsexit['firstexits']
    ucsexit=ucsexit[:-1].reset_index(drop=True)
    ucsexit=ucsexit[['id','unit','firstdate','time','exits']].reset_index(drop=True)
    ucsexit['id']=ucsid
    ucsexit['flagtime']=np.where(pd.isna(ucsexit['exits']),1,0)
    ucsexit['flagexit']=np.where((ucsexit['exits']<0)|(ucsexit['exits']>5000),1,0)
    ucsexit['exits']=ucsexit['exits'].fillna(0)
    return ucsexit



start=datetime.datetime.now()

# Compile data
tp=pd.DataFrame()
for i in sorted(os.listdir(path+'DATA')):
    tp=pd.concat([tp,pd.read_csv(path+'DATA/'+str(i),dtype=str)],ignore_index=True)
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
        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
            rttp+=4*3600
            rtlist+=[rttp]
        rtlist=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in rtlist]
        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
        rtunit['unit']=rtunit['Remote'].copy()
        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpentry).reset_index(drop=True)
        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
        print(str(i)+': success')
    except:
        print(str(i)+': fail')
df=df[['id','unit','firstdate','time','entries','flagtime','flagentry']].reset_index(drop=True)
dfflagtime=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
dfflagentry=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagentry':'sum'}).reset_index(drop=True)
dfunitentry=df[(df['flagtime']==0)&(df['flagentry']==0)].reset_index(drop=True)
dfunitentry=dfunitentry.groupby(['unit','firstdate','time'],as_index=False).agg({'entries':'sum','id':'count'}).reset_index(drop=True)
dfunitentry.columns=['unit','firstdate','time','entries','gooducs']
dfunitentry=pd.merge(dfunitentry,dfflagtime,how='left',on=['unit','firstdate','time'])
dfunitentry=pd.merge(dfunitentry,dfflagentry,how='left',on=['unit','firstdate','time'])
dfunitentry.to_csv(path+'dfunitentry.csv',index=False)

dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfdateentry=dfunitentry.groupby('firstdate',as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
dfdateentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry=dfdateentry.sort_values('firstdate').reset_index(drop=True)
dfdateentry['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry.to_csv(path+'dfdateentry.csv',index=False)

dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfwk=pd.DataFrame()
dfwk['firstdate']=sorted([datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitentry['firstdate'].unique()])
dfwk['firstdate']=[x.strftime('%m/%d/%Y') for x in dfwk['firstdate']]
dfwk['weekid']=np.repeat(list(range(1,int(len(dfwk)/7)+2)),7)[0:len(dfwk)]
dfwk['weekfirstdate']=np.repeat(list(dfwk.drop_duplicates('weekid',keep='first')['firstdate']),7)[0:len(dfwk)]
dfunitwkentry=pd.merge(dfunitentry,dfwk,how='left',on='firstdate')
dfunitwkentry=dfunitwkentry.groupby(['unit','weekid','weekfirstdate'],as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
dfunitwkentry.to_csv(path+'dfunitwkentry.csv',index=False)



# Exits
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    try:
        tpunit=tp[tp['unit']==i].reset_index(drop=True)
        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
            rttp+=4*3600
            rtlist+=[rttp]
        rtlist=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in rtlist]
        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
        rtunit['unit']=rtunit['Remote'].copy()
        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpexit).reset_index(drop=True)
        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
        print(str(i)+': success')
    except:
        print(str(i)+': fail')
df=df[['id','unit','firstdate','time','exits','flagtime','flagexit']].reset_index(drop=True)
dfflagtime=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
dfflagexit=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagexit':'sum'}).reset_index(drop=True)
dfunitexit=df[(df['flagtime']==0)&(df['flagexit']==0)].reset_index(drop=True)
dfunitexit=dfunitexit.groupby(['unit','firstdate','time'],as_index=False).agg({'exits':'sum','id':'count'}).reset_index(drop=True)
dfunitexit.columns=['unit','firstdate','time','exits','gooducs']
dfunitexit=pd.merge(dfunitexit,dfflagtime,how='left',on=['unit','firstdate','time'])
dfunitexit=pd.merge(dfunitexit,dfflagexit,how='left',on=['unit','firstdate','time'])
dfunitexit.to_csv(path+'dfunitexit.csv',index=False)
dfunitexit=pd.read_csv(path+'dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
dfdateexit=dfunitexit.groupby('firstdate',as_index=False).agg({'exits':'sum'}).reset_index(drop=True)
dfdateexit['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
dfdateexit=dfdateexit.sort_values('firstdate').reset_index(drop=True)
dfdateexit['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
dfdateexit.to_csv(path+'dfdateexit.csv',index=False)
dfunitexit=pd.read_csv(path+'dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
dfwk=pd.DataFrame()
dfwk['firstdate']=sorted([datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitexit['firstdate'].unique()])
dfwk['firstdate']=[x.strftime('%m/%d/%Y') for x in dfwk['firstdate']]
dfwk['weekid']=np.repeat(list(range(1,int(len(dfwk)/7)+2)),7)[0:len(dfwk)]
dfwk['weekfirstdate']=np.repeat(list(dfwk.drop_duplicates('weekid',keep='first')['firstdate']),7)[0:len(dfwk)]
dfunitwkexit=pd.merge(dfunitexit,dfwk,how='left',on='firstdate')
dfunitwkexit=dfunitwkexit.groupby(['unit','weekid','weekfirstdate'],as_index=False).agg({'exits':'sum','gooducs':'sum','flagtime':'sum','flagexit':'sum'}).reset_index(drop=True)
dfunitwkexit.to_csv(path+'dfunitwkexit.csv',index=False)

print(datetime.datetime.now()-start)
# 80 mins



# Validation
turnstile=pd.read_csv(path+'dfunitwkentry.csv',dtype=float,converters={'unit':str,'weekfirstdate':str})
fare=pd.read_csv('C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/FARE/fare.csv',dtype=str,converters={'fare':float})
fare['weekfirstdate']=[str(x)[0:10] for x in fare['week']]
unitwkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
unitwkvld=unitwkvld[['unit','weekid','week','weekfirstdate','fare','entries','gooducs','flagtime','flagentry']].sort_values(['unit','weekid']).reset_index(drop=True)
unitwkvld['diff']=unitwkvld['entries']-unitwkvld['fare']
unitwkvld['diffpct']=unitwkvld['diff']/unitwkvld['fare']
unitwkvld.to_csv(path+'unitwkvld.csv',index=False)
wkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
wkvld=wkvld.groupby(['weekid','week','weekfirstdate'],as_index=False).agg({'fare':'sum','entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
wkvld=wkvld.sort_values('weekid').reset_index(drop=True)
wkvld['diff']=wkvld['entries']-wkvld['fare']
wkvld['diffpct']=wkvld['diff']/wkvld['fare']
wkvld.to_csv(path+'wkvld.csv',index=False)
