import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz
import geopandas as gpd
import shapely
import holidays



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/TURNSTILE/'
# path='/home/mayijun/TURNSTILE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float,'Hub':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)



## Download data
#dl=datetime.datetime(2020,4,11)
#for i in range(0,20):
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
#ucsentry=tpunit[tpunit['id']=='R044|R210|00-00-01'].reset_index(drop=True)
#ucsentry=tpunit[tpunit['id']=='R152|H041|00-00-01'].reset_index(drop=True)
# ucsentry=tpunit[tpunit['id']=='R280|N006A|00-00-02'].reset_index(drop=True)
# ucsentry=tpunit[tpunit['id']=='R104|N207|00-00-00'].reset_index(drop=True)
def unitcascpentry(ucsentry):
    global rtunit
    ucsentry=ucsentry.reset_index(drop=True)
    ucsid=ucsentry.loc[0,'id']
    ucsentry=pd.merge(rtunit,ucsentry,how='left',on=['unit','firstdate','firsttime'])
    ucsentry['nextdate']=np.roll(ucsentry['firstdate'],-1)
    ucsentry['nexttime']=np.roll(ucsentry['firsttime'],-1)
    ucsentry['nextdesc']=np.roll(ucsentry['firstdesc'],-1)
    ucsentry['nextentries']=np.roll(ucsentry['firstentries'],-1)
    ucsentry['time']=ucsentry['firsttime']+'-'+ucsentry['nexttime']
    ucsentry['entries']=abs(ucsentry['nextentries']-ucsentry['firstentries'])
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
    ucsexit=ucsexit.reset_index(drop=True)
    ucsid=ucsexit.loc[0,'id']
    ucsexit=pd.merge(rtunit,ucsexit,how='left',on=['unit','firstdate','firsttime'])
    ucsexit['nextdate']=np.roll(ucsexit['firstdate'],-1)
    ucsexit['nexttime']=np.roll(ucsexit['firsttime'],-1)
    ucsexit['nextdesc']=np.roll(ucsexit['firstdesc'],-1)
    ucsexit['nextexits']=np.roll(ucsexit['firstexits'],-1)
    ucsexit['time']=ucsexit['firsttime']+'-'+ucsexit['nexttime']
    ucsexit['exits']=abs(ucsexit['nextexits']-ucsexit['firstexits'])
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
        for j in range(0,len(tp['firstdate'].unique())*6-1):
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
dfunitentry.to_csv(path+'OUTPUT/dfunitentry.csv',index=False)
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfdateentry=dfunitentry.groupby('firstdate',as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
dfdateentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry=dfdateentry.sort_values('firstdate').reset_index(drop=True)
dfdateentry['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry.to_csv(path+'OUTPUT/dfdateentry.csv',index=False)

# Exits
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    try:
        tpunit=tp[tp['unit']==i].reset_index(drop=True)
        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tp['firstdate'].unique())*6-1):
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
dfunitexit.to_csv(path+'OUTPUT/dfunitexit.csv',index=False)
dfunitexit=pd.read_csv(path+'OUTPUT/dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
dfdateexit=dfunitexit.groupby('firstdate',as_index=False).agg({'exits':'sum'}).reset_index(drop=True)
dfdateexit['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
dfdateexit=dfdateexit.sort_values('firstdate').reset_index(drop=True)
dfdateexit['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
dfdateexit.to_csv(path+'OUTPUT/dfdateexit.csv',index=False)

print(datetime.datetime.now()-start)
# 40 mins



# Fare Validation
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfwk=pd.DataFrame()
dfwk['firstdate']=sorted([datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitentry['firstdate'].unique()])
dfwk['firstdate']=[x.strftime('%m/%d/%Y') for x in dfwk['firstdate']]
dfwk['weekid']=np.repeat(list(range(1,int(len(dfwk)/7)+2)),7)[0:len(dfwk)]
dfwk['weekfirstdate']=np.repeat(list(dfwk.drop_duplicates('weekid',keep='first')['firstdate']),7)[0:len(dfwk)]
dfunitwkentry=pd.merge(dfunitentry,dfwk,how='left',on='firstdate')
dfunitwkentry=dfunitwkentry.groupby(['unit','weekid','weekfirstdate'],as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
dfunitwkentry.to_csv(path+'VALIDATION/FARE/dfunitwkentry.csv',index=False)
turnstile=pd.read_csv(path+'VALIDATION/FARE/dfunitwkentry.csv',dtype=float,converters={'unit':str,'weekfirstdate':str})
fare=pd.read_csv('C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/FARE/fare.csv',dtype=str,converters={'fare':float})
fare['weekfirstdate']=[str(x)[0:10] for x in fare['week']]
unitwkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
unitwkvld=unitwkvld[['unit','weekid','week','weekfirstdate','fare','entries','gooducs','flagtime','flagentry']].sort_values(['unit','weekid']).reset_index(drop=True)
unitwkvld['diff']=unitwkvld['entries']-unitwkvld['fare']
unitwkvld['diffpct']=unitwkvld['diff']/unitwkvld['fare']
unitwkvld.to_csv(path+'VALIDATION/FARE/unitwkvld.csv',index=False)
wkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
wkvld=wkvld.groupby(['weekid','week','weekfirstdate'],as_index=False).agg({'fare':'sum','entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
wkvld=wkvld.sort_values('weekid').reset_index(drop=True)
wkvld['diff']=wkvld['entries']-wkvld['fare']
wkvld['diffpct']=wkvld['diff']/wkvld['fare']
wkvld.to_csv(path+'VALIDATION/FARE/wkvld.csv',index=False)

## Hourly Validation
#turnstile=pd.read_csv(path+'VALIDATION/HOURLY/dfunitentry2017.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#turnstile=turnstile[[str(x)[6:11]=='2017' for x in turnstile['firstdate']]].reset_index(drop=True)
#turnstile=pd.merge(turnstile,rc,how='left',left_on='unit',right_on='Remote')
#turnstile['weekday']=[datetime.datetime.strptime(str(x),'%m/%d/%Y').weekday() for x in turnstile['firstdate']]
#turnstileweekday=turnstile[np.isin(turnstile['weekday'],range(0,5))].reset_index(drop=True)
#turnstileweekday['time']=['T'+str(x)[0:2]+'-'+str(x)[9:11] for x in turnstileweekday['time']]
#turnstileweekday=turnstileweekday.groupby(['unit','CplxID','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#turnstileweekday=turnstileweekday.groupby(['CplxID','time'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#turnstileweekday=turnstileweekday.pivot(index='CplxID',columns='time',values='entries').reset_index(drop=False)
#turnstileweekday.to_csv(path+'VALIDATION/HOURLY/turnstileweekday2017.csv',index=False)



## Adjust Exits
#dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#dfunitentry['weekday']=[datetime.datetime.strptime(str(x),'%m/%d/%Y').weekday() for x in dfunitentry['firstdate']]
#dfunitentry2019wkd=dfunitentry[[str(x)[6:11]=='2019' for x in dfunitentry['firstdate']]].reset_index(drop=True)
#dfunitentry2019wkd=dfunitentry2019wkd[np.isin(dfunitentry2019wkd['weekday'],range(0,5))].reset_index(drop=True)
#dfunitentry2019wkd=dfunitentry2019wkd.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#dfunitentry2018wkd=dfunitentry[[str(x)[6:11]=='2018' for x in dfunitentry['firstdate']]].reset_index(drop=True)
#dfunitentry2018wkd=dfunitentry2018wkd[np.isin(dfunitentry2018wkd['weekday'],range(0,5))].reset_index(drop=True)
#dfunitentry2018wkd=dfunitentry2018wkd.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#
#dfunitexit=pd.read_csv(path+'OUTPUT/dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
#dfunitexit['weekday']=[datetime.datetime.strptime(str(x),'%m/%d/%Y').weekday() for x in dfunitexit['firstdate']]
#dfunitexit2019wkd=dfunitexit[[str(x)[6:11]=='2019' for x in dfunitexit['firstdate']]].reset_index(drop=True)
#dfunitexit2019wkd=dfunitexit2019wkd[np.isin(dfunitexit2019wkd['weekday'],range(0,5))].reset_index(drop=True)
#dfunitexit2019wkd=dfunitexit2019wkd.groupby(['unit','firstdate'],as_index=False).agg({'exits':'sum'}).reset_index(drop=True)
#dfunitexit2018wkd=dfunitexit[[str(x)[6:11]=='2018' for x in dfunitexit['firstdate']]].reset_index(drop=True)
#dfunitexit2018wkd=dfunitexit2018wkd[np.isin(dfunitexit2018wkd['weekday'],range(0,5))].reset_index(drop=True)
#dfunitexit2018wkd=dfunitexit2018wkd.groupby(['unit','firstdate'],as_index=False).agg({'exits':'sum'}).reset_index(drop=True)
#
#dfunitdate2019wkd=pd.merge(dfunitentry2019wkd,dfunitexit2019wkd,how='inner',on=['unit','firstdate'])
#dfunitdate2019wkd['medianadj2019']=dfunitdate2019wkd['entries']/dfunitdate2019wkd['exits']
#dfunitdate2019wkdmedian=dfunitdate2019wkd.groupby('unit',as_index=False).agg({'medianadj2019':'median'}).reset_index(drop=True)
#dfunitdate2019wkdsum=dfunitdate2019wkd.groupby('unit',as_index=False).agg({'entries':'sum','exits':'sum'}).reset_index(drop=True)
#dfunitdate2019wkdsum['sumadj2019']=dfunitdate2019wkdsum['entries']/dfunitdate2019wkdsum['exits']
#dfunitdate2019wkdsum=dfunitdate2019wkdsum[['unit','sumadj2019']].reset_index(drop=True)
#dfunitdate2019wkdadjrate=pd.merge(dfunitdate2019wkdmedian,dfunitdate2019wkdsum,how='inner',on='unit')
#dfunitdate2019wkdadjrate.to_csv(path+'EXITADJ/dfunitdate2019wkdadjrate.csv',index=False)
#
#dfunitdate2018wkd=pd.merge(dfunitentry2018wkd,dfunitexit2018wkd,how='inner',on=['unit','firstdate'])
#dfunitdate2018wkd['medianadj2018']=dfunitdate2018wkd['entries']/dfunitdate2018wkd['exits']
#dfunitdate2018wkdmedian=dfunitdate2018wkd.groupby('unit',as_index=False).agg({'medianadj2018':'median'}).reset_index(drop=True)
#dfunitdate2018wkdsum=dfunitdate2018wkd.groupby('unit',as_index=False).agg({'entries':'sum','exits':'sum'}).reset_index(drop=True)
#dfunitdate2018wkdsum['sumadj2018']=dfunitdate2018wkdsum['entries']/dfunitdate2018wkdsum['exits']
#dfunitdate2018wkdsum=dfunitdate2018wkdsum[['unit','sumadj2018']].reset_index(drop=True)
#dfunitdate2018wkdadjrate=pd.merge(dfunitdate2018wkdmedian,dfunitdate2018wkdsum,how='inner',on='unit')
#dfunitdate2018wkdadjrate.to_csv(path+'EXITADJ/dfunitdate2018wkdadjrate.csv',index=False)
#
#dfunitdate2019wkd=pd.merge(dfunitentry2019wkd,dfunitexit2019wkd,how='inner',on=['unit','firstdate'])
#dfunitdate2019wkdadj=pd.merge(dfunitdate2019wkd,dfunitdate2019wkdadjrate,how='inner',on='unit')
#dfunitdate2019wkdadj['exitsmedianadj']=dfunitdate2019wkdadj['exits']*dfunitdate2019wkdadj['medianadj2019']
#dfunitdate2019wkdadj['exitssumadj']=dfunitdate2019wkdadj['exits']*dfunitdate2019wkdadj['sumadj2019']
#dfunitdate2019wkdadj=dfunitdate2019wkdadj.groupby('firstdate',as_index=False).agg({'entries':'sum','exitsmedianadj':'sum','exitssumadj':'sum'}).reset_index(drop=True)
#dfunitdate2019wkdadj.to_csv(path+'EXITADJ/dfunitdate2019wkdadj.csv',index=False)



# # Comparison
# # AM Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019','07/29/2019','07/30/2019','07/31/2019','08/01/2019']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020','07/24/2020','07/27/2020','07/28/2020','07/29/2020','07/30/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxamdiff.to_csv(path+'OUTPUT/cplxamdiff.csv',index=False)

# # PM Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019','07/29/2019','07/30/2019','07/31/2019','08/01/2019']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020','07/24/2020','07/27/2020','07/28/2020','07/29/2020','07/30/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxpmdiff.to_csv(path+'OUTPUT/cplxpmdiff.csv',index=False)

# # Period Diff
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019','07/29/2019','07/30/2019','07/31/2019','08/01/2019']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020','07/24/2020','07/27/2020','07/28/2020','07/29/2020','07/30/2020']
# period1=['01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
# period2=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# period3=['09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
# period4=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# period5=['17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
# period6=['21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
# period1=pd.DataFrame(period1,columns=['timeperiod'])
# period1['periodid']=1
# period2=pd.DataFrame(period2,columns=['timeperiod'])
# period2['periodid']=2
# period3=pd.DataFrame(period3,columns=['timeperiod'])
# period3['periodid']=3
# period4=pd.DataFrame(period4,columns=['timeperiod'])
# period4['periodid']=4
# period5=pd.DataFrame(period5,columns=['timeperiod'])
# period5['periodid']=5
# period6=pd.DataFrame(period6,columns=['timeperiod'])
# period6['periodid']=6
# periodlist=pd.concat([period1,period2,period3,period4,period5,period6],ignore_index=True)
# pdpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# pdpre=pd.merge(pdpre,periodlist,how='left',left_on='time',right_on='timeperiod')
# pdpre=pdpre.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
# pdpre.columns=['Date','Period','PreEntries']
# pdpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# pdpost=pd.merge(pdpost,periodlist,how='left',left_on='time',right_on='timeperiod')
# pdpost=pdpost.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
# pdpost.columns=['Date','Period','PostEntries']
# pddiff=pd.concat([pdpre,pdpost],ignore_index=True)
# pddiff.to_csv(path+'OUTPUT/pddiff.csv',index=False)

# Hub Bound
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['03/11/2019','03/12/2019','03/13/2019','03/14/2019','03/15/2019','03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019',
          '03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019','04/01/2019','04/02/2019','04/03/2019','04/04/2019','04/05/2019',
          '04/08/2019','04/09/2019','04/10/2019','04/11/2019','04/12/2019','04/15/2019','04/16/2019','04/17/2019','04/18/2019',
          '04/22/2019','04/23/2019','04/24/2019','04/25/2019','04/26/2019','04/29/2019','04/30/2019','05/01/2019','05/02/2019','05/03/2019',
          '05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019','05/13/2019','05/14/2019','05/15/2019','05/16/2019','05/17/2019',
          '05/20/2019','05/21/2019','05/22/2019','05/23/2019','05/28/2019','05/29/2019','05/30/2019','05/31/2019',
          '06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019','06/10/2019','06/11/2019','06/12/2019','06/13/2019','06/14/2019',
          '06/17/2019','06/18/2019','06/19/2019','06/20/2019','06/21/2019','06/24/2019','06/25/2019','06/26/2019','06/27/2019','06/28/2019',
          '07/01/2019','07/02/2019','07/03/2019','07/08/2019','07/09/2019','07/10/2019','07/11/2019','07/12/2019',
          '07/15/2019','07/16/2019','07/17/2019','07/18/2019','07/19/2019','07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019',
          '07/29/2019','07/30/2019','07/31/2019','08/01/2019','08/02/2019','08/05/2019','08/07/2019','08/08/2019','08/09/2019',
          '08/12/2019','08/13/2019','08/14/2019','08/15/2019','08/16/2019','08/19/2019','08/20/2019','08/21/2019','08/22/2019','08/23/2019',
          '08/26/2019','08/27/2019','08/28/2019','08/29/2019','08/30/2019','09/03/2019','09/04/2019','09/05/2019','09/06/2019',
          '09/09/2019','09/10/2019','09/11/2019','09/12/2019']
postdates=['03/09/2020','03/10/2020','03/11/2020','03/12/2020','03/13/2020','03/16/2020','03/17/2020','03/18/2020','03/19/2020','03/20/2020',
            '03/23/2020','03/24/2020','03/25/2020','03/26/2020','03/27/2020','03/30/2020','03/31/2020','04/01/2020','04/02/2020','04/03/2020',
            '04/06/2020','04/07/2020','04/08/2020','04/09/2020','04/13/2020','04/14/2020','04/15/2020','04/16/2020','04/17/2020',
            '04/20/2020','04/21/2020','04/22/2020','04/23/2020','04/24/2020','04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020',
            '05/04/2020','05/05/2020','05/06/2020','05/07/2020','05/08/2020','05/11/2020','05/12/2020','05/13/2020','05/14/2020','05/15/2020',
            '05/18/2020','05/19/2020','05/20/2020','05/21/2020','05/26/2020','05/27/2020','05/28/2020','05/29/2020',
            '06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020','07/01/2020','07/06/2020','07/07/2020','07/08/2020','07/09/2020','07/10/2020',
            '07/13/2020','07/14/2020','07/15/2020','07/16/2020','07/17/2020','07/20/2020','07/21/2020','07/22/2020','07/23/2020','07/24/2020',
            '07/27/2020','07/28/2020','07/29/2020','07/30/2020','07/31/2020','08/03/2020','08/05/2020','08/06/2020','08/07/2020',
            '08/10/2020','08/11/2020','08/12/2020','08/13/2020','08/14/2020','08/17/2020','08/18/2020','08/19/2020','08/20/2020','08/21/2020',
            '08/24/2020','08/25/2020','08/26/2020','08/27/2020','08/28/2020','08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020',
            '09/08/2020','09/09/2020','09/10/2020']
pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre[cplxpmpre['Hub']==1].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost[cplxpmpost['Hub']==1].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
hub=pd.concat([cplxpmpre,cplxpmpost],axis=0,ignore_index=True)
hub.to_csv(path+'OUTPUT/hub.csv',index=False)

## Subway Closure
#dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#postdates=['04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020','05/04/2020','05/05/2020']
#offtime=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
#cplxamoff=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
#cplxamoff=cplxamoff[np.isin(cplxamoff['time'],offtime)].reset_index(drop=True)
#cplxamoff=cplxamoff.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#cplxamoff=pd.merge(cplxamoff,rc,how='left',left_on='unit',right_on='Remote')
#cplxamoff=cplxamoff.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
#cplxamoff.columns=['CplxID','PostTime','PostEntries']
#cplxamoff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamoff,how='left',on='CplxID')
#cplxamoff=cplxamoff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','PostTime','PostEntries']].reset_index(drop=True)
#cplxamoff['PostEntries']=cplxamoff['PostEntries'].fillna(0)
#cplxamoff.to_csv(path+'OUTPUT/cplxamoff.csv',index=False)
#
#dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#predates=['04/29/2020','04/30/2020']
#postdates=['05/06/2020','05/07/2020']
#offtime=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
#cplxamoffpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
#cplxamoffpre=cplxamoffpre[np.isin(cplxamoffpre['time'],offtime)].reset_index(drop=True)
#cplxamoffpre=cplxamoffpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#cplxamoffpre=pd.merge(cplxamoffpre,rc,how='left',left_on='unit',right_on='Remote')
#cplxamoffpre=cplxamoffpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
#cplxamoffpre.columns=['CplxID','PreTime','PreEntries']
#cplxamoffpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
#cplxamoffpost=cplxamoffpost[np.isin(cplxamoffpost['time'],offtime)].reset_index(drop=True)
#cplxamoffpost=cplxamoffpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#cplxamoffpost=pd.merge(cplxamoffpost,rc,how='left',left_on='unit',right_on='Remote')
#cplxamoffpost=cplxamoffpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
#cplxamoffpost.columns=['CplxID','PostTime','PostEntries']
#cplxamoffdiff=pd.merge(cplxamoffpre,cplxamoffpost,how='inner',on='CplxID')
#cplxamoffdiff['Time']=cplxamoffdiff['PreTime'].copy()
#cplxamoffdiff['Diff']=cplxamoffdiff['PostEntries']-cplxamoffdiff['PreEntries']
#cplxamoffdiff['DiffPct']=cplxamoffdiff['Diff']/cplxamoffdiff['PreEntries']
#cplxamoffdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamoffdiff,how='left',on='CplxID')
#cplxamoffdiff=cplxamoffdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
#cplxamoffdiff.to_csv(path+'OUTPUT/cplxamoffdiff.csv',index=False)
#
#dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#predates=['04/29/2020','04/30/2020']
#postdates=['05/06/2020','05/07/2020']
#latestdates=['05/27/2020','05/28/2020']
#period1=['11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00','13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00']
#period2=['15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00','17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00']
#period3=['19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00','21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00']
#period4=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
#period5=['03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00','05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00']
#period6=['07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00','09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00']
#period1=pd.DataFrame(period1,columns=['timeperiod'])
#period1['periodid']=1
#period2=pd.DataFrame(period2,columns=['timeperiod'])
#period2['periodid']=2
#period3=pd.DataFrame(period3,columns=['timeperiod'])
#period3['periodid']=3
#period4=pd.DataFrame(period4,columns=['timeperiod'])
#period4['periodid']=4
#period5=pd.DataFrame(period5,columns=['timeperiod'])
#period5['periodid']=5
#period6=pd.DataFrame(period6,columns=['timeperiod'])
#period6['periodid']=6
#periodlist=pd.concat([period1,period2,period3,period4,period5,period6],ignore_index=True)
#pdpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
#pdpre=pd.merge(pdpre,periodlist,how='left',left_on='time',right_on='timeperiod')
#pdpre=pdpre.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#pdpre=pdpre.groupby(['periodid'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre.columns=['Period','PreEntries']
#pdpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
#pdpost=pd.merge(pdpost,periodlist,how='left',left_on='time',right_on='timeperiod')
#pdpost=pdpost.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#pdpost=pdpost.groupby(['periodid'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost.columns=['Period','PostEntries']
#pdlatest=dfunitentry[np.isin(dfunitentry['firstdate'],latestdates)].reset_index(drop=True)
#pdlatest=pd.merge(pdlatest,periodlist,how='left',left_on='time',right_on='timeperiod')
#pdlatest=pdlatest.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#pdlatest=pdlatest.groupby(['periodid'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdlatest.columns=['Period','LatestEntries']
#pddiff=pd.concat([pdpre,pdpost,pdlatest],ignore_index=True)
#pddiff.to_csv(path+'OUTPUT/pddiffclosure.csv',index=False)
#
## Period Diff by Station
#dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#predates=['04/20/2020','04/21/2020','04/22/2020','04/23/2020','04/24/2020']
#postdates=['04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020']
#period1=['11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00','13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00']
#period2=['15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00','17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00']
#period3=['19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00','21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00']
#period4=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
#period5=['03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00','05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00']
#period6=['07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00','09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00']
#pdpre1=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period1))].reset_index(drop=True)
#pdpre1=pdpre1.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre1.columns=['unit','pdpre1']
#pdpost1=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period1))].reset_index(drop=True)
#pdpost1=pdpost1.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost1.columns=['unit','pdpost1']
#pdpre2=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period2))].reset_index(drop=True)
#pdpre2=pdpre2.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre2.columns=['unit','pdpre2']
#pdpost2=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period2))].reset_index(drop=True)
#pdpost2=pdpost2.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost2.columns=['unit','pdpost2']
#pdpre3=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period3))].reset_index(drop=True)
#pdpre3=pdpre3.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre3.columns=['unit','pdpre3']
#pdpost3=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period3))].reset_index(drop=True)
#pdpost3=pdpost3.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost3.columns=['unit','pdpost3']
#pdpre4=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period4))].reset_index(drop=True)
#pdpre4=pdpre4.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre4.columns=['unit','pdpre4']
#pdpost4=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period4))].reset_index(drop=True)
#pdpost4=pdpost4.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost4.columns=['unit','pdpost4']
#pdpre5=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period5))].reset_index(drop=True)
#pdpre5=pdpre5.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre5.columns=['unit','pdpre5']
#pdpost5=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period5))].reset_index(drop=True)
#pdpost5=pdpost5.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost5.columns=['unit','pdpost5']
#pdpre6=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period6))].reset_index(drop=True)
#pdpre6=pdpre6.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpre6.columns=['unit','pdpre6']
#pdpost6=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period6))].reset_index(drop=True)
#pdpost6=pdpost6.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#pdpost6.columns=['unit','pdpost6']
#cplxpddiff=pd.merge(pdpre1,pdpost1,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpre2,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpost2,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpre3,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpost3,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpre4,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpost4,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpre5,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpost5,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpre6,how='outer',on='unit')
#cplxpddiff=pd.merge(cplxpddiff,pdpost6,how='outer',on='unit')
#cplxpddiff['pdpre']=cplxpddiff['pdpre1']+cplxpddiff['pdpre2']+cplxpddiff['pdpre3']+cplxpddiff['pdpre4']+cplxpddiff['pdpre5']+cplxpddiff['pdpre6']
#cplxpddiff['pdpost']=cplxpddiff['pdpost1']+cplxpddiff['pdpost2']+cplxpddiff['pdpost3']+cplxpddiff['pdpost4']+cplxpddiff['pdpost5']+cplxpddiff['pdpost6']
#cplxpddiff=pd.merge(rc,cplxpddiff,how='outer',left_on='Remote',right_on='unit')
#cplxpddiff=cplxpddiff.fillna(0)
#cplxpddiff=cplxpddiff.groupby(['CplxID'],as_index=False).agg({'pdpre1':'sum','pdpost1':'sum','pdpre2':'sum','pdpost2':'sum',
#                             'pdpre3':'sum','pdpost3':'sum','pdpre4':'sum','pdpost4':'sum','pdpre5':'sum','pdpost5':'sum','pdpre6':'sum',
#                             'pdpost6':'sum','pdpre':'sum','pdpost':'sum'}).reset_index(drop=True)
#cplxpddiff['pddiff1']=(cplxpddiff['pdpost1']-cplxpddiff['pdpre1'])/cplxpddiff['pdpre1']
#cplxpddiff['pddiff2']=(cplxpddiff['pdpost2']-cplxpddiff['pdpre2'])/cplxpddiff['pdpre2']
#cplxpddiff['pddiff3']=(cplxpddiff['pdpost3']-cplxpddiff['pdpre3'])/cplxpddiff['pdpre3']
#cplxpddiff['pddiff4']=(cplxpddiff['pdpost4']-cplxpddiff['pdpre4'])/cplxpddiff['pdpre4']
#cplxpddiff['pddiff5']=(cplxpddiff['pdpost5']-cplxpddiff['pdpre5'])/cplxpddiff['pdpre5']
#cplxpddiff['pddiff6']=(cplxpddiff['pdpost6']-cplxpddiff['pdpre6'])/cplxpddiff['pdpre6']
#cplxpddiff['pddiff']=(cplxpddiff['pdpost']-cplxpddiff['pdpre'])/cplxpddiff['pdpre']
#cplxpddiff=cplxpddiff.fillna(0)
#cplxpddiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpddiff,how='left',on='CplxID')
#cplxpddiff=cplxpddiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','pdpre1','pdpost1','pdpre2','pdpost2','pdpre3',
#                       'pdpost3','pdpre4','pdpost4','pdpre5','pdpost5','pdpre6','pdpost6','pdpre','pdpost','pddiff1','pddiff2','pddiff3',
#                       'pddiff4','pddiff5','pddiff6','pddiff']].reset_index(drop=True)
#cplxpddiff.to_csv(path+'OUTPUT/cplxpddiff.csv',index=False)

# # AM Peak Pre-Nadir-Recent
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# # predates=['03/11/2019','03/12/2019','03/13/2019','03/14/2019','03/15/2019',
# #           '03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019',
# #           '03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019',
# #           '04/01/2019','04/02/2019','04/03/2019','04/04/2019','04/05/2019',
# #           '04/08/2019','04/09/2019','04/10/2019','04/11/2019','04/12/2019',
# #           '04/15/2019','04/16/2019','04/17/2019','04/18/2019',
# #           '04/22/2019','04/23/2019','04/24/2019','04/25/2019','04/26/2019',
# #           '04/29/2019','04/30/2019','05/01/2019','05/02/2019','05/03/2019',
# #           '05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019',
# #           '05/13/2019','05/14/2019','05/15/2019','05/16/2019','05/17/2019',
# #           '05/20/2019','05/21/2019','05/22/2019','05/23/2019','05/24/2019',
# #           '05/28/2019','05/29/2019','05/30/2019','05/31/2019',
# #           '06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019',
# #           '06/10/2019','06/11/2019','06/12/2019','06/13/2019','06/14/2019',
# #           '06/17/2019','06/18/2019','06/19/2019','06/20/2019','06/21/2019',
# #           '06/24/2019','06/25/2019','06/26/2019','06/27/2019','06/28/2019']
# predates=['06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019',
#           '06/10/2019','06/11/2019','06/12/2019','06/13/2019','06/14/2019',
#           '06/17/2019','06/18/2019','06/19/2019','06/20/2019','06/21/2019',
#           '06/24/2019','06/25/2019','06/26/2019','06/27/2019','06/28/2019',
#           '07/01/2019','07/02/2019','07/03/2019',
#           '07/08/2019','07/09/2019','07/10/2019','07/11/2019','07/12/2019',
#           '07/15/2019','07/16/2019','07/17/2019','07/18/2019','07/19/2019',
#           '07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019',
#           '07/29/2019','07/30/2019','07/31/2019']
# nadirdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# latestdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxamnadir=dfunitentry[np.isin(dfunitentry['firstdate'],nadirdates)].reset_index(drop=True)
# cplxamnadir=cplxamnadir[np.isin(cplxamnadir['time'],amlist)].reset_index(drop=True)
# cplxamnadir=cplxamnadir.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamnadir=pd.merge(cplxamnadir,rc,how='left',left_on='unit',right_on='Remote')
# cplxamnadir=cplxamnadir.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamnadir.columns=['CplxID','NadirTime','NadirEntries']
# cplxamlatest=dfunitentry[np.isin(dfunitentry['firstdate'],latestdates)].reset_index(drop=True)
# cplxamlatest=cplxamlatest[np.isin(cplxamlatest['time'],amlist)].reset_index(drop=True)
# cplxamlatest=cplxamlatest.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamlatest=pd.merge(cplxamlatest,rc,how='left',left_on='unit',right_on='Remote')
# cplxamlatest=cplxamlatest.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamlatest.columns=['CplxID','LatestTime','LatestEntries']
# cplxam=pd.merge(cplxampre,cplxamnadir,how='inner',on='CplxID')
# cplxam=pd.merge(cplxam,cplxamlatest,how='inner',on='CplxID')
# cplxam['Time']=cplxam['PreTime'].copy()
# cplxam['Diff1']=cplxam['NadirEntries']-cplxam['PreEntries']
# cplxam['DiffPct1']=cplxam['Diff1']/cplxam['PreEntries']
# cplxam['Diff2']=cplxam['LatestEntries']-cplxam['NadirEntries']
# cplxam['DiffPct2']=cplxam['Diff2']/cplxam['NadirEntries']
# cplxam=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxam,how='left',on='CplxID')
# cplxam=cplxam[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','NadirEntries','LatestEntries','Diff1',
#                 'DiffPct1','Diff2','DiffPct2']].reset_index(drop=True)
# cplxam=cplxam.fillna(0)
# cplxam.to_csv(path+'OUTPUT/cplxam.csv',index=False)

# # PM Peak Pre-Nadir-Recent
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# # predates=['03/11/2019','03/12/2019','03/13/2019','03/14/2019','03/15/2019',
# #           '03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019',
# #           '03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019',
# #           '04/01/2019','04/02/2019','04/03/2019','04/04/2019','04/05/2019',
# #           '04/08/2019','04/09/2019','04/10/2019','04/11/2019','04/12/2019',
# #           '04/15/2019','04/16/2019','04/17/2019','04/18/2019',
# #           '04/22/2019','04/23/2019','04/24/2019','04/25/2019','04/26/2019',
# #           '04/29/2019','04/30/2019','05/01/2019','05/02/2019','05/03/2019',
# #           '05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019',
# #           '05/13/2019','05/14/2019','05/15/2019','05/16/2019','05/17/2019',
# #           '05/20/2019','05/21/2019','05/22/2019','05/23/2019','05/24/2019',
# #           '05/28/2019','05/29/2019','05/30/2019','05/31/2019',
# #           '06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019',
# #           '06/10/2019','06/11/2019','06/12/2019','06/13/2019','06/14/2019',
# #           '06/17/2019','06/18/2019','06/19/2019','06/20/2019','06/21/2019',
# #           '06/24/2019','06/25/2019','06/26/2019','06/27/2019','06/28/2019']
# predates=['06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019',
#           '06/10/2019','06/11/2019','06/12/2019','06/13/2019','06/14/2019',
#           '06/17/2019','06/18/2019','06/19/2019','06/20/2019','06/21/2019',
#           '06/24/2019','06/25/2019','06/26/2019','06/27/2019','06/28/2019',
#           '07/01/2019','07/02/2019','07/03/2019',
#           '07/08/2019','07/09/2019','07/10/2019','07/11/2019','07/12/2019',
#           '07/15/2019','07/16/2019','07/17/2019','07/18/2019','07/19/2019',
#           '07/22/2019','07/23/2019','07/24/2019','07/25/2019','07/26/2019',
#           '07/29/2019','07/30/2019','07/31/2019']
# nadirdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# latestdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmnadir=dfunitentry[np.isin(dfunitentry['firstdate'],nadirdates)].reset_index(drop=True)
# cplxpmnadir=cplxpmnadir[np.isin(cplxpmnadir['time'],pmlist)].reset_index(drop=True)
# cplxpmnadir=cplxpmnadir.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmnadir=pd.merge(cplxpmnadir,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmnadir=cplxpmnadir.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmnadir.columns=['CplxID','NadirTime','NadirEntries']
# cplxpmlatest=dfunitentry[np.isin(dfunitentry['firstdate'],latestdates)].reset_index(drop=True)
# cplxpmlatest=cplxpmlatest[np.isin(cplxpmlatest['time'],pmlist)].reset_index(drop=True)
# cplxpmlatest=cplxpmlatest.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmlatest=pd.merge(cplxpmlatest,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmlatest=cplxpmlatest.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmlatest.columns=['CplxID','LatestTime','LatestEntries']
# cplxpm=pd.merge(cplxpmpre,cplxpmnadir,how='inner',on='CplxID')
# cplxpm=pd.merge(cplxpm,cplxpmlatest,how='inner',on='CplxID')
# cplxpm['Time']=cplxpm['PreTime'].copy()
# cplxpm['Diff1']=cplxpm['NadirEntries']-cplxpm['PreEntries']
# cplxpm['DiffPct1']=cplxpm['Diff1']/cplxpm['PreEntries']
# cplxpm['Diff2']=cplxpm['LatestEntries']-cplxpm['NadirEntries']
# cplxpm['DiffPct2']=cplxpm['Diff2']/cplxpm['NadirEntries']
# cplxpm=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpm,how='left',on='CplxID')
# cplxpm=cplxpm[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','NadirEntries','LatestEntries','Diff1',
#                 'DiffPct1','Diff2','DiffPct2']].reset_index(drop=True)
# cplxpm=cplxpm.fillna(0)
# cplxpm.to_csv(path+'OUTPUT/cplxpm.csv',index=False)

# # AM Peak Pre and Post Phase1
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxamdiff.to_csv(path+'OUTPUT/cplxamdiffp1.csv',index=False)

# # PM Peak Pre and Post Phase1
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxpmdiff.to_csv(path+'OUTPUT/cplxpmdiffp1.csv',index=False)

# # AM Peak Previous and Curren Week during Phase 2
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/13/2020','07/14/2020','07/15/2020','07/16/2020']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxamdiff.to_csv(path+'OUTPUT/cplxamdiffp2.csv',index=False)

# # PM Peak Previous and Curren Week during Phase 2
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/13/2020','07/14/2020','07/15/2020','07/16/2020']
# postdates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
# cplxpmdiff.to_csv(path+'OUTPUT/cplxpmdiffp2.csv',index=False)

# # AM Peak Pre and Post Phase1 by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['06/01/2020','06/03/2020','06/04/2020']
# postdates=['08/03/2020','08/05/2020','08/06/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:6539'})
# cplxamdiff['geometry']=cplxamdiff.buffer(2640)
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
# cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxamdiffnta['Diff']=cplxamdiffnta['PostEntries']-cplxamdiffnta['PreEntries']
# cplxamdiffnta['DiffPct']=cplxamdiffnta['Diff']/cplxamdiffnta['PreEntries']
# cplxamdiffnta=pd.merge(nta,cplxamdiffnta,how='inner',on='NTACode')
# cplxamdiffnta.to_file(path+'OUTPUT/cplxamdiffp1nta.shp')

# # PM Peak Pre and Post Phase1 by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['06/01/2020','06/03/2020','06/04/2020']
# postdates=['08/03/2020','08/05/2020','08/06/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:6539'})
# cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
# cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxpmdiffnta['Diff']=cplxpmdiffnta['PostEntries']-cplxpmdiffnta['PreEntries']
# cplxpmdiffnta['DiffPct']=cplxpmdiffnta['Diff']/cplxpmdiffnta['PreEntries']
# cplxpmdiffnta=pd.merge(nta,cplxpmdiffnta,how='inner',on='NTACode')
# cplxpmdiffnta.to_file(path+'OUTPUT/cplxpmdiffp1nta.shp')

# # AM Peak Previous and Current Week in Phase 2 by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# postdates=['07/27/2020','07/28/2020','07/29/2020','07/30/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:6539'})
# cplxamdiff['geometry']=cplxamdiff.buffer(2640)
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
# cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxamdiffnta['Diff']=cplxamdiffnta['PostEntries']-cplxamdiffnta['PreEntries']
# cplxamdiffnta['DiffPct']=cplxamdiffnta['Diff']/cplxamdiffnta['PreEntries']
# cplxamdiffnta=pd.merge(nta,cplxamdiffnta,how='inner',on='NTACode')
# cplxamdiffnta.to_file(path+'OUTPUT/cplxamdiffp2nta.shp')

# # PM Peak Previous and Current Week in Phase 2 by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# postdates=['07/27/2020','07/28/2020','07/29/2020','07/30/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:6539'})
# cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
# cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxpmdiffnta['Diff']=cplxpmdiffnta['PostEntries']-cplxpmdiffnta['PreEntries']
# cplxpmdiffnta['DiffPct']=cplxpmdiffnta['Diff']/cplxpmdiffnta['PreEntries']
# cplxpmdiffnta=pd.merge(nta,cplxpmdiffnta,how='inner',on='NTACode')
# cplxpmdiffnta.to_file(path+'OUTPUT/cplxpmdiffp2nta.shp')

# DBK AM Peak Pre and Post
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# unitlist=['R252','R301','R108','R089','R127','R225','R456','R099','R129','R217','R056','R318','R283','R057']
# dbkdates=['03/29/2020','03/30/2020','03/31/2020','04/01/2020','04/02/2020','04/03/2020','04/04/2020',
#           '06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020','06/27/2020','06/28/2020',
#           '06/29/2020','06/30/2020','07/01/2020','07/02/2020','07/03/2020','07/04/2020','07/05/2020',
#           '07/06/2020','07/07/2020','07/08/2020','07/09/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxamdbk=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
# cplxamdbk=cplxamdbk[np.isin(cplxamdbk['firstdate'],dbkdates)].reset_index(drop=True)
# cplxamdbk=cplxamdbk[np.isin(cplxamdbk['time'],amlist)].reset_index(drop=True)
# cplxamdbk=pd.merge(cplxamdbk,rc,how='left',left_on='unit',right_on='Remote')
# cplxamdbk=cplxamdbk.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamdbk.columns=['CplxID','Date','TimePeriod','Entries']
# cplxamdbk=pd.merge(cplxamdbk,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
# cplxamdbk=cplxamdbk[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
# cplxamdbk.to_csv(path+'OUTPUT/cplxamdbk.csv',index=False)

dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
unitlist=['R252','R301','R108','R089','R127','R225','R456','R099','R129','R217','R056','R318','R283','R057']
dbkdates1=['09/29/2019','09/30/2019','10/01/2019','10/02/2019','10/03/2019','10/04/2019','10/05/2019']
dbkdates2=['09/27/2020','09/28/2020','09/29/2020','09/30/2020','10/01/2020','10/02/2020','10/03/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxamdbk1=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxamdbk1=cplxamdbk1[np.isin(cplxamdbk1['firstdate'],dbkdates1)].reset_index(drop=True)
cplxamdbk1=cplxamdbk1[np.isin(cplxamdbk1['time'],amlist)].reset_index(drop=True)
cplxamdbk1=pd.merge(cplxamdbk1,rc,how='left',left_on='unit',right_on='Remote')
cplxamdbk1=cplxamdbk1.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxamdbk1.columns=['CplxID','Date','TimePeriod','Entries']
cplxamdbk1=pd.merge(cplxamdbk1,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxamdbk1=cplxamdbk1[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
cplxamdbk2=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxamdbk2=cplxamdbk2[np.isin(cplxamdbk2['firstdate'],dbkdates2)].reset_index(drop=True)
cplxamdbk2=cplxamdbk2[np.isin(cplxamdbk2['time'],amlist)].reset_index(drop=True)
cplxamdbk2=pd.merge(cplxamdbk2,rc,how='left',left_on='unit',right_on='Remote')
cplxamdbk2=cplxamdbk2.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxamdbk2.columns=['CplxID','Date','TimePeriod','Entries']
cplxamdbk2=pd.merge(cplxamdbk2,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxamdbk2=cplxamdbk2[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
cplxpmdbk1=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxpmdbk1=cplxpmdbk1[np.isin(cplxpmdbk1['firstdate'],dbkdates1)].reset_index(drop=True)
cplxpmdbk1=cplxpmdbk1[np.isin(cplxpmdbk1['time'],pmlist)].reset_index(drop=True)
cplxpmdbk1=pd.merge(cplxpmdbk1,rc,how='left',left_on='unit',right_on='Remote')
cplxpmdbk1=cplxpmdbk1.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmdbk1.columns=['CplxID','Date','TimePeriod','Entries']
cplxpmdbk1=pd.merge(cplxpmdbk1,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxpmdbk1=cplxpmdbk1[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
cplxpmdbk2=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxpmdbk2=cplxpmdbk2[np.isin(cplxpmdbk2['firstdate'],dbkdates2)].reset_index(drop=True)
cplxpmdbk2=cplxpmdbk2[np.isin(cplxpmdbk2['time'],pmlist)].reset_index(drop=True)
cplxpmdbk2=pd.merge(cplxpmdbk2,rc,how='left',left_on='unit',right_on='Remote')
cplxpmdbk2=cplxpmdbk2.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmdbk2.columns=['CplxID','Date','TimePeriod','Entries']
cplxpmdbk2=pd.merge(cplxpmdbk2,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxpmdbk2=cplxpmdbk2[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
cplxdbk=pd.concat([cplxamdbk1,cplxamdbk2,cplxpmdbk1,cplxpmdbk2],axis=0,ignore_index=True)
cplxdbk.to_csv(path+'OUTPUT/cplxdbk.csv',index=False)



dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
unitlist=['R056','R057','R089','R099','R108','R127','R217']
cplxdbk=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxdbk=pd.merge(cplxdbk,rc,how='left',left_on='unit',right_on='Remote')
cplxdbk=cplxdbk.groupby(['CplxID','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)   
cplxdbk['year']=[str(x)[6:] for x in cplxdbk['firstdate']]
cplxdbk['month']=[str(x)[:2] for x in cplxdbk['firstdate']]
cplxdbkpre=cplxdbk[(cplxdbk['year']=='2019')&(cplxdbk['month']=='06')].reset_index(drop=True)
cplxdbkpost=cplxdbk[(cplxdbk['year']=='2021')&(cplxdbk['month']=='06')].reset_index(drop=True)
cplxdbk=pd.concat([cplxdbkpre,cplxdbkpost],axis=0,ignore_index=True)
cplxdbk=cplxdbk[['CplxID','firstdate','entries']].reset_index(drop=True)
cplxdbk.columns=['CplxID','Date','Entries']
cplxdbk=pd.merge(cplxdbk,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxdbk=cplxdbk[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Date','Entries']].reset_index(drop=True)
cplxdbk.to_csv(path+'OUTPUT/cplxdbkjune.csv',index=False)






# # PM Peak Pre and Post Phase1
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# unitlist=['R252','R301','R108','R089','R127','R225','R456','R099','R129','R217','R056','R318','R283','R057']
# dbkdates=['03/29/2020','03/30/2020','03/31/2020','04/01/2020','04/02/2020','04/03/2020','04/04/2020',
#           '06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020','06/27/2020','06/28/2020',
#           '06/29/2020','06/30/2020','07/01/2020','07/02/2020','07/03/2020','07/04/2020','07/05/2020',
#           '07/06/2020','07/07/2020','07/08/2020','07/09/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmdbk=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
# cplxpmdbk=cplxpmdbk[np.isin(cplxpmdbk['firstdate'],dbkdates)].reset_index(drop=True)
# cplxpmdbk=cplxpmdbk[np.isin(cplxpmdbk['time'],pmlist)].reset_index(drop=True)
# cplxpmdbk=pd.merge(cplxpmdbk,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmdbk=cplxpmdbk.groupby(['CplxID','firstdate'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmdbk.columns=['CplxID','Date','TimePeriod','Entries']
# cplxpmdbk=pd.merge(cplxpmdbk,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
# cplxpmdbk=cplxpmdbk[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','TimePeriod','Date','Entries']].reset_index(drop=True)
# cplxpmdbk.to_csv(path+'OUTPUT/cplxpmdbk.csv',index=False)




# Coney Island
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
unitlist=['R151','R312']
cplxci=dfunitentry[np.isin(dfunitentry['unit'],unitlist)].reset_index(drop=True)
cplxci=pd.merge(cplxci,rc,how='left',left_on='unit',right_on='Remote')
cplxci=cplxci.groupby(['CplxID','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxci.columns=['CplxID','Date','Entries']
cplxci=pd.merge(cplxci,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxci=cplxci[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Date','Entries']].reset_index(drop=True)
cplxci.to_csv(path+'OUTPUT/cplxci.csv',index=False)









# # AM Peak Phase4 vs last year by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['08/05/2019','08/07/2019','08/08/2019']
# postdates=['08/03/2020','08/05/2020','08/06/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:6539'})
# cplxamdiff['geometry']=cplxamdiff.buffer(2640)
# cplxamdiff=cplxamdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
# cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxamdiffnta['Diff']=cplxamdiffnta['PostEntries']-cplxamdiffnta['PreEntries']
# cplxamdiffnta['DiffPct']=cplxamdiffnta['Diff']/cplxamdiffnta['PreEntries']
# cplxamdiffnta=pd.merge(nta,cplxamdiffnta,how='inner',on='NTACode')
# cplxamdiffnta.to_file(path+'OUTPUT/cplxamdiffp4nta.shp')

# # PM Peak Phase4 vs last year by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['08/05/2019','08/07/2019','08/08/2019']
# postdates=['08/03/2020','08/05/2020','08/06/2020']
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
# cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpre.columns=['CplxID','PreTime','PreEntries']
# cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
# cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
# cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxpmpost.columns=['CplxID','PostTime','PostEntries']
# cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
# cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
# cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
# cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
# cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
# cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:6539'})
# cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
# cplxpmdiff=cplxpmdiff.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
# cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
# cplxpmdiffnta['Diff']=cplxpmdiffnta['PostEntries']-cplxpmdiffnta['PreEntries']
# cplxpmdiffnta['DiffPct']=cplxpmdiffnta['Diff']/cplxpmdiffnta['PreEntries']
# cplxpmdiffnta=pd.merge(nta,cplxpmdiffnta,how='inner',on='NTACode')
# cplxpmdiffnta.to_file(path+'OUTPUT/cplxpmdiffp4nta.shp')

# # PM Peak Pre and Post Phase1 by NTA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# week1=['06/01/2020','06/02/2020','06/03/2020','06/04/2020']
# week2=['06/08/2020','06/09/2020','06/10/2020','06/11/2020']
# week3=['06/15/2020','06/16/2020','06/17/2020','06/18/2020']
# week4=['06/22/2020','06/23/2020','06/24/2020','06/25/2020']
# week5=['06/29/2020','06/30/2020','07/01/2020']
# week6=['07/06/2020','07/07/2020','07/08/2020','07/09/2020']
# week7=['07/13/2020','07/14/2020','07/15/2020','07/16/2020']
# week8=['07/20/2020','07/21/2020','07/22/2020','07/23/2020']
# wk=[week1,week2,week3,week4,week5,week6,week7,week8]
# pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxpm=dfunitentry[np.isin(dfunitentry['firstdate'],wk[0])].reset_index(drop=True)
# cplxpm=cplxpm[np.isin(cplxpm['time'],pmlist)].reset_index(drop=True)
# cplxpm=cplxpm.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxpm=pd.merge(cplxpm,rc,how='left',left_on='unit',right_on='Remote')
# cplxpm=cplxpm.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
# cplxpm.columns=['CplxID','Week1']
# for i in range (1,8):
#     tp=dfunitentry[np.isin(dfunitentry['firstdate'],wk[i])].reset_index(drop=True)
#     tp=tp[np.isin(tp['time'],pmlist)].reset_index(drop=True)
#     tp=tp.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#     tp=pd.merge(tp,rc,how='left',left_on='unit',right_on='Remote')
#     tp=tp.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#     tp.columns=['CplxID','Week'+str(i+1)]
#     cplxpm=pd.merge(cplxpm,tp,how='outer',on='CplxID')
# cplxpm=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpm,how='left',on='CplxID')
# cplxpm=cplxpm[['CplxID','CplxLat','CplxLong','Week1','Week2','Week3','Week4','Week5','Week6','Week7','Week8']].reset_index(drop=True)
# cplxpm=gpd.GeoDataFrame(cplxpm,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs={'init' :'epsg:4326'})
# cplxpm=cplxpm.to_crs({'init':'epsg:6539'})
# cplxpm['geometry']=cplxpm.buffer(2640)
# cplxpm=cplxpm.to_crs({'init':'epsg:4326'})
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs={'init' :'epsg:4326'}
# cplxpmweeknta=gpd.sjoin(nta,cplxpm,how='left',op='intersects')
# cplxpmweeknta=cplxpmweeknta.groupby(['NTACode'],as_index=False).agg({'Week1':'sum','Week2':'sum','Week3':'sum',
#                                                                      'Week4':'sum','Week5':'sum','Week6':'sum',
#                                                                      'Week7':'sum','Week8':'sum'}).reset_index(drop=True)
# cplxpmweeknta=cplxpmweeknta[cplxpmweeknta['Week1']!=0].reset_index(drop=True)
# cplxpmweeknta['Week2DP']=(cplxpmweeknta['Week2']-cplxpmweeknta['Week1'])/cplxpmweeknta['Week1']
# cplxpmweeknta['Week3DP']=(cplxpmweeknta['Week3']-cplxpmweeknta['Week2'])/cplxpmweeknta['Week2']
# cplxpmweeknta['Week4DP']=(cplxpmweeknta['Week4']-cplxpmweeknta['Week3'])/cplxpmweeknta['Week3']
# cplxpmweeknta['Week5DP']=(cplxpmweeknta['Week5']-cplxpmweeknta['Week4'])/cplxpmweeknta['Week4']
# cplxpmweeknta['Week6DP']=(cplxpmweeknta['Week6']-cplxpmweeknta['Week5'])/cplxpmweeknta['Week5']
# cplxpmweeknta['Week7DP']=(cplxpmweeknta['Week7']-cplxpmweeknta['Week6'])/cplxpmweeknta['Week6']
# cplxpmweeknta['Week8DP']=(cplxpmweeknta['Week8']-cplxpmweeknta['Week7'])/cplxpmweeknta['Week7']
# cplxpmweeknta['AVGDP']=(cplxpmweeknta['Week2DP']+cplxpmweeknta['Week3DP']+cplxpmweeknta['Week4DP']+
#                         cplxpmweeknta['Week5DP']+cplxpmweeknta['Week6DP']+cplxpmweeknta['Week7DP']+
#                         cplxpmweeknta['Week8DP'])/7
# cplxpmweeknta['W1W8DP']=(cplxpmweeknta['Week8']-cplxpmweeknta['Week1'])/cplxpmweeknta['Week1']
# cplxpmweeknta=pd.merge(nta,cplxpmweeknta,how='inner',on='NTACode')
# cplxpmweeknta.to_file(path+'OUTPUT/cplxpmweeknta.shp')



# AM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E201909','E202009']
cplxamhed=cplxamdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E202004','E202006']
cplxamhed=pd.merge(cplxamhed,cplxamdiffnta,how='inner',on='NTACode')
cplxamhed['Diff1']=cplxamhed['E202009']-cplxamhed['E201909']
cplxamhed['DiffPct1']=cplxamhed['Diff1']/cplxamhed['E201909']
cplxamhed['Diff2']=cplxamhed['E202009']-cplxamhed['E202004']
cplxamhed['DiffPct2']=cplxamhed['Diff2']/cplxamhed['E202004']
cplxamhed['Diff3']=cplxamhed['E202009']-cplxamhed['E202006']
cplxamhed['DiffPct3']=cplxamhed['Diff3']/cplxamhed['E202006']
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat1']=np.where(cplxamhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxamhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxamhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxamhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxamhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat2']=np.where(cplxamhed['DiffPct2']<=2,'<=200%',
          np.where(cplxamhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxamhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxamhed['DiffPct2']<=3.5,'301%~350%',
          '>350%'))))
cplxamhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat3']=np.where(cplxamhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxamhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxamhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxamhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxamhed['Pct']=cplxamhed['E202009']/cplxamhed['E201909']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.25,'13%~25%',
                    np.where(cplxamhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxamhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxamhed['Pct']<=0.4,'36%~40%',
                             '41%~51%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/ntaamhed.geojson',driver='GeoJSON')



# PM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E201909','E202009']
cplxpmhed=cplxpmdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E202004','E202006']
cplxpmhed=pd.merge(cplxpmhed,cplxpmdiffnta,how='inner',on='NTACode')
cplxpmhed['Diff1']=cplxpmhed['E202009']-cplxpmhed['E201909']
cplxpmhed['DiffPct1']=cplxpmhed['Diff1']/cplxpmhed['E201909']
cplxpmhed['Diff2']=cplxpmhed['E202009']-cplxpmhed['E202004']
cplxpmhed['DiffPct2']=cplxpmhed['Diff2']/cplxpmhed['E202004']
cplxpmhed['Diff3']=cplxpmhed['E202009']-cplxpmhed['E202006']
cplxpmhed['DiffPct3']=cplxpmhed['Diff3']/cplxpmhed['E202006']
cplxpmhed=pd.merge(nta,cplxpmhed,how='inner',on='NTACode')
cplxpmhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat1']=np.where(cplxpmhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxpmhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxpmhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxpmhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxpmhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat2']=np.where(cplxpmhed['DiffPct2']<=2,'<=200%',
          np.where(cplxpmhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxpmhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxpmhed['DiffPct2']<=3.5,'301%~350%',
          '>350%'))))
cplxpmhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat3']=np.where(cplxpmhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxpmhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxpmhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxpmhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxpmhed['Pct']=cplxpmhed['E202009']/cplxpmhed['E201909']
cplxpmhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['PctCat']=np.where(cplxpmhed['Pct']<=0.25,'16%~25%',
                    np.where(cplxpmhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxpmhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxpmhed['Pct']<=0.4,'36%~40%',
                             '41%~55%'))))
cplxpmhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/ntapmhed.geojson',driver='GeoJSON')









# # Commuter Profile
# # AM Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# nadirdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# latestdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201904']
# cplxamnadir=dfunitentry[np.isin(dfunitentry['firstdate'],nadirdates)].reset_index(drop=True)
# cplxamnadir=cplxamnadir[np.isin(cplxamnadir['time'],amlist)].reset_index(drop=True)
# cplxamnadir=cplxamnadir.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamnadir=pd.merge(cplxamnadir,rc,how='left',left_on='unit',right_on='Remote')
# cplxamnadir=cplxamnadir.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamnadir.columns=['CplxID','NadirTime','E201909']
# cplxamlatest=dfunitentry[np.isin(dfunitentry['firstdate'],latestdates)].reset_index(drop=True)
# cplxamlatest=cplxamlatest[np.isin(cplxamlatest['time'],amlist)].reset_index(drop=True)
# cplxamlatest=cplxamlatest.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamlatest=pd.merge(cplxamlatest,rc,how='left',left_on='unit',right_on='Remote')
# cplxamlatest=cplxamlatest.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamlatest.columns=['CplxID','LatestTime','E202004']
# cplxamcp=pd.merge(cplxampre,cplxamnadir,how='inner',on='CplxID')
# cplxamcp=pd.merge(cplxamcp,cplxamlatest,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201904','E201909','E202004']].reset_index(drop=True)
# predates=['06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202006']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202009']
# cplxamcp=pd.merge(cplxamcp,cplxampre,how='inner',on='CplxID')
# cplxamcp=pd.merge(cplxamcp,cplxampost,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Time','E201904','E201909','E202004','E202006','E202009']].reset_index(drop=True)
# cplxamcp['Diff1']=cplxamcp['E202004']-cplxamcp['E201904']
# cplxamcp['DiffPct1']=cplxamcp['Diff1']/cplxamcp['E201904']
# cplxamcp['Diff2']=cplxamcp['E202006']-cplxamcp['E202004']
# cplxamcp['DiffPct2']=cplxamcp['Diff2']/cplxamcp['E202004']
# cplxamcp['Diff3']=cplxamcp['E202009']-cplxamcp['E202004']
# cplxamcp['DiffPct3']=cplxamcp['Diff3']/cplxamcp['E202004']
# cplxamcp['Diff4']=cplxamcp['E202009']-cplxamcp['E201909']
# cplxamcp['DiffPct4']=cplxamcp['Diff4']/cplxamcp['E201909']
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201904','E201909',
#                    'E202004','E202006','E202009','Diff1','DiffPct1','Diff2','DiffPct2','Diff3','DiffPct3',
#                    'Diff4','DiffPct4',]].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file(path+'OUTPUT/cplxamcp.shp')



# Commuter Profile
# AM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# predates=['12/03/2019','12/04/2019','12/05/2019','12/06/2019',
#           '12/09/2019','12/10/2019','12/11/2019','12/12/2019','12/13/2019',
#           '12/16/2019','12/17/2019','12/18/2019','12/19/2019','12/20/2019',
#           '12/23/2019','12/26/2019','12/27/2019',
#           '12/30/2019','12/31/2019','01/02/2020','01/03/2020']
# postdates=['12/01/2020','12/02/2020','12/03/2020','12/04/2020',
#            '12/07/2020','12/08/2020','12/09/2020','12/10/2020','12/11/2020',
#            '12/14/2020','12/15/2020','12/16/2020','12/17/2020','12/18/2020',
#            '12/21/2020','12/22/2020','12/23/2020',
#            '12/28/2020','12/29/2020','12/30/2020','12/31/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
                       np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
                       np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
                       np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
                       '<=-80%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.2,'8%~20%',
                   np.where(cplxamcp['Pct']<=0.4,'21%~40%',
                            '>40%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp.geojson',driver='GeoJSON')


# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/08/2020','09/09/2020','09/10/2020','09/11/2020']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202009']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E202009','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E202009']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202009']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>0.2,'>20%',
#                        np.where(cplxamcp['DiffPct']>0.15,'16%~20%',
#                        np.where(cplxamcp['DiffPct']>0.1,'11%~15%',
#                        np.where(cplxamcp['DiffPct']>0.05,'6%~10%',
#                        '<=5%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202009','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp2.geojson',driver='GeoJSON')


# PM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# predates=['12/03/2019','12/04/2019','12/05/2019','12/06/2019','12/09/2019','12/10/2019','12/11/2019','12/12/2019']
# postdates=['12/01/2020','12/02/2020','12/03/2020','12/04/2020','12/07/2020','12/08/2020','12/09/2020','12/10/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
                       np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
                       np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
                       np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
                       '<=-80%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.2,'8%~20%',
                   np.where(cplxamcp['Pct']<=0.4,'21%~40%',
                            '>40%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmcp.geojson',driver='GeoJSON')


# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/08/2020','09/09/2020','09/10/2020','09/11/2020']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202009']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E202009','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E202009']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202009']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>0.2,'>20%',
#                        np.where(cplxamcp['DiffPct']>0.15,'16%~20%',
#                        np.where(cplxamcp['DiffPct']>0.1,'11%~15%',
#                        np.where(cplxamcp['DiffPct']>0.05,'6%~10%',
#                        '<=5%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202009','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmcp2.geojson',driver='GeoJSON')


# Telework capability
# tel=gpd.read_file(path+'OUTPUT/telework0.geojson')
tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson')
tel['ntacode']=tel['NTACode'].copy()
tel['telework']=pd.to_numeric(tel['NYC Employed by Workplace and Residence_Industry_Race-Ethn_TELE Sheet1_Telework'])
tel['telework'].describe(percentiles=np.arange(0.2,1,0.2))
tel['cat']=np.where(tel['telework']<=0.25,'15%~25%',
            np.where(tel['telework']<=0.3,'26%~30%',
                        '31%~58%'))
tel=tel[['ntacode','telework','cat','geometry']].reset_index(drop=True)
# tel.to_file(path+'OUTPUT/teleworkam.geojson',driver='GeoJSON')
tel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson',driver='GeoJSON')

# tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm0.geojson')
tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm.geojson')
tel=tel[[x not in ['MN99','BX98','BX99','BK99','QN98','QN99','SI99'] for x in tel['ntacode']]]
tel['telework']=pd.to_numeric(tel['teleworkable_rate'])
tel['telework'].describe(percentiles=np.arange(0.2,1,0.2))
tel['cat']=np.where(tel['telework']<=0.25,'15%~25%',
            np.where(tel['telework']<=0.3,'26%~30%',
                        '31%~58%'))
tel=tel[['ntacode','telework','cat','geometry']].reset_index(drop=True)
tel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm.geojson',driver='GeoJSON')

# Telework Subway Commuters
telsub=pd.read_csv(path+'notel subway.csv')
telsub['telsubpct']=telsub['teleworkbySubway']/telsub['totalWorkerbySubway']
puma=gpd.read_file(path+'nypuma.shp')
puma=puma.to_crs('epsg:4326')
puma['puma']=pd.to_numeric(puma['PUMA'])
telsub=pd.merge(puma,telsub,left_on='puma',right_on='PUMA',how='inner')
telsub['telsubpct'].describe(percentiles=np.arange(0.2,1,0.2))
telsub['cat']=np.where(telsub['telsubpct']<=0.3,'22%~30%',
              np.where(telsub['telsubpct']<=0.35,'31%~35%',
                       '36%~54%'))
telsub=telsub[['puma','telsubpct','cat','geometry']].reset_index(drop=True)
telsub.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson',driver='GeoJSON')




# Scatter Plot
cplxamcp=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp.geojson')
cplxamcp.crs='epsg:4326'
tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson')
tel.crs='epsg:4326'
cplxamcptel=gpd.sjoin(cplxamcp,tel,how='left',op='intersects')
cplxamcptelbf=cplxamcptel[pd.isna(cplxamcptel['ntacode'])]
cplxamcptelbf=cplxamcptelbf.drop(['index_right','ntacode','telework'],axis=1)
cplxamcptelbf=cplxamcptelbf.to_crs('epsg:6539')
cplxamcptelbf['geometry']=[x.buffer(100) for x in cplxamcptelbf['geometry']]
cplxamcptelbf=cplxamcptelbf.to_crs('epsg:4326')
cplxamcptelbf=gpd.sjoin(cplxamcptelbf,tel,how='left',op='intersects')
cplxamcptelbf=cplxamcptelbf[pd.notna(cplxamcptelbf['ntacode'])]
cplxamcptelbf=cplxamcptelbf[['CplxID','ntacode','telework']].drop_duplicates('CplxID').reset_index(drop=True)
cplxamcptel=pd.merge(cplxamcptel,cplxamcptelbf,how='left',on='CplxID')
cplxamcptel['telework']=np.where(pd.notna(cplxamcptel['telework_x']),cplxamcptel['telework_x'],cplxamcptel['telework_y'])
import plotly.io as pio
import plotly.express as px
pio.renderers.default = "browser"
fig=px.scatter(cplxamcptel,x='telework', y='DiffPct',trendline='ols',template='plotly_white',width=800,height=600)
fig.update_layout(
    xaxis_title="NTA Telework Capability",
    yaxis_title="Current subway ridership vs early September 2020 ridership",
    font=dict(
        size=15,
    )
)
fig.show()
fig.write_html('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/scatter.html',include_plotlyjs='cdn')






# Non-Telework
# Non-Telework capability by Residence Place
nontel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson')
nontel['nontelework']=1-nontel['telework']
nontel['nontelework'].describe(percentiles=np.arange(0.2,1,0.2))
nontel['cat']=np.where(nontel['nontelework']>=0.7,'70%~85%',
              np.where(nontel['nontelework']>=0.65,'65%~69%',
                       '42%~64%'))
nontel=nontel[['ntacode','nontelework','cat','geometry']].reset_index(drop=True)
nontel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nonteleworkam.geojson',driver='GeoJSON')

# Non-Telework capability by Workplace
nontel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm.geojson')
nontel['nontelework']=1-nontel['telework']
nontel['nontelework'].describe(percentiles=np.arange(0.2,1,0.2))
nontel['cat']=np.where(nontel['nontelework']>=0.7,'70%~85%',
              np.where(nontel['nontelework']>=0.65,'65%~69%',
                       '42%~64%'))
nontel=nontel[['ntacode','nontelework','cat','geometry']].reset_index(drop=True)
nontel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nonteleworkpm.geojson',driver='GeoJSON')







# Non-Telework Subway Commuters
nontelsub=pd.read_csv(path+'notel subway.csv')
nontelsub['notelsubpct']=nontelsub['nonTeleSubway']/nontelsub['totalWorkerbySubway']
puma=gpd.read_file(path+'nypuma.shp')
puma=puma.to_crs('epsg:4326')
puma['puma']=pd.to_numeric(puma['PUMA'])
nontelsub=pd.merge(puma,nontelsub,left_on='puma',right_on='PUMA',how='inner')
nontelsub['notelsubpct'].describe(percentiles=np.arange(0.2,1,0.2))
nontelsub['cat']=np.where(nontelsub['notelsubpct']>=0.7,'70%~78%',
              np.where(nontelsub['notelsubpct']>=0.65,'65%~69%',
                       '46%~64%'))
nontelsub=nontelsub[['puma','notelsubpct','cat','geometry']].reset_index(drop=True)
nontelsub.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nontelsubam.geojson',driver='GeoJSON')





# # Weekday Off Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# dtoplist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
#           '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
# ntoplist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
#           '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00',
#           '21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
#           '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00',
#           '01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
#           '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
# oplist=dtoplist+ntoplist
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# postdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],oplist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','OP201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],oplist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','OP201909']
# cplxop=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxop['Time']=cplxop['PreTime'].copy()
# cplxop=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxop,how='left',on='CplxID')
# cplxop=cplxop[['CplxID','CplxLat','CplxLong','OP201904','OP201909']].reset_index(drop=True)
# cplxop=gpd.GeoDataFrame(cplxop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxop['CplxLong'],cplxop['CplxLat'])],crs='epsg:4326')
# cplxop=cplxop.to_crs('epsg:6539')
# cplxop['geometry']=cplxop.buffer(2640)
# cplxop=cplxop.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxop=gpd.sjoin(nta,cplxop,how='left',op='intersects')
# cplxop=cplxop.groupby(['NTACode'],as_index=False).agg({'OP201904':'sum','OP201909':'sum'}).reset_index(drop=True)
# cplxop=cplxop[cplxop['OP201904']!=0].reset_index(drop=True)
# cplxop=cplxop[cplxop['OP201909']!=0].reset_index(drop=True)
# cplxop.columns=['NTACode','OP201904','OP201909']
# cplxopnta=cplxop.copy()
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],oplist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','OP202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],oplist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','OP202009']
# cplxop=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxop['Time']=cplxop['PreTime'].copy()
# cplxop=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxop,how='left',on='CplxID')
# cplxop=cplxop[['CplxID','CplxLat','CplxLong','OP202004','OP202009']].reset_index(drop=True)
# cplxop=gpd.GeoDataFrame(cplxop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxop['CplxLong'],cplxop['CplxLat'])],crs='epsg:4326')
# cplxop=cplxop.to_crs('epsg:6539')
# cplxop['geometry']=cplxop.buffer(2640)
# cplxop=cplxop.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxop=gpd.sjoin(nta,cplxop,how='left',op='intersects')
# cplxop=cplxop.groupby(['NTACode'],as_index=False).agg({'OP202004':'sum','OP202009':'sum'}).reset_index(drop=True)
# cplxop=cplxop[cplxop['OP202004']!=0].reset_index(drop=True)
# cplxop=cplxop[cplxop['OP202009']!=0].reset_index(drop=True)
# cplxop.columns=['NTACode','OP202004','OP202009']
# cplxopnta=pd.merge(cplxopnta,cplxop,how='inner',on='NTACode')
# cplxopnta['OPDiff1']=cplxopnta['OP202004']-cplxopnta['OP201904']
# cplxopnta['OPDiffPct1']=cplxopnta['OPDiff1']/cplxopnta['OP201904']
# cplxopnta['OPDiff2']=cplxopnta['OP202009']-cplxopnta['OP201909']
# cplxopnta['OPDiffPct2']=cplxopnta['OPDiff2']/cplxopnta['OP201909']
# cplxopnta['OPDiff3']=cplxopnta['OP202009']-cplxopnta['OP202004']
# cplxopnta['OPDiffPct3']=cplxopnta['OPDiff3']/cplxopnta['OP202004']
# cplxopnta=pd.merge(nta,cplxopnta,how='inner',on='NTACode')
# cplxopnta.to_file(path+'OUTPUT/cplxopnta.shp')

# cplxopnta=cplxopnta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxopnta['NTACode']]]
# (sum(cplxopnta['OP202009'])-sum(cplxopnta['OP201909']))/sum(cplxopnta['OP201909'])



# # Weekend
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['04/13/2019','04/14/2019']
# postdates=['08/31/2019','09/01/2019','09/07/2019','09/08/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','WK201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','WK201909']
# cplxwk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxwk['Time']=cplxwk['PreTime'].copy()
# cplxwk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxwk,how='left',on='CplxID')
# cplxwk=cplxwk[['CplxID','CplxLat','CplxLong','WK201904','WK201909']].reset_index(drop=True)
# cplxwk=gpd.GeoDataFrame(cplxwk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxwk['CplxLong'],cplxwk['CplxLat'])],crs='epsg:4326')
# cplxwk=cplxwk.to_crs('epsg:6539')
# cplxwk['geometry']=cplxwk.buffer(2640)
# cplxwk=cplxwk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxwk=gpd.sjoin(nta,cplxwk,how='left',op='intersects')
# cplxwk=cplxwk.groupby(['NTACode'],as_index=False).agg({'WK201904':'sum','WK201909':'sum'}).reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK201904']!=0].reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK201909']!=0].reset_index(drop=True)
# cplxwk.columns=['NTACode','WK201904','WK201909']
# cplxwknta=cplxwk.copy()
# predates=['04/18/2020','04/19/2020']
# postdates=['08/29/2020','08/30/2020','09/05/2020','09/06/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','WK202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','WK202009']
# cplxwk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxwk['Time']=cplxwk['PreTime'].copy()
# cplxwk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxwk,how='left',on='CplxID')
# cplxwk=cplxwk[['CplxID','CplxLat','CplxLong','WK202004','WK202009']].reset_index(drop=True)
# cplxwk=gpd.GeoDataFrame(cplxwk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxwk['CplxLong'],cplxwk['CplxLat'])],crs='epsg:4326')
# cplxwk=cplxwk.to_crs('epsg:6539')
# cplxwk['geometry']=cplxwk.buffer(2640)
# cplxwk=cplxwk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxwk=gpd.sjoin(nta,cplxwk,how='left',op='intersects')
# cplxwk=cplxwk.groupby(['NTACode'],as_index=False).agg({'WK202004':'sum','WK202009':'sum'}).reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK202004']!=0].reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK202009']!=0].reset_index(drop=True)
# cplxwk.columns=['NTACode','WK202004','WK202009']
# cplxwknta=pd.merge(cplxwknta,cplxwk,how='inner',on='NTACode')
# cplxwknta['WKDiff1']=cplxwknta['WK202004']-cplxwknta['WK201904']
# cplxwknta['WKDiffPct1']=cplxwknta['WKDiff1']/cplxwknta['WK201904']
# cplxwknta['WKDiff2']=cplxwknta['WK202009']-cplxwknta['WK201909']
# cplxwknta['WKDiffPct2']=cplxwknta['WKDiff2']/cplxwknta['WK201909']
# cplxwknta['WKDiff3']=cplxwknta['WK202009']-cplxwknta['WK202004']
# cplxwknta['WKDiffPct3']=cplxwknta['WKDiff3']/cplxwknta['WK202004']
# cplxwknta=pd.merge(nta,cplxwknta,how='inner',on='NTACode')
# cplxwknta.to_file(path+'OUTPUT/cplxwknta.shp')

# cplxwknta=cplxwknta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxwknta['NTACode']]]
# (sum(cplxwknta['WK202009'])-sum(cplxwknta['WK201909']))/sum(cplxwknta['WK201909'])






# # Weekday Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# pklist=amlist+pmlist
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# postdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pklist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PK201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pklist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PK201909']
# cplxpk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxpk['Time']=cplxpk['PreTime'].copy()
# cplxpk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpk,how='left',on='CplxID')
# cplxpk=cplxpk[['CplxID','CplxLat','CplxLong','PK201904','PK201909']].reset_index(drop=True)
# cplxpk=gpd.GeoDataFrame(cplxpk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpk['CplxLong'],cplxpk['CplxLat'])],crs='epsg:4326')
# cplxpk=cplxpk.to_crs('epsg:6539')
# cplxpk['geometry']=cplxpk.buffer(2640)
# cplxpk=cplxpk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxpk=gpd.sjoin(nta,cplxpk,how='left',op='intersects')
# cplxpk=cplxpk.groupby(['NTACode'],as_index=False).agg({'PK201904':'sum','PK201909':'sum'}).reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK201904']!=0].reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK201909']!=0].reset_index(drop=True)
# cplxpk.columns=['NTACode','PK201904','PK201909']
# cplxpknta=cplxpk.copy()
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pklist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PK202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pklist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PK202009']
# cplxpk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxpk['Time']=cplxpk['PreTime'].copy()
# cplxpk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpk,how='left',on='CplxID')
# cplxpk=cplxpk[['CplxID','CplxLat','CplxLong','PK202004','PK202009']].reset_index(drop=True)
# cplxpk=gpd.GeoDataFrame(cplxpk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpk['CplxLong'],cplxpk['CplxLat'])],crs='epsg:4326')
# cplxpk=cplxpk.to_crs('epsg:6539')
# cplxpk['geometry']=cplxpk.buffer(2640)
# cplxpk=cplxpk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxpk=gpd.sjoin(nta,cplxpk,how='left',op='intersects')
# cplxpk=cplxpk.groupby(['NTACode'],as_index=False).agg({'PK202004':'sum','PK202009':'sum'}).reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK202004']!=0].reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK202009']!=0].reset_index(drop=True)
# cplxpk.columns=['NTACode','PK202004','PK202009']
# cplxpknta=pd.merge(cplxpknta,cplxpk,how='inner',on='NTACode')
# cplxpknta['PKDiff1']=cplxpknta['PK202004']-cplxpknta['PK201904']
# cplxpknta['PKDiffPct1']=cplxpknta['PKDiff1']/cplxpknta['PK201904']
# cplxpknta['PKDiff2']=cplxpknta['PK202009']-cplxpknta['PK201909']
# cplxpknta['PKDiffPct2']=cplxpknta['PKDiff2']/cplxpknta['PK201909']
# cplxpknta['PKDiff3']=cplxpknta['PK202009']-cplxpknta['PK202004']
# cplxpknta['PKDiffPct3']=cplxpknta['PKDiff3']/cplxpknta['PK202004']
# cplxpknta=pd.merge(nta,cplxpknta,how='inner',on='NTACode')
# cplxpknta.to_file(path+'OUTPUT/cplxpknta.shp')

# cplxpknta=cplxpknta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxpknta['NTACode']]]
# (sum(cplxpknta['PK202009'])-sum(cplxpknta['PK201909']))/sum(cplxpknta['PK201909'])












# # AM Peak for DOHMH
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201909']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202009']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='left',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201909','E202009']].reset_index(drop=True)
# cplxamcp.to_csv('C:/Users/mayij/Desktop/cplxamcp.csv',index=False)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp=cplxamcp.to_crs('epsg:6539')
# cplxamcp['geometry']=cplxamcp.buffer(2640)
# cplxamcp=cplxamcp.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxamcp=gpd.sjoin(cplxamcp,nta,how='left',op='intersects')
# cplxamcp.to_csv('C:/Users/mayij/Desktop/cplxamcpnta.csv',index=False)


















# # Con Ed
# # AM Peak Mapbox
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['10/07/2019','10/08/2019','10/09/2019','10/10/2019','10/11/2019','10/15/2019','10/16/2019','10/17/2019','10/18/2019',
#           '10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020','10/16/2020',
#            '10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201910']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
#                        np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
#                        np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
#                        np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
#                        '<=-80%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamconed.geojson',driver='GeoJSON')




# # PM Peak Mapbox
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['10/07/2019','10/08/2019','10/09/2019','10/10/2019','10/11/2019','10/15/2019','10/16/2019','10/17/2019','10/18/2019',
#           '10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020','10/16/2020',
#            '10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201910']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
#                        np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
#                        np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
#                        np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
#                        '<=-80%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmconed.geojson',driver='GeoJSON')










# AM Peak Pre and Post by PUMA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['11/18/2019','11/19/2019','11/20/2019','11/21/2019','11/22/2019','11/25/2019','11/26/2019','11/27/2019']
# postdates=['11/16/2020','11/17/2020','11/18/2020','11/19/2020','11/20/2020','11/23/2020','11/24/2020','11/25/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
# puma=gpd.read_file(path+'nycpuma.shp')
# puma.crs='epsg:4326'
# cplxamdiffpuma=gpd.sjoin(puma,cplxamdiff,how='left',op='intersects')
# cplxamdiffpuma=cplxamdiffpuma.groupby(['puma'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxamdiffpuma=cplxamdiffpuma[cplxamdiffpuma['PreEntries']!=0].reset_index(drop=True)
# cplxamdiffpuma.columns=['PUMA','E201911','E202011']
# cplxamdiffpuma['Diff']=cplxamdiffpuma['E202011']-cplxamdiffpuma['E201911']
# cplxamdiffpuma['DiffPct']=cplxamdiffpuma['Diff']/cplxamdiffpuma['E201911']
# cplxamdiffpuma['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamdiffpuma['DiffPctCat']=np.where(cplxamdiffpuma['DiffPct']>-0.6,'>-60%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.65,'-64%~-60%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.7,'-69%~-65%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.75,'-74%~-70%',
#           '<=-75%'))))
# cplxamdiffpuma.to_csv(path+'OUTPUT/amdiffpuma.csv',index=False)










# Nadir AM
# AM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
postdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>=-0.8,'-80%~-77%',
                       np.where(cplxamcp['DiffPct']>=-0.85,'-85%~-81%',
                       np.where(cplxamcp['DiffPct']>=-0.9,'-90%~-86%',
                       np.where(cplxamcp['DiffPct']>=-0.95,'-95%~-91%',
                       '-99%~-96%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.05,'1%~5%',
                   np.where(cplxamcp['Pct']<=0.1,'6%~10%',
                            '11%~23%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nadiram.geojson',driver='GeoJSON')







# Reopening
# AM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/16/2019','09/17/2019','09/18/2019','09/19/2019','09/20/2019']
postdates=['09/14/2020','09/15/2020','09/16/2020','09/17/2020','09/18/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E201909','E202009']
cplxamhed=cplxamdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E202004','E202006']
cplxamhed=pd.merge(cplxamhed,cplxamdiffnta,how='inner',on='NTACode')
cplxamhed['Diff1']=cplxamhed['E202009']-cplxamhed['E201909']
cplxamhed['DiffPct1']=cplxamhed['Diff1']/cplxamhed['E201909']
cplxamhed['Diff2']=cplxamhed['E202009']-cplxamhed['E202004']
cplxamhed['DiffPct2']=cplxamhed['Diff2']/cplxamhed['E202004']
cplxamhed['Diff3']=cplxamhed['E202009']-cplxamhed['E202006']
cplxamhed['DiffPct3']=cplxamhed['Diff3']/cplxamhed['E202006']
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat1']=np.where(cplxamhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxamhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxamhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxamhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxamhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat2']=np.where(cplxamhed['DiffPct2']<=2,'79%~200%',
          np.where(cplxamhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxamhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxamhed['DiffPct2']<=3.5,'301%~350%',
          '351%~581%'))))
cplxamhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat3']=np.where(cplxamhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxamhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxamhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxamhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxamhed['Pct']=cplxamhed['E202009']/cplxamhed['E201909']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.25,'13%~25%',
                    np.where(cplxamhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxamhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxamhed['Pct']<=0.4,'36%~40%',
                             '41%~51%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/reopenntaam.geojson',driver='GeoJSON')







# PM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/16/2019','09/17/2019','09/18/2019','09/19/2019','09/20/2019']
postdates=['09/14/2020','09/15/2020','09/16/2020','09/17/2020','09/18/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E201909','E202009']
cplxpmhed=cplxpmdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E202004','E202006']
cplxpmhed=pd.merge(cplxpmhed,cplxpmdiffnta,how='inner',on='NTACode')
cplxpmhed['Diff1']=cplxpmhed['E202009']-cplxpmhed['E201909']
cplxpmhed['DiffPct1']=cplxpmhed['Diff1']/cplxpmhed['E201909']
cplxpmhed['Diff2']=cplxpmhed['E202009']-cplxpmhed['E202004']
cplxpmhed['DiffPct2']=cplxpmhed['Diff2']/cplxpmhed['E202004']
cplxpmhed['Diff3']=cplxpmhed['E202009']-cplxpmhed['E202006']
cplxpmhed['DiffPct3']=cplxpmhed['Diff3']/cplxpmhed['E202006']
cplxpmhed=pd.merge(nta,cplxpmhed,how='inner',on='NTACode')
cplxpmhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat1']=np.where(cplxpmhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxpmhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxpmhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxpmhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxpmhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat2']=np.where(cplxpmhed['DiffPct2']<=2,'79%~200%',
          np.where(cplxpmhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxpmhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxpmhed['DiffPct2']<=3.5,'301%~350%',
          '351%~581%'))))
cplxpmhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat3']=np.where(cplxpmhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxpmhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxpmhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxpmhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxpmhed['Pct']=cplxpmhed['E202009']/cplxpmhed['E201909']
cplxpmhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['PctCat']=np.where(cplxpmhed['Pct']<=0.25,'16%~25%',
                    np.where(cplxpmhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxpmhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxpmhed['Pct']<=0.4,'36%~40%',
                             '41%~55%'))))
cplxpmhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/reopenntapm.geojson',driver='GeoJSON')













# Slider
# Pre and Post by NTA
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
ntaam=gpd.read_file(path+'ntaclippedadj.shp')
ntaam.crs=4326
ntapm=gpd.read_file(path+'ntaclippedadj.shp')
ntapm.crs=4326
# Pre March 2021
td=datetime.datetime.strptime('03/02/2020','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(364)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,52):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    predates=[x for x in predates if x not in holidays.US(state='NY')]
    postdates=[x for x in postdates if x not in holidays.US(state='NY')]
    # AM Peak
    cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
    cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
    cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampre.columns=['CplxID','PreTime','PreEntries']
    cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
    cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
    cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampost.columns=['CplxID','PostTime','PostEntries']
    cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
    cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
    cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
    cplxamdiff=cplxamdiff.to_crs('epsg:6539')
    cplxamdiff['geometry']=cplxamdiff.buffer(2640)
    cplxamdiff=cplxamdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
    cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxamdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxamdiffnta['Pct']=cplxamdiffnta['PostEntries']/cplxamdiffnta['PreEntries']
    cplxamdiffnta['PctCat']=np.where(cplxamdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxamdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxamdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxamdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxamdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxamdiffnta.columns=['NTACode','Week'+str(i)+'Pre','Week'+str(i)+'Post',
                           'Week'+str(i)+'Pct','Week'+str(i)+'PctCat']    
    ntaam=pd.merge(ntaam,cplxamdiffnta,how='inner',on='NTACode')
    # PM Peak
    cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
    cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpre.columns=['CplxID','PreTime','PreEntries']
    cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
    cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpost.columns=['CplxID','PostTime','PostEntries']
    cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
    cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
    cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
    cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
    cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
    cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
    cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxpmdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxpmdiffnta['Pct']=cplxpmdiffnta['PostEntries']/cplxpmdiffnta['PreEntries']
    cplxpmdiffnta['PctCat']=np.where(cplxpmdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxpmdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxpmdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxpmdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxpmdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxpmdiffnta.columns=['NTACode','Week'+str(i)+'Pre','Week'+str(i)+'Post',
                           'Week'+str(i)+'Pct','Week'+str(i)+'PctCat']    
    ntapm=pd.merge(ntapm,cplxpmdiffnta,how='inner',on='NTACode')
# Post March 2021
td=datetime.datetime.strptime('03/01/2021','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(731)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,53):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    predates=[x for x in predates if x not in holidays.US(state='NY')]
    postdates=[x for x in postdates if x not in holidays.US(state='NY')]
    # AM Peak
    cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
    cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
    cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampre.columns=['CplxID','PreTime','PreEntries']
    cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
    cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
    cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampost.columns=['CplxID','PostTime','PostEntries']
    cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
    cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
    cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
    cplxamdiff=cplxamdiff.to_crs('epsg:6539')
    cplxamdiff['geometry']=cplxamdiff.buffer(2640)
    cplxamdiff=cplxamdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
    cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxamdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxamdiffnta['Pct']=cplxamdiffnta['PostEntries']/cplxamdiffnta['PreEntries']
    cplxamdiffnta['PctCat']=np.where(cplxamdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxamdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxamdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxamdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxamdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxamdiffnta.columns=['NTACode','Week'+str(i+52)+'Pre','Week'+str(i+52)+'Post',
                           'Week'+str(i+52)+'Pct','Week'+str(i+52)+'PctCat']    
    ntaam=pd.merge(ntaam,cplxamdiffnta,how='inner',on='NTACode')
    # PM Peak
    cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
    cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpre.columns=['CplxID','PreTime','PreEntries']
    cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
    cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpost.columns=['CplxID','PostTime','PostEntries']
    cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
    cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
    cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
    cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
    cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
    cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
    cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxpmdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxpmdiffnta['Pct']=cplxpmdiffnta['PostEntries']/cplxpmdiffnta['PreEntries']
    cplxpmdiffnta['PctCat']=np.where(cplxpmdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxpmdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxpmdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxpmdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxpmdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxpmdiffnta.columns=['NTACode','Week'+str(i+52)+'Pre','Week'+str(i+52)+'Post',
                           'Week'+str(i+52)+'Pct','Week'+str(i+52)+'PctCat']    
    ntapm=pd.merge(ntapm,cplxpmdiffnta,how='inner',on='NTACode')
# Post March 2022
td=datetime.datetime.strptime('03/07/2022','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(1099)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,1):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    predates=[x for x in predates if x not in holidays.US(state='NY')]
    postdates=[x for x in postdates if x not in holidays.US(state='NY')]
    # AM Peak
    cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
    cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
    cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampre.columns=['CplxID','PreTime','PreEntries']
    cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
    cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
    cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampost.columns=['CplxID','PostTime','PostEntries']
    cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
    cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
    cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
    cplxamdiff=cplxamdiff.to_crs('epsg:6539')
    cplxamdiff['geometry']=cplxamdiff.buffer(2640)
    cplxamdiff=cplxamdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
    cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxamdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxamdiffnta['Pct']=cplxamdiffnta['PostEntries']/cplxamdiffnta['PreEntries']
    cplxamdiffnta['PctCat']=np.where(cplxamdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxamdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxamdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxamdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxamdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxamdiffnta.columns=['NTACode','Week'+str(i+105)+'Pre','Week'+str(i+105)+'Post',
                           'Week'+str(i+105)+'Pct','Week'+str(i+105)+'PctCat']    
    ntaam=pd.merge(ntaam,cplxamdiffnta,how='inner',on='NTACode')
    # PM Peak
    cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
    cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpre.columns=['CplxID','PreTime','PreEntries']
    cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
    cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpost.columns=['CplxID','PostTime','PostEntries']
    cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
    cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
    cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
    cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
    cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
    cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
    cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxpmdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxpmdiffnta['Pct']=cplxpmdiffnta['PostEntries']/cplxpmdiffnta['PreEntries']
    cplxpmdiffnta['PctCat']=np.where(cplxpmdiffnta['Pct']>0.5,'> 50%',
                            np.where(cplxpmdiffnta['Pct']>0.4,'41% ~ 50%',
                            np.where(cplxpmdiffnta['Pct']>0.3,'31% ~ 40%',
                            np.where(cplxpmdiffnta['Pct']>0.2,'21% ~ 30%',
                            np.where(cplxpmdiffnta['Pct']>0.1,'11% ~ 20%','<= 10%')))))
    cplxpmdiffnta.columns=['NTACode','Week'+str(i+105)+'Pre','Week'+str(i+105)+'Post',
                           'Week'+str(i+105)+'Pct','Week'+str(i+105)+'PctCat']    
    ntapm=pd.merge(ntapm,cplxpmdiffnta,how='inner',on='NTACode')
ntaam.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/slider/ntaam.geojson',driver='GeoJSON')
ntapm.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/slider/ntapm.geojson',driver='GeoJSON')


# Print Weeklist
td=datetime.datetime.strptime('03/02/2020','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(364)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,52):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    print("'"+postdates[0]+'-'+postdates[-1]+' vs '+predates[0]+'-'+predates[-1]+"',")

td=datetime.datetime.strptime('03/01/2021','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(731)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,53):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    print("'"+postdates[0]+'-'+postdates[-1]+' vs '+predates[0]+'-'+predates[-1]+"',")

td=datetime.datetime.strptime('03/07/2022','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
pr=td-datetime.timedelta(1099)
pr=pr-datetime.timedelta(pr.weekday())
for i in range(0,1):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(pr+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
    print("'"+postdates[0]+'-'+postdates[-1]+' vs '+predates[0]+'-'+predates[-1]+"',")    




# Slider Week over Week
# Pre and Post by NTA
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
ntaam=gpd.read_file(path+'ntaclippedadj.shp')
ntaam.crs=4326
ntapm=gpd.read_file(path+'ntaclippedadj.shp')
ntapm.crs=4326
# Post March 2021
td=datetime.datetime.strptime('03/15/2021','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
for i in range(0,46):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta((i+1)*7+j)).strftime('%m/%d/%Y')]
    predates=[x for x in predates if x not in holidays.US(state='NY')]
    postdates=[x for x in postdates if x not in holidays.US(state='NY')]
    # AM Peak
    cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
    cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
    cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampre.columns=['CplxID','PreTime','PreEntries']
    cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
    cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
    cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampost.columns=['CplxID','PostTime','PostEntries']
    cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
    cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
    cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
    cplxamdiff=cplxamdiff.to_crs('epsg:6539')
    cplxamdiff['geometry']=cplxamdiff.buffer(2640)
    cplxamdiff=cplxamdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
    cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxamdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxamdiffnta['Diff']=cplxamdiffnta['PostEntries']-cplxamdiffnta['PreEntries']
    cplxamdiffnta['DiffPct']=cplxamdiffnta['Diff']/cplxamdiffnta['PreEntries']
    cplxamdiffnta['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
    cplxamdiffnta['DiffPctCat']=np.where(cplxamdiffnta['DiffPct']<=0,'<= 0%',
                                np.where(cplxamdiffnta['DiffPct']<=0.05,'1% ~ 5%',
                                np.where(cplxamdiffnta['DiffPct']<=0.1,'6% ~ 10%',
                                np.where(cplxamdiffnta['DiffPct']<=0.15,'11% ~ 15%',
                                np.where(cplxamdiffnta['DiffPct']<=0.2,'16% ~ 20%','> 20%')))))
    cplxamdiffnta.columns=['NTACode','Week'+str(i)+'Pre','Week'+str(i)+'Post','Week'+str(i)+'Diff',
                           'Week'+str(i)+'DiffPct','Week'+str(i)+'DiffPctCat']    
    ntaam=pd.merge(ntaam,cplxamdiffnta,how='inner',on='NTACode')
    # PM Peak
    cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
    cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
    cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpre.columns=['CplxID','PreTime','PreEntries']
    cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
    cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
    cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
    cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxpmpost.columns=['CplxID','PostTime','PostEntries']
    cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
    cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
    cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
    cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
    cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
    cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
    cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
    nta=gpd.read_file(path+'ntaclippedadj.shp')
    nta.crs='epsg:4326'
    cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
    cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
    cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
    cplxpmdiffnta.columns=['NTACode','PreEntries','PostEntries']
    cplxpmdiffnta['Diff']=cplxpmdiffnta['PostEntries']-cplxpmdiffnta['PreEntries']
    cplxpmdiffnta['DiffPct']=cplxpmdiffnta['Diff']/cplxpmdiffnta['PreEntries']
    cplxpmdiffnta['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
    cplxpmdiffnta['DiffPctCat']=np.where(cplxpmdiffnta['DiffPct']<=0,'<= 0%',
                                np.where(cplxpmdiffnta['DiffPct']<=0.05,'1% ~ 5%',
                                np.where(cplxpmdiffnta['DiffPct']<=0.1,'6% ~ 10%',
                                np.where(cplxpmdiffnta['DiffPct']<=0.15,'11% ~ 15%',
                                np.where(cplxpmdiffnta['DiffPct']<=0.2,'16% ~ 20%','> 20%')))))
    cplxpmdiffnta.columns=['NTACode','Week'+str(i)+'Pre','Week'+str(i)+'Post','Week'+str(i)+'Diff',
                           'Week'+str(i)+'DiffPct','Week'+str(i)+'DiffPctCat']      
    ntapm=pd.merge(ntapm,cplxpmdiffnta,how='inner',on='NTACode')
ntaam.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/slider/ntawowam.geojson',driver='GeoJSON')
ntapm.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/slider/ntawowpm.geojson',driver='GeoJSON')


# Print Weeklist
td=datetime.datetime.strptime('03/15/2021','%m/%d/%Y')
td=td-datetime.timedelta(td.weekday())
for i in range(0,46):
    predates=[]
    postdates=[]
    for j in range(0,5):
        predates+=[(td+datetime.timedelta(i*7+j)).strftime('%m/%d/%Y')]
        postdates+=[(td+datetime.timedelta((i+1)*7+j)).strftime('%m/%d/%Y')]
    print("'"+postdates[0]+'-'+postdates[-1]+' vs '+predates[0]+'-'+predates[-1]+"',")





# Subway Vaccine
df=dfunitentry[np.isin(dfunitentry['firstdate'],['04/26/2021','04/27/2021','04/28/2021','04/29/2021'])]
df=df.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
df=pd.merge(df,rc,how='left',left_on='unit',right_on='Remote')
df=df.sort_values('entries',ascending=False).reset_index(drop=True)
df=df.groupby(['CplxID','time'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
df=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),df,how='inner',on='CplxID')
df=df[['CplxID','CplxName','Routes','CplxLat','CplxLong','Borough','time','entries']].reset_index(drop=True)
df.columns=['CplxID','CplxName','Routes','CplxLat','CplxLong','Borough','TimePeriod','Entries']
df.to_csv('C:/Users/mayij/Desktop/Turnstile.csv',index=False)









# RTO1
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019']
postdates=['06/07/2021','06/08/2021','06/09/2021','06/10/2021','06/11/2021']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201906']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202106']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto['Pct']=cplxrto['E202106']/cplxrto['E201906']
cplxrto['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxrto['PctCat']=np.where(cplxrto['Pct']<=0.4,'20%~40%',
                  np.where(cplxrto['Pct']<=0.5,'41%~50%','>50%'))
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='inner',on='CplxID')
cplxrto=cplxrto[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','E201906','E202106',
                 'Pct','PctCat']].reset_index(drop=True)
cplxrto=gpd.GeoDataFrame(cplxrto,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrto['CplxLong'],cplxrto['CplxLat'])],crs='epsg:4326')
cplxrto.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrto.geojson',driver='GeoJSON')



# RTO2
# AM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019']
postdates=['06/07/2021','06/08/2021','06/09/2021','06/10/2021','06/11/2021']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201906']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202106']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201906','E202106']].reset_index(drop=True)
cplxamcp['Pct']=cplxamcp['E202106']/cplxamcp['E201906']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.4,'15%~40%',
                   np.where(cplxamcp['Pct']<=0.5,'41%~50%','>50%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201906','E202106',
                   'Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtoam.geojson',driver='GeoJSON')

# PM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['06/03/2019','06/04/2019','06/05/2019','06/06/2019','06/07/2019']
postdates=['06/07/2021','06/08/2021','06/09/2021','06/10/2021','06/11/2021']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201906']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202106']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201906','E202106']].reset_index(drop=True)
cplxamcp['Pct']=cplxamcp['E202106']/cplxamcp['E201906']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.4,'15%~40%',
                   np.where(cplxamcp['Pct']<=0.5,'41%~50%','>50%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201906','E202106',
                   'Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtopm.geojson',driver='GeoJSON')



# RTO3
# AM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/05/2021','04/06/2021','04/07/2021','04/08/2021','04/09/2021',
          '04/12/2021','04/13/2021','04/14/2021','04/15/2021','04/16/2021',
          '04/19/2021','04/20/2021','04/21/2021','04/22/2021','04/23/2021',
          '04/26/2021','04/27/2021','04/28/2021','04/29/2021','04/30/2021']
postdates=['05/17/2021','05/18/2021','05/19/2021','05/20/2021','05/21/2021',
           '05/24/2021','05/25/2021','05/26/2021','05/27/2021','05/28/2021',
           '05/31/2021','06/01/2021','06/02/2021','06/03/2021','06/04/2021',
           '06/07/2021','06/08/2021','06/09/2021','06/10/2021','06/11/2021',
           '06/14/2021','06/15/2021','06/16/2021','06/17/2021','06/18/2021',
           '06/21/2021','06/22/2021','06/23/2021','06/24/2021','06/25/2021']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E202104']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202106']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E202104','E202106']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202106']-cplxamcp['E202104']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202104']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']<=0.05,'<=5%',
                       np.where(cplxamcp['DiffPct']<=0.1,'6%~10%',
                       np.where(cplxamcp['DiffPct']<=0.25,'11%~25%','>25%')))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202104','E202106',
                   'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtoam21.geojson',driver='GeoJSON')

# PM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/05/2021','04/06/2021','04/07/2021','04/08/2021','04/09/2021',
          '04/12/2021','04/13/2021','04/14/2021','04/15/2021','04/16/2021',
          '04/19/2021','04/20/2021','04/21/2021','04/22/2021','04/23/2021',
          '04/26/2021','04/27/2021','04/28/2021','04/29/2021','04/30/2021']
postdates=['05/17/2021','05/18/2021','05/19/2021','05/20/2021','05/21/2021',
           '05/24/2021','05/25/2021','05/26/2021','05/27/2021','05/28/2021',
           '05/31/2021','06/01/2021','06/02/2021','06/03/2021','06/04/2021',
           '06/07/2021','06/08/2021','06/09/2021','06/10/2021','06/11/2021',
           '06/14/2021','06/15/2021','06/16/2021','06/17/2021','06/18/2021',
           '06/21/2021','06/22/2021','06/23/2021','06/24/2021','06/25/2021']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E202104']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202106']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E202104','E202106']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202106']-cplxamcp['E202104']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202104']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']<=0.05,'<=5%',
                       np.where(cplxamcp['DiffPct']<=0.1,'6%~10%',
                       np.where(cplxamcp['DiffPct']<=0.25,'11%~25%','>25%')))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202104','E202106',
                   'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtopm21.geojson',driver='GeoJSON')



# RTO chart telework
# 202004
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
postdates=['04/13/2020','04/14/2020','04/15/2020','04/16/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre[np.isin(cplxrtopre['time'],amlist)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201904']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost[np.isin(cplxrtopost['time'],amlist)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202004']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='left',on='CplxID')
cplxrtodiff=cplxrto[['CplxID','CplxLat','CplxLong','E201904','E202004']].reset_index(drop=True)
cplxrtodiff=gpd.GeoDataFrame(cplxrtodiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrtodiff['CplxLong'],cplxrtodiff['CplxLat'])],crs='epsg:4326')
cplxrtodiff=cplxrtodiff.to_crs(6539)
cplxrtodiff['geometry']=cplxrtodiff.buffer(2640)
cplxrtodiff=cplxrtodiff.to_crs(4326)
telsubam=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson')
telsubam.crs=4326
cplxrtodiffpuma=gpd.sjoin(telsubam,cplxrtodiff,how='left',op='intersects')
cplxrtodiffpuma=cplxrtodiffpuma.groupby(['puma'],as_index=False).agg({'E201904':'sum','E202004':'sum','telsubpct':'mean'}).reset_index(drop=True)
cplxrtodiffpuma=cplxrtodiffpuma[cplxrtodiffpuma['E201904']!=0].reset_index(drop=True)
cplxrtodiffpuma['Pct202004']=cplxrtodiffpuma['E202004']/cplxrtodiffpuma['E201904']
cplxrtodiffpuma['nontelsubpct']=1-cplxrtodiffpuma['telsubpct']
cplxrtodiffpuma=cplxrtodiffpuma[['puma','nontelsubpct','Pct202004']].reset_index(drop=True)
pumarto=cplxrtodiffpuma.copy()

# 202009
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/16/2019','09/17/2019','09/18/2019','09/19/2019']
postdates=['09/14/2020','09/15/2020','09/16/2020','09/17/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre[np.isin(cplxrtopre['time'],amlist)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201909']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost[np.isin(cplxrtopost['time'],amlist)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202009']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='left',on='CplxID')
cplxrtodiff=cplxrto[['CplxID','CplxLat','CplxLong','E201909','E202009']].reset_index(drop=True)
cplxrtodiff=gpd.GeoDataFrame(cplxrtodiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrtodiff['CplxLong'],cplxrtodiff['CplxLat'])],crs='epsg:4326')
cplxrtodiff=cplxrtodiff.to_crs(6539)
cplxrtodiff['geometry']=cplxrtodiff.buffer(2640)
cplxrtodiff=cplxrtodiff.to_crs(4326)
telsubam=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson')
telsubam.crs=4326
cplxrtodiffpuma=gpd.sjoin(telsubam,cplxrtodiff,how='left',op='intersects')
cplxrtodiffpuma=cplxrtodiffpuma.groupby(['puma'],as_index=False).agg({'E201909':'sum','E202009':'sum','telsubpct':'mean'}).reset_index(drop=True)
cplxrtodiffpuma=cplxrtodiffpuma[cplxrtodiffpuma['E201909']!=0].reset_index(drop=True)
cplxrtodiffpuma['Pct202009']=cplxrtodiffpuma['E202009']/cplxrtodiffpuma['E201909']
cplxrtodiffpuma=cplxrtodiffpuma[['puma','Pct202009']].reset_index(drop=True)
pumarto=pumarto.merge(cplxrtodiffpuma,how='inner',on='puma')

#202104
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
postdates=['04/19/2021','04/20/2021','04/21/2021','04/22/2021']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre[np.isin(cplxrtopre['time'],amlist)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201904']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost[np.isin(cplxrtopost['time'],amlist)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202104']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='left',on='CplxID')
cplxrtodiff=cplxrto[['CplxID','CplxLat','CplxLong','E201904','E202104']].reset_index(drop=True)
cplxrtodiff=gpd.GeoDataFrame(cplxrtodiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrtodiff['CplxLong'],cplxrtodiff['CplxLat'])],crs='epsg:4326')
cplxrtodiff=cplxrtodiff.to_crs(6539)
cplxrtodiff['geometry']=cplxrtodiff.buffer(2640)
cplxrtodiff=cplxrtodiff.to_crs(4326)
telsubam=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson')
telsubam.crs=4326
cplxrtodiffpuma=gpd.sjoin(telsubam,cplxrtodiff,how='left',op='intersects')
cplxrtodiffpuma=cplxrtodiffpuma.groupby(['puma'],as_index=False).agg({'E201904':'sum','E202104':'sum','telsubpct':'mean'}).reset_index(drop=True)
cplxrtodiffpuma=cplxrtodiffpuma[cplxrtodiffpuma['E201904']!=0].reset_index(drop=True)
cplxrtodiffpuma['Pct202104']=cplxrtodiffpuma['E202104']/cplxrtodiffpuma['E201904']
cplxrtodiffpuma=cplxrtodiffpuma[['puma','Pct202104']].reset_index(drop=True)
pumarto=pumarto.merge(cplxrtodiffpuma,how='inner',on='puma')

# 202105
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['05/13/2019','05/14/2019','05/15/2019','05/16/2019']
postdates=['05/17/2021','05/18/2021','05/19/2021','05/20/2021']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre[np.isin(cplxrtopre['time'],amlist)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201905']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost[np.isin(cplxrtopost['time'],amlist)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202105']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='left',on='CplxID')
cplxrtodiff=cplxrto[['CplxID','CplxLat','CplxLong','E201905','E202105']].reset_index(drop=True)
cplxrtodiff=gpd.GeoDataFrame(cplxrtodiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrtodiff['CplxLong'],cplxrtodiff['CplxLat'])],crs='epsg:4326')
cplxrtodiff=cplxrtodiff.to_crs(6539)
cplxrtodiff['geometry']=cplxrtodiff.buffer(2640)
cplxrtodiff=cplxrtodiff.to_crs(4326)
telsubam=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson')
telsubam.crs=4326
cplxrtodiffpuma=gpd.sjoin(telsubam,cplxrtodiff,how='left',op='intersects')
cplxrtodiffpuma=cplxrtodiffpuma.groupby(['puma'],as_index=False).agg({'E201905':'sum','E202105':'sum','telsubpct':'mean'}).reset_index(drop=True)
cplxrtodiffpuma=cplxrtodiffpuma[cplxrtodiffpuma['E201905']!=0].reset_index(drop=True)
cplxrtodiffpuma['Pct202105']=cplxrtodiffpuma['E202105']/cplxrtodiffpuma['E201905']
cplxrtodiffpuma=cplxrtodiffpuma[['puma','Pct202105']].reset_index(drop=True)
pumarto=pumarto.merge(cplxrtodiffpuma,how='inner',on='puma')
pumarto.to_csv(path+'OUTPUT/pumartoam.csv',index=False)




# RTO chart lUDI
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['05/13/2019','05/14/2019','05/15/2019','05/16/2019']
middates=['05/11/2020','05/12/2020','05/13/2020','05/14/2020']
postdates=['05/17/2021','05/18/2021','05/19/2021','05/20/2021']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201905']
cplxrtomid=dfunitentry[np.isin(dfunitentry['firstdate'],middates)].reset_index(drop=True)
cplxrtomid=cplxrtomid.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtomid=cplxrtomid.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtomid=pd.merge(cplxrtomid,rc,how='left',left_on='unit',right_on='Remote')
cplxrtomid=cplxrtomid.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtomid.columns=['CplxID','E202005']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202105']
cplxrto=pd.merge(cplxrtopre,cplxrtomid,how='inner',on='CplxID')
cplxrto=pd.merge(cplxrto,cplxrtopost,how='inner',on='CplxID')
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='left',on='CplxID')
cplxrtodiff=cplxrto[['CplxID','CplxLat','CplxLong','E201905','E202005','E202105']].reset_index(drop=True)
cplxrtodiff=gpd.GeoDataFrame(cplxrtodiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrtodiff['CplxLong'],cplxrtodiff['CplxLat'])],crs='epsg:4326')
cplxrtodiff=cplxrtodiff.to_crs(6539)
cplxrtodiff['geometry']=cplxrtodiff.buffer(2640)
cplxrtodiff=cplxrtodiff.to_crs(4326)
ludi=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-landuse/ntacat5wgtludi.geojson')
ludi.crs=4326
cplxrtodiffnta=gpd.sjoin(ludi,cplxrtodiff,how='left',op='intersects')
cplxrtodiffnta=cplxrtodiffnta.groupby(['ntacode'],as_index=False).agg({'E201905':'sum','E202005':'sum',
                                                                       'E202105':'sum','ludi':'mean'}).reset_index(drop=True)
cplxrtodiffnta=cplxrtodiffnta[cplxrtodiffnta['E201905']!=0].reset_index(drop=True)
cplxrtodiffnta.to_csv(path+'OUTPUT/ntaludi.csv',index=False)




# RTO Full1
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/09/2019','09/10/2019','09/11/2019','09/12/2019','09/13/2019','09/16/2019','09/17/2019','09/18/2019','09/19/2019']
postdates=['09/13/2021','09/14/2021','09/15/2021','09/16/2021','09/17/2021','09/20/2021','09/21/2021','09/22/2021','09/23/2021']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E201909']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202109']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto['Pct']=cplxrto['E202109']/cplxrto['E201909']
cplxrto['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxrto['PctCat']=np.where(cplxrto['Pct']<=0.4,'18%~40%',
                  np.where(cplxrto['Pct']<=0.5,'41%~50%','>50%'))
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='inner',on='CplxID')
cplxrto=cplxrto[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','E201909','E202109',
                 'Pct','PctCat']].reset_index(drop=True)
cplxrto=gpd.GeoDataFrame(cplxrto,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrto['CplxLong'],cplxrto['CplxLat'])],crs='epsg:4326')
cplxrto.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtofull1.geojson',driver='GeoJSON')

# RTO Full2
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/19/2021','04/20/2021','04/21/2021','04/22/2021','04/23/2021','04/26/2021','04/27/2021','04/28/2021','04/29/2021']
postdates=['09/13/2021','09/14/2021','09/15/2021','09/16/2021','09/17/2021','09/20/2021','09/21/2021','09/22/2021','09/23/2021']
cplxrtopre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre=cplxrtopre.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopre=pd.merge(cplxrtopre,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopre=cplxrtopre.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopre.columns=['CplxID','E202104']
cplxrtopost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost=cplxrtopost.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxrtopost=pd.merge(cplxrtopost,rc,how='left',left_on='unit',right_on='Remote')
cplxrtopost=cplxrtopost.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
cplxrtopost.columns=['CplxID','E202109']
cplxrto=pd.merge(cplxrtopre,cplxrtopost,how='inner',on='CplxID')
cplxrto['Pct']=(cplxrto['E202109']-cplxrto['E202104'])/cplxrto['E202104']
cplxrto['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxrto['PctCat']=np.where(cplxrto['Pct']<=0.2,'<=20%',
                  np.where(cplxrto['Pct']<=0.4,'21%~40%','>40%'))
cplxrto=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxrto,how='inner',on='CplxID')
cplxrto=cplxrto[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','E202104','E202109',
                 'Pct','PctCat']].reset_index(drop=True)
cplxrto=gpd.GeoDataFrame(cplxrto,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxrto['CplxLong'],cplxrto['CplxLat'])],crs='epsg:4326')
cplxrto.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxrtofull2.geojson',driver='GeoJSON')




# NTA Time of Day
# AM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/am.geojson',driver='GeoJSON')

# Midday
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
mdlist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
          '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],mdlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],mdlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/md.geojson',driver='GeoJSON')

# PM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/pm.geojson',driver='GeoJSON')

# Early Night
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
enlist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
        '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],enlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],enlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/en.geojson',driver='GeoJSON')

# Late Night
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
lnlist=['21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
        '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],lnlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],lnlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/ln.geojson',driver='GeoJSON')

# Early Morning
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
emlist=['01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
        '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],emlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],emlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamhed=cplxamdiffnta.copy()
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['Pct']=cplxamhed['PostEntries']/cplxamhed['PreEntries']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.3,'<=30%',
                    np.where(cplxamhed['Pct']<=0.4,'31%~40%',
                    np.where(cplxamhed['Pct']<=0.5,'41%~50%',
                    np.where(cplxamhed['Pct']<=0.6,'51%~60%',
                             '>60%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/em.geojson',driver='GeoJSON')

# Summary
df=pd.DataFrame(columns=['Time Period','Late January 2020','Late January 2022'])
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020']
postdates=['01/24/2022','01/25/2022','01/26/2022','01/27/2022','01/28/2022']
# predates=['09/23/2019','09/24/2019','09/25/2019','09/26/2019','09/27/2019']
# postdates=['09/27/2021','09/28/2021','09/29/2021','09/30/2021','10/01/2021']
emlist=['01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
        '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
mdlist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
        '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
enlist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
        '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
lnlist=['21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
        '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
pre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
post=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
df.loc[0]=['Early Morning',sum(pre.loc[np.isin(pre['time'],emlist),'entries']),sum(post.loc[np.isin(post['time'],emlist),'entries'])]
df.loc[1]=['AM Peak',sum(pre.loc[np.isin(pre['time'],amlist),'entries']),sum(post.loc[np.isin(post['time'],amlist),'entries'])]
df.loc[2]=['Midday',sum(pre.loc[np.isin(pre['time'],mdlist),'entries']),sum(post.loc[np.isin(post['time'],mdlist),'entries'])]
df.loc[3]=['PM Peak',sum(pre.loc[np.isin(pre['time'],pmlist),'entries']),sum(post.loc[np.isin(post['time'],pmlist),'entries'])]
df.loc[4]=['Early Night',sum(pre.loc[np.isin(pre['time'],enlist),'entries']),sum(post.loc[np.isin(post['time'],enlist),'entries'])]
df.loc[5]=['Late Night',sum(pre.loc[np.isin(pre['time'],lnlist),'entries']),sum(post.loc[np.isin(post['time'],lnlist),'entries'])]
df['Percentage']=df['Late January 2022']/df['Late January 2020']
df.to_csv('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/tod/summary.csv',index=False)







# Compare with MTA August Weekday data (AFC+OMNY)
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
df=dfunitentry.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
df=pd.merge(df,rc,how='left',left_on='unit',right_on='Remote')
df=df.groupby(['CplxID','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
df['firstdate']=pd.to_datetime(df['firstdate'],format='%m/%d/%Y')
df['year']=df['firstdate'].dt.year
df['month']=df['firstdate'].dt.month
df['weekday']=df['firstdate'].dt.weekday+1
df=df[df['month']==9].reset_index(drop=True)
df=df[np.isin(df['weekday'],[1,2,3,4,5])].reset_index(drop=True)
df=df[[x not in holidays.US(state='NY') for x in df['firstdate']]].reset_index(drop=True)
df=df.groupby(['CplxID','year'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# df[df['CplxID']==303]
df=df.pivot(index='CplxID',columns='year').reset_index(drop=False)
df.to_csv(path+'VALIDATION/MTA/TURNSTILE09.csv',index=False)

# Compare with MTA Daily Estimate
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfunitentry=dfunitentry.groupby(['firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
dfunitentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitentry['firstdate']]
df=pd.read_csv('https://new.mta.info/document/20441',dtype=str)
df['Date']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in df['Date']]
df['Subway']=[int(x) for x in df['Subways: Total Estimated Ridership']]
df=df[['Date','Subway']].reset_index(drop=True)
df=pd.merge(df,dfunitentry,how='inner',left_on='Date',right_on='firstdate')
df.to_csv(path+'VALIDATION/MTA/DAILY.csv',index=False)





# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# predates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# dtoplist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
#           '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
# ntoplist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
#           '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00',
#           '21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
#           '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00',
#           '01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
#           '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
# enlist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
#         '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
# lnlist=['21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
#         '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
# emlist=['01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
#         '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']





