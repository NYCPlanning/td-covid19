import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/TURNSTILE/'
#path='/home/mayijun/TURNSTILE/'



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
    ucsexit=ucsexit.reset_index(drop=True)
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
# 50 mins



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
fare=pd.read_csv('C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/FARE/fare.csv',dtype=str,converters={'fare':float})
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



# Comparison
# AM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019','05/13/2019','05/14/2019','05/15/2019','05/16/2019']
postdates=['05/04/2020','05/05/2020','05/06/2020','05/07/2020','05/08/2020','05/11/2020','05/12/2020','05/13/2020','05/14/2020']
amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
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
cplxamdiff=cplxamdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
cplxamdiff.to_csv(path+'OUTPUT/cplxamdiff.csv',index=False)

# PM Peak
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019','05/13/2019','05/14/2019','05/15/2019','05/16/2019']
postdates=['05/04/2020','05/05/2020','05/06/2020','05/07/2020','05/08/2020','05/11/2020','05/12/2020','05/13/2020','05/14/2020']
pmlist=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
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
cplxpmdiff=cplxpmdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
cplxpmdiff.to_csv(path+'OUTPUT/cplxpmdiff.csv',index=False)

# Period Diff
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019','05/13/2019','05/14/2019','05/15/2019','05/16/2019']
postdates=['05/04/2020','05/05/2020','05/06/2020','05/07/2020','05/08/2020','05/11/2020','05/12/2020','05/13/2020','05/14/2020']
period1=['01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
period2=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
period3=['09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
period4=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
period5=['17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
period6=['21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
period1=pd.DataFrame(period1,columns=['timeperiod'])
period1['periodid']=1
period2=pd.DataFrame(period2,columns=['timeperiod'])
period2['periodid']=2
period3=pd.DataFrame(period3,columns=['timeperiod'])
period3['periodid']=3
period4=pd.DataFrame(period4,columns=['timeperiod'])
period4['periodid']=4
period5=pd.DataFrame(period5,columns=['timeperiod'])
period5['periodid']=5
period6=pd.DataFrame(period6,columns=['timeperiod'])
period6['periodid']=6
periodlist=pd.concat([period1,period2,period3,period4,period5,period6],ignore_index=True)
pdpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
pdpre=pd.merge(pdpre,periodlist,how='left',left_on='time',right_on='timeperiod')
pdpre=pdpre.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
pdpre.columns=['Date','Period','PreEntries']
pdpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
pdpost=pd.merge(pdpost,periodlist,how='left',left_on='time',right_on='timeperiod')
pdpost=pdpost.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
pdpost.columns=['Date','Period','PostEntries']
pddiff=pd.concat([pdpre,pdpost],ignore_index=True)
pddiff.to_csv(path+'OUTPUT/pddiff.csv',index=False)

# Hub Bound
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['03/11/2019','03/12/2019','03/13/2019','03/14/2019','03/15/2019','03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019',
          '03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019','04/01/2019','04/02/2019','04/03/2019','04/04/2019','04/05/2019',
          '04/08/2019','04/09/2019','04/10/2019','04/11/2019','04/12/2019','04/15/2019','04/16/2019','04/17/2019','04/18/2019',
          '04/22/2019','04/23/2019','04/24/2019','04/25/2019','04/26/2019','04/29/2019','04/30/2019','05/01/2019','05/02/2019','05/03/2019',
          '05/06/2019','05/07/2019','05/08/2019','05/09/2019','05/10/2019','05/13/2019','05/14/2019','05/15/2019','05/16/2019']
postdates=['03/09/2020','03/10/2020','03/11/2020','03/12/2020','03/13/2020','03/16/2020','03/17/2020','03/18/2020','03/19/2020','03/20/2020',
           '03/23/2020','03/24/2020','03/25/2020','03/26/2020','03/27/2020','03/30/2020','03/31/2020','04/01/2020','04/02/2020','04/03/2020',
           '04/06/2020','04/07/2020','04/08/2020','04/09/2020','04/13/2020','04/14/2020','04/15/2020','04/16/2020','04/17/2020',
           '04/20/2020','04/21/2020','04/22/2020','04/23/2020','04/24/2020','04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020',
           '05/04/2020','05/05/2020','05/06/2020','05/07/2020','05/08/2020','05/11/2020','05/12/2020','05/13/2020','05/14/2020']
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

# Subway Closure
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
postdates=['04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020','05/04/2020','05/05/2020']
offtime=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
cplxamoff=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxamoff=cplxamoff[np.isin(cplxamoff['time'],offtime)].reset_index(drop=True)
cplxamoff=cplxamoff.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxamoff=pd.merge(cplxamoff,rc,how='left',left_on='unit',right_on='Remote')
cplxamoff=cplxamoff.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxamoff.columns=['CplxID','PostTime','PostEntries']
cplxamoff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamoff,how='left',on='CplxID')
cplxamoff=cplxamoff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','PostTime','PostEntries']].reset_index(drop=True)
cplxamoff['PostEntries']=cplxamoff['PostEntries'].fillna(0)
cplxamoff.to_csv(path+'OUTPUT/cplxamoff.csv',index=False)

dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/29/2020','04/30/2020']
postdates=['05/06/2020','05/07/2020']
offtime=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
cplxamoffpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxamoffpre=cplxamoffpre[np.isin(cplxamoffpre['time'],offtime)].reset_index(drop=True)
cplxamoffpre=cplxamoffpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxamoffpre=pd.merge(cplxamoffpre,rc,how='left',left_on='unit',right_on='Remote')
cplxamoffpre=cplxamoffpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxamoffpre.columns=['CplxID','PreTime','PreEntries']
cplxamoffpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxamoffpost=cplxamoffpost[np.isin(cplxamoffpost['time'],offtime)].reset_index(drop=True)
cplxamoffpost=cplxamoffpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxamoffpost=pd.merge(cplxamoffpost,rc,how='left',left_on='unit',right_on='Remote')
cplxamoffpost=cplxamoffpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxamoffpost.columns=['CplxID','PostTime','PostEntries']
cplxamoffdiff=pd.merge(cplxamoffpre,cplxamoffpost,how='inner',on='CplxID')
cplxamoffdiff['Time']=cplxamoffdiff['PreTime'].copy()
cplxamoffdiff['Diff']=cplxamoffdiff['PostEntries']-cplxamoffdiff['PreEntries']
cplxamoffdiff['DiffPct']=cplxamoffdiff['Diff']/cplxamoffdiff['PreEntries']
cplxamoffdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamoffdiff,how='left',on='CplxID')
cplxamoffdiff=cplxamoffdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
cplxamoffdiff.to_csv(path+'OUTPUT/cplxamoffdiff.csv',index=False)

dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/29/2020','04/30/2020']
postdates=['05/06/2020','05/07/2020']
period1=['11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00','13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00']
period2=['15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00','17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00']
period3=['19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00','21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00']
period4=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
period5=['03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00','05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00']
period6=['07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00','09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00']
period1=pd.DataFrame(period1,columns=['timeperiod'])
period1['periodid']=1
period2=pd.DataFrame(period2,columns=['timeperiod'])
period2['periodid']=2
period3=pd.DataFrame(period3,columns=['timeperiod'])
period3['periodid']=3
period4=pd.DataFrame(period4,columns=['timeperiod'])
period4['periodid']=4
period5=pd.DataFrame(period5,columns=['timeperiod'])
period5['periodid']=5
period6=pd.DataFrame(period6,columns=['timeperiod'])
period6['periodid']=6
periodlist=pd.concat([period1,period2,period3,period4,period5,period6],ignore_index=True)
pdpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
pdpre=pd.merge(pdpre,periodlist,how='left',left_on='time',right_on='timeperiod')
pdpre=pdpre.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
pdpre=pdpre.groupby(['periodid'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre.columns=['Period','PreEntries']
pdpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
pdpost=pd.merge(pdpost,periodlist,how='left',left_on='time',right_on='timeperiod')
pdpost=pdpost.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
pdpost=pdpost.groupby(['periodid'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost.columns=['Period','PostEntries']
pddiff=pd.concat([pdpre,pdpost],ignore_index=True)
pddiff.to_csv(path+'OUTPUT/pddiffclosure.csv',index=False)

# Period Diff by Station
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/20/2020','04/21/2020','04/22/2020','04/23/2020','04/24/2020']
postdates=['04/27/2020','04/28/2020','04/29/2020','04/30/2020','05/01/2020']
period1=['11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00','13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00']
period2=['15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00','17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00']
period3=['19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00','21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00']
period4=['23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00','01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00']
period5=['03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00','05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00']
period6=['07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00','09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00']
pdpre1=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period1))].reset_index(drop=True)
pdpre1=pdpre1.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre1.columns=['unit','pdpre1']
pdpost1=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period1))].reset_index(drop=True)
pdpost1=pdpost1.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost1.columns=['unit','pdpost1']
pdpre2=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period2))].reset_index(drop=True)
pdpre2=pdpre2.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre2.columns=['unit','pdpre2']
pdpost2=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period2))].reset_index(drop=True)
pdpost2=pdpost2.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost2.columns=['unit','pdpost2']
pdpre3=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period3))].reset_index(drop=True)
pdpre3=pdpre3.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre3.columns=['unit','pdpre3']
pdpost3=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period3))].reset_index(drop=True)
pdpost3=pdpost3.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost3.columns=['unit','pdpost3']
pdpre4=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period4))].reset_index(drop=True)
pdpre4=pdpre4.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre4.columns=['unit','pdpre4']
pdpost4=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period4))].reset_index(drop=True)
pdpost4=pdpost4.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost4.columns=['unit','pdpost4']
pdpre5=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period5))].reset_index(drop=True)
pdpre5=pdpre5.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre5.columns=['unit','pdpre5']
pdpost5=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period5))].reset_index(drop=True)
pdpost5=pdpost5.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost5.columns=['unit','pdpost5']
pdpre6=dfunitentry[(np.isin(dfunitentry['firstdate'],predates))&(np.isin(dfunitentry['time'],period6))].reset_index(drop=True)
pdpre6=pdpre6.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpre6.columns=['unit','pdpre6']
pdpost6=dfunitentry[(np.isin(dfunitentry['firstdate'],postdates))&(np.isin(dfunitentry['time'],period6))].reset_index(drop=True)
pdpost6=pdpost6.groupby('unit',as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
pdpost6.columns=['unit','pdpost6']
cplxpddiff=pd.merge(pdpre1,pdpost1,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpre2,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpost2,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpre3,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpost3,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpre4,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpost4,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpre5,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpost5,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpre6,how='outer',on='unit')
cplxpddiff=pd.merge(cplxpddiff,pdpost6,how='outer',on='unit')
cplxpddiff['pdpre']=cplxpddiff['pdpre1']+cplxpddiff['pdpre2']+cplxpddiff['pdpre3']+cplxpddiff['pdpre4']+cplxpddiff['pdpre5']+cplxpddiff['pdpre6']
cplxpddiff['pdpost']=cplxpddiff['pdpost1']+cplxpddiff['pdpost2']+cplxpddiff['pdpost3']+cplxpddiff['pdpost4']+cplxpddiff['pdpost5']+cplxpddiff['pdpost6']
cplxpddiff=pd.merge(rc,cplxpddiff,how='outer',left_on='Remote',right_on='unit')
cplxpddiff=cplxpddiff.fillna(0)
cplxpddiff=cplxpddiff.groupby(['CplxID'],as_index=False).agg({'pdpre1':'sum','pdpost1':'sum','pdpre2':'sum','pdpost2':'sum',
                             'pdpre3':'sum','pdpost3':'sum','pdpre4':'sum','pdpost4':'sum','pdpre5':'sum','pdpost5':'sum','pdpre6':'sum',
                             'pdpost6':'sum','pdpre':'sum','pdpost':'sum'}).reset_index(drop=True)
cplxpddiff['pddiff1']=(cplxpddiff['pdpost1']-cplxpddiff['pdpre1'])/cplxpddiff['pdpre1']
cplxpddiff['pddiff2']=(cplxpddiff['pdpost2']-cplxpddiff['pdpre2'])/cplxpddiff['pdpre2']
cplxpddiff['pddiff3']=(cplxpddiff['pdpost3']-cplxpddiff['pdpre3'])/cplxpddiff['pdpre3']
cplxpddiff['pddiff4']=(cplxpddiff['pdpost4']-cplxpddiff['pdpre4'])/cplxpddiff['pdpre4']
cplxpddiff['pddiff5']=(cplxpddiff['pdpost5']-cplxpddiff['pdpre5'])/cplxpddiff['pdpre5']
cplxpddiff['pddiff6']=(cplxpddiff['pdpost6']-cplxpddiff['pdpre6'])/cplxpddiff['pdpre6']
cplxpddiff['pddiff']=(cplxpddiff['pdpost']-cplxpddiff['pdpre'])/cplxpddiff['pdpre']
cplxpddiff=cplxpddiff.fillna(0)
cplxpddiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpddiff,how='left',on='CplxID')
cplxpddiff=cplxpddiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','pdpre1','pdpost1','pdpre2','pdpost2','pdpre3',
                       'pdpost3','pdpre4','pdpost4','pdpre5','pdpost5','pdpre6','pdpost6','pdpre','pdpost','pddiff1','pddiff2','pddiff3',
                       'pddiff4','pddiff5','pddiff6','pddiff']].reset_index(drop=True)
cplxpddiff.to_csv(path+'OUTPUT/cplxpddiff.csv',index=False)









