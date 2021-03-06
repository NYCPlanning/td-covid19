import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz
import geopandas as gpd
import shapely



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/FARE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float,'Hub':float})



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
    dt=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=2,header=0)
    for j in dt.columns[2:]:
        dt[j]=pd.to_numeric(dt[j],errors='coerce')
    dt=dt.fillna(0)
    dt['fare']=dt.iloc[:,2:].sum(axis=1)
    dt['unit']=dt['REMOTE'].copy()
    dt['week']=wk
    dt=dt[['unit','week','fare']].reset_index(drop=True)
    tp=pd.concat([tp,dt],ignore_index=True)
tp=pd.merge(tp,rc[['Remote']].drop_duplicates(keep='first'),how='inner',left_on='unit',right_on='Remote')
tp['date']=[datetime.datetime.strptime(x[0:10],'%m/%d/%Y') for x in tp['week']]
tp=tp.sort_values(['unit','date']).reset_index(drop=True)
tp=tp[['unit','week','fare']].reset_index(drop=True)
tp.to_csv(path+'fare.csv',index=False)



# Compare the data
df=pd.read_csv(path+'fare.csv',dtype=str,converters={'fare':float})
preweek='04/20/2019-04/26/2019'
postweek='04/18/2020-04/24/2020'
cplxpre=df[np.isin(df['week'],preweek)].reset_index(drop=True)
cplxpre=pd.merge(cplxpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpre=cplxpre.groupby(['CplxID'],as_index=False).agg({'fare':'sum'}).reset_index(drop=True)
cplxpre.columns=['CplxID','PreEntries']
cplxpost=df[np.isin(df['week'],postweek)].reset_index(drop=True)
cplxpost=pd.merge(cplxpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpost=cplxpost.groupby(['CplxID'],as_index=False).agg({'fare':'sum'}).reset_index(drop=True)
cplxpost.columns=['CplxID','PostEntries']
cplxdiff=pd.merge(cplxpre,cplxpost,how='inner',on='CplxID')
cplxdiff['Pct']=cplxdiff['PostEntries']/cplxdiff['PreEntries']
cplxdiff['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxdiff['PctCat']=np.where(cplxdiff['Pct']<=0.1,'1%~10%',
                   np.where(cplxdiff['Pct']<=0.15,'11%~15%',
                            '16%~38%'))
cplxdiff=pd.merge(cplxdiff,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxdiff=cplxdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','PreEntries','PostEntries','Pct','PctCat']].reset_index(drop=True)
cplxdiff=gpd.GeoDataFrame(cplxdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxdiff['CplxLong'],cplxdiff['CplxLat'])],crs='epsg:4326')
cplxdiff.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nadirfare.geojson',driver='GeoJSON')









