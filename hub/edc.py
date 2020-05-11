import pandas as pd
import geopandas as gpd



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/HUB/EDC/'



hub=['36061005200','36061007000','36061010100','36061007100','36061007300','36061007500','36061007600','36061007800','36061007900',
     '36061008100','36061008400','36061008700','36061000201','36061008800','36061000202','36061009000','36061009200','36061000600',
     '36061009300','36061000700','36061009400','36061009600','36061000900','36061008603','36061001002','36061031900','36061009800',
     '36061001200','36061009900','36061001300','36061010000','36061001401','36061010200','36061010300','36061010400','36061031704',
     '36061011500','36061011700','36061011900','36061012100','36061012700','36061012900','36061013300','36061013500','36061013700',
     '36061009700','36061001502','36061013900','36061008300','36061003602','36061005502','36061012500','36061004800','36061009100',
     '36061006600','36061001001','36061007700','36061011201','36061008000','36061007400','36061003200','36061004100','36061001501',
     '36061002100','36061001600','36061001402','36061001800','36061002000','36061004000','36061002201','36061002202','36061011300',
     '36061002400','36061002601','36061009500','36061008601','36061002602','36061002700','36061002800','36061010601','36061005900',
     '36061002900','36061003001','36061003100','36061006300','36061003300','36061003601','36061003800','36061003400','36061003900',
     '36061010800','36061004200','36061004300','36061003002','36061008602','36061013100','36061004500','36061008200','36061004700',
     '36061004900','36061008900','36061005400','36061002500','36061005501','36061011100','36061005600','36061005000','36061005700',
     '36061000800','36061005800','36061006000','36061010900','36061006100','36061007200','36061006200','36061011202','36061006400',
     '36061006500','36061011203','36061006700','36061031703','36061006800','36061004400','36061003700','36061006900']



allaux=pd.read_csv(path+'LEHDCTPP/ny_od_aux_JT00_2017.csv',dtype=str)
allmain=pd.read_csv(path+'LEHDCTPP/ny_od_main_JT00_2017.csv',dtype=str)
alljob=pd.concat([allaux,allmain],axis=0,ignore_index=True)
alljob=alljob[[str(x)[0:11] in hub for x in alljob['w_geocode']]]
alljob['alljob']=pd.to_numeric(alljob['S000'])
sum(alljob['alljob'])



alljob=pd.read_csv(path+'LEHDCTPP/ny_wac_S000_JT00_2017.csv',dtype=str)
alljob=alljob[[str(x)[0:11] in hub for x in alljob['w_geocode']]]
alljob['alljob']=pd.to_numeric(alljob['C000'])
sum(alljob['alljob'])



primaryaux=pd.read_csv(path+'LEHDCTPP/ny_od_aux_JT01_2017.csv',dtype=str)
primarymain=pd.read_csv(path+'LEHDCTPP/ny_od_main_JT01_2017.csv',dtype=str)
primaryjob=pd.concat([primaryaux,primarymain],axis=0,ignore_index=True)
primaryjob=primaryjob[[str(x)[0:11] in hub for x in primaryjob['w_geocode']]]
primaryjob['primaryjob']=pd.to_numeric(primaryjob['S000'])
sum(primaryjob['primaryjob'])



primaryres=pd.read_csv(path+'LEHDCTPP/ny_rac_S000_JT01_2017.csv',dtype=str)
primaryres=primaryres[[str(x)[0:11] in hub for x in primaryres['h_geocode']]]
primaryres['primaryres']=pd.to_numeric(primaryres['C000'])
sum(primaryres['primaryres'])



primaryaux=pd.read_csv(path+'LEHDCTPP/ny_od_aux_JT01_2017.csv',dtype=str)
primarymain=pd.read_csv(path+'LEHDCTPP/ny_od_main_JT01_2017.csv',dtype=str)
primarycbdrescbdjob=pd.concat([primaryaux,primarymain],axis=0,ignore_index=True)
primarycbdrescbdjob=primarycbdrescbdjob[[str(x)[0:11] in hub for x in primarycbdrescbdjob['w_geocode']]]
primarycbdrescbdjob=primarycbdrescbdjob[[str(x)[0:11] in hub for x in primarycbdrescbdjob['h_geocode']]]
primarycbdrescbdjob['primarycbdrescbdjob']=pd.to_numeric(primarycbdrescbdjob['S000'])
sum(primarycbdrescbdjob['primarycbdrescbdjob'])



primaryaux=pd.read_csv(path+'LEHDCTPP/ny_od_aux_JT01_2017.csv',dtype=str)
primarymain=pd.read_csv(path+'LEHDCTPP/ny_od_main_JT01_2017.csv',dtype=str)
primaryoutrescbdjob=pd.concat([primaryaux,primarymain],axis=0,ignore_index=True)
primaryoutrescbdjob=primaryoutrescbdjob[[str(x)[0:11] in hub for x in primaryoutrescbdjob['w_geocode']]]
primaryoutrescbdjob=primaryoutrescbdjob[[str(x)[0:11] not in hub for x in primaryoutrescbdjob['h_geocode']]]
primaryoutrescbdjob['primaryoutrescbdjob']=pd.to_numeric(primaryoutrescbdjob['S000'])
primaryoutrescbdjob['RESCT']=[str(x)[0:11] for x in primaryoutrescbdjob['h_geocode']]
primaryoutrescbdjob=primaryoutrescbdjob.groupby('RESCT',as_index=False).agg({'primaryoutrescbdjob':'sum'}).reset_index(drop=True)



nyctpp=pd.read_csv(path+'LEHDCTPP/NY_2012thru2016_A302103.csv',dtype=str)
njctpp=pd.read_csv(path+'LEHDCTPP/NJ_2012thru2016_A302103.csv',dtype=str)
pactpp=pd.read_csv(path+'LEHDCTPP/PA_2012thru2016_A302103.csv',dtype=str)
ctctpp=pd.read_csv(path+'LEHDCTPP/CT_2012thru2016_A302103.csv',dtype=str)
ctpp=pd.concat([nyctpp,njctpp,pactpp,ctctpp],axis=0,ignore_index=True)
ctpp=ctpp[[str(x)[0:5]=='C5400' for x in ctpp['GEOID']]].reset_index(drop=True)
ctpp['RESCT']=[str(x)[7:18] for x in ctpp['GEOID']]
ctpp['WORKCT']=[str(x)[18:29] for x in ctpp['GEOID']]
ctpp=ctpp[[x in hub for x in ctpp['WORKCT']]].reset_index(drop=True)
ctpp=ctpp[[x not in hub for x in ctpp['RESCT']]].reset_index(drop=True)
ctpp=ctpp[[x in ['1','14'] for x in ctpp['LINENO']]].reset_index(drop=True)
ctpp['EST']=[x.replace(',','') for x in ctpp['EST']]
ctpp['EST']=pd.to_numeric(ctpp['EST'])
ctpp=ctpp.groupby(['RESCT','LINENO'],as_index=False).agg({'EST':'sum'}).reset_index(drop=True)
ctpp=ctpp.pivot(index='RESCT', columns='LINENO', values='EST').reset_index(drop=False)
ctpp=ctpp[pd.notna(ctpp['14'])]
ctpp['walkp']=ctpp['14']/ctpp['1']
quadstatectclipped=gpd.read_file(path+'LEHDCTPP/quadstatectclipped.shp')
ctpp=pd.merge(quadstatectclipped,ctpp,how='inner',left_on='tractid',right_on='RESCT')
ctpp.to_file(path+'LEHDCTPP/ctpp.shp')



primaryoutrescbdjobwalk=pd.merge(ctpp,primaryoutrescbdjob,how='inner',on='RESCT')
primaryoutrescbdjobwalk['walk']=primaryoutrescbdjobwalk['primaryoutrescbdjob']*primaryoutrescbdjobwalk['walkp']
sum(primaryoutrescbdjobwalk['walk'])
primaryoutrescbdjobwalk.to_file(path+'LEHDCTPP/primaryoutrescbdjobwalk.shp')
sum(primaryoutrescbdjobwalk.loc[primaryoutrescbdjobwalk['walk']>50,'walk'])






doserver='http://159.65.64.166:8801/'

pops=pd.read_csv(path+'pops/COVID REQUESTS 2020.csv')
popsorg=pd.read_csv(path+'pops/nycpops_20191220csv/nycpops_20191220.csv')
pops=pd.merge(pops,popsorg,how='left',left_on='POPS Number',right_on='POPS_Number')
popspt=gpd.GeoDataFrame(pops,geometry=[shapely.geometry.Point(x, y) for x, y in zip(pops['Longitude'],pops['Latitude'])],crs={'init':'epsg:4326'})
popspt['CITIBIKE']=popspt['CITIBIKE'].fillna('N')
popspt['TESTING']=popspt['TESTING'].fillna('N')
popspt['FOOD']=popspt['FOOD'].fillna('N')
popspt['Combo?']=popspt['Combo?'].fillna('N')
popspt['High Potential']=popspt['High Potential'].fillna('N')
popspt.to_file(path+'output/popspt.shp')

popsiso=pops.copy()
popsiso['isogeom']=''
for i in pops.index:
    url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK'
    url+='&fromPlace='+str(popsiso.loc[i,'Latitude'])+','+str(popsiso.loc[i,'Longitude'])
    url+='&cutoffSec=600'
    headers={'Accept':'application/json'}  
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
    popsiso.loc[i,'isogeom']=str(iso.loc[0,'geometry'])
popsiso=gpd.GeoDataFrame(popsiso,geometry=popsiso['isogeom'].map(wkt.loads),crs={'init':'epsg:4326'})
popsiso.to_file(path+'output/popsiso.shp')
