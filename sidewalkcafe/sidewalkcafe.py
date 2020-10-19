import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport
import usaddress



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/'



# Open Restaurant
g = Geosupport()
df=pd.read_csv(path+'SIDEWALK CAFE/Open_Restaurant_Applications_20201014.csv',dtype=str)
df['TIME']=[datetime.datetime.strptime(x,'%m/%d/%Y %H:%M:%S %p') for x in df['Time of Submission']]
df=df.sort_values('TIME',ascending=True).reset_index(drop=True)
df['TIME']=[datetime.datetime.strftime(x,'%m/%d/%Y %H:%M:%S %p') for x in df['TIME']]
df['ID']=range(0,len(df))
df['TYPE']=[str(x).strip().upper() for x in df['Seating Interest (Sidewalk/Roadway/Both)']]
df['NAME']=[str(x).strip().upper() for x in df['Restaurant Name']]
df['LEGAL']=[str(x).strip().upper() for x in df['Legal Business Name']]
df['DBA']=[str(x).strip().upper() for x in df['Doing Business As (DBA)']]
df['ADDRESS']=[str(x).strip().upper() for x in df['Business Address']]
df['BLDGNUM']=[str(x).strip().upper() for x in df['Building Number']]
df['STNAME']=[str(x).strip().upper() for x in df['Street']]
df['BORO']=[str(x).strip().upper() for x in df['Borough']]
df['ZIP']=[str(x).strip().upper() for x in df['Postcode']]
df['PMTNUM']=[str(x).strip().upper() for x in df['Food Service Establishment Permit #']]
df['SDWKLEN']=pd.to_numeric(df['Sidewalk Dimensions (Length)'])
df['SDWKWDTH']=pd.to_numeric(df['Sidewalk Dimensions (Width)'])
df['SDWKAREA']=pd.to_numeric(df['Sidewalk Dimensions (Area)'])
df['RDWYLEN']=pd.to_numeric(df['Roadway Dimensions (Length)'])
df['RDWYWDTH']=pd.to_numeric(df['Roadway Dimensions (Width)'])
df['RDWYAREA']=pd.to_numeric(df['Roadway Dimensions (Area)'])
df['APPSDWK']=[str(x).strip().upper() for x in df['Approved for Sidewalk Seating']]
df['APPRDWY']=[str(x).strip().upper() for x in df['Approved for Roadway Seating']]
df['ALCOHOL']=[str(x).strip().upper() for x in df['Qualify Alcohol']]
df['SLANUM']=[str(x).strip().upper() for x in df['SLA Serial Number']]
df['SLATYPE']=[str(x).strip().upper() for x in df['SLA License Type']]
df['LANDMARK']=[str(x).strip().upper() for x in df['Landmark District or Building']]
df['TERMLDMK']=[str(x).strip().upper() for x in df['landmarkDistrict_terms']]
df['TERMHEALTH']=[str(x).strip().upper() for x in df['healthCompliance_terms']]
df['BBL']=np.nan
df['LAT']=np.nan
df['LONG']=np.nan
df['X']=np.nan
df['Y']=np.nan
df['BKFACE']=np.nan
df=df[['ID','TIME','TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
       'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
       'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG','X','Y','BKFACE']].reset_index(drop=True)
df=df.drop_duplicates(['TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
                       'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
                       'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG',
                       'X','Y','BKFACE'],keep='first').reset_index(drop=True)
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=df.loc[i,'ZIP']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
                df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
                df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
            else:
                print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
len(df[pd.notna(df['BBL'])])
# 9888/11183
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(df.loc[i,'BORO']=='MANHATTAN',1,np.where(df.loc[i,'BORO']=='BRONX',2,
                        np.where(df.loc[i,'BORO']=='BROOKLYN',3,np.where(df.loc[i,'BORO']=='QUEENS',4,
                        np.where(df.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
                df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
                df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
            else:
                print(str(df.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(df.loc[i,'ID'])+' not geocoded with borough!')
len(df[pd.notna(df['BBL'])])
# 9937/11183
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            housenumber=df.loc[i,'BLDGNUM']
            streetname=df.loc[i,'STNAME']
            zipcode=df.loc[i,'ZIP']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
                df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
                df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
            else:
                print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
len(df[pd.notna(df['BBL'])])
# 9993/11183
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            housenumber=df.loc[i,'BLDGNUM']
            streetname=df.loc[i,'STNAME']
            boroughcode=np.where(df.loc[i,'BORO']=='MANHATTAN',1,np.where(df.loc[i,'BORO']=='BRONX',2,
                        np.where(df.loc[i,'BORO']=='BROOKLYN',3,np.where(df.loc[i,'BORO']=='QUEENS',4,
                        np.where(df.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
                df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
                df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
            else:
                print(str(df.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(df.loc[i,'ID'])+' not geocoded with borough!')
len(df[pd.notna(df['BBL'])])
# 9995/11183
df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)&(pd.notna(df['LAT']))&(pd.notna(df['LONG']))&(pd.notna(df['BKFACE']))].reset_index(drop=True)
# 9959/11183
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['LONG'],df['LAT'])],crs='epsg:4326')
df.to_file(path+'SIDEWALK CAFE/or.shp')
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['X'],df['Y'])],crs='epsg:6539')
df=df.to_crs('epsg:4326')
df.to_file(path+'SIDEWALK CAFE/or_xy.shp')



# Adjust Open Restaurant to MapPluto Lot Line
df=gpd.read_file(path+'SIDEWALK CAFE/or_xy.shp')
df.crs='epsg:4326'
df=df.to_crs('epsg:6539')
mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
mappluto.crs='epsg:4326'
mappluto=mappluto.to_crs('epsg:6539')
df['XADJ']=np.nan
df['YADJ']=np.nan
for i in df.index:
    try:
        tp=shapely.ops.nearest_points(df.loc[i,'geometry'],list(mappluto.loc[mappluto['BBL']==df.loc[i,'BBL'],'geometry'])[0])[1]
        df.loc[i,'XADJ']=tp.x
        df.loc[i,'YADJ']=tp.y
    except:
        print(str(i)+' error')
df=df[pd.notna(df['XADJ'])].reset_index(drop=True)
# 9950/11183
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['XADJ'],df['YADJ'])],crs='epsg:6539')
df=df.to_crs('epsg:4326')
df.to_file(path+'SIDEWALK CAFE/or_xyadj.shp')



# Join Open Restaurant to Sidewalk Cafe Reg
sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
sdwkcafe.crs='epsg:4326'
sdwkcafe=sdwkcafe.to_crs('epsg:6539')
sdwkcafe['geometry']=[x.buffer(1) for x in sdwkcafe['geometry']]
sdwkcafe=sdwkcafe.to_crs('epsg:4326')
sdwkcafe['CAFETYPE']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
sdwkcafe=sdwkcafe[['CAFETYPE','geometry']].reset_index(drop=True)
df=gpd.read_file(path+'SIDEWALK CAFE/or_xyadj.shp')
df.crs='epsg:4326'
dfcafe=gpd.sjoin(df,sdwkcafe,how='left',op='intersects')
dfcafe=dfcafe[['ID','CAFETYPE']].drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dfcafe['CAFETYPE']=np.where(pd.notna(dfcafe['CAFETYPE']),dfcafe['CAFETYPE'],'NONE')
dfcafe=pd.merge(df,dfcafe,how='inner',on='ID')
dfcafe.to_file(path+'SIDEWALK CAFE/or_cafe.shp')



# Join Open Restaurant to Zoning
mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
mappluto.crs='epsg:4326'
dfcafe=gpd.read_file(path+'SIDEWALK CAFE/or_cafe.shp')
dfcafe.crs='epsg:4326'
dfcafezn=pd.merge(dfcafe,mappluto,how='left',on='BBL')
dfcafezn=dfcafezn.loc[pd.notna(dfcafezn['ZoneDist1']),['ID','ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4',
                                                       'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
dfcafezn['ZD']=''
dfcafezn['OL']=''
dfcafezn['SP']=''
for i in dfcafezn.index:
    dfcafezn.loc[i,'ZD']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4']]) if pd.notna(x)]))
    dfcafezn.loc[i,'OL']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['Overlay1','Overlay2']]) if pd.notna(x)]))
    dfcafezn.loc[i,'SP']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['SPDist1','SPDist2','SPDist3']]) if pd.notna(x)]))
dfcafezn=dfcafezn[['ID','ZD','OL','SP']].reset_index(drop=True)
dfcafezn=pd.merge(dfcafe,dfcafezn,how='left',on='ID')
dfcafezn.to_file(path+'SIDEWALK CAFE/or_cafe_zn.shp')



# Join Open Restaurant to Elevated Rail
el=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/planimetrics/transpstruct.shp')
el.crs='epsg:4326'
el['EL']='YES'
el=el.loc[np.isin(el['FEATURE_CO'],[2320,2340]),['EL','geometry']].reset_index(drop=True)
dfcafezn=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn.shp')
dfcafezn.crs='epsg:4326'
dfcafeznel=dfcafezn.copy()
dfcafeznel=gpd.GeoDataFrame(dfcafeznel,geometry=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznel['X'],dfcafeznel['Y'])],crs='epsg:6539')
dfcafeznel=dfcafeznel.to_crs('epsg:4326')
dfcafeznel=gpd.sjoin(dfcafeznel,el,how='left',op='intersects')
dfcafeznel['EL']=np.where(pd.notna(dfcafeznel['EL']),dfcafeznel['EL'],'NO')
dfcafeznel=dfcafeznel[['ID','EL']].drop_duplicates(keep='first').reset_index(drop=True)
dfcafeznel=pd.merge(dfcafezn,dfcafeznel,how='left',on='ID')
dfcafeznel.to_file(path+'SIDEWALK CAFE/or_cafe_zn_el.shp')



# Join Open Restaurant to Sidewalk Width
dfcafeznel=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el.shp')
dfcafeznel.crs='epsg:4326'
sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
sdwkwdimp.crs='epsg:4326'
sdwkwdimp=sdwkwdimp[['bkfaceid','orgswmedia','impswmedia','length']].reset_index(drop=True)
sdwkwdimp['orgswlen']=sdwkwdimp['orgswmedia']*sdwkwdimp['length']
sdwkwdimp['impswlen']=sdwkwdimp['impswmedia']*sdwkwdimp['length']
sdwkwdimp=sdwkwdimp.groupby(['bkfaceid'],as_index=False).agg({'orgswlen':'sum',
                                                              'impswlen':'sum',
                                                              'length':'sum'}).reset_index(drop=True)
sdwkwdimp['BKFACE']=pd.to_numeric(sdwkwdimp['bkfaceid'])
sdwkwdimp['ORGSWMDN']=sdwkwdimp['orgswlen']/sdwkwdimp['length']
sdwkwdimp['IMPSWMDN']=sdwkwdimp['impswlen']/sdwkwdimp['length']
sdwkwdimp=sdwkwdimp[['BKFACE','ORGSWMDN','IMPSWMDN']].reset_index(drop=True)
dfcafeznelwd=pd.merge(dfcafeznel,sdwkwdimp,how='left',on='BKFACE')
dfcafeznelwd.to_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd.shp')
dfcafeznelwd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')







sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
sdwkwdimp.crs='epsg:4326'
sdwkwdimp=sdwkwdimp[['bkfaceid','orgswmedia','impswmedia','geometry']].reset_index(drop=True)
sdwkwdimp['BKFACE']=pd.to_numeric(sdwkwdimp['bkfaceid'])
sdwkwdimp['ORGSWMDN']=pd.to_numeric(sdwkwdimp['orgswmedia'])
sdwkwdimp['IMPSWMDN']=pd.to_numeric(sdwkwdimp['impswmedia'])
sdwkwdimp['CAT']=np.where(sdwkwdimp['IMPSWMDN']<11,'<11',np.where(sdwkwdimp['IMPSWMDN']<=14,'11~14','>14'))
sdwkwdimp=sdwkwdimp[['BKFACE','ORGSWMDN','IMPSWMDN','CAT','geometry']].reset_index(drop=True)
sdwkwdimp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/sdwkwdimp.geojson',driver='GeoJSON')




import plotly.io as pio
import plotly.express as px
pio.renderers.default = "browser"
px.box(k,x='SDWKWDTH',y='ORGSWMDN')




import numpy as np
a=list(range(1,11))
b=[1-0.7**x for x in a]
c=np.roll(b,-1)
c-b
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
pio.renderers.default = "browser"
px.scatter(x=a,y=b)
fig=go.Figure()
fig.add_trace(go.Scatter(x=a,y=b,mode='lines+markers'))
