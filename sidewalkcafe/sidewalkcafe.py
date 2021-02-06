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
# path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/'
path='/home/mayijun/'



# # Open Restaurant
# g = Geosupport()
# df=pd.read_csv(path+'SIDEWALK CAFE/Open_Restaurant_Applications_20201014.csv',dtype=str)
# df['TIME']=[datetime.datetime.strptime(x,'%m/%d/%Y %H:%M:%S %p') for x in df['Time of Submission']]
# df=df.sort_values('TIME',ascending=True).reset_index(drop=True)
# df['TIME']=[datetime.datetime.strftime(x,'%m/%d/%Y %H:%M:%S %p') for x in df['TIME']]
# df['ID']=range(0,len(df))
# df['TYPE']=[str(x).strip().upper() for x in df['Seating Interest (Sidewalk/Roadway/Both)']]
# df['NAME']=[str(x).strip().upper() for x in df['Restaurant Name']]
# df['LEGAL']=[str(x).strip().upper() for x in df['Legal Business Name']]
# df['DBA']=[str(x).strip().upper() for x in df['Doing Business As (DBA)']]
# df['ADDRESS']=[str(x).strip().upper() for x in df['Business Address']]
# df['BLDGNUM']=[str(x).strip().upper() for x in df['Building Number']]
# df['STNAME']=[str(x).strip().upper() for x in df['Street']]
# df['BORO']=[str(x).strip().upper() for x in df['Borough']]
# df['ZIP']=[str(x).strip().upper() for x in df['Postcode']]
# df['PMTNUM']=[str(x).strip().upper() for x in df['Food Service Establishment Permit #']]
# df['SDWKLEN']=pd.to_numeric(df['Sidewalk Dimensions (Length)'])
# df['SDWKWDTH']=pd.to_numeric(df['Sidewalk Dimensions (Width)'])
# df['SDWKAREA']=pd.to_numeric(df['Sidewalk Dimensions (Area)'])
# df['RDWYLEN']=pd.to_numeric(df['Roadway Dimensions (Length)'])
# df['RDWYWDTH']=pd.to_numeric(df['Roadway Dimensions (Width)'])
# df['RDWYAREA']=pd.to_numeric(df['Roadway Dimensions (Area)'])
# df['APPSDWK']=[str(x).strip().upper() for x in df['Approved for Sidewalk Seating']]
# df['APPRDWY']=[str(x).strip().upper() for x in df['Approved for Roadway Seating']]
# df['ALCOHOL']=[str(x).strip().upper() for x in df['Qualify Alcohol']]
# df['SLANUM']=[str(x).strip().upper() for x in df['SLA Serial Number']]
# df['SLATYPE']=[str(x).strip().upper() for x in df['SLA License Type']]
# df['LANDMARK']=[str(x).strip().upper() for x in df['Landmark District or Building']]
# df['TERMLDMK']=[str(x).strip().upper() for x in df['landmarkDistrict_terms']]
# df['TERMHEALTH']=[str(x).strip().upper() for x in df['healthCompliance_terms']]
# df['BBL']=np.nan
# df['LAT']=np.nan
# df['LONG']=np.nan
# df['X']=np.nan
# df['Y']=np.nan
# df['BKFACE']=np.nan
# df=df[['ID','TIME','TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
#        'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
#        'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG','X','Y','BKFACE']].reset_index(drop=True)
# df=df.drop_duplicates(['TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
#                        'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
#                        'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG',
#                        'X','Y','BKFACE'],keep='first').reset_index(drop=True)
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
#             streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
#             zipcode=df.loc[i,'ZIP']
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
# len(df[pd.notna(df['BBL'])])
# # 9888/11183
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
#             streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
#             boroughcode=np.where(df.loc[i,'BORO']=='MANHATTAN',1,np.where(df.loc[i,'BORO']=='BRONX',2,
#                         np.where(df.loc[i,'BORO']=='BROOKLYN',3,np.where(df.loc[i,'BORO']=='QUEENS',4,
#                         np.where(df.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with borough!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with borough!')
# len(df[pd.notna(df['BBL'])])
# # 9937/11183
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=df.loc[i,'BLDGNUM']
#             streetname=df.loc[i,'STNAME']
#             zipcode=df.loc[i,'ZIP']
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
# len(df[pd.notna(df['BBL'])])
# # 9993/11183
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=df.loc[i,'BLDGNUM']
#             streetname=df.loc[i,'STNAME']
#             boroughcode=np.where(df.loc[i,'BORO']=='MANHATTAN',1,np.where(df.loc[i,'BORO']=='BRONX',2,
#                         np.where(df.loc[i,'BORO']=='BROOKLYN',3,np.where(df.loc[i,'BORO']=='QUEENS',4,
#                         np.where(df.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with borough!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with borough!')
# len(df[pd.notna(df['BBL'])])
# # 9995/11183
# df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)&(pd.notna(df['LAT']))&(pd.notna(df['LONG']))&(pd.notna(df['BKFACE']))].reset_index(drop=True)
# # 9959/11183
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['LONG'],df['LAT'])],crs='epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/or.shp')
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['X'],df['Y'])],crs='epsg:6539')
# df=df.to_crs('epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/or_xy.shp')



# # Adjust Open Restaurant to MapPluto Lot Line
# df=gpd.read_file(path+'SIDEWALK CAFE/or_xy.shp')
# df.crs='epsg:4326'
# df=df.to_crs('epsg:6539')
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# mappluto=mappluto.to_crs('epsg:6539')
# mappluto['geometry']=[x.boundary for x in mappluto['geometry']]
# df['XADJ']=np.nan
# df['YADJ']=np.nan
# for i in df.index:
#     try:
#         tp=shapely.ops.nearest_points(df.loc[i,'geometry'],list(mappluto.loc[mappluto['BBL']==df.loc[i,'BBL'],'geometry'])[0])[1]
#         df.loc[i,'XADJ']=tp.x
#         df.loc[i,'YADJ']=tp.y
#     except:
#         print(str(i)+' error')
# df=df[pd.notna(df['XADJ'])].reset_index(drop=True)
# # 9950/11183
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['XADJ'],df['YADJ'])],crs='epsg:6539')
# df=df.to_crs('epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/or_xyadj.shp')



# # Join Open Restaurant to Sidewalk Cafe Reg
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe=sdwkcafe.to_crs('epsg:6539')
# sdwkcafe['geometry']=[x.buffer(5) for x in sdwkcafe['geometry']]
# sdwkcafe=sdwkcafe.to_crs('epsg:4326')
# sdwkcafe['CAFETYPE']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
# sdwkcafe=sdwkcafe[['CAFETYPE','geometry']].reset_index(drop=True)
# df=gpd.read_file(path+'SIDEWALK CAFE/or_xyadj.shp')
# df.crs='epsg:4326'
# dfcafe=gpd.sjoin(df,sdwkcafe,how='left',op='intersects')
# dfcafe=dfcafe[['ID','CAFETYPE']].drop_duplicates(['ID'],keep='first').reset_index(drop=True)
# dfcafe['CAFETYPE']=np.where(pd.notna(dfcafe['CAFETYPE']),dfcafe['CAFETYPE'],'NONE')
# dfcafe=pd.merge(df,dfcafe,how='inner',on='ID')
# dfcafe.to_file(path+'SIDEWALK CAFE/or_cafe.shp')



# # Join Open Restaurant to Zoning
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# dfcafe=gpd.read_file(path+'SIDEWALK CAFE/or_cafe.shp')
# dfcafe.crs='epsg:4326'
# dfcafezn=pd.merge(dfcafe,mappluto,how='left',on='BBL')
# dfcafezn=dfcafezn.loc[pd.notna(dfcafezn['ZoneDist1']),['ID','ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4',
#                                                        'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
# dfcafezn['ZD']=''
# dfcafezn['RM']=np.nan
# dfcafezn['OL']=''
# dfcafezn['SP']=''
# for i in dfcafezn.index:
#     dfcafezn.loc[i,'ZD']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4']]) if pd.notna(x)]))
#     dfcafezn.loc[i,'RM']=np.where((dfcafezn.loc[i,'ZD'].startswith('C')==False)&(dfcafezn.loc[i,'ZD']!='BPC')&(dfcafezn.loc[i,'ZD']!='PARK'),1,0)
#     dfcafezn.loc[i,'OL']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['Overlay1','Overlay2']]) if pd.notna(x)]))
#     dfcafezn.loc[i,'SP']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['SPDist1','SPDist2','SPDist3']]) if pd.notna(x)]))
# dfcafezn=dfcafezn[['ID','ZD','RM','OL','SP']].reset_index(drop=True)
# dfcafezn=pd.merge(dfcafe,dfcafezn,how='left',on='ID')
# dfcafezn.to_file(path+'SIDEWALK CAFE/or_cafe_zn.shp')



# # Join Open Restaurant to Elevated Rail
# el=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/planimetrics/transpstruct.shp')
# el.crs='epsg:4326'
# el['EL']='YES'
# el=el.loc[np.isin(el['FEATURE_CO'],[2320,2340]),['EL','geometry']].reset_index(drop=True)
# dfcafezn=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn.shp')
# dfcafezn.crs='epsg:4326'
# dfcafeznel=dfcafezn.copy()
# dfcafeznel=gpd.GeoDataFrame(dfcafeznel,geometry=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznel['X'],dfcafeznel['Y'])],crs='epsg:6539')
# dfcafeznel=dfcafeznel.to_crs('epsg:4326')
# dfcafeznel=gpd.sjoin(dfcafeznel,el,how='left',op='intersects')
# dfcafeznel['EL']=np.where(pd.notna(dfcafeznel['EL']),dfcafeznel['EL'],'NO')
# dfcafeznel=dfcafeznel[['ID','EL']].drop_duplicates(keep='first').reset_index(drop=True)
# dfcafeznel=pd.merge(dfcafezn,dfcafeznel,how='left',on='ID')
# dfcafeznel.to_file(path+'SIDEWALK CAFE/or_cafe_zn_el.shp')



# # Join Open Restaurant to Sidewalk Width
# dfcafeznel=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el.shp')
# dfcafeznel.crs='epsg:4326'
# sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp[['bkfaceid','orgswmedia','impswmedia','length']].reset_index(drop=True)
# sdwkwdimp['orgswlen']=sdwkwdimp['orgswmedia']*sdwkwdimp['length']
# sdwkwdimp['impswlen']=sdwkwdimp['impswmedia']*sdwkwdimp['length']
# sdwkwdimp=sdwkwdimp.groupby(['bkfaceid'],as_index=False).agg({'orgswlen':'sum',
#                                                               'impswlen':'sum',
#                                                               'length':'sum'}).reset_index(drop=True)
# sdwkwdimp['BKFACE']=pd.to_numeric(sdwkwdimp['bkfaceid'])
# sdwkwdimp['ORGSWMDN']=sdwkwdimp['orgswlen']/sdwkwdimp['length']
# sdwkwdimp['IMPSWMDN']=sdwkwdimp['impswlen']/sdwkwdimp['length']
# sdwkwdimp=sdwkwdimp[['BKFACE','ORGSWMDN','IMPSWMDN']].reset_index(drop=True)
# dfcafeznelwd=pd.merge(dfcafeznel,sdwkwdimp,how='left',on='BKFACE')
# dfcafeznelwd.to_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd.shp')
# dfcafeznelwd=dfcafeznelwd[np.isin(dfcafeznelwd['TYPE'],['BOTH','SIDEWALK'])].reset_index(drop=True)
# dfcafeznelwd['SWCAT']=np.where(dfcafeznelwd['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwd['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
# dfcafeznelwd['CP']=np.where(dfcafeznelwd['IMPSWMDN']>=15,'>=12 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=14,'11 ft ~ 12 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=13,'10 ft ~ 11 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=12,'9 ft ~ 10 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=11,'8 ft ~ 9 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                             '<5 ft'))))))))
# dfcafeznelwd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')



# # Analyses
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdan=dfcafeznelwd.copy()

# dfcafeznelwdan['CAFETYPE'].value_counts(dropna=False)

# dfcafeznelwdan['ZDR']=[x.find('R') for x in dfcafeznelwdan['ZD']]
# dfcafeznelwdan['ZDR']=np.where(dfcafeznelwdan['ZDR']==-1,0,1)
# dfcafeznelwdan['ZDR'].value_counts(dropna=False)
# dfcafeznelwdan['ZDOL']=np.where(pd.notna(dfcafeznelwdan['OL']),1,0)
# dfcafeznelwdan.loc[dfcafeznelwdan['ZDR']==1,'ZDOL'].value_counts(dropna=False)

# dfcafeznelwdan['ZDC']=[x.find('C') for x in dfcafeznelwdan['ZD']]
# dfcafeznelwdan['ZDC']=np.where(dfcafeznelwdan['ZDC']==-1,0,1)
# dfcafeznelwdan['ZDC'].value_counts(dropna=False)
# dfcafeznelwdan.loc[dfcafeznelwdan['ZDC']==1,'CAFETYPE'].value_counts(dropna=False)

# dfcafeznelwdan['ZDM1']=[x.find('M1') for x in dfcafeznelwdan['ZD']]
# dfcafeznelwdan['ZDM2']=[x.find('M2') for x in dfcafeznelwdan['ZD']]
# dfcafeznelwdan['ZDM3']=[x.find('M3') for x in dfcafeznelwdan['ZD']]
# dfcafeznelwdan['ZDM']=np.where(dfcafeznelwdan['ZDM1']>=0,1,
#                       np.where(dfcafeznelwdan['ZDM2']>=0,1,
#                       np.where(dfcafeznelwdan['ZDM3']>=0,1,0)))
# dfcafeznelwdan['ZDM'].value_counts(dropna=False)
# dfcafeznelwdan.loc[dfcafeznelwdan['ZDM']==1,'OL'].value_counts(dropna=False)

# dfcafeznelwdan['ZDSP']=np.where(pd.notna(dfcafeznelwdan['SP']),1,0)
# dfcafeznelwdan['ZDSP'].value_counts(dropna=False)
# dfcafeznelwdan.loc[dfcafeznelwdan['ZDSP']==1,'SP'].value_counts(dropna=False)

# dfcafeznelwdan['EL'].value_counts(dropna=False)

# dfcafeznelwdan['WD']=np.where(dfcafeznelwdan['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdan['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
# dfcafeznelwdan['WD'].value_counts(dropna=False)
# dfcafeznelwdan[dfcafeznelwdan['ID']==6474]
# 4474+2325+1738
# [4474/8537,2325/8537,1738/8537]

# k=dfcafeznelwdan.groupby('BKFACE').agg({'ID':'count'})
# len(k['ID'])
# len(k[np.isin(k['ID'],[1,2])])/len(k['ID'])
# len(k[np.isin(k['ID'],[3,4])])/len(k['ID'])
# len(k[np.isin(k['ID'],[5,6])])/len(k['ID'])
# len(k[k['ID']>=7])/len(k['ID'])



# # Convert to GeoJSON
# # Sidewalk Width
# sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp[['bkfaceid','orgswmedia','impswmedia','geometry']].reset_index(drop=True)
# sdwkwdimp['bkface']=pd.to_numeric(sdwkwdimp['bkfaceid'])
# sdwkwdimp['orgswmdn']=pd.to_numeric(sdwkwdimp['orgswmedia'])
# sdwkwdimp['impswmdn']=pd.to_numeric(sdwkwdimp['impswmedia'])
# sdwkwdimp['cat']=np.where(sdwkwdimp['impswmdn']>14,'>14 ft',np.where(sdwkwdimp['impswmdn']>=11,'11 ft ~ 14 ft','<11 ft'))
# sdwkwdimp['geometry']=[x.simplify(tolerance=0.01,preserve_topology=True) for x in sdwkwdimp['geometry']]
# sdwkwdimp=sdwkwdimp[['bkface','orgswmdn','impswmdn','cat','geometry']].reset_index(drop=True)
# sdwkwdimp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/sdwkwdimp.geojson',driver='GeoJSON')

# # Sidewalk Cafe
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe['CAFETYPE']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
# sdwkcafe=sdwkcafe.loc[sdwkcafe['CAFETYPE']!='NONE',['CAFETYPE','geometry']].reset_index(drop=True)
# sdwkcafe.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/sdwkcafe.geojson',driver='GeoJSON')

# # Zoning District
# zd=gpd.read_file(path+'SIDEWALK CAFE/nycgiszoningfeatures_202009shp/nyzd.shp')
# zd.crs='epsg:2263'
# zd=zd.to_crs('epsg:4326')
# zd['cat']=''
# zd=zd[['ZONEDIST','cat','geometry']].reset_index(drop=True)
# zd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/zd.geojson',driver='GeoJSON')

# # Comemrcial Overlay
# co=gpd.read_file(path+'SIDEWALK CAFE/nycgiszoningfeatures_202009shp/nyco.shp')
# co.crs='epsg:2263'
# co=co.to_crs('epsg:4326')
# co['cat']=''
# co=co[['OVERLAY','cat','geometry']].reset_index(drop=True)
# co.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/co.geojson',driver='GeoJSON')

# # Special District
# sp=gpd.read_file(path+'SIDEWALK CAFE/nycgiszoningfeatures_202009shp/nysp.shp')
# sp.crs='epsg:2263'
# sp=sp.to_crs('epsg:4326')
# sp['cat']=''
# sp=sp[['SDNAME','SDLBL','cat','geometry']].reset_index(drop=True)
# sp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/sp.geojson',driver='GeoJSON')

# # Elevated Rail
# el=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/planimetrics/transpstruct.shp')
# el.crs='epsg:4326'
# el['cat']=''
# el=el.loc[np.isin(el['FEATURE_CO'],[2320,2340]),['cat','geometry']].reset_index(drop=True)
# el.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/el.geojson',driver='GeoJSON')












# # Impediments Buffer
# imp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/impediment.shp')
# imp.crs='epsg:4326'
# imp=imp.to_crs('epsg:6539')
# imp=imp[imp['imptype']!='Utility Strip'].reset_index(drop=True)
# # Curb Tree
# impct=imp[imp['imptype']=='Curb Tree'].reset_index(drop=True)
# impct['geometry']=[x.buffer(8,cap_style=3,join_style=2) for x in impct['geometry']]
# # Hydrant
# imphd=imp[imp['imptype']=='Hydrant'].reset_index(drop=True)
# imphd['geometry']=[x.buffer(10,cap_style=3,join_style=2) for x in imphd['geometry']]
# # Litter Bin
# implb=imp[imp['imptype']=='Litter Bin'].reset_index(drop=True)
# implb['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in implb['geometry']]
# # Meter
# impmt=imp[imp['imptype']=='Meter'].reset_index(drop=True)
# impmt['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in impmt['geometry']]
# # Bus Shelter
# impbs=imp[imp['imptype']=='Bus Shelter'].reset_index(drop=True)
# impbs['geometry']=[x.buffer(15,cap_style=3,join_style=2) for x in impbs['geometry']]
# # Pay Phone
# imppp=imp[imp['imptype']=='Pay Phone'].reset_index(drop=True)
# imppp['geometry']=[x.buffer(8,cap_style=3,join_style=2) for x in imppp['geometry']]
# # LinkNYC
# impln=imp[imp['imptype']=='LinkNYC'].reset_index(drop=True)
# impln['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in impln['geometry']]
# # City Bench
# impcb=imp[imp['imptype']=='City Bench'].reset_index(drop=True)
# impcb['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in impcb['geometry']]
# # Railroad Structure
# imprs=imp[imp['imptype']=='Railroad Structure'].reset_index(drop=True)
# imprs['geometry']=[x.buffer(15,cap_style=3,join_style=2) for x in imprs['geometry']]
# imprs['imptype']='Transit Entrance'
# # WalkNYC
# impwn=imp[imp['imptype']=='WalkNYC'].reset_index(drop=True)
# impwn['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in impwn['geometry']]
# # Recycle Bin
# imprb=imp[imp['imptype']=='Recycle Bin'].reset_index(drop=True)
# imprb['geometry']=[x.buffer(5,cap_style=3,join_style=2) for x in imprb['geometry']]
# # News Stand
# impns=imp[imp['imptype']=='News Stand'].reset_index(drop=True)
# impns['geometry']=[x.buffer(15,cap_style=3,join_style=2) for x in impns['geometry']]
# # Combine All
# impbf=pd.concat([impct,imphd,implb,impmt,impbs,imppp,impln,impcb,imprs,impwn,imprb,impns],axis=0,ignore_index=True)
# impbf=impbf.to_crs('epsg:4326')
# impbf.to_file(path+'SIDEWALK CAFE/impbf.shp')








# # Restaurant Buffer
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdbf=dfcafeznelwd.to_crs('epsg:6539')
# dfcafeznelwdbf['geometry']=[x.buffer(3) for x in dfcafeznelwdbf['geometry']]
# dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:4326')
# impbf=gpd.read_file(path+'SIDEWALK CAFE/impbf.shp')
# impbf.crs='epsg:4326'
# dfcafeznelwdbfimp=gpd.sjoin(dfcafeznelwdbf,impbf,how='inner',op='intersects')
# dfcafeznelwdbfimp.imptype.value_counts()
















# # DCA License
# g = Geosupport()
# df=pd.read_csv(path+'SIDEWALK CAFE/DCA License.csv',dtype=str)
# df['ID']=[str(x).strip().upper() for x in df['Record ID']]
# df['TYPE']=[str(x).strip().upper() for x in df['Sidewalk Cafe Type']]
# df['STATUS']=[str(x).strip().upper() for x in df['Application Status']]
# df['EXPDATE']=[str(x).strip().upper() for x in df['Expiration Date']]
# df['NAME']=[str(x).strip().upper() for x in df['Business Name']]
# df['DBA']=[str(x).strip().upper() for x in df['DBA / Trade Name']]
# df['BLDGNUM']=[str(x).strip().upper() for x in df['Building No']]
# df['STNAME']=[str(x).strip().upper() for x in df['Street 1']]
# df['CITY']=[str(x).strip().upper() for x in df['City']]
# df['BORO']=np.where(df['CITY']=='NEW YORK','MANHATTAN',
#            np.where(df['CITY']=='BRONX','BRONX',
#            np.where(df['CITY']=='BROOKLYN','BROOKLYN','QUEENS')))
# df['ZIP']=[str(x).strip().upper() for x in df['ZIP']]
# df['BBL']=np.nan
# df['LAT']=np.nan
# df['LONG']=np.nan
# df['X']=np.nan
# df['Y']=np.nan
# df['BKFACE']=np.nan
# df=df[['ID','TYPE','STATUS','EXPDATE','NAME','DBA','BLDGNUM','STNAME','CITY','BORO','ZIP','BBL','LAT','LONG','X','Y','BKFACE']].reset_index(drop=True)
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=df.loc[i,'BLDGNUM']
#             streetname=df.loc[i,'STNAME']
#             zipcode=df.loc[i,'ZIP']
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
# len(df[pd.notna(df['BBL'])])
# # 1359/1363
# for i in df.index:
#     if pd.isna(df.loc[i,'BBL']):
#         try:
#             housenumber=df.loc[i,'BLDGNUM']
#             streetname=df.loc[i,'STNAME']
#             boroughcode=np.where(df.loc[i,'BORO']=='MANHATTAN',1,np.where(df.loc[i,'BORO']=='BRONX',2,
#                         np.where(df.loc[i,'BORO']=='BROOKLYN',3,np.where(df.loc[i,'BORO']=='QUEENS',4,
#                         np.where(df.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
#                 df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
#                 df.loc[i,'BKFACE']=pd.to_numeric(addr['Blockface ID'])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with borough!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with borough!')
# len(df[pd.notna(df['BBL'])])
# # 1359/1363
# df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)&(pd.notna(df['LAT']))&(pd.notna(df['LONG']))&(pd.notna(df['BKFACE']))].reset_index(drop=True)
# # 1352/1363
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['LONG'],df['LAT'])],crs='epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/dca.shp')
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['X'],df['Y'])],crs='epsg:6539')
# df=df.to_crs('epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/dca_xy.shp')



# # Adjust DCA License to MapPluto Lot Line
# df=gpd.read_file(path+'SIDEWALK CAFE/dca_xy.shp')
# df.crs='epsg:4326'
# df=df.to_crs('epsg:6539')
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# mappluto=mappluto.to_crs('epsg:6539')
# mappluto['geometry']=[x.boundary for x in mappluto['geometry']]
# df['XADJ']=np.nan
# df['YADJ']=np.nan
# for i in df.index:
#     try:
#         tp=shapely.ops.nearest_points(df.loc[i,'geometry'],list(mappluto.loc[mappluto['BBL']==df.loc[i,'BBL'],'geometry'])[0])[1]
#         df.loc[i,'XADJ']=tp.x
#         df.loc[i,'YADJ']=tp.y
#     except:
#         print(str(i)+' error')
# df=df[pd.notna(df['XADJ'])].reset_index(drop=True)
# # 1351/1363
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['XADJ'],df['YADJ'])],crs='epsg:6539')
# df=df.to_crs('epsg:4326')
# df.to_file(path+'SIDEWALK CAFE/dca_xyadj.shp')



# # Join DCA License to Sidewalk Cafe Reg
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe=sdwkcafe.to_crs('epsg:6539')
# sdwkcafe['geometry']=[x.buffer(5) for x in sdwkcafe['geometry']]
# sdwkcafe=sdwkcafe.to_crs('epsg:4326')
# sdwkcafe['CAFETYPE']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
# sdwkcafe=sdwkcafe[['CAFETYPE','geometry']].reset_index(drop=True)
# df=gpd.read_file(path+'SIDEWALK CAFE/dca_xyadj.shp')
# df.crs='epsg:4326'
# dfcafe=gpd.sjoin(df,sdwkcafe,how='left',op='intersects')
# dfcafe=dfcafe[['ID','CAFETYPE']].drop_duplicates(['ID'],keep='first').reset_index(drop=True)
# dfcafe['CAFETYPE']=np.where(pd.notna(dfcafe['CAFETYPE']),dfcafe['CAFETYPE'],'NONE')
# dfcafe=pd.merge(df,dfcafe,how='inner',on='ID')
# dfcafe.to_file(path+'SIDEWALK CAFE/dca_cafe.shp')



# # Join DCA License to Zoning
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# dfcafe=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe.shp')
# dfcafe.crs='epsg:4326'
# dfcafezn=pd.merge(dfcafe,mappluto,how='left',on='BBL')
# dfcafezn=dfcafezn.loc[pd.notna(dfcafezn['ZoneDist1']),['ID','ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4',
#                                                        'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
# dfcafezn['ZD']=''
# dfcafezn['RM']=np.nan
# dfcafezn['OL']=''
# dfcafezn['SP']=''
# for i in dfcafezn.index:
#     dfcafezn.loc[i,'ZD']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4']]) if pd.notna(x)]))
#     dfcafezn.loc[i,'RM']=np.where((dfcafezn.loc[i,'ZD'].startswith('C')==False)&(dfcafezn.loc[i,'ZD']!='BPC')&(dfcafezn.loc[i,'ZD']!='PARK'),1,0)
#     dfcafezn.loc[i,'OL']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['Overlay1','Overlay2']]) if pd.notna(x)]))
#     dfcafezn.loc[i,'SP']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['SPDist1','SPDist2','SPDist3']]) if pd.notna(x)]))
# dfcafezn=dfcafezn[['ID','ZD','RM','OL','SP']].reset_index(drop=True)
# dfcafezn=pd.merge(dfcafe,dfcafezn,how='left',on='ID')
# dfcafezn.to_file(path+'SIDEWALK CAFE/dca_cafe_zn.shp')



# # Join DCA License to Elevated Rail
# el=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/planimetrics/transpstruct.shp')
# el.crs='epsg:4326'
# el['EL']='YES'
# el=el.loc[np.isin(el['FEATURE_CO'],[2320,2340]),['EL','geometry']].reset_index(drop=True)
# dfcafezn=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn.shp')
# dfcafezn.crs='epsg:4326'
# dfcafeznel=dfcafezn.copy()
# dfcafeznel=gpd.GeoDataFrame(dfcafeznel,geometry=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznel['X'],dfcafeznel['Y'])],crs='epsg:6539')
# dfcafeznel=dfcafeznel.to_crs('epsg:4326')
# dfcafeznel=gpd.sjoin(dfcafeznel,el,how='left',op='intersects')
# dfcafeznel['EL']=np.where(pd.notna(dfcafeznel['EL']),dfcafeznel['EL'],'NO')
# dfcafeznel=dfcafeznel[['ID','EL']].drop_duplicates(keep='first').reset_index(drop=True)
# dfcafeznel=pd.merge(dfcafezn,dfcafeznel,how='left',on='ID')
# dfcafeznel.to_file(path+'SIDEWALK CAFE/dca_cafe_zn_el.shp')



# # Join DCA License to Sidewalk Width
# dfcafeznel=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn_el.shp')
# dfcafeznel.crs='epsg:4326'
# sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp[['bkfaceid','orgswmedia','impswmedia','length']].reset_index(drop=True)
# sdwkwdimp['orgswlen']=sdwkwdimp['orgswmedia']*sdwkwdimp['length']
# sdwkwdimp['impswlen']=sdwkwdimp['impswmedia']*sdwkwdimp['length']
# sdwkwdimp=sdwkwdimp.groupby(['bkfaceid'],as_index=False).agg({'orgswlen':'sum',
#                                                               'impswlen':'sum',
#                                                               'length':'sum'}).reset_index(drop=True)
# sdwkwdimp['BKFACE']=pd.to_numeric(sdwkwdimp['bkfaceid'])
# sdwkwdimp['ORGSWMDN']=sdwkwdimp['orgswlen']/sdwkwdimp['length']
# sdwkwdimp['IMPSWMDN']=sdwkwdimp['impswlen']/sdwkwdimp['length']
# sdwkwdimp=sdwkwdimp[['BKFACE','ORGSWMDN','IMPSWMDN']].reset_index(drop=True)
# dfcafeznelwd=pd.merge(dfcafeznel,sdwkwdimp,how='left',on='BKFACE')
# dfcafeznelwd.to_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd.shp')
# dfcafeznelwd=dfcafeznelwd[dfcafeznelwd['TYPE']!='ENCLOSED'].reset_index(drop=True)
# dfcafeznelwd['SWCAT']=np.where(dfcafeznelwd['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwd['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
# dfcafeznelwd['CP']=np.where(dfcafeznelwd['IMPSWMDN']>=15,'>=12 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=14,'11 ft ~ 12 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=13,'10 ft ~ 11 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=12,'9 ft ~ 10 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=11,'8 ft ~ 9 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                    np.where(dfcafeznelwd['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                             '<5 ft'))))))))
# dfcafeznelwd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_cafe_zn_el_wd.geojson',driver='GeoJSON')




# # DCA License
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_cafe_zn_el_wd.geojson')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdco=dfcafeznelwd.reset_index(drop=True)
# dfcafeznelwdco['CP']=np.where(dfcafeznelwdco['IMPSWMDN']>=15,'>=12 ft',
#                      np.where(dfcafeznelwdco['IMPSWMDN']>=11,'8 ft ~ 12 ft',
#                               '<8 ft'))
# dfcafeznelwdco.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_co.geojson',driver='GeoJSON')





# # Open Restaurants in Commercial Overlays
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdco=dfcafeznelwd[pd.notna(dfcafeznelwd['OL'])].reset_index(drop=True)
# dfcafeznelwdco['CP']=np.where(dfcafeznelwdco['IMPSWMDN']>=15,'>=12 ft',
#                      np.where(dfcafeznelwdco['IMPSWMDN']>=11,'8 ft ~ 12 ft',
#                               '<8 ft'))
# dfcafeznelwdco.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_co.geojson',driver='GeoJSON')






# # Open Restaurants in R or M districts without Commercial Overlays
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdrm=dfcafeznelwd[(dfcafeznelwd['RM']==1)&(pd.isna(dfcafeznelwd['OL']))].reset_index(drop=True)
# dfcafeznelwdrm['CP']=np.where(dfcafeznelwdrm['IMPSWMDN']>=11,'>=8 ft',
#                    np.where(dfcafeznelwdrm['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                    np.where(dfcafeznelwdrm['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                    np.where(dfcafeznelwdrm['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                             '<5 ft'))))
# dfcafeznelwdrm.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_rm.geojson',driver='GeoJSON')


















# # Lot front based analyses
# # Create lot front
# start=datetime.datetime.now()
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# mappluto=mappluto.to_crs('epsg:6539')
# mappluto['block']=[str(x)[0:6] for x in mappluto['BBL']]
# mappluto['bbl']=mappluto['BBL'].copy()
# mappluto=mappluto[['block','bbl','geometry']].reset_index(drop=True)
# mapplutobk=mappluto[['block','geometry']].reset_index(drop=True)
# mapplutobk=mapplutobk.dissolve(by='block',aggfunc='first').reset_index(drop=False)
# mapplutobk['geometry']=[x.boundary for x in mapplutobk['geometry']]
# mapplutolf=[]
# for i in mapplutobk.index:
#     tps=mapplutobk.loc[i,'geometry']
#     if type(tps)==shapely.geometry.linestring.LineString:
#         splitter=shapely.geometry.MultiPoint(tps.coords)
#         splitseg=gpd.GeoDataFrame(shapely.ops.split(tps,splitter))
#         splitseg['block']=mapplutobk.loc[i,'block']
#         mapplutolf+=[splitseg]
#     else:
#         for j in range(0,len(tps)):
#             splitter=shapely.geometry.MultiPoint(tps[j].coords)
#             splitseg=gpd.GeoDataFrame(shapely.ops.split(tps[j],splitter))
#             splitseg['block']=mapplutobk.loc[i,'block']
#             mapplutolf+=[splitseg]
#     print(str(i))
# mapplutolf=pd.concat(mapplutolf,axis=0,ignore_index=True)
# mapplutolf.columns=['geometry','block']
# mapplutolf=gpd.GeoDataFrame(mapplutolf,geometry=mapplutolf['geometry'],crs='epsg:6539')
# mapplutolf['lfid']=range(0,len(mapplutolf))
# mapplutolf=mapplutolf[['lfid','block','geometry']].reset_index(drop=True)
# mapplutolf=mapplutolf.to_crs('epsg:4326')
# mapplutolf.to_file(path+'SIDEWALK CAFE/mapplutolf.shp')
# print(datetime.datetime.now()-start)
# # 30 mins

# # Join BBL and Cafe Reg to centroid
# start=datetime.datetime.now()
# mapplutolf=gpd.read_file(path+'SIDEWALK CAFE/mapplutolf.shp')
# mapplutolf.crs='epsg:4326'
# mapplutolf=mapplutolf.to_crs('epsg:6539')
# mapplutolfctd=mapplutolf[['lfid','geometry']].reset_index(drop=True)
# mapplutolfctd['geometry']=mapplutolfctd.centroid.buffer(5)
# mapplutolfctd=gpd.sjoin(mapplutolfctd,mappluto,how='left',op='intersects')
# mapplutolfctd=mapplutolfctd[['lfid','bbl','geometry']].reset_index(drop=True)
# mapplutolfctd['geometry']=[x.centroid for x in mapplutolfctd['geometry']]
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe=sdwkcafe.to_crs('epsg:6539')
# sdwkcafe['cafe']=sdwkcafe['CafeType'].copy()
# sdwkcafe['geometry']=sdwkcafe.buffer(5)
# mapplutolfctd=gpd.sjoin(mapplutolfctd,sdwkcafe,how='left',op='intersects')
# mapplutolfctd=mapplutolfctd[['lfid','bbl','cafe','geometry']].reset_index(drop=True)
# mapplutolfctd=mapplutolfctd.to_crs('epsg:4326')
# mapplutolfctd.to_file(path+'SIDEWALK CAFE/mapplutolfctd.shp')
# print(datetime.datetime.now()-start)
# # 30 mins

# Join sidewalk width to centroid
start=datetime.datetime.now()
mapplutolfctd=gpd.read_file(path+'SIDEWALK CAFE/mapplutolfctd.shp')
mapplutolfctd.crs='epsg:4326'
mapplutolfctd=mapplutolfctd.to_crs('epsg:6539')
mapplutolfctdbf=mapplutolfctd.copy()
mapplutolfctdbf['geometry']=mapplutolfctdbf.buffer(50)
sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
sdwkwdimp.crs='epsg:4326'
sdwkwdimp=sdwkwdimp.to_crs('epsg:6539')
mapplutolfctdbf=gpd.sjoin(mapplutolfctdbf,sdwkwdimp,how='inner',op='intersects')
mapplutolfctdsw=[]
for i in mapplutolfctd['lfid']:
    mapplutolfctdtp=mapplutolfctd[mapplutolfctd['lfid']==i,'lfid'].reset_index(drop=True)
    mapplutolfctdbfpv=sdwkwdimp[np.isin(sdwkwdimp['pvid'],mapplutolfctdbf.loc[mapplutolfctdbf['lfid']==i,'pvid'])].reset_index(drop=True)
    if len(mapplutolfctdbfpv)>0:
        try:
            mapplutolfctdbfpv=mapplutolfctdbfpv.loc[[np.argmin([mapplutolfctdtp.loc[0,'geometry'].distance(x) for x in mapplutolfctdbfpv['geometry']])]].reset_index(drop=True)
            mapplutolfctdbfpv=mapplutolfctdbfpv.drop(['length','geometry'],axis=1).reset_index(drop=True)
            mapplutolfctdtp=pd.concat([mapplutolfctdtp,mapplutolfctdbfpv],axis=1,ignore_index=False)
            mapplutolfctdsw+=[mapplutolfctdtp]
        except:
            print(str(i)+' error!')
    else:
        print(str(i)+' no pvid joined!')
    print(str(i))
mapplutolfctdsw=pd.concat(mapplutolfctdsw,ignore_index=True)

mapplutolfctdsw=mapplutolfctdsw.drop('geometry',axis=1)


mapplutolfsw=pd.merge(mapplutolf,mapplutolfctdsw,how='left',on='lfid')
mapplutolfsw=mapplutolfsw[['lfid','block','bbl','cafe','pvid','bkfaceid','spid','side','orgswmin','orgswmax',
                            'orgswmedia','impswmin','impswmax','impswmedia','geometry']].reset_index(drop=True)
mapplutolfsw=mapplutolfsw.to_crs('epsg:4326')
mapplutolfsw.to_file('C:/Users/mayij/Desktop/mapplutolfsw.geojson',driver='GeoJSON')







def curbtreeadjust(ct):
    global curbtree
    global pvmtsp
    global curbtreebuffer
    ct=ct.reset_index(drop=True)
    curbtreetp=pd.concat([ct]*2,ignore_index=True)
    curbtreepv=pvmtsp[np.isin(pvmtsp['pvid'],curbtreebuffer.loc[curbtreebuffer['ctid']==ct.loc[0,'ctid'],'pvid'])].reset_index(drop=True)
    if len(curbtreepv)>0:
        try:
            curbtreepv=curbtreepv.loc[[np.argmin([curbtreetp.loc[0,'geometry'].distance(x) for x in curbtreepv['geometry']])]].reset_index(drop=True)
            curbtreetp['pvid']=curbtreepv.loc[0,'pvid']
            curbtreetp['snapdist']=curbtreetp.loc[0,'geometry'].distance(curbtreepv.loc[0,'geometry'])
            adjgeom=shapely.ops.nearest_points(curbtreetp.loc[0,'geometry'],curbtreepv.loc[0,'geometry'])[1]
            intplt=curbtreepv.loc[0,'geometry'].project(adjgeom)
            splitter=shapely.geometry.MultiPoint([curbtreepv.loc[0,'geometry'].interpolate(x) for x in [intplt-2.5,intplt+2.5]])
            splitseg=shapely.ops.split(curbtreepv.loc[0,'geometry'],splitter.buffer(0.01))[2]
            curbtreetp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(6)]).convex_hull.wkt
            curbtreetp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-6)]).convex_hull.wkt
            return curbtreetp
        except:
            print(str(ct.loc[0,'ctid'])+' error!')
    else:
        print(str(ct.loc[0,'ctid'])+' no pvid joined!')

def curbtreeadjustcompile(ctcp):
    curbtreeadjtp=ctcp.groupby('ctid',as_index=False).apply(curbtreeadjust)
    return curbtreeadjtp

def parallelize(data,func):
    data_split=np.array_split(data,mp.cpu_count()-1)
    pool=mp.Pool(mp.cpu_count()-1)
    dt=pool.map(func,data_split)
    dt=pd.concat(dt,axis=0,ignore_index=True)
    pool.close()
    pool.join()
    return dt

if __name__=='__main__':
    curbtreeadj=parallelize(curbtree,curbtreeadjustcompile)




























# # Lot front without reg backup
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
# mappluto.crs='epsg:4326'
# mappluto=mappluto.to_crs('epsg:6539')
# mapplutosplit=mappluto.copy()
# mapplutosplit['Block']=[str(x)[0:6] for x in mapplutosplit['BBL']]
# mapplutosplit=mapplutosplit[['Block','geometry']].reset_index(drop=True)
# mapplutosplit=mapplutosplit.dissolve(by='Block',aggfunc='first').reset_index(drop=False)
# mapplutosplit['geometry']=[x.boundary for x in mapplutosplit['geometry']]

# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwd=dfcafeznelwd.to_crs('epsg:6539')
# dfcafeznelwd['Block']=[str(x)[0:6] for x in dfcafeznelwd['BBL']]
# dfcafeznelwd=dfcafeznelwd.loc[dfcafeznelwd['CAFETYPE']=='NONE',['Block','geometry']].reset_index(drop=True)

# k=pd.merge(mapplutosplit,dfcafeznelwd[['Block']],how='inner',on='Block')
# dfcafeznelwd.to_file(path+'SIDEWALK CAFE/k.shp')

# mapplutocafe=[]
# for i in k.index:
#     tps=k.loc[i,'geometry']
#     if type(tps)==shapely.geometry.linestring.LineString:
#         splitter=shapely.geometry.MultiPoint(tps.coords)
#         splitseg=gpd.GeoDataFrame(shapely.ops.split(tps,splitter))
#         splitseg['Block']=k.loc[i,'Block']
#         mapplutocafe+=[splitseg]
#     else:
#         for j in range(0,len(tps)):
#             splitter=shapely.geometry.MultiPoint(tps[j].coords)
#             splitseg=gpd.GeoDataFrame(shapely.ops.split(tps[j],splitter))
#             splitseg['Block']=k.loc[i,'Block']
#             mapplutocafe+=[splitseg]
# mapplutocafe=pd.concat(mapplutocafe,axis=0,ignore_index=True)
# mapplutocafe.columns=['geometry','Block']
# mapplutocafe=gpd.GeoDataFrame(mapplutocafe,geometry=mapplutocafe['geometry'],crs='epsg:6539')
# mapplutocafe.to_file(path+'SIDEWALK CAFE/mappluto_cafe.shp')


# mapplutocafe=gpd.sjoin(mapplutocafe,dfcafeznelwd,how='inner',op='intersects')
# mapplutocafe.to_file(path+'SIDEWALK CAFE/mappluto_cafe.shp')




# Viz backup
# import plotly.io as pio
# import plotly.express as px
# pio.renderers.default = "browser"
# px.box(k,x='SDWKWDTH',y='ORGSWMDN')


# import numpy as np
# a=list(range(1,11))
# b=[1-0.7**x for x in a]
# c=np.roll(b,-1)
# c-b
# import plotly.express as px
# import plotly.io as pio
# import plotly.graph_objects as go
# pio.renderers.default = "browser"
# px.scatter(x=a,y=b)
# fig=go.Figure()
# fig.add_trace(go.Scatter(x=a,y=b,mode='lines+markers'))

















