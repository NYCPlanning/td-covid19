import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport
import usaddress
import multiprocessing as mp



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/'
# path='/home/mayijun/'



# # Open Restaurant
# g = Geosupport()
# # df=pd.read_csv(path+'SIDEWALK CAFE/Open_Restaurant_Applications_20201014.csv',dtype=str)
# df=pd.read_csv(path+'SIDEWALK CAFE/Open_Restaurant_Applications_20210316.csv',dtype=str)
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
# df['XAP']=np.nan
# df['YAP']=np.nan
# df['BKFACE']=np.nan
# df=df[['ID','TIME','TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
#         'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
#         'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG','X','Y',
#         'XAP','YAP','BKFACE']].reset_index(drop=True)
# df=df.drop_duplicates(['TYPE','NAME','LEGAL','DBA','ADDRESS','BLDGNUM','STNAME','BORO','ZIP','PMTNUM',
#                         'SDWKLEN','SDWKWDTH','SDWKAREA','RDWYLEN','RDWYWDTH','RDWYAREA','APPSDWK','APPRDWY',
#                         'ALCOHOL','SLANUM','SLATYPE','LANDMARK','TERMLDMK','TERMHEALTH','BBL','LAT','LONG',
#                         'X','Y','XAP','YAP','BKFACE'],keep='first').reset_index(drop=True)
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
#                 print(str(df.loc[i,'ID'])+' not geocoded with 1B zipcode!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP zipcode!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
# len(df[pd.notna(df['BBL'])])
# # 10534/11838
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
#                 print(str(df.loc[i,'ID'])+' not geocoded with 1B borough!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP borough!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with borough!')
# len(df[pd.notna(df['BBL'])])
# # 10584/11838
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
#                print(str(df.loc[i,'ID'])+' not geocoded with 1B zipcode!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP zipcode!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with zipcode!')
# len(df[pd.notna(df['BBL'])])
# # 10640/11838
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
#                 print(str(df.loc[i,'ID'])+' not geocoded with 1B borough!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP borough!')
#         except:
#             print(str(df.loc[i,'ID'])+' not geocoded with borough!')
# len(df[pd.notna(df['BBL'])])
# # 10642/11838
# df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)&(pd.notna(df['LAT']))&(pd.notna(df['LONG']))&(pd.notna(df['BKFACE']))].reset_index(drop=True)
# # 10603/11838
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
# # 10594/11838
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
#                                                         'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
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
#                     np.where(dfcafeznelwd['IMPSWMDN']>=14,'11 ft ~ 12 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=13,'10 ft ~ 11 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=12,'9 ft ~ 10 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=11,'8 ft ~ 9 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                             '<5 ft'))))))))
# dfcafeznelwd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd.geojson',driver='GeoJSON')




# Join Open Restaurant to Lot Front Sidewalk Width
dfcafeznelwd=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd.shp')
dfcafeznelwd.crs='epsg:4326'
dfcafeznelwdbf=dfcafeznelwd.copy()
dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:6539')
dfcafeznelwdbf['geometry']=dfcafeznelwdbf.buffer(5)
dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:4326')
mapplutolftmswsp=gpd.read_file(path+'SIDEWALK CAFE/mapplutolftmswsp.geojson')
mapplutolftmswsp.crs='epsg:4326'
mapplutolftmswsp['LFIMPSWMDN']=mapplutolftmswsp['impswmdn'].copy()
mapplutolftmswsp=mapplutolftmswsp[['LFIMPSWMDN','geometry']].reset_index(drop=True)
dfcafeznelwdbf=gpd.sjoin(dfcafeznelwdbf,mapplutolftmswsp,how='left',op='intersects')
dfcafeznelwdbf=dfcafeznelwdbf[['ID','LFIMPSWMDN']].drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dfcafeznelwdlf=pd.merge(dfcafeznelwd,dfcafeznelwdbf,how='left',on='ID')
dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd_lf.shp')
dfcafeznelwdlf=dfcafeznelwdlf[np.isin(dfcafeznelwdlf['TYPE'],['BOTH','SIDEWALK'])].reset_index(drop=True)
dfcafeznelwdlf['SWCAT']=np.where(dfcafeznelwdlf['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
dfcafeznelwdlf['CP']=np.where(dfcafeznelwdlf['IMPSWMDN']>=15,'>=12 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=14,'11 ft ~ 12 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=13,'10 ft ~ 11 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=12,'9 ft ~ 10 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'8 ft ~ 9 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=10,'7 ft ~ 8 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=9,'6 ft ~ 7 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=8,'5 ft ~ 6 ft',
                              '<5 ft'))))))))
dfcafeznelwdlf['LFSWCAT']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
dfcafeznelwdlf['LFCP']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>=15,'>=12 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=14,'11 ft ~ 12 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=13,'10 ft ~ 11 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=12,'9 ft ~ 10 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'8 ft ~ 9 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=10,'7 ft ~ 8 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=9,'6 ft ~ 7 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=8,'5 ft ~ 6 ft',
                                '<5 ft'))))))))
dfcafeznelwdlf['LFCPCAT']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'Likely Eligible','Likely Ineligible')
dfcafeznelwdlf.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/or_cafe_zn_el_wd_lf.geojson',driver='GeoJSON')






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
#             np.where(df['CITY']=='BRONX','BRONX',
#             np.where(df['CITY']=='BROOKLYN','BROOKLYN','QUEENS')))
# df['ZIP']=[str(x).strip().upper() for x in df['ZIP']]
# df['BBL']=np.nan
# df['LAT']=np.nan
# df['LONG']=np.nan
# df['X']=np.nan
# df['Y']=np.nan
# df['XAP']=np.nan
# df['YAP']=np.nan
# df['BKFACE']=np.nan
# df=df[['ID','TYPE','STATUS','EXPDATE','NAME','DBA','BLDGNUM','STNAME','CITY','BORO','ZIP','BBL',
#        'LAT','LONG','X','Y','XAP','YAP','BKFACE']].reset_index(drop=True)
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
#                 print(str(df.loc[i,'ID'])+' not geocoded with 1B zipcode!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP zipcode!')
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
#                 print(str(df.loc[i,'ID'])+' not geocoded with 1B borough!')
#             addr=g['AP']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
#                 df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
#             else:
#                 print(str(df.loc[i,'ID'])+' not geocoded with AP borough!')           
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
#                                                         'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
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
#                     np.where(dfcafeznelwd['IMPSWMDN']>=14,'11 ft ~ 12 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=13,'10 ft ~ 11 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=12,'9 ft ~ 10 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=11,'8 ft ~ 9 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                     np.where(dfcafeznelwd['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                             '<5 ft'))))))))
# dfcafeznelwd.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_cafe_zn_el_wd.geojson',driver='GeoJSON')





# # Join DCA License to Lot Front Sidewalk Width
# dfcafeznelwd=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd.shp')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdbf=dfcafeznelwd.copy()
# dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:6539')
# dfcafeznelwdbf['geometry']=dfcafeznelwdbf.buffer(5)
# dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:4326')
# mapplutolftmswsp=gpd.read_file(path+'SIDEWALK CAFE/mapplutolftmswsp.geojson')
# mapplutolftmswsp.crs='epsg:4326'
# mapplutolftmswsp['LFIMPSWMDN']=mapplutolftmswsp['impswmdn'].copy()
# mapplutolftmswsp=mapplutolftmswsp[['LFIMPSWMDN','geometry']].reset_index(drop=True)
# dfcafeznelwdbf=gpd.sjoin(dfcafeznelwdbf,mapplutolftmswsp,how='left',op='intersects')
# dfcafeznelwdbf=dfcafeznelwdbf[['ID','LFIMPSWMDN']].drop_duplicates(['ID'],keep='first').reset_index(drop=True)
# dfcafeznelwdlf=pd.merge(dfcafeznelwd,dfcafeznelwdbf,how='left',on='ID')
# dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd_lf.shp')
# dfcafeznelwdlf=dfcafeznelwdlf[dfcafeznelwdlf['TYPE']!='ENCLOSED'].reset_index(drop=True)
# dfcafeznelwdlf['SWCAT']=np.where(dfcafeznelwdlf['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
# dfcafeznelwdlf['CP']=np.where(dfcafeznelwdlf['IMPSWMDN']>=15,'>=12 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=14,'11 ft ~ 12 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=13,'10 ft ~ 11 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=12,'9 ft ~ 10 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'8 ft ~ 9 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=10,'7 ft ~ 8 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=9,'6 ft ~ 7 ft',
#                       np.where(dfcafeznelwdlf['IMPSWMDN']>=8,'5 ft ~ 6 ft',
#                               '<5 ft'))))))))
# dfcafeznelwdlf['LFSWCAT']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
# dfcafeznelwdlf['LFCP']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>=15,'>=12 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=14,'11 ft ~ 12 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=13,'10 ft ~ 11 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=12,'9 ft ~ 10 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'8 ft ~ 9 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=10,'7 ft ~ 8 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=9,'6 ft ~ 7 ft',
#                         np.where(dfcafeznelwdlf['LFIMPSWMDN']>=8,'5 ft ~ 6 ft',
#                                 '<5 ft'))))))))
# dfcafeznelwdlf.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_cafe_zn_el_wd_lf.geojson',driver='GeoJSON')





# # DCA License
# dfcafeznelwd=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dca_cafe_zn_el_wd.geojson')
# dfcafeznelwd.crs='epsg:4326'
# dfcafeznelwdco=dfcafeznelwd.reset_index(drop=True)
# dfcafeznelwdco['CP']=np.where(dfcafeznelwdco['IMPSWMDN']>=15,'>=12 ft',
#                       np.where(dfcafeznelwdco['IMPSWMDN']>=11,'8 ft ~ 12 ft',
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
# mappluto=gpd.read_file(path+'SIDEWALK CAFE/mapplutoclipped.shp')
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
# mapplutolfctd=mapplutolf[['lfid','geometry']].reset_index(drop=True)
# mapplutolfctd['geometry']=mapplutolfctd.centroid.buffer(5)
# mapplutolfctd=gpd.sjoin(mapplutolfctd,mappluto,how='left',op='intersects')
# mapplutolfctd=mapplutolfctd[['lfid','bbl','geometry']].reset_index(drop=True)
# mapplutolfctd=mapplutolfctd.drop_duplicates('lfid',keep='first').reset_index(drop=True)
# mapplutolfctd['geometry']=[x.centroid for x in mapplutolfctd['geometry']]
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe=sdwkcafe.to_crs('epsg:6539')
# sdwkcafe['cafe']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
# sdwkcafe['geometry']=sdwkcafe.buffer(5)
# mapplutolfctd=gpd.sjoin(mapplutolfctd,sdwkcafe,how='left',op='intersects')
# mapplutolfctd=mapplutolfctd[['lfid','bbl','cafe']].reset_index(drop=True)
# mapplutolfctd=mapplutolfctd.drop_duplicates('lfid',keep='first').reset_index(drop=True)
# mapplutolf=pd.merge(mapplutolf,mapplutolfctd,how='left',on='lfid')
# mapplutolf=mapplutolf[['lfid','block','bbl','cafe','geometry']].reset_index(drop=True)
# mapplutolf=mapplutolf.drop_duplicates('lfid',keep='first').reset_index(drop=True)
# mapplutolf['cafe']=mapplutolf['cafe'].fillna('NONE')
# mapplutolf=mapplutolf.to_crs('epsg:4326')
# mapplutolf.to_file(path+'SIDEWALK CAFE/mapplutolf.shp')
# print(datetime.datetime.now()-start)
# # 60 mins




# # Join sidewalk width to centroid
# start=datetime.datetime.now()
# mapplutolf=gpd.read_file(path+'SIDEWALK CAFE/mapplutolf.shp')
# mapplutolf.crs='epsg:4326'
# mapplutolf=mapplutolf.to_crs('epsg:6539')
# mapplutolfctd=mapplutolf[['lfid','geometry']].reset_index(drop=True)
# mapplutolfctd['geometry']=mapplutolfctd.centroid
# mapplutolfctdbf=mapplutolfctd.copy()
# mapplutolfctdbf['geometry']=mapplutolfctdbf.buffer(50)
# sdwkwdimp=gpd.read_file(path+'sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp.to_crs('epsg:6539')
# mapplutolfctdbf=gpd.sjoin(mapplutolfctdbf,sdwkwdimp,how='inner',op='intersects')

# def ctdsw(ctd):
#     global mapplutolfctdbf
#     global sdwkwdimp
#     ctd=ctd[['lfid','geometry']].reset_index(drop=True)
#     lfctdbfpv=sdwkwdimp[np.isin(sdwkwdimp['pvid'],mapplutolfctdbf.loc[mapplutolfctdbf['lfid']==ctd.loc[0,'lfid'],'pvid'])].reset_index(drop=True)
#     if len(lfctdbfpv)>0:
#         try:
#             lfctdbfpv=lfctdbfpv.loc[[np.argmin([ctd.loc[0,'geometry'].distance(x) for x in lfctdbfpv['geometry']])]].reset_index(drop=True)
#             lfctdbfpv=lfctdbfpv.drop(['length','geometry'],axis=1).reset_index(drop=True)
#             ctd=ctd.drop('geometry',axis=1).reset_index(drop=True)
#             ctdswtp=pd.concat([ctd,lfctdbfpv],axis=1,ignore_index=False)
#             return ctdswtp
#         except:
#             print(str(ctd.loc[0,'lfid'])+' error!')
#     else:
#         print(str(ctd.loc[0,'lfid'])+' no pvid joined!')
    
# def ctdswcompile(ctdcp):
#     ctdswtp=ctdcp.groupby('lfid',as_index=False).apply(ctdsw)
#     return ctdswtp

# def parallelize(data,func):
#     data_split=np.array_split(data,mp.cpu_count()-1)
#     pool=mp.Pool(mp.cpu_count()-1)
#     dt=pool.map(func,data_split)
#     dt=pd.concat(dt,axis=0,ignore_index=True)
#     pool.close()
#     pool.join()
#     return dt

# if __name__=='__main__':
#     mapplutolfctdsw=parallelize(mapplutolfctd,ctdswcompile)
#     mapplutolfctdsw=mapplutolfctdsw.drop_duplicates('lfid',keep='first').reset_index(drop=True)
#     mapplutolfsw=pd.merge(mapplutolf,mapplutolfctdsw,how='left',on='lfid')
#     mapplutolfsw=mapplutolfsw[['lfid','block','bbl','cafe','pvid','impswmedia','geometry']].reset_index(drop=True)
#     mapplutolfsw=mapplutolfsw.to_crs('epsg:4326')
#     mapplutolfsw.to_file(path+'SIDEWALK CAFE/mapplutolfsw.shp')
#     print(datetime.datetime.now()-start)
#     # 120 mins




# # Join sidewalk tickmarks to lot front
# start=datetime.datetime.now()
# mapplutolf=gpd.read_file(path+'SIDEWALK CAFE/mapplutolf.shp')
# mapplutolf.crs='epsg:4326'
# mapplutolf=mapplutolf.to_crs('epsg:6539')
# mapplutolfbf=mapplutolf[['lfid','geometry']].reset_index(drop=True)
# mapplutolfbf['geometry']=mapplutolfbf.buffer(10)
# sdwktmimp=gpd.read_file(path+'sidewalk/output/sdwktmimp.shp')
# sdwktmimp.crs='epsg:4326'
# sdwktmimp=sdwktmimp.to_crs('epsg:6539')
# sdwktmimp=sdwktmimp[['impsw','geometry']].reset_index(drop=True)
# mapplutolftm=gpd.sjoin(mapplutolfbf,sdwktmimp,how='inner',op='intersects')
# mapplutolftm=mapplutolftm.groupby(['lfid'],as_index=False).agg({'impsw':'median'}).reset_index(drop=True)
# mapplutolftm=mapplutolftm.drop_duplicates('lfid',keep='first').reset_index(drop=True)
# mapplutolftmsw=pd.merge(mapplutolf,mapplutolftm,how='left',on='lfid')
# mapplutolftmsw=mapplutolftmsw[['lfid','block','bbl','cafe','impsw','geometry']].reset_index(drop=True)
# mapplutolftmsw.columns=['lfid','block','bbl','cafe','impswmdn','geometry']
# mapplutolftmsw=mapplutolftmsw.to_crs('epsg:4326')
# mapplutolftmsw.to_file(path+'SIDEWALK CAFE/mapplutolftmsw.shp')
# print(datetime.datetime.now()-start)
# # 60 mins

# # Clean lot front sidewalk tickmarks for upload
# mapplutolftmsw=gpd.read_file(path+'SIDEWALK CAFE/mapplutolftmsw.shp')
# mapplutolftmsw.crs='epsg:4326'
# mapplutolftmswsp=mapplutolftmsw[['cafe','impswmdn','geometry']].reset_index(drop=True)
# mapplutolftmswsp['impswmdn']=[round(x,2) for x in mapplutolftmswsp['impswmdn']]
# mapplutolftmswsp.to_file(path+'SIDEWALK CAFE/mapplutolftmswsp.geojson',driver='GeoJSON')





# Join sidewalk width to centroid backup
# start=datetime.datetime.now()
# mapplutolfctd=gpd.read_file(path+'SIDEWALK CAFE/mapplutolfctd.shp')
# mapplutolfctd.crs='epsg:4326'
# mapplutolfctd=mapplutolfctd.to_crs('epsg:6539')
# mapplutolfctdbf=mapplutolfctd.copy()
# mapplutolfctdbf['geometry']=mapplutolfctdbf.buffer(50)
# sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp.to_crs('epsg:6539')
# mapplutolfctdbf=gpd.sjoin(mapplutolfctdbf,sdwkwdimp,how='inner',op='intersects')
# mapplutolfctdsw=[]
# for i in mapplutolfctd['lfid']:
#     mapplutolfctdtp=mapplutolfctd.loc[mapplutolfctd['lfid']==i,['lfid','geometry']].reset_index(drop=True)
#     mapplutolfctdbfpv=sdwkwdimp[np.isin(sdwkwdimp['pvid'],mapplutolfctdbf.loc[mapplutolfctdbf['lfid']==i,'pvid'])].reset_index(drop=True)
#     if len(mapplutolfctdbfpv)>0:
#         try:
#             mapplutolfctdbfpv=mapplutolfctdbfpv.loc[[np.argmin([mapplutolfctdtp.loc[0,'geometry'].distance(x) for x in mapplutolfctdbfpv['geometry']])]].reset_index(drop=True)
#             mapplutolfctdbfpv=mapplutolfctdbfpv.drop(['length','geometry'],axis=1).reset_index(drop=True)
#             mapplutolfctdtp=pd.concat([mapplutolfctdtp,mapplutolfctdbfpv],axis=1,ignore_index=False)
#             mapplutolfctdsw+=[mapplutolfctdtp]
#         except:
#             print(str(i)+' error!')
#     else:
#         print(str(i)+' no pvid joined!')
#     print(str(i))
# mapplutolfctdsw=pd.concat(mapplutolfctdsw,ignore_index=True)
# mapplutolfctdsw=mapplutolfctdsw.drop('geometry',axis=1)
# mapplutolfsw=pd.merge(mapplutolf,mapplutolfctdsw,how='left',on='lfid')
# mapplutolfsw=mapplutolfsw[['lfid','block','bbl','cafe','pvid','bkfaceid','spid','side','orgswmin','orgswmax',
#                             'orgswmedia','impswmin','impswmax','impswmedia','geometry']].reset_index(drop=True)
# mapplutolfsw=mapplutolfsw.to_crs('epsg:4326')
# mapplutolfsw.to_file('C:/Users/mayij/Desktop/mapplutolfsw.geojson',driver='GeoJSON')









# # Join sidewalk cafe regulation to sidewalk width
# start=datetime.datetime.now()
# sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
# sdwkcafe.crs='epsg:4326'
# sdwkcafe=sdwkcafe.to_crs('epsg:6539')
# sdwkcafe['cfid']=range(0,len(sdwkcafe))
# sdwkcafe['cafe']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
# sdwkcafe=sdwkcafe[['cfid','cafe','geometry']].reset_index(drop=True)
# sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
# sdwkwdimp.crs='epsg:4326'
# sdwkwdimp=sdwkwdimp.to_crs('epsg:6539')
# sdwkwdimpctd=sdwkwdimp[['pvid','geometry']].reset_index(drop=True)
# sdwkwdimpctd['geometry']=sdwkwdimpctd.centroid
# sdwkwdimpctdbf=sdwkwdimpctd.copy()
# sdwkwdimpctdbf['geometry']=sdwkwdimpctd.buffer(50)
# sdwkwdimpctdbf=gpd.sjoin(sdwkwdimpctdbf,sdwkcafe,how='inner',op='intersects')
# sdwkwdimpcafe=[]
# for i in sdwkwdimp['pvid']:
#     sdwkwdimptp=sdwkwdimpctd.loc[sdwkwdimpctd['pvid']==i,['pvid','geometry']].reset_index(drop=True)
#     sdwkwdimpctdbfcf=sdwkcafe[np.isin(sdwkcafe['cfid'],sdwkwdimpctdbf.loc[sdwkwdimpctdbf['pvid']==i,'cfid'])].reset_index(drop=True)
#     if len(sdwkwdimpctdbfcf)>0:
#         try:
#             sdwkwdimpctdbfcf=sdwkwdimpctdbfcf.loc[[np.argmin([sdwkwdimptp.loc[0,'geometry'].distance(x) for x in sdwkwdimpctdbfcf['geometry']])]].reset_index(drop=True)
#             sdwkwdimpctdbfcf=sdwkwdimpctdbfcf.drop('geometry',axis=1).reset_index(drop=True)
#             sdwkwdimptp=sdwkwdimptp.drop('geometry',axis=1).reset_index(drop=True)
#             sdwkwdimptp=pd.concat([sdwkwdimptp,sdwkwdimpctdbfcf],axis=1,ignore_index=False)
#             sdwkwdimpcafe+=[sdwkwdimptp]
#         except:
#             print(str(i)+' error!')
#     else:
#         print(str(i)+' no pvid joined!')
#     print(str(i))
# sdwkwdimpcafe=pd.concat(sdwkwdimpcafe,ignore_index=True)
# sdwkwdimpcafe=sdwkwdimpcafe.drop_duplicates('cfid',keep='first').reset_index(drop=True)
# sdwkwdimpcafe=pd.merge(sdwkwdimp,sdwkwdimpcafe,how='left',on='pvid')
# sdwkwdimpcafe['cafe']=sdwkwdimpcafe['cafe'].fillna('NONE')
# sdwkwdimpcafe=sdwkwdimpcafe[['pvid','impswmedia','cafe','geometry']].reset_index(drop=True)
# sdwkwdimpcafe=sdwkwdimpcafe.to_crs('epsg:4326')
# sdwkwdimpcafe.to_file(path+'SIDEWALK CAFE/sdwkwdimpcafe.shp')
# print(datetime.datetime.now()-start)
# # 20 mins









# # Geocode corridors
# g=Geosupport()
# df=pd.read_excel(path+'SIDEWALK CAFE/CORRIDOR/CORRIDOR.xlsx',
#                  sheet_name=0,
#                  dtype=str)
# df=df.fillna('')

# dfgeocode=[]
# for i in df.index[0:120]:
#     borocode=str(df.loc[i,'BORO'])
#     onstreet=str(df.loc[i,'ON'])
#     fromstreet=str(df.loc[i,'FROM'])
#     tostreet=str(df.loc[i,'TO'])
#     stretch=g['3S']({'borough_code':borocode,
#                      'on':onstreet,
#                      'from':fromstreet,
#                      'to':tostreet,
#                      'compass_direction':'w',
#                      'compass_direction_1':'w',
#                      'compass_direction_2':'s'})
#     stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
#     stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
#     stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
#     stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
#     segment=pd.concat([df.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
#     segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
#     dfgeocode+=[segment]
# dfgeocode=pd.concat(dfgeocode,axis=0,ignore_index=True)
# dfgeocode=dfgeocode.drop_duplicates(keep='first').reset_index(drop=True)
# dfgeocoderev=dfgeocode.copy()
# dfgeocoderev['temp']=dfgeocoderev['segfromnode'].copy()
# dfgeocoderev['segfromnode']=dfgeocoderev['segtonode'].copy()
# dfgeocoderev['segtonode']=dfgeocoderev['temp'].copy()
# dfgeocoderev=dfgeocoderev.drop('temp',axis=1).reset_index(drop=True)
# dfgeocode=pd.concat([dfgeocode,dfgeocoderev],axis=0,ignore_index=True)

# lion=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
# lion.crs=4326
# lion['SEGMENTID']=pd.to_numeric(lion['SegmentID'],errors='coerce')
# lion['segfromnode']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
# lion['segtonode']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
# lion=lion[['SEGMENTID','segfromnode','segtonode','geometry']].reset_index(drop=True)
# lion=lion.drop_duplicates(['SEGMENTID','segfromnode','segtonode'],keep='first').reset_index(drop=True)

# crdgeocode=pd.merge(lion,dfgeocode,how='inner',on=['segfromnode','segtonode'])
# crdgeocode=crdgeocode.sort_values('SEGMENTID').reset_index(drop=True)
# crdgeocode=crdgeocode.drop_duplicates(['CORRIDOR','segfromnode','segtonode'],keep='first').reset_index(drop=True)
# crdgeocode=crdgeocode[['CORRIDOR','ON','FROM','TO','FT','BORO','FLAG','geometry']].reset_index(drop=True)
# crdgeocode.to_file(path+'SIDEWALK CAFE/CORRIDOR/crdgeocode.shp')



# # Geocode corridors add-ons
# g=Geosupport()
# df=pd.read_excel(path+'SIDEWALK CAFE/CORRIDOR/CORRIDOR.xlsx',
#                  sheet_name=1,
#                  dtype=str)
# df=df.fillna('')

# dfgeocode=[]
# for i in df.index[0:55]:
#     borocode=str(df.loc[i,'BORO'])
#     onstreet=str(df.loc[i,'ON'])
#     fromstreet=str(df.loc[i,'FROM'])
#     tostreet=str(df.loc[i,'TO'])
#     stretch=g['3S']({'borough_code':borocode,
#                      'on':onstreet,
#                      'from':fromstreet,
#                      'to':tostreet,
#                      'compass_direction':'w',
#                      'compass_direction_1':'w',
#                      'compass_direction_2':'s'})
#     stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
#     stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
#     stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
#     stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
#     segment=pd.concat([df.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
#     segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
#     dfgeocode+=[segment]
# dfgeocode=pd.concat(dfgeocode,axis=0,ignore_index=True)
# dfgeocode=dfgeocode.drop_duplicates(keep='first').reset_index(drop=True)
# dfgeocoderev=dfgeocode.copy()
# dfgeocoderev['temp']=dfgeocoderev['segfromnode'].copy()
# dfgeocoderev['segfromnode']=dfgeocoderev['segtonode'].copy()
# dfgeocoderev['segtonode']=dfgeocoderev['temp'].copy()
# dfgeocoderev=dfgeocoderev.drop('temp',axis=1).reset_index(drop=True)
# dfgeocode=pd.concat([dfgeocode,dfgeocoderev],axis=0,ignore_index=True)

# lion=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
# lion.crs=4326
# lion['SEGMENTID']=pd.to_numeric(lion['SegmentID'],errors='coerce')
# lion['segfromnode']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
# lion['segtonode']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
# lion=lion[['SEGMENTID','segfromnode','segtonode','geometry']].reset_index(drop=True)
# lion=lion.drop_duplicates(['SEGMENTID','segfromnode','segtonode'],keep='first').reset_index(drop=True)

# crdgeocode=pd.merge(lion,dfgeocode,how='inner',on=['segfromnode','segtonode'])
# crdgeocode=crdgeocode.sort_values('SEGMENTID').reset_index(drop=True)
# crdgeocode=crdgeocode.drop_duplicates(['CORRIDOR','segfromnode','segtonode'],keep='first').reset_index(drop=True)
# crdgeocode=crdgeocode[['CORRIDOR','ON','FROM','TO','FT','BORO','FLAG','geometry']].reset_index(drop=True)
# crdgeocode.to_file(path+'SIDEWALK CAFE/CORRIDOR/crdgeocodeadd.shp')










# Geocode corridors v2
g=Geosupport()
df=pd.read_excel(path+'SIDEWALK CAFE/CORRIDOR2/CORRIDOR2.xlsx',
                  sheet_name=0,
                  dtype=str)
df=df.fillna('')

dfgeocode=[]
for i in df.index[0:147]:
    borocode=str(df.loc[i,'BORO'])
    onstreet=str(df.loc[i,'ON'])
    fromstreet=str(df.loc[i,'FROM'])
    tostreet=str(df.loc[i,'TO'])
    stretch=g['3S']({'borough_code':borocode,
                      'on':onstreet,
                      'from':fromstreet,
                      'to':tostreet,
                      'compass_direction':'w',
                      'compass_direction_1':'w',
                      'compass_direction_2':'s'})
    stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
    stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
    stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
    stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
    segment=pd.concat([df.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
    segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
    dfgeocode+=[segment]
dfgeocode=pd.concat(dfgeocode,axis=0,ignore_index=True)
dfgeocode=dfgeocode.drop_duplicates(keep='first').reset_index(drop=True)
dfgeocoderev=dfgeocode.copy()
dfgeocoderev['temp']=dfgeocoderev['segfromnode'].copy()
dfgeocoderev['segfromnode']=dfgeocoderev['segtonode'].copy()
dfgeocoderev['segtonode']=dfgeocoderev['temp'].copy()
dfgeocoderev=dfgeocoderev.drop('temp',axis=1).reset_index(drop=True)
dfgeocode=pd.concat([dfgeocode,dfgeocoderev],axis=0,ignore_index=True)

lion=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
lion.crs=4326
lion['SEGMENTID']=pd.to_numeric(lion['SegmentID'],errors='coerce')
lion['segfromnode']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
lion['segtonode']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
lion=lion[['SEGMENTID','segfromnode','segtonode','geometry']].reset_index(drop=True)
lion=lion.drop_duplicates(['SEGMENTID','segfromnode','segtonode'],keep='first').reset_index(drop=True)

crdgeocode=pd.merge(lion,dfgeocode,how='inner',on=['segfromnode','segtonode'])
crdgeocode=crdgeocode.sort_values('SEGMENTID').reset_index(drop=True)
crdgeocode=crdgeocode.drop_duplicates(['CORRIDOR','segfromnode','segtonode'],keep='first').reset_index(drop=True)
crdgeocode=crdgeocode[['SEGMENTID','CORRIDOR','Previous Limitations','Previous Clear Path','Est. Sidewalk Width','ON','FROM','TO','FT','BORO','FLAG','geometry']].reset_index(drop=True)
crdgeocode.to_file(path+'SIDEWALK CAFE/CORRIDOR2/crdgeocode.shp')


# Rest corridors
restcr=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
restcr.crs=4326
restcr=restcr[np.isin(restcr['SegmentTyp'],['B','R','T','C','U','S'])].reset_index(drop=True)
restcr=restcr[np.isin(restcr['FeatureTyp'],['0','6','C','W','A'])].reset_index(drop=True)
restcr=restcr[np.isin(restcr['RB_Layer'],['R','B'])].reset_index(drop=True)
restcr=restcr[restcr['NonPed']!='V'].reset_index(drop=True)
restcr['SEGMENTID']=pd.to_numeric(restcr['SegmentID'])
crd=list(crdgeocode.SEGMENTID.unique())
union=[32786,32790,32797,32799,32806,32808,32809,32812,32941,32945,32956,32957,110559,133903,138570,139521,139522,139523,139524,139525,139526,139527,145393,145394,164328,164381,164382,164383,164384,250870,250951,251073,283287]
queens=[67235,67247,67248,67252,172580,172581,172582,172585,172586,189355,189356,9009910,9009911]
broadway=[299595,257485,257484,254134,254133,216991,114275,110862,23370,23364,23232,23230,23228,23204,23199,23197,23195,23094,23065,23060]
restcr=restcr[~np.isin(restcr['SEGMENTID'],crd)].reset_index(drop=True)
restcr=restcr[~np.isin(restcr['SEGMENTID'],union)].reset_index(drop=True)
restcr=restcr[~np.isin(restcr['SEGMENTID'],queens)].reset_index(drop=True)
restcr=restcr[~np.isin(restcr['SEGMENTID'],broadway)].reset_index(drop=True)
restcr=restcr[['geometry']].reset_index(drop=True)
restcr['FT']='8'
restcr.to_file(path+'SIDEWALK CAFE/CORRIDOR2/restcr.shp')









# DCA vs DCP corridor
dca=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd_lf.shp')
dca.crs=4326
dca=dca.to_crs(6539)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['X'],dca['Y'])]
dca['geometry']=dca.buffer(10)
dcpcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR/DCP_Greater_than_8__Corridors_merged.shp')
dcpcrd.crs=4326
dcpcrd=dcpcrd.to_crs(6539)
dca=gpd.sjoin(dca,dcpcrd,how='left',op='intersects')
dca=dca.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['XADJ'],dca['YADJ'])]
dca=dca.to_crs(4326)
dca=dca.drop(['index_right','boro','flag','on','from','to'],axis=1).reset_index(drop=True)
dca['ALLOWED']=np.where(pd.isna(dca['ft']),'YES',
               np.where((dca['ft']=='15')&(dca['LFIMPSWMDN']>=18),'YES',
               np.where((dca['ft']=='12')&(dca['LFIMPSWMDN']>=15),'YES','NO')))
dca.to_file(path+'SIDEWALK CAFE/dca_dcp.shp')

# DCA vs DCP corridor v2
dca=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd_lf.shp')
dca.crs=4326
dca=dca.to_crs(6539)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['X'],dca['Y'])]
dca['geometry']=dca.buffer(10)
dcpcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR2/CORRIDOR2.shp')
dcpcrd.crs=4326
dcpcrd=dcpcrd.drop(['BORO'],axis=1).reset_index(drop=True)
dcpcrd=dcpcrd.to_crs(6539)
dca=gpd.sjoin(dca,dcpcrd,how='left',op='intersects')
dca=dca.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['XADJ'],dca['YADJ'])]
dca=dca.to_crs(4326)
dca=dca.drop(['index_right','FLAG','ON','FROM','TO'],axis=1).reset_index(drop=True)
dca['ALLOWED']=np.where(pd.isna(dca['FT']),'YES',
               np.where((dca['FT']=='11')&(dca['LFIMPSWMDN']>=14),'YES','NO'))
dca.to_file(path+'SIDEWALK CAFE/dca_dcp2.shp')



# Open Restaurant vs DCP corridor
dfcafeznelwdlf=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd_lf.shp')
dfcafeznelwdlf.crs=4326
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(6539)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['X'],dfcafeznelwdlf['Y'])]
dfcafeznelwdlf['geometry']=dfcafeznelwdlf.buffer(10)
dcpcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR/DCP_Greater_than_8__Corridors_merged.shp')
dcpcrd.crs=4326
dcpcrd=dcpcrd.to_crs(6539)
dfcafeznelwdlf=gpd.sjoin(dfcafeznelwdlf,dcpcrd,how='left',op='intersects')
dfcafeznelwdlf=dfcafeznelwdlf.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['XADJ'],dfcafeznelwdlf['YADJ'])]
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(4326)
dfcafeznelwdlf=dfcafeznelwdlf.drop(['index_right','boro','flag','on','from','to'],axis=1).reset_index(drop=True)
dfcafeznelwdlf['ALLOWED']=np.where(pd.isna(dfcafeznelwdlf['ft']),'YES',
                          np.where((dfcafeznelwdlf['ft']=='15')&(dfcafeznelwdlf['LFIMPSWMDN']>=18),'YES',
                          np.where((dfcafeznelwdlf['ft']=='12')&(dfcafeznelwdlf['LFIMPSWMDN']>=15),'YES','NO')))
dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/or_dcp.shp')

# Open Restaurant vs DCP corridor v2
dfcafeznelwdlf=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd_lf.shp')
dfcafeznelwdlf.crs=4326
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(6539)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['X'],dfcafeznelwdlf['Y'])]
dfcafeznelwdlf['geometry']=dfcafeznelwdlf.buffer(10)
dcpcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR2/CORRIDOR2.shp')
dcpcrd.crs=4326
dcpcrd=dcpcrd.drop(['BORO'],axis=1).reset_index(drop=True)
dcpcrd=dcpcrd.to_crs(6539)
dfcafeznelwdlf=gpd.sjoin(dfcafeznelwdlf,dcpcrd,how='left',op='intersects')
dfcafeznelwdlf=dfcafeznelwdlf.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['XADJ'],dfcafeznelwdlf['YADJ'])]
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(4326)
dfcafeznelwdlf=dfcafeznelwdlf.drop(['index_right','FLAG','ON','FROM','TO'],axis=1).reset_index(drop=True)
dfcafeznelwdlf['ALLOWED']=np.where(pd.isna(dfcafeznelwdlf['FT'])&(dfcafeznelwdlf['LFIMPSWMDN']>=11),'YES',
                          np.where((dfcafeznelwdlf['FT']=='11')&(dfcafeznelwdlf['LFIMPSWMDN']>=14),'YES','NO'))
dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/or_dcp2.shp')



# DCA vs DOT corridor
dca=gpd.read_file(path+'SIDEWALK CAFE/dca_cafe_zn_el_wd_lf.shp')
dca.crs=4326
dca=dca.to_crs(6539)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['X'],dca['Y'])]
dca['geometry']=dca.buffer(10)
dotcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR/DOT/DOT CORRIDORS.shp')
dotcrd.crs=4326
dotcrd=dotcrd.to_crs(6539)
dca=gpd.sjoin(dca,dotcrd,how='left',op='intersects')
dca=dca.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dca['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dca['XADJ'],dca['YADJ'])]
dca=dca.to_crs(4326)
dca=dca.drop(['index_right','cartodb_id'],axis=1).reset_index(drop=True)
dca['ALLOWED']=np.where(pd.isna(dca['walklane']),'YES',
               np.where(dca['walklane']==8,'YES',
               np.where((dca['walklane']==15)&(dca['LFIMPSWMDN']>=18),'YES',
               np.where((dca['walklane']==12)&(dca['LFIMPSWMDN']>=15),'YES','NO'))))
dca.to_file(path+'SIDEWALK CAFE/dca_dot.shp')



# Open Restaurant vs DOT corridor
dfcafeznelwdlf=gpd.read_file(path+'SIDEWALK CAFE/or_cafe_zn_el_wd_lf.shp')
dfcafeznelwdlf.crs=4326
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(6539)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['X'],dfcafeznelwdlf['Y'])]
dfcafeznelwdlf['geometry']=dfcafeznelwdlf.buffer(10)
dotcrd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR/DOT/DOT CORRIDORS.shp')
dotcrd.crs=4326
dotcrd=dotcrd.to_crs(6539)
dfcafeznelwdlf=gpd.sjoin(dfcafeznelwdlf,dotcrd,how='left',op='intersects')
dfcafeznelwdlf=dfcafeznelwdlf.drop_duplicates(['ID'],keep='first').reset_index(drop=True)
dfcafeznelwdlf['geometry']=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznelwdlf['XADJ'],dfcafeznelwdlf['YADJ'])]
dfcafeznelwdlf=dfcafeznelwdlf.to_crs(4326)
dfcafeznelwdlf=dfcafeznelwdlf.drop(['index_right','cartodb_id'],axis=1).reset_index(drop=True)
dfcafeznelwdlf['ALLOWED']=np.where(pd.isna(dfcafeznelwdlf['walklane']),'YES',
                          np.where(dfcafeznelwdlf['walklane']==8,'YES',
                          np.where((dfcafeznelwdlf['walklane']==15)&(dfcafeznelwdlf['LFIMPSWMDN']>=18),'YES',
                          np.where((dfcafeznelwdlf['walklane']==12)&(dfcafeznelwdlf['LFIMPSWMDN']>=15),'YES','NO'))))
dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/or_dot.shp')








# DOHMH
dohmh=pd.read_csv(path+'SIDEWALK CAFE/lettergraderestaurants.csv',dtype=str)
dohmh=dohmh.drop(['cartodb_id','the_geom','the_geom_str'],axis=1).reset_index(drop=True)
dohmh['BBL']=pd.to_numeric(dohmh['bbl'])
mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
mappluto.crs='epsg:4326'
dohmh=pd.merge(dohmh,mappluto,how='inner',on='BBL')
dohmh=dohmh.drop(['geometry'],axis=1).reset_index(drop=True)
#26491/26826
for i in dohmh.index:
    dohmh.loc[i,'ZD']='; '.join(sorted([x for x in list(dohmh.loc[i,['ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4']]) if pd.notna(x)]))
    dohmh.loc[i,'OL']='; '.join(sorted([x for x in list(dohmh.loc[i,['Overlay1','Overlay2']]) if pd.notna(x)]))
    dohmh.loc[i,'SP']='; '.join(sorted([x for x in list(dohmh.loc[i,['SPDist1','SPDist2','SPDist3']]) if pd.notna(x)]))
dohmh.to_csv(path+'SIDEWALK CAFE/dohmh.csv',index=False)


# DOHMH
g = Geosupport()
df=pd.read_csv(path+'SIDEWALK CAFE/lettergraderestaurants.csv',dtype=str)
df=df.drop(['cartodb_id','the_geom','the_geom_str'],axis=1).reset_index(drop=True)
df['CAMIS']=[str(x).strip().upper() for x in df['camis']]
df['DBA']=[str(x).strip().upper() for x in df['dba']]
df['BORO']=[str(x).strip().upper() for x in df['boro']]
df['BLDGNUM']=[str(x).strip().upper() for x in df['building']]
df['STNAME']=[str(x).strip().upper() for x in df['street']]
df['ZIP']=[str(x).strip().upper() for x in df['zipcode']]
df['TEL']=[str(x).strip().upper() for x in df['phone']]
df['CUISINE']=[str(x).strip().upper() for x in df['cuisine_de']]
df['ORGLAT']=pd.to_numeric(df['latitude'])
df['ORGLONG']=pd.to_numeric(df['longitude'])
df['ORGBBL']=pd.to_numeric(df['bbl'])
df['NTA']=[str(x).strip().upper() for x in df['nta']]
df['NTANAME']=[str(x).strip().upper() for x in df['ntaname']]
df['LOTFRONT']=pd.to_numeric(df['lotfront'])
df['BLDGFRONT']=pd.to_numeric(df['bldgfront'])
df['BBL']=np.nan
df['LAT']=np.nan
df['LONG']=np.nan
df['X']=np.nan
df['Y']=np.nan
df['XAP']=np.nan
df['YAP']=np.nan
df['BKFACE']=np.nan
df=df[['CAMIS','DBA','BORO','BLDGNUM','STNAME','ZIP','TEL','CUISINE','ORGLAT','ORGLONG','ORGBBL',
       'NTA','NTANAME','LOTFRONT','BLDGFRONT','BBL','LAT','LONG','X','Y','XAP','YAP','BKFACE']].reset_index(drop=True)
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
                print(str(df.loc[i,'CAMIS'])+' not geocoded with 1B zipcode!')
            addr=g['AP']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
                df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
            else:
                print(str(df.loc[i,'CAMIS'])+' not geocoded with AP zipcode!')
        except:
            print(str(df.loc[i,'CAMIS'])+' not geocoded with zipcode!')
len(df[pd.notna(df['BBL'])])
# 26452/26826
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
                print(str(df.loc[i,'CAMIS'])+' not geocoded with 1B borough!')
            addr=g['AP']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'XAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][0:7])
                df.loc[i,'YAP']=pd.to_numeric(addr['X-Y Coordinates of Address Point'][7:14])
            else:
                print(str(df.loc[i,'CAMIS'])+' not geocoded with AP borough!')           
        except:
            print(str(df.loc[i,'CAMIS'])+' not geocoded with borough!')
len(df[pd.notna(df['BBL'])])
# 26453/26826
df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)&(pd.notna(df['LAT']))&(pd.notna(df['LONG']))&(pd.notna(df['BKFACE']))].reset_index(drop=True)
# 26264/26826
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['LONG'],df['LAT'])],crs='epsg:4326')
df.to_file(path+'SIDEWALK CAFE/dohmh.shp')
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['X'],df['Y'])],crs='epsg:6539')
df=df.to_crs('epsg:4326')
df.to_file(path+'SIDEWALK CAFE/dohmh_xy.shp')



# Adjust DOHMH to MapPluto Lot Line
df=gpd.read_file(path+'SIDEWALK CAFE/dohmh_xy.shp')
df.crs='epsg:4326'
df=df.to_crs('epsg:6539')
mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
mappluto.crs='epsg:4326'
mappluto=mappluto.to_crs('epsg:6539')
mappluto['geometry']=[x.boundary for x in mappluto['geometry']]
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
# 26235/26826
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['XADJ'],df['YADJ'])],crs='epsg:6539')
df=df.to_crs('epsg:4326')
df.to_file(path+'SIDEWALK CAFE/dohmh_xyadj.shp')



# Join DOHMH to Sidewalk Cafe Reg
sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
sdwkcafe.crs='epsg:4326'
sdwkcafe=sdwkcafe.to_crs('epsg:6539')
sdwkcafe['geometry']=[x.buffer(5) for x in sdwkcafe['geometry']]
sdwkcafe=sdwkcafe.to_crs('epsg:4326')
sdwkcafe['CAFETYPE']=[str(x).strip().upper() for x in sdwkcafe['CafeType']]
sdwkcafe=sdwkcafe[['CAFETYPE','geometry']].reset_index(drop=True)
df=gpd.read_file(path+'SIDEWALK CAFE/dohmh_xyadj.shp')
df.crs='epsg:4326'
dfcafe=gpd.sjoin(df,sdwkcafe,how='left',op='intersects')
dfcafe=dfcafe[['CAMIS','CAFETYPE']].drop_duplicates(['CAMIS'],keep='first').reset_index(drop=True)
dfcafe['CAFETYPE']=np.where(pd.notna(dfcafe['CAFETYPE']),dfcafe['CAFETYPE'],'NONE')
dfcafe=pd.merge(df,dfcafe,how='inner',on='CAMIS')
dfcafe.to_file(path+'SIDEWALK CAFE/dohmh_cafe.shp')



# Join DOHMH to Zoning
mappluto=gpd.read_file(path+'SIDEWALK CAFE/mappluto.shp')
mappluto.crs='epsg:4326'
dfcafe=gpd.read_file(path+'SIDEWALK CAFE/dohmh_cafe.shp')
dfcafe.crs='epsg:4326'
dfcafezn=pd.merge(dfcafe,mappluto,how='left',on='BBL')
dfcafezn=dfcafezn.loc[pd.notna(dfcafezn['ZoneDist1']),['CAMIS','ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4',
                                                        'Overlay1','Overlay2','SPDist1','SPDist2','SPDist3']].reset_index(drop=True)
dfcafezn['ZD']=''
dfcafezn['RM']=np.nan
dfcafezn['OL']=''
dfcafezn['SP']=''
for i in dfcafezn.index:
    dfcafezn.loc[i,'ZD']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['ZoneDist1','ZoneDist2','ZoneDist3','ZoneDist4']]) if pd.notna(x)]))
    dfcafezn.loc[i,'RM']=np.where((dfcafezn.loc[i,'ZD'].startswith('C')==False)&(dfcafezn.loc[i,'ZD']!='BPC')&(dfcafezn.loc[i,'ZD']!='PARK'),1,0)
    dfcafezn.loc[i,'OL']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['Overlay1','Overlay2']]) if pd.notna(x)]))
    dfcafezn.loc[i,'SP']='; '.join(sorted([x for x in list(dfcafezn.loc[i,['SPDist1','SPDist2','SPDist3']]) if pd.notna(x)]))
dfcafezn=dfcafezn[['CAMIS','ZD','RM','OL','SP']].reset_index(drop=True)
dfcafezn=pd.merge(dfcafe,dfcafezn,how='left',on='CAMIS')
dfcafezn.to_file(path+'SIDEWALK CAFE/dohmh_cafe_zn.shp')



# Join DOHMH to Elevated Rail
el=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/planimetrics/transpstruct.shp')
el.crs='epsg:4326'
el['EL']='YES'
el=el.loc[np.isin(el['FEATURE_CO'],[2320,2340]),['EL','geometry']].reset_index(drop=True)
dfcafezn=gpd.read_file(path+'SIDEWALK CAFE/dohmh_cafe_zn.shp')
dfcafezn.crs='epsg:4326'
dfcafeznel=dfcafezn.copy()
dfcafeznel=gpd.GeoDataFrame(dfcafeznel,geometry=[shapely.geometry.Point(x,y) for x,y in zip(dfcafeznel['X'],dfcafeznel['Y'])],crs='epsg:6539')
dfcafeznel=dfcafeznel.to_crs('epsg:4326')
dfcafeznel=gpd.sjoin(dfcafeznel,el,how='left',op='intersects')
dfcafeznel['EL']=np.where(pd.notna(dfcafeznel['EL']),dfcafeznel['EL'],'NO')
dfcafeznel=dfcafeznel[['CAMIS','EL']].drop_duplicates(keep='first').reset_index(drop=True)
dfcafeznel=pd.merge(dfcafezn,dfcafeznel,how='left',on='CAMIS')
dfcafeznel.to_file(path+'SIDEWALK CAFE/dohmh_cafe_zn_el.shp')



# Join DOHMH to Sidewalk Width
dfcafeznel=gpd.read_file(path+'SIDEWALK CAFE/dohmh_cafe_zn_el.shp')
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
dfcafeznelwd.to_file(path+'SIDEWALK CAFE/dohmh_cafe_zn_el_wd.shp')




# Join DOHMH to Lot Front Sidewalk Width
dfcafeznelwd=gpd.read_file(path+'SIDEWALK CAFE/dohmh_cafe_zn_el_wd.shp')
dfcafeznelwd.crs='epsg:4326'
dfcafeznelwdbf=dfcafeznelwd.copy()
dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:6539')
dfcafeznelwdbf['geometry']=dfcafeznelwdbf.buffer(5)
dfcafeznelwdbf=dfcafeznelwdbf.to_crs('epsg:4326')
mapplutolftmswsp=gpd.read_file(path+'SIDEWALK CAFE/mapplutolftmswsp.geojson')
mapplutolftmswsp.crs='epsg:4326'
mapplutolftmswsp['LFIMPSWMDN']=mapplutolftmswsp['impswmdn'].copy()
mapplutolftmswsp=mapplutolftmswsp[['LFIMPSWMDN','geometry']].reset_index(drop=True)
dfcafeznelwdbf=gpd.sjoin(dfcafeznelwdbf,mapplutolftmswsp,how='left',op='intersects')
dfcafeznelwdbf=dfcafeznelwdbf[['CAMIS','LFIMPSWMDN']].drop_duplicates(['CAMIS'],keep='first').reset_index(drop=True)
dfcafeznelwdlf=pd.merge(dfcafeznelwd,dfcafeznelwdbf,how='left',on='CAMIS')
dfcafeznelwdlf.to_file(path+'SIDEWALK CAFE/dohmh_cafe_zn_el_wd_lf.shp')
dfcafeznelwdlf['SWCAT']=np.where(dfcafeznelwdlf['IMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
dfcafeznelwdlf['CP']=np.where(dfcafeznelwdlf['IMPSWMDN']>=15,'>=12 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=14,'11 ft ~ 12 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=13,'10 ft ~ 11 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=12,'9 ft ~ 10 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=11,'8 ft ~ 9 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=10,'7 ft ~ 8 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=9,'6 ft ~ 7 ft',
                      np.where(dfcafeznelwdlf['IMPSWMDN']>=8,'5 ft ~ 6 ft',
                              '<5 ft'))))))))
dfcafeznelwdlf['LFSWCAT']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>14,'>14 ft',np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'11 ft ~ 14 ft','<11 ft'))
dfcafeznelwdlf['LFCP']=np.where(dfcafeznelwdlf['LFIMPSWMDN']>=15,'>=12 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=14,'11 ft ~ 12 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=13,'10 ft ~ 11 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=12,'9 ft ~ 10 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=11,'8 ft ~ 9 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=10,'7 ft ~ 8 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=9,'6 ft ~ 7 ft',
                        np.where(dfcafeznelwdlf['LFIMPSWMDN']>=8,'5 ft ~ 6 ft',
                                '<5 ft'))))))))
dfcafeznelwdlf.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/sidewalkcafe/dohmh_cafe_zn_el_wd_lf.geojson',driver='GeoJSON')













# Blockface
crd=gpd.read_file(path+'SIDEWALK CAFE/CORRIDOR2/crdgeocode.shp')
crd.crs=4326
crd=list(crd.SEGMENTID.unique())
union=[32786,32790,32797,32799,32806,32808,32809,32812,32941,32945,32956,32957,110559,133903,138570,139521,139522,139523,139524,139525,139526,139527,145393,145394,164328,164381,164382,164383,164384,250870,250951,251073,283287]
queens=[67235,67247,67248,67252,172580,172581,172582,172585,172586,189355,189356,9009910,9009911]
broadway=[299595,257485,257484,254134,254133,216991,114275,110862,23370,23364,23232,23230,23228,23204,23199,23197,23195,23094,23065,23060]
add=[148189,148190,148191,148192,144897,144898,281709,281723,281708,281730,281731,281699,192209,192210,
     192208,192212,262020,262483,262482,262019,261850,261860,261872,261876,261871,261875,148807,148700,
     148660,257576,257575,257569,257588,257587,257585,158915,158827,158577,158629,159076,159091,262006,
     262475,155013,155159,159036,158678,158641,159181,158642,158987,158715,159104,284713,284714,284712,
     284723,284725,284726,158589,158933,159267,159103,159286,158730,158731,158761,159285,159063,138148,
     158564,158900,158573,159216,139555,158858,158808,158905,158813,158942,158620,159032,158621,158895,
     151913,158896,158893,158855,158669,158822,158596,159009,158597]
lion=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
lion.crs=4326
lion=lion[np.isin(lion['SegmentTyp'],['B','R','T','C','U','S'])].reset_index(drop=True)
lion=lion[np.isin(lion['FeatureTyp'],['0','6','C','W','A'])].reset_index(drop=True)
lion=lion[np.isin(lion['RB_Layer'],['R','B'])].reset_index(drop=True)
lion=lion[lion['NonPed']!='V'].reset_index(drop=True)
lion['SEGMENTID']=pd.to_numeric(lion['SegmentID'],errors='coerce')
lion['LBKFACE']=pd.to_numeric(lion['LBlockFace'],errors='coerce')
lion['RBKFACE']=pd.to_numeric(lion['RBlockFace'],errors='coerce')
lion['FT']=np.where(np.isin(lion['SEGMENTID'],crd),11,
           np.where(np.isin(lion['SEGMENTID'],union),11,
           np.where(np.isin(lion['SEGMENTID'],queens),11,
           np.where(np.isin(lion['SEGMENTID'],broadway),11,
           np.where(np.isin(lion['SEGMENTID'],add),11,8)))))
lion=lion[['SEGMENTID','LBKFACE','RBKFACE','FT','geometry']].reset_index(drop=True)
lionl=lion[['LBKFACE','FT']].reset_index(drop=True)
lionl.columns=['BKFACE','FT']
lionr=lion[['RBKFACE','FT']].reset_index(drop=True)
lionr.columns=['BKFACE','FT']
lion=pd.concat([lionl,lionr],axis=0,ignore_index=True)
lion=lion.sort_values(['BKFACE','FT'],ascending=False).reset_index(drop=True)
lion=lion.drop_duplicates(['BKFACE'],keep='first').reset_index(drop=True)
lion.columns=['bkfaceid','ft']
sdwkwdimp=gpd.read_file(path+'STREET CLOSURE/sidewalk/output/sdwkwdimp.shp')
sdwkwdimp.crs='epsg:4326'
bkface=pd.merge(sdwkwdimp,lion,how='inner',on='bkfaceid')
bkface['allowed']=np.where(bkface['impswmedia']>bkface['ft']+3,'YES','NO')
bkface=bkface[['impswmedia','ft','allowed','geometry']].reset_index(drop=True)
bkface.to_file(path+'SIDEWALK CAFE/bkface.shp')





# Small cafe
sdwkcafe=gpd.read_file(path+'SIDEWALK CAFE/sidewalk_cafe.shp')
sdwkcafe.crs=4326
sdwkcafe=sdwkcafe.to_crs(6539)
sdwkcafe=sdwkcafe[sdwkcafe['CafeType']=='Small Only'].reset_index(drop=True)
smallcafe=[]
for i in sdwkcafe.index:
    tps=sdwkcafe.loc[i,'geometry']
    splitter=shapely.geometry.MultiPoint(tps.coords)
    splitseg=gpd.GeoDataFrame(shapely.ops.split(tps,splitter))
    smallcafe+=[splitseg]
smallcafe=pd.concat(smallcafe,axis=0,ignore_index=True)
smallcafe['scid']=range(0,len(smallcafe))
smallcafe['cafetype']='Small Only'
smallcafe.columns=['geometry','scid','cafetype']
smallcafe=gpd.GeoDataFrame(smallcafe,geometry=smallcafe['geometry'],crs=6539)
smallcafectd=smallcafe.copy()
smallcafectd['geometry']=smallcafectd.centroid
smallcafectdbf=smallcafectd.copy()
smallcafectdbf['geometry']=smallcafectdbf.buffer(100)
bkface=gpd.read_file(path+'SIDEWALK CAFE/bkface.shp')
bkface.crs=4326
bkface=bkface.to_crs(6539)
bkface['bkid']=range(0,len(bkface))
smallcafectdbf=gpd.sjoin(smallcafectdbf,bkface,how='inner',op='intersects')
smallcafebk=[]
for i in smallcafe['scid']:
    smallcafetp=smallcafectd.loc[smallcafectd['scid']==i,['scid','geometry']].reset_index(drop=True)
    bkfacesmallcafe=bkface[np.isin(bkface['bkid'],smallcafectdbf.loc[smallcafectdbf['scid']==i,'bkid'])].reset_index(drop=True)
    if len(bkfacesmallcafe)>0:
        try:
            bkfacesmallcafe=bkfacesmallcafe.loc[[np.argmin([smallcafetp.loc[0,'geometry'].distance(x) for x in bkfacesmallcafe['geometry']])]].reset_index(drop=True)
            bkfacesmallcafe=bkfacesmallcafe.drop(['bkid','geometry'],axis=1).reset_index(drop=True)
            smallcafetp=smallcafetp.drop(['geometry'],axis=1).reset_index(drop=True)
            smallcafetp=pd.concat([smallcafetp,bkfacesmallcafe],axis=1,ignore_index=False)
            smallcafebk+=[smallcafetp]
        except:
            print(str(i)+' error!')
    else:
        print(str(i)+' no bkface joined!')
    print(str(i))
smallcafebk=pd.concat(smallcafebk,ignore_index=True)
smallcafebk=smallcafebk.drop_duplicates('scid',keep='first').reset_index(drop=True)
smallcafebk=pd.merge(smallcafe,smallcafebk,how='inner',on='scid')
smallcafebk=smallcafebk.to_crs(4326)
smallcafebk.to_file(path+'SIDEWALK CAFE/smallcafebk.shp')























# Complaints
df=pd.read_excel(path+'SIDEWALK CAFE/COMPLAINTS/Copy of Outdoor Music 04-01-20 to 02-05-21.xlsx',sheet_name='Sheet1',dtype=str)
df['BBL']=np.nan
df['LAT']=np.nan
df['LONG']=np.nan
df['X']=np.nan
df['Y']=np.nan

g=Geosupport()
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'SR Address']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(df.loc[i,'SR Address']) if re.search('StreetName',x[1])])
            zipcode=df.loc[i,'Zip (SR Address) (NYC Address)']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                df.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                df.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                df.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                df.loc[i,'X']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][0:7])
                df.loc[i,'Y']=pd.to_numeric(addr['Spatial X-Y Coordinates of Address'][7:14])
            elif addr['SPATIAL COORDINATES OF ACTUAL SEGMENT']!='':
                df.loc[i,'BBL']=0
                df.loc[i,'LAT']=0
                df.loc[i,'LONG']=0
                df.loc[i,'X']=(pd.to_numeric(addr['SPATIAL COORDINATES OF ACTUAL SEGMENT']['X Coordinate, Low Address End'])+
                              pd.to_numeric(addr['SPATIAL COORDINATES OF ACTUAL SEGMENT']['X Coordinate, High Address End']))/2
                df.loc[i,'Y']=(pd.to_numeric(addr['SPATIAL COORDINATES OF ACTUAL SEGMENT']['Y Coordinate, Low Address End'])+
                              pd.to_numeric(addr['SPATIAL COORDINATES OF ACTUAL SEGMENT']['Y Coordinate, High Address End']))/2
            else:
                print(str(df.loc[i,'SR Number'])+' not geocoded with 1B zipcode!')
        except:
            print(str(df.loc[i,'SR Number'])+' error!')
len(df[pd.notna(df['BBL'])])
# 907/941

for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            streetname1=df.loc[i,'SR Address'].split(' AND ')[0].strip().upper()
            streetname2=df.loc[i,'SR Address'].split(' AND ')[1].split(',')[0].strip().upper()
            boroughcode=np.where(df.loc[i,'City (SR Address) (NYC Address)']=='NEW YORK',1,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='BRONX',2,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='BROOKLYN',3,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='STATEN ISLAND',5,4)))).tolist()
            addr=g['2']({'borough_code_1':boroughcode,'street_name_1':streetname1,
                         'borough_code_2':boroughcode,'street_name_2':streetname2})
            if addr['SPATIAL COORDINATES']!='':
                df.loc[i,'BBL']=0
                df.loc[i,'LAT']=0
                df.loc[i,'LONG']=0
                df.loc[i,'X']=pd.to_numeric(addr['SPATIAL COORDINATES']['X Coordinate'])
                df.loc[i,'Y']=pd.to_numeric(addr['SPATIAL COORDINATES']['Y Coordinate'])
            else:
                print(str(df.loc[i,'SR Number'])+' not geocoded with 2 borough!')
        except:
            print(str(df.loc[i,'SR Number'])+' not geocoded with 2!')
len(df[pd.notna(df['BBL'])])
# 937/941

lion=gpd.read_file(path+'STREET CLOSURE/sidewalk/input/lion/lion.shp')
lion.crs=4326
lion=lion.to_crs(6539)
for i in df.index:
    if pd.isna(df.loc[i,'BBL']):
        try:
            onstreet=df.loc[i,'SR Address'].split(' BETWEEN ')[0].strip().upper()
            fromstreet=df.loc[i,'SR Address'].split(' BETWEEN ')[1].split(' AND ')[0].strip().upper()
            tostreet=df.loc[i,'SR Address'].split(' BETWEEN ')[1].split(' AND ')[1].split(',')[0].strip().upper()
            boroughcode=np.where(df.loc[i,'City (SR Address) (NYC Address)']=='NEW YORK',1,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='BRONX',2,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='BROOKLYN',3,
                        np.where(df.loc[i,'City (SR Address) (NYC Address)']=='STATEN ISLAND',5,4)))).tolist()
            addr=g['3']({'borough_code':boroughcode,'on':onstreet,'from':fromstreet,'to':tostreet})
            if addr['Segment Identifier']!='':
                seg=lion.loc[lion['SegmentID']==addr['Segment Identifier'],'geometry'].reset_index(drop=True)
                df.loc[i,'BBL']=0
                df.loc[i,'LAT']=0
                df.loc[i,'LONG']=0
                df.loc[i,'X']=seg.centroid.x[0]
                df.loc[i,'Y']=seg.centroid.y[0]
            else:
                print(str(df.loc[i,'SR Number'])+' not geocoded with 3 borough!')
        except:
            print(str(df.loc[i,'SR Number'])+' not geocoded with 3!')
len(df[pd.notna(df['BBL'])])
# 941/941
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['X'],df['Y'])],crs=6539)
df=df.to_crs(4326)
df.to_file(path+'SIDEWALK CAFE/COMPLAINTS/COMPLAINTS_xy.shp')


df.read_file(path+'SIDEWALK CAFE/COMPLAINTS/COMPLAINTS_xy.shp')
df=df[(pd.notna(df['BBL']))&(df['BBL']!=0)].reset_index(drop=True)
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['LONG'],df['LAT'])],crs=4326)
df.to_file(path+'SIDEWALK CAFE/COMPLAINTS/COMPLAINTS.shp')












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





































