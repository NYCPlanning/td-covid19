import s3fs
import pandas as pd
import numpy as np
import sqlalchemy as sal
import geopandas as gpd
import shapely
import json



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SAFEGRAPH/'
eg=pd.read_csv(path+'engine.csv').loc[0,'engine']
accesskey=pd.read_csv(path+'key.csv').loc[0,'access']
secretkey=pd.read_csv(path+'key.csv').loc[0,'secret']
fs=s3fs.S3FileSystem(key=accesskey,secret=secretkey,client_kwargs={'endpoint_url':'https://s3.wasabisys.com','region_name':'us-east-1'})





# Brand Table
df=pd.read_csv(path+'brand_info.csv',dtype=str,encoding='utf-8')
df.to_csv(path+'brand_info.csv',index=False,encoding='utf-8')

# Set up database table schema
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    CREATE TABLE sgbrand202008
    (
      safegraph_brand_id TEXT,
      brand_name TEXT,
      parent_safegraph_brand_id TEXT,
      naics_code REAL,
      top_category TEXT,
      sub_category TEXT,
      stock_symbol TEXT,
      stock_exchange TEXT
      )
    """
con.execute(sql)
trans.commit()
con.close()

# Manually copy the csv to the cloud
# Copy the csv to the table
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    COPY sgbrand
    FROM '/home/mayijun/SAFEGRAPH/brand_info.csv'
    DELIMITER ','
    CSV header
    """
con.execute(sql)
trans.commit()
con.close()






# Core POI Table
df=pd.read_csv(path+'core_poi-part1.csv',dtype=str,encoding='utf-8')

# Set up database table schema
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    CREATE TABLE sgcorepoi202008
    (
      safegraph_place_id TEXT,
      parent_safegraph_place_id TEXT,
      location_name TEXT,
      safegraph_brand_ids TEXT,
      brands TEXT,
      top_category TEXT,
      sub_category TEXT,
      naics_code REAL,
      latitude REAL,
      longitude REAL,
      street_address TEXT,
      city TEXT,
      region TEXT,
      postal_code TEXT,
      iso_country_code TEXT,
      phone_number TEXT,
      open_hours JSON,
      category_tags TEXT
      )
    """
con.execute(sql)
trans.commit()
con.close()

# Manually copy the csv to the cloud
# Copy the csv to the table
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    COPY sgcorepoi
    FROM '/home/mayijun/SAFEGRAPH/core_poi-part5.csv'
    DELIMITER ','
    CSV header
    """
con.execute(sql)
trans.commit()
con.close()















# Patterns Table
df=[]
for i in range(1,5):
    with fs.open('sg-c19-response/weekly-patterns-delivery/weekly/patterns/2020/08/12/19/patterns-part'+str(i)+'.csv.gz','rb') as f:
        tp=pd.read_csv(f,escapechar='\\',compression='gzip',dtype=str)
    tp=tp[np.isin(tp['region'],['NY','NJ','PA','CT'])].reset_index(drop=True)
    df+=[tp]
df=pd.concat(df,axis=0,ignore_index=True)
df.to_csv(path+'patterns20200812.csv',index=False)

# Set up database table schema
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    CREATE TABLE sgpatterns20200812
    (
      safegraph_place_id TEXT,
      location_name TEXT,
      street_address TEXT,
      city TEXT,
      region TEXT,
      postal_code TEXT,
      iso_country_code TEXT,
      safegraph_brand_ids TEXT,
      brands TEXT,
      date_range_start TEXT,
      date_range_end TEXT,
      raw_visit_counts REAL,
      raw_visitor_counts REAL,
      visits_by_day JSON,
      visits_by_each_hour JSON,
      poi_cbg TEXT,
      visitor_home_cbgs JSON,
      visitor_daytime_cbgs JSON,
      visitor_country_of_origin JSON,
      distance_from_home REAL,
      median_dwell REAL,
      bucketed_dwell_times JSON,
      related_same_day_brand JSON,
      related_same_week_brand JSON,
      device_type JSON
      )
    """
con.execute(sql)
trans.commit()
con.close()

# Manually copy the csv to the cloud
# Copy the csv to the table
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    COPY sgpatterns20200812
    FROM '/home/mayijun/SAFEGRAPH/patterns20200812.csv'
    DELIMITER ','
    CSV header
    """
con.execute(sql)
trans.commit()
con.close()





engine=sal.create_engine(str(eg))
con=engine.connect()
sql="""
    SELECT sgpatterns20200812.safegraph_place_id,sgpatterns20200812.location_name,sgcorepoi202008.brands,
    sgcorepoi202008.top_category,sgcorepoi202008.sub_category,sgcorepoi202008.naics_code,
    sgcorepoi202008.latitude,sgcorepoi202008.longitude,sgpatterns20200812.raw_visit_counts,
    sgpatterns20200812.raw_visitor_counts,sgpatterns20200812.median_dwell
    FROM sgpatterns20200812
    LEFT JOIN sgcorepoi202008
    ON sgpatterns20200812.safegraph_place_id=sgcorepoi202008.safegraph_place_id
    """
df=pd.read_sql_query(sql,con)
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['longitude'],df['latitude'])],crs={'init' :'epsg:4326'})
df.to_file(path+'df.shp')



k=json.loads(df.loc[0,'device_type']).get('ios')










# Social Distancing Table
with fs.open('sg-c19-response/social-distancing/v2/2020/08/09/2020-08-09-social-distancing.csv.gz','rb') as f:
    tp=pd.read_csv(f,escapechar='\\',compression='gzip',dtype=str)
df=[]
for i in tp.index:
    k=pd.DataFrame.from_dict(data=json.loads(tp.loc[i,'destination_cbgs']),orient='index').reset_index(drop=False)
    k['origin']=tp.loc[i,'origin_census_block_group']
    k.columns=['destination','D20200809','origin']
    k=k[['origin','destination','D20200809']].reset_index(drop=True)
    df+=[k]
df=pd.concat(df,axis=0,ignore_index=True)
df.to_csv(path+'20200809.csv',index=False)

df=pd.read_csv(path+'20200809.csv',dtype=str,converters={'D20200809':float})
df=df[np.logical_or([x[0:5] in ['36005','36047','36061','36081','36085'] for x in df['origin']],
                    [x[0:5] in ['36005','36047','36061','36081','36085'] for x in df['destination']])].reset_index(drop=True)
df=df.sort_values('D20200809',ascending=False).reset_index(drop=True)

k=df.groupby('origin',as_index=False).agg({'D20200809':'sum'}).sort_values('D20200809',ascending=False).reset_index(drop=True)
k=k[k['origin']!='360610031001'].reset_index(drop=True)
k.D20200809.hist(bins=100)
bkgp=gpd.read_file(path+'quadstatebkgpclipped.shp')
bkgp.crs={'init':'epsg:4326'}
k=pd.merge(bkgp,k,how='inner',left_on='blockgroup',right_on='origin')
k.to_file(path+'ko.shp')

k=df.groupby('destination',as_index=False).agg({'D20200809':'sum'}).sort_values('D20200809',ascending=False).reset_index(drop=True)
k=k[k['destination']!='360610031001'].reset_index(drop=True)
k.D20200809.hist(bins=100)
bkgp=gpd.read_file(path+'quadstatebkgpclipped.shp')
bkgp.crs={'init':'epsg:4326'}
k=pd.merge(bkgp,k,how='inner',left_on='blockgroup',right_on='destination')
k.to_file(path+'kd.shp')




# Shapefile and PostGIS
# pip install geoalchemy2 to enable to_postgis
engine=sal.create_engine(str(eg))
bkgp=gpd.read_file(path+'quadstatebkgpclipped.shp')
bkgp.crs={'init':'epsg:4326'}
bkgp.to_postgis('quadstatebkgpclipped',engine)

sql="""
    SELECT * FROM quadstatebkgpclipped
    WHERE blockgroup='360610247001'
    """
k=gpd.read_postgis(sql,engine,geom_col='geometry')







