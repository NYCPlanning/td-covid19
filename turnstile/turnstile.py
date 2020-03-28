import pandas as pd

pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/TURNSTILE2020/'

df=pd.read_csv(path+'turnstile_200321.txt',dtype=str)
#df=pd.read_csv(path+'turnstile_200314.txt',dtype=str)


df['id']=df['UNIT']+'|'+df['C/A']+'|'+df['SCP']

df=df[['UNIT','STATION','LINENAME']].drop_duplicates(keep='first')
