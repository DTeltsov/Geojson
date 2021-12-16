from sqlalchemy import create_engine
import pandas as pd
import polyline
from geojson import  Point
import json


DRIVER = "ODBC Driver 17 for SQL Server"
DSN = "Some DSN" 
DB = "Some DB" 
UID = 'Some User' 
PS = 'Some PS'

engine = create_engine(
        f"mssql+pyodbc://{UID}:{PS}@{DSN}/{DB}?driver={DRIVER}", fast_executemany=True
    )

    


sql = ('''select distinct \
		o.Name  \
	   ,ot.Name \
	   ,wt.Name  \
	   ,e.Value    \
	   ,case when swt.Is = 1 then rdsp.1 else 0 end  \
	   ,x.Sum_Expr   \
	   ,mv.Coordinates as geometry                 \
from [DB].[dbo].[Table] as ord \
left join [DB].[dbo].[Table] as o on o.[Id] = ord.[Obj_Id]   \
left join [DB].[dbo].[Table] as rdsp on rdsp.ObjDistance_Id = ord.Id    \
left join [DB].[dbo].[Table] as e on e.EKey = ord.DistanceState and e.Enum_Group = 'State'      \
left join [DB].[DIM].[Table] as ot on ot.Id = ord.Type_Id   \
left join [DB].[DIM].[Table] as wt on wt.Id = o.Type_Id \
left join [DB].[DIM].[Table] as swt on swt.Id = rdsp.Id  \
left join [DB].[dbo].[Table] as mv on mv.[ObjectDistanceId] = ord.Id   \
left join (select sum(rfp.M) as Sum_Expr, rfp.Obj_Id from [DB].[dbo].[Table] as rfp group by rfp.Obj_Id) x  on x.Obj_Id = ord.Obj_Id    \
where rdsp.YDB = 2021 and ord.DistanceState <> 3'')
''')

df = pd.read_sql_query(sql, engine)
for i in range(len(df)):
    if df.iloc[i]['geometry'] is not None:
        try:
            df.at[i,'geometry'] = str(polyline.decode(df.iloc[i]['geometry']))[1:-1]
        except ValueError:
            df.at[i,'geometry'] = str(Point(polyline.decode(df.iloc[i]['geometry'])))[1:-1]
    else:
        pass

df = df.fillna('Null')

def df_to_geojson(df, properties):
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':[]}
        feature['geometry'] = row['geometry']
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)

    return geojson

cols = ['TrackCode', 'Name', 'DistanceFrom', 'DistanceTo']
states = df_to_geojson(df, cols)



with open('Roads.geojson', 'w', encoding='utf8') as f:
  json.dump(df_to_geojson(df, cols), f, ensure_ascii=False, indent=4)





