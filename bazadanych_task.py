from sqlalchemy import Table, Column, Float, MetaData, Integer, String, Date, create_engine
from datetime import datetime

engine=create_engine('sqlite:///stationsdb.db')

meta=MetaData()

stations=Table(
    'stations', meta, 
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)

measure=Table(
    'measure', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', Date),
    Column('precip', Float),
    Column('tobs', Integer),
)

meta.create_all(engine)

station_ins=stations.insert()

measure_ins=measure.insert()



st_content=[]

with open('clean_stations.csv') as mf:
    z=mf.readlines()
    
for line in z[1:]:
    station=line.strip().split(',')
    st_content.append({'station':station[0],'latitude':float(station[1]),'longitude':float(station[2]),'elevation':float(station[3]),'name':station[4],'country':station[5],'state':station[6]})

me_content=[]

with open('clean_measure.csv') as mf:
    z=mf.readlines()

for line in z[1:]:
    measure=line.strip().split(',')
    me_content.append({'station':measure[0],'date':datetime.strptime(measure[1],'%Y-%m-%d').date(),'precip':float(measure[2]),'tobs':int(measure[3])})

conn=engine.connect()
conn.execute(station_ins , st_content)
conn.execute(measure_ins , me_content)

print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())