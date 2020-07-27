import datetime
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='localhost', port=8086, username='root', password='root', database='SCG')
dbclient.create_database('SCG')

def tensecondspos():
    if len(dbclient.query('select count(*) from tenseconds').keys()) == 0:
        upper = dbclient.query('select * from realtime order by time desc limit 1')['realtime'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select last(value) - first(value) as diff into tenseconds from realtime where time <= $upper group by *,time(10s)', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from tenseconds order by time desc limit 1')['tenseconds'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        upper = dbclient.query('select * from realtime order by time desc limit 1')['realtime'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select last(value) - first(value) as diff into tenseconds from realtime where time >= $lower and time <= $upper group by *,time(10s)', bind_params={'lower':lower, 'upper':upper})
import time
while True:
    tensecondspos()
    time.sleep(10)