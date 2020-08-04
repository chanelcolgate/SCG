import datetime
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='nangluong.iotmind.vn', port=8086, username='root', password='root', database='SCG')

def sumvolume():
    if len(dbclient.query('select count(*) from sumvolume').keys()) == 0:
        upper = dbclient.query('select * from realtime order by time desc limit 1')['realtime'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select last(value) - first(value) + max(value)*((max(value)-last(value))/(max(value)-last(value))) as sumvolume \
                        into sumvolume \
                        from realtime \
                        where parameter =~ /klt/ AND time <= $upper \
                        group by time(1h),*', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from sumvolume order by time desc limit 1')['sumvolume'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        upper = dbclient.query('select * from realtime order by time desc limit 1')['realtime'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select last(value) - first(value) + max(value)*((max(value)-last(value))/(max(value)-last(value))) as sumvolume \
                        into sumvolume \
                        from realtime \
                        where parameter =~ /klt/ and time >= $lower and time <= $upper \
                        group by *,time(1h)', bind_params={'lower':lower, 'upper':upper})

import time
while True:
    sumvolume()
    time.sleep(5)