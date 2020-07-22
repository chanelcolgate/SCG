import paho.mqtt.client as mqtt
import json
import datetime
import pandas as pd
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='SCG')
dbclient.create_database('SCG')

def on_connect(client, userdata, flags, rc):
    client.subscribe('data/command')

def on_message(client, userdata, msg):
    topic = msg.topic
    content = msg.payload
    objpayload = json.loads(content)
client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
#client.on_connect = on_connect
#client.on_message = on_message

client.connect("nangluong.iotmind.vn", 16766, 60)
#client.loop_forever()

df = dbclient.query('select spread(value) into report from realtime group by *,time(1h)')

#client.publish(topic='data/rs', payload='{"tong":50, "ts":"2020-07-21T13:00:00Z", "kl_c1":25, "kl_c2":25}')
#
#str({"tong":[50,50], "ts":["2020-07-21T13:00:00Z","2020-07-21T13:00:05Z"], "kl_c1":[25,25], "kl_c2":[25,25]})
x = df.to_json(orient="records")
df = dbclient.query('select * from report')['report']