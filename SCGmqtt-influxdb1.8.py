import paho.mqtt.client as mqtt
import json
import datetime
import pandas as pd
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='nangluong.iotmind.vn', port=8086, username='root', password='root', database='SCG')
dbclient.create_database('SCG')

def on_connect(client, userdata, flags, rc):
    client.subscribe('data/plc')

def on_message(client, userdata, msg):
    topic = msg.topic
    content = msg.payload
    objpayload = json.loads(content)
    data = objpayload["d"]
    data = pd.DataFrame(data)
    data[['device', 'parameter']] = data.tag.str.split(':', expand=True)
    data[['parameter_trans', 'feeder_trans']] = data.parameter.str.split('_', expand=True)
    data.replace({'parameter_trans':{'klt':'Sum. Volume', 'tld':'Set point rate', 'tlph':'Actual rate', 'nsd':'Set point capacity', 'nsph':'Actual capacity'}}, inplace=True)
    data.replace({'feeder_trans':{'c1':'Clinker 1', 'c2':'Clinker 2', 'c3':'Clinker 3', 'pg2':'Additives 2', 'pgm':'New additives', 'thachcao':'Plaster', 'pg3':'Additives 3', 'trobay':'Fly ash'}}, inplace=True)
    data['topic'] = topic
    data['time'] = datetime.datetime.utcnow()
    data.set_index('time', inplace=True)
    data = data.drop(['tag'], axis=1)
    dbclient.write_points(data, 'realtime', field_columns=['value'])

client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
client.on_connect = on_connect
client.on_message = on_message
# Connect
client.connect("nangluong.iotmind.vn", 16766, 60)
client.loop_forever()
