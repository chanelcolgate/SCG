import paho.mqtt.client as mqtt
import json
import datetime
import pandas as pd
from influxdb import DataFrameClient

# dbclient
dbclient = DataFrameClient(host='nangluong.iotmind.vn', port=8086, username='root', password='root', database='SCG')
dbclient.create_database('SCG')

# mqtt on connect
def on_connect(client, userdata, flags, rc):
    client.subscribe('data/plc')
# mqtt on message
def on_message(client, userdata, msg):
    topic = msg.topic
    content = msg.payload
    objpayload = json.loads(content)
    data = objpayload["d"]
    data = pd.DataFrame(data)

    # split and and rename    
    data[['device', 'parameter']] = data.tag.str.split(':', expand=True)
    data[['parameter_1', 'parameter_2']] = data.parameter.str.split('_', expand=True)
    data[['device_1', 'device_2']] = data.device.str.split('_', expand=True)
    data = data.drop(['parameter', 'device'], axis=1)
    
    data.replace({'parameter_1':{'klt':'sv', 'tld':'spr', 'tlph':'ar', 'nsd':'spc', 'nsph':'ac', 'vt':'fs', 'ts':'mf', 'kl':'fv'}}, inplace=True)
    data.replace({'parameter_2':{'c1':'f1', 'c2':'f2', 'c3':'f3', 'pg2':'f4', 'pg3':'f5', 'trobay':'f6', 'thachcao':'f7', 'pgm':'f8', 'dong':'i', 'nhietvao':'it', 'nhietra':'ot', 'nhietdaubac':'ct', 'ts':'mf'}}, inplace=True)
    data.replace({'device_1':{'PLC1':'', 'PLC2':'', 'PLC3':'', 'vantoc':'mf', 'tong':'fa'}}, inplace=True)
    data.replace({'device_2':{'c1':'f1', 'c2':'f2', 'c3':'f3', 'pg2':'f4', 'pg3':'f5', 'trobay':'f6', 'thachcao':'f7', 'klt':'sv', 'tld':'spr', 'tlph':'ar', 'nsd':'spc', 'nsph':'ac'}}, inplace=True)
    
    # topic
    data['topic'] = topic
    
    ts = datetime.datetime.utcnow()
    # shift
    h = ts.hour
    if h >= 0 and h < 8:
        shift = 'shift 1'
    if h >= 8 and h < 16:
        shift = 'shift 2'
    if h >= 16 and h <=23:
        shift = 'shift 3'
    data['shift'] = shift
    
    # time
    data['time'] = ts
    
    # clean data    
    data.set_index('time', inplace=True)
    data = data.drop(['tag'], axis=1)
    data = data[data['parameter_1'] != 'mf']
    
    df1 = data[data['parameter_2'].str.contains('f1|f2|f3|f4|f5|f6|f7|f8', regex=True, na=False)].copy()
    df1.drop(['device_1', 'device_2'], axis=1, inplace=True)
    df1.rename(columns={"parameter_1": "parameter", "parameter_2": "device"}, inplace=True)
    
    df2 = data[data['parameter_2'].str.contains('mf|i|t', regex=True, na=False)].copy()
    df2.drop(['device_1', 'device_2'], axis=1, inplace=True)
    df2.rename(columns={"parameter_1": "device", "parameter_2": "parameter"}, inplace=True)
    
    df3 = data[data['device_1'].str.contains('mf', regex=True, na=False)].copy()
    df3.drop(['parameter_1', 'parameter_2'], axis=1, inplace=True)
    df3.rename(columns={"device_1": "parameter", "device_2": "device"}, inplace=True)
    
    df4 = data[data['device_1'].str.contains('fa', regex=True, na=False)].copy()
    df4.drop(['parameter_1', 'parameter_2'], axis=1, inplace=True)
    df4.rename(columns={"device_1": "device", "device_2": "parameter"}, inplace=True)
    
    df = pd.concat([df1, df2, df3, df4])
    df.replace({'device':{'f1':'Clinker VCM', 'f2':'Clinker SG 1', 'f3':'Clinker SG 2', 'f4':'Gypsum 1', 'f5':'ND Blackstone', 'f6':'Fly ash', 'f7':'Gypsum 2', 'f8':'Mapei', 'fa':'Total'}}, inplace=True)
    
    # cement
    clinker_spr = df[df.device.str.contains('Clinker', regex=True) & df.parameter.str.contains('spr')].value.sum()
    if clinker_spr > 92.5:
        cement = 'PC'
    else:
        cement = 'PCB'
    df['cement'] = cement
    
    # write to database
    dbclient.write_points(df, 'realtime', field_columns=['value'])

client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
client.on_connect = on_connect
client.on_message = on_message
# Connect
client.connect("nangluong.iotmind.vn", 16766, 60)
client.loop_forever()