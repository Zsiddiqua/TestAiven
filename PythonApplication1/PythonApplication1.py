from kafka import KafkaProducer
from kafka_connect_api import kafka_connect_api
import csv
from csv import DictReader
import pandas as pd
import json
import pprint

producer = KafkaProducer(
 bootstrap_servers='kafka-test-zafeerah-4b9c.aivencloud.com:22928',
 security_protocol="SSL",
 ssl_cafile="C:\\Users\\SiddiquaZafeerah\\Downloads\\ca.pem",
 ssl_certfile="C:\\Users\\SiddiquaZafeerah\\Downloads\\service.cert",
 ssl_keyfile="C:\\Users\\SiddiquaZafeerah\\Downloads\\service.key"
 )

csv_file_path = r'C:\\Users\\SiddiquaZafeerah\\Downloads\\nationGridUpcomingTrades.csv'
json_file_path = r'C:\\Users\\SiddiquaZafeerah\\Downloads\\nationGridUpcomingTrades.json'
topic = 'NationalGridTrades'
 
##df = pd.read_json(json_file_path, orient='records')

##with open(json_file_path, 'r') as fp:
##    data = json.load(fp)
    #keys= data.keys()
 ##   for messages in data:
   ##     producer.send(topic, messages)
     ##   producer.flush()

with open(csv_file_path,'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        keys = row["uuid"]
        ack = producer.send(topic,key=keys.encode('utf-8'),value=json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata)
#ack = producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')
#metadata = ack.get()
#print(metadata)




