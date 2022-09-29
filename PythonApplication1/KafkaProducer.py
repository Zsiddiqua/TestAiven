from kafka import KafkaProducer
import csv
from csv import DictReader
import json
import pprint

filepath = r'<Path to folder>'
#set configurations for Kafka producer
producer = KafkaProducer(
 bootstrap_servers='<hostname>:<port>',
 security_protocol="SSL",
 ssl_cafile=filepath+"\\ca.pem",
 ssl_certfile=filepath+"\\service.cert",
 ssl_keyfile=filepath+"\\service.key"
 )
#Define name and location of the file containing data to be published to Kafka topic
csv_file_path = filepath+r'\\nationGridUpcomingTrades.csv'

#Define the name of Kafka Topic
topic = 'NationalGridTrades'
 
with open(csv_file_path,'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
    #Select a unique identifier column as Key
        keys = row["uuid"]
     #Publish messages to Kafka Topic
        ack = producer.send(topic,key=keys.encode('utf-8'),value=json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata)





