from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import *

import psutil
import time 
import json
import socket
import struct
import random

def on_send_success(record_metadata):
    print('[Offset] ' + str(record_metadata.offset) )
    print('Topic: ' + str(record_metadata.topic) )
    print('Partition: ' + str(record_metadata.partition) )

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

while(1):
	activity = {}

	cpu_percent = psutil.cpu_percent()
	memory_percent = psutil.virtual_memory()[2]
	disk_percent = psutil.disk_usage('/')[3]
	ts = int(round(time.time()))
	ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))

	activity['cpu_percent'] = cpu_percent
	activity['disk_percent'] = disk_percent
	activity['memory_percent'] = memory_percent
	activity['timestamp'] = ts
	activity['ip'] = ip

	producer.send('metrics-topic', activity).add_callback(on_send_success).add_errback(on_send_error)
	producer.flush()
	time.sleep(0.1)