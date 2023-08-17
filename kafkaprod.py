#!/usr/bin/python3                     

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json
import os
import boto3
from s3fs import S3FileSystem
import sys
from random import randint

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='ACCESS_KEY_ID',
    aws_secret_access_key='SECRET_ACCESS_KEY'
)

obj = s3.Bucket('berka-staging-data').Object('transaction.csv').get()
df = pd.read_csv(obj['Body'])


BROKER = '18.222.161.239:9092'
TOPIC = 'trans'

if _name_ == "_main_":
        try:
                producer = KafkaProducer(bootstrap_servers=BROKER, 
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        except Exception as e:
                print(f"ERROR --> {e}")
                sys.exit(1)


        while True:
                dict_trans = df.sample(1).to_dict(orient="records")[0]
                producer.send(TOPIC, value=dict_trans)
                print(dict_trans)
                sleep(1)
