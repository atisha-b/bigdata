#!/usr/bin/python3  

import mysql.connector
import os
import boto3
import numpy as np
import pandas as pd
from s3fs import S3FileSystem

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='ACCESS_KEY_ID',
    aws_secret_access_key='SECRET_ACCESS_KEY'
)


acc_obj = s3.Bucket('berka-staging-data').Object('account.csv').get()
account = pd.read_csv(acc_obj['Body'],sep=';')

cl_obj = s3.Bucket('berka-staging-data').Object('client.csv').get()
client = pd.read_csv(cl_obj['Body'])

dist_obj = s3.Bucket('berka-staging-data').Object('district.csv').get()
district = pd.read_csv(dist_obj['Body'],sep=';')

loan_obj = s3.Bucket('berka-staging-data').Object('loan.csv').get()
loan = pd.read_csv(loan_obj['Body'],sep=';')

card_obj = s3.Bucket('berka-staging-data').Object('card.csv').get()
card = pd.read_csv(card_obj['Body'])

disp_obj = s3.Bucket('berka-staging-data').Object('disposition.csv').get()
dispo = pd.read_csv(disp_obj['Body'])


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="admin"
)

mycursor = mydb.cursor(buffered=True)
mycursor.execute("use berka;")

# importing the product list


# insering the products into the database
for index, values in district.iterrows():
    sql = "INSERT INTO district (district_id, district_name, region_name ) VALUES (%s, %s, %s)"
    val = (values["A1"], values["A2"], values["A3"])
    mycursor.execute(sql, val)

for index, values in account.iterrows():
    sql = "INSERT INTO account (account_id, district_id, frequency,dt ) VALUES (%s, %s, %s,%s)"
    val = (values["account_id"], values["district_id"], values["frequency"], values["date"])
    mycursor.execute(sql, val)

for index, values in client.iterrows():
    sql = "INSERT INTO client (client_id, district_id, gender, age, age_levels ) VALUES (%s, %s, %s, %s, %s)"
    val = (values["client_id"], values["district_id"], values["gender"], values["age"], values["age_levels"])
    mycursor.execute(sql, val)

for index, values in loan.iterrows():
    sql = "INSERT INTO loan (loan_id, account_id, date, amount, duration, payments, status ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (values["loan_id"], values["account_id"], values["date"], values["amount"], values["duration"], values["payments"],values["status"])
    mycursor.execute(sql, val)

for index, values in dispo.iterrows():
    sql = "INSERT INTO dispo (disp_id, client_id, account_id, type ) VALUES (%s, %s, %s, %s)"
    val = (values["disp_id"], values["client_id"], values["account_id"], values["type"])
    mycursor.execute(sql, val)

for index, values in card.iterrows():
    sql = "INSERT INTO card (card_id, disp_id, type, issued ) VALUES (%s, %s, %s, %s)"
    val = (values["card_id"], values["disp_id"], values["type"], values["issued"])
    mycursor.execute(sql, val)

mydb.commit()

# mandatory to run this
mycursor.close()
