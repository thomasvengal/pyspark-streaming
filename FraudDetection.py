import pyspark 
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
from sklearn.ensemble import RandomForestClassifier
from xgboost.sklearn import XGBClassifier
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
import time


import numpy as np
import pandas as pd
import pickle

from kafka import KafkaProducer
import json

conf = SparkConf().setAppName("FraudDetection").setMaster("local").set("spark.io.compression.codec", "snappy")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

jsonschema = StructType().add("step", StringType()) \
                         .add("type", StringType()) \
                         .add("amount", StringType()) \
                         .add("nameOrig", StringType()) \
                         .add("oldbalanceOrg", StringType()) \
                         .add("newbalanceOrig", StringType()) \
                         .add("nameDest",StringType()) \
                         .add("oldbalanceDest",StringType()) \
                         .add("newbalanceDest",StringType())


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest").load() \
    .select(from_json(col("value").cast("string"), jsonschema).alias("parsed_mta_values"))

df.printSchema()

mta_data = df.select("parsed_mta_values.*")

print("Mta_data Type", type(mta_data))

raw_table= mta_data.writeStream.outputMode("append").queryName("raw_table").format("memory").start()
if (raw_table.isActive):
	time.sleep(2)
	raw=spark.sql("select * from raw_table")

print("raw Type", type(raw))
print("raw ", raw)
raw.show()

mta_data = raw.toPandas()
org_mta_data = mta_data
print("mta_data",mta_data)

mta_data['step'] = mta_data['step'].astype(int)
mta_data['amount'] = pd.to_numeric(mta_data['amount'], errors='coerce')
mta_data['oldbalanceOrg'] = pd.to_numeric(mta_data['oldbalanceOrg'], errors='coerce')
mta_data['newbalanceOrig'] = pd.to_numeric(mta_data['newbalanceOrig'], errors='coerce')
mta_data['oldbalanceDest'] = pd.to_numeric(mta_data['oldbalanceDest'], errors='coerce')
mta_data['newbalanceDest'] = pd.to_numeric(mta_data['newbalanceDest'], errors='coerce')
mta_data['type'] = mta_data['type'].astype('category')
mta_data['type'] = mta_data['type'].cat.codes
mta_data = mta_data.drop("nameOrig", axis=1)
mta_data = mta_data.drop("nameDest", axis=1)

mta_data.head()
print("New Mta data\n", mta_data)


with open('Classifier2.pickle', 'rb') as handle:
        RFmodel = pickle.load(handle)
RFmodel.keys()

RFclf = RFmodel['classifier']
predictions = RFclf.predict(mta_data)
print (predictions)

org_mta_data['isFraud']=predictions
new_mta_data=org_mta_data
print("New Mta data with predictions", new_mta_data)


# prepared_record => X_test which is fraud
for i in range (0,len(new_mta_data)):
	prepared_record=new_mta_data.iloc[i,:]
	if (prepared_record.isFraud==1) :
		print("\nFraud_prepared", prepared_record)
		producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		producer.send("FraudData", value=prepared_record.to_json()).get(timeout=30)
	else:
		print("\nNo Fraud_prepared", prepared_record)
		producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		producer.send("NonFraudData", value=prepared_record.to_json()).get(timeout=30)


