import sys
import os
import kafka

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.sql.context import SQLContext

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/tvengal/opt/anaconda3/lib/python3.7/site-packages/pyspark/jars/spark-streaming-kafka-0-8_2.11-2.0.2.jar pyspark-shell'

print("Before SparkSession\n")

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 10)
kafkastream = KafkaUtils.createStream(ssc, 'localhost:2181', "console-consumer", {'test1': 1})  
lines = kafkastream.map(lambda x: x[1])
lines.pprint()
ssc.start()
ssc.awaitTermination()
