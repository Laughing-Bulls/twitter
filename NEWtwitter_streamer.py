""" Set up to receive tweet streams"""
import sys
import time
import pyspark
import pyspark.sql
import pyspark.streaming
from pyspark.sql import Row,SQLContext
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from processing import ProcessSparkStreaming
from processing import ProcessDataframes
from processing import ProcessTweets


# create Spark Conf
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,.5)
ssc.checkpoint("checkpoint_TwitterApp")

# establish connection and collect data
# pprint() can output file to terminal from stream
dataStream = ssc.socketTextStream("127.0.1.1", 5555)
print(dataStream.pprint())
words = dataStream.flatMap(lambda line: line.split(" "))

# map hashtags with Key: Word, Value: 1
hashtags = words.map(lambda x: (x, 1)) 
tags_totals = hashtags.updateStateByKey(ProcessTweets.tag_numbers)
tags_totals.foreachRDD(ProcessSparkStreaming.process_rdd)

# Start Stream Analysis, use time delay for tweet collection
# stop stream service after delay for data capture
ssc.start()
time.sleep(60)
ssc.stop()

