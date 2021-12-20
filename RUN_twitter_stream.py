""" Set up to receive tweet streams"""
# import sys
import time
# import pyspark
# import pyspark.sql
# import pyspark.streaming
# from pyspark.sql import Row,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from processing_spark import ProcessSparkStreaming
# from processing_dataframes import ProcessDataframes
from processing_tweets import ProcessTweets
from machine_learning import AnalyzeDataFrames


# create Spark Conf
interval = 2
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, interval)
ssc.checkpoint("checkpoint_TwitterApp")

# establish connection and collect data
# pprint() can output file to terminal from stream
dataStream = ssc.socketTextStream("127.0.1.1", 5555)
print("LISTENING TO SOCKET")

# send tweet text for analysis
processed_tweets = ProcessTweets.process_tweets(dataStream)
processed_train_file = "processed_training_tweets_SMALL.csv"
scores = AnalyzeDataFrames.calculate_score(processed_train_file, processed_tweets)
print("ANALYSIS COMPLETE")

# construct and save results to database
# final_result = ProcessDataframes.add_a_column(dataStream, scores)
ProcessSparkStreaming.export_dstream_to_text_file(processed_tweets)
print("OUTPUT SAVE COMPLETE")

# map hashtags with Key: Word, Value: 1
# hashtags = words.map(lambda x: (x, 1))
# tags_totals = hashtags.updateStateByKey(ProcessTweets.tag_numbers)
# tags_totals.foreachRDD(ProcessSparkStreaming.process_rdd)

# Start Stream Analysis, use time delay for tweet collection
# stop stream service after delay for data capture
ssc.start()
time.sleep(60)
ssc.stop()
