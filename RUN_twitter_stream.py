""" Receive tweet streams and process them with ML algorithm"""
import time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from processing_spark import ProcessSparkStreaming
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

# train machine learning model
processed_train_file = "processed_training_tweets.csv"
naive_bayes = AnalyzeDataFrames.train_naive_bayes(processed_train_file, sc)

# establish connection and collect data
dataStream = ssc.socketTextStream("127.0.1.1", 5555)
print("LISTENING TO SOCKET")

# process tweets and send text to learning algorithm for analysis
processed_tweets = ProcessTweets.process_tweets(dataStream)
scores = AnalyzeDataFrames.calculate_score(naive_bayes, processed_tweets)

# construct and save results to database
ProcessSparkStreaming.add_data_to_mongodb(dataStream, processed_tweets, scores)

# Start Stream Analysis, use time delay for tweet collection
# stop stream service after delay for data capture
stream_time = 60
ssc.start()
time.sleep(stream_time)
ssc.stop()
