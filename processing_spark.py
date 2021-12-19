# import nltk
# from nltk.tokenize import TweetTokenizer
# import numpy as np
# import pyspark
from pyspark.sql import SparkSession
# import pandas as pd
# import re
# import string
# from pyspark.ml.feature import Tokenizer
from pyspark.sql import Row, SQLContext
# from processing_dataframes import ProcessDataframes


class ProcessSparkStreaming:
    """ For i/o associated with Spark Streaming"""

    # USEFUL SPARK STREAMING COMMANDS
    # readstream.format("socket") - from Spark session object to read data fom TCP socket
    # writestream.format("console") - write streaming dataframe to console
    # print() - Prints the first ten elements of every batch of data in a DStream on the driver node running the application.
    # saveAsTextFiles(prefix, [suffix]) - Save this DStream’s contents as text files. The file name at each batch interval is generated based on prefix.
    # saveAsHadoopFiles(prefix, [suffix]) - Save this DStream’s contents as Hadoop files.
    # saveAsObjectFiles(prefix, [suffix]) - Save this DStream’s contents as SequenceFiles of serialized Java objects.
    # foreachRDD(func) - Generic output operator that applies a function, func, to each RDD generated from the stream.

    @staticmethod
    def import_from_csv(filename):
        # import data to Spark Streaming from csv file
        pass

    @staticmethod
    def import_from_db(filename):
        # import data to Spark Streaming from MongoDB
        pass

    @staticmethod
    def import_data_file(filename):
        # import test data
        spark = SparkSession.builder.appName('ml-testing').getOrCreate()
        df = spark.read.csv(filename, inferSchema=True)
        return df

    @staticmethod
    def export_dataframe_to_csv(sdf, filename):
        # export Spark df to a csv file
        sdf.coalesce(1).write.csv(filename)
        return True

    @staticmethod
    def export_pandas_dataframe_to_csv(pdf, filename):
        # export pandas df to a csv file
        pdf.to_csv(filename)
        return True

    @staticmethod
    def export_dataframe_to_mongodb(processed_tweets):
        # export Spark df to MongoDB
        processed_tweets.write.format('mongo').mode('append').save()
        return True

    @staticmethod
    def export_to_db(sdf):
        # export data from Spark Streaming to MongoDB
        ProcessSparkStreaming.export_dataframe_to_csv(sdf, "database-csv-output.csv")
        print("SAVING TO DATABASE -- really just a cvs output file")
        """
        from pymongo import MongoClient

        uri = "mongodb+srv://cluster0.vtked.mongodb.net/myFirstDatabase?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
        client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='<path_to_certificate>')

        db = client['testDB']
        collection = db['testCol']
        doc_count = collection.count_documents({})
        print(doc_count)
        """

        return True

    @staticmethod
    def process_rdd(time, rdd):
        # take incoming data from connector for pre-processing and transformation
        try:
            sql_context = ProcessSparkStreaming.get_SQL_context(rdd.context)
            row_rdd = rdd.map(lambda w: Row(word=w))
            hashtags_df = sql_context.createDataFrame(row_rdd)
            hashtags_df.registerTempTable("hashtags")
        except:
            print("ERROR IN process_rdd")

    @staticmethod
    def get_SQL_context(spark_context):
        # SQL context
        if 'sqlContextSingletonInstance' not in globals():
            globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
        return globals()['sqlContextSingletonInstance']