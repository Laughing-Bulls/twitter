# import nltk
# from nltk.tokenize import TweetTokenizer
# import numpy as np
# import pyspark
from pyspark.sql import SparkSession
# import pandas as pd
# import re
# import string
# from pyspark.ml.feature import Tokenizer
# from pyspark.sql import Row, SQLContext


class ProcessDataframes:
    """ To manipulate Spark and Pandas dataframes"""

    @staticmethod
    def reduce_to_two_columns(df, one, two):
        # distill to sentiment and text
        return df.select(one, two)

    @staticmethod
    def rename_spark_column(df, old, new):
        # change column name in Spark dataframe
        return df.withColumnRenamed(old, new)

    @staticmethod
    def subset_of_spark_df(df, col, value):
        # this command will split out df by sentiment score
        return df[df[col] == value]

    @staticmethod
    def convert_spark_to_pandas(spark_df, one, two):
        # convert spark df to pandas df
        spark = SparkSession.builder.appName('ml-testing2').getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return spark_df.select(one, two).toPandas()

    @staticmethod
    def convert_tokenized_spark_to_pandas(spark_df):
        # convert tokenized spark df to pandas df
        spark = SparkSession.builder.appName('ml-testing4').getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return spark_df.toPandas()

    @staticmethod
    def convert_pandas_to_spark(pandas_df):
        # convert pandas df to spark df
        spark = SparkSession.builder.appName('ml-testing3').getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return spark.createDataFrame(pandas_df)

    @staticmethod
    def add_a_column(df, column):
        print("JOINING RESULTS TO ORIGINAL TWEET")
        return df
