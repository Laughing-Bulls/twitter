# pip install pyarrow
# import dependencies
import nltk
from nltk.tokenize import TweetTokenizer
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import re
import string


class ProcessDataframes:

    @staticmethod
    def import_data_file(filename):
        # import test data
        spark = SparkSession.builder.appName('ml-testing').getOrCreate()
        df = spark.read.csv(filename, inferSchema=True)
        return df

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
    def convert_pandas_to_spark(pandas_df):
        # convert pandas df to spark df
        spark = SparkSession.builder.appName('ml-testing3').getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return spark.createDataFrame(pandas_df)


class ProcessTweets:

    @staticmethod
    def clean_pandas_tweets(pandas_df, text_column):
        # remove web addresses
        pandas_df[text_column] = pandas_df[text_column].apply(lambda x: ProcessTweets.clean_urls(x))
        # convert to lowercase
        for each in pandas_df:
            pandas_df[text_column] = pandas_df[text_column].str.lower()
        # remove neutral words
        pandas_df[text_column] = pandas_df[text_column].apply(lambda tweet: ProcessTweets.clean_words(tweet))
        # remove punctuation
        pandas_df[text_column] = pandas_df[text_column].apply(lambda x: ProcessTweets.clean_punctuation(x))
        # remove numbers
        pandas_df[text_column] = pandas_df[text_column].apply(lambda x: ProcessTweets.clean_numbers(x))
        return pandas_df

    @staticmethod
    def clean_urls(text):
        # remove web addresses
        return re.sub('((www.[^s]+) | (http[^s]+) | (@[^s]+))', ' ', text)

    @staticmethod
    def clean_words(text):
        # remove neutral words
        no_bias_words = ['a', 'an', 'the', 'and', 'or', 'my', 'our', 'to', 'from', 'of', 'for', 'i', 'is', 'are',
                         'was', 'were', 'in', 'it', 'with']
        chopwords = set(no_bias_words)
        return " ".join([word for word in str(text).split() if word not in chopwords])

    @staticmethod
    def clean_punctuation(text):
        # remove punctuation
        punct = string.punctuation
        no_punct = str.maketrans('', '', punct)
        return text.translate(no_punct)

    @staticmethod
    def clean_numbers(text):
        # remove numbers
        return re.sub('[0-9]+', '', text)

    @staticmethod
    def tokenize_and_stem_tweet(pandas_df, text_column):
        # tokenize the text in the tweets of pandas df
        tokenized = TweetTokenizer()
        pandas_df[text_column] = pandas_df[text_column].apply(tokenized.tokenize)
        pandas_df[text_column] = pandas_df[text_column].apply(lambda x: ProcessTweets.text_stemmer(x))
        return pandas_df

    @staticmethod
    def text_stemmer(input_text):
        # Convert words to stems
        st = nltk.PorterStemmer()
        text = [st.stem(word) for word in input_text]
        return text

    @staticmethod
    def show_nulls(df):
        # show missing values in spark df
        from pyspark.sql.functions import isnull, when, count, col
        df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()
        # dataset = df.replace('null', None).dropna(how='any')
        return True

    @staticmethod
    def clean_up_tweets(df):
        # process tweets for analysis
        # convert spark df to pandas df for tweet manipulation
        df_modify = ProcessDataframes.convert_spark_to_pandas(df, 'score', 'tweet')
        # process tweets
        df_modify = ProcessTweets.clean_pandas_tweets(df_modify, 'tweet')
        # convert pandas df back to spark df before tokenizing
        return ProcessDataframes.convert_pandas_to_spark(df_modify)

"""
    @staticmethod
    def wordcloud(df):
        # WordCloud? - wordcloud module doesn't work
        import matplotlib.pyplot as plt
        # import seaborn as sns
        from wordcloud import WordCloud
        plt.figure(figsize=(20, 20))
        wc = WordCloud(maxwords=500, width=1600, height=800, collocations=False).generate(" ".join(df))
        plt.imshow(wc)
        return True
        """