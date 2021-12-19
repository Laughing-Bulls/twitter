# import nltk
# from nltk.tokenize import TweetTokenizer
# import numpy as np
# import pyspark
# from pyspark.sql import SparkSession
# import pandas as pd
import re
import string
# from pyspark.ml.feature import Tokenizer
# from pyspark.sql import Row, SQLContext
from processing_dataframes import ProcessDataframes


class ProcessTweets:
    """ To manipulate Twitter text data"""

    @staticmethod
    def process_tweets(tweet):
        print("PROCESSING DATAFRAME")
        half_processed = ProcessDataframes.reduce_to_two_columns(tweet, '_c0', '_c1')
        return half_processed

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
        no_web = re.sub('((www.[^s]+) | (http://[^s]+))', '', text)
        # remove @mentions
        no_at = re.sub('@[A-Za-z0-9]+', '', no_web)
        return no_at

    @staticmethod
    def clean_words(text):
        # remove neutral words
        no_bias_words = ['a', 'an', 'the', 'and', 'or', 'my', 'our', 'to', 'from', 'of', 'for', 'i', 'you', 'he', 'she',
                         'is', 'are', 'was', 'were', 'in', 'it', 'with', 'am', 'has', 'had', 'would', 'could', 'be']
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

    """
    @staticmethod
    def tokenize_tweets(spark_df, tweet_column):
        # tokenize the text in the tweets of spark df
        tokenizer = Tokenizer(inputCol=tweet_column, outputCol='words')
        tokenized = tokenizer.transform(spark_df)
        tokenized_twit = ProcessDataframes.reduce_to_two_columns(tokenized, 'score', 'words')
        # PANDAS CODE: tokenized = TweetTokenizer()
        # PANDAS CODE: pandas_df[text_column] = pandas_df[text_column].apply(tokenized.tokenize)
        return tokenized_twit

    @staticmethod
    def stem_tweet(pandas_df, text_column):
        # stem words in the text of the tweets of pandas df
        pandas_df[text_column] = pandas_df[text_column].apply(lambda x: ProcessTweets.text_stemmer(x))
        return pandas_df

    @staticmethod
    def text_stemmer(input_text):
        # Convert words to stems
        st = nltk.PorterStemmer()
        text = [st.stem(word) for word in input_text]
        return text
    """

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

    @staticmethod
    def tag_numbers(new_values, total_sum):
        # return number of tags or 0 if none
        return sum(new_values) + (total_sum or 0)

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
