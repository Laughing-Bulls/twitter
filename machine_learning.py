""" THIS IS WHERE MACHINE LEARNING WILL BE APPLIED TO RETURN SCORE"""
# install pyspark if needed
from pyspark.ml.feature import HashingTF
# from pyspark import SparkConf, SparkContext
from pyspark.ml.classification import NaiveBayes
from pyspark.sql.functions import split, regexp_replace
# import pandas as pd
from pyspark.sql.types import ArrayType, StringType
from processing_tweets import ProcessTweets


class AnalyzeDataFrames:

    @staticmethod
    def train_naive_bayes(processed_train_file, spark):

        # We read the processed data files
        # Currently using processed_training_tweets_SMALL.csv, but should try to use the larger file 
        train = spark.read.csv(processed_train_file, inferSchema=True, header=True)
        train = train.withColumn('words', split(regexp_replace(train["words"], '\[|\]', ''), ',').cast('array<string>'))

        # We now transform the words to a numerical number and keep track of the count
        hashTF = HashingTF(inputCol="words", outputCol="numerical")
        num_train = hashTF.transform(train).select('score', 'words', 'numerical')

        # Naive Bayes Training
        naive_bayes = NaiveBayes(labelCol="score", featuresCol="numerical", smoothing=1.0, modelType="multinomial").fit(num_train)
        return naive_bayes

    @staticmethod
    def calculate_score(naive_bayes, dataStream, spark):
        """
        #spark = SparkSession(sc) 
        # test = spark.read.csv(dstr, inferSchema=True, header=True)
        test = dstr.withColumn('words',split(regexp_replace(dstr["words"], '\[|\]',''),',').cast('array<string>'))

        # We now transform the words to a numerical number and keep track of the count
        hashTF = HashingTF(inputCol="words", outputCol="numerical")
        test= hashTF.transform(test).select('score', 'words', 'numerical')

        return naive_bayes.transform(test).select("score").replace(1.0, 4.0)
        
        print("ANALYSIS COMPLETE")
        return 4
        """
        # preprocess the words in the tweets
        processed_words = ProcessTweets.process_tweets(dataStream)
        # processed_words have a format ['this', 'is', 'a', 'processed', 'tweet']
        
        column = spark.createDataFrame([processed_words], ArrayType(StringType())).toDF("words")

        hashTF = HashingTF(inputCol="words", outputCol="numerical")
        num_column= hashTF.transform(column).select('words', 'numerical')

        pred_naive_bayes = naive_bayes.transform(num_column)
        results_naive_bayes = pred_naive_bayes.select("prediction").replace(1.0, 4.0)

        results = results_naive_bayes.toPandas()
        prediction = results.iloc[0,0]

        if processed_words:
            tweet_words_with_score = processed_words.append(str(prediction))
            print("SUCCESSFUL ANALYSIS OF PROCESSED WORDS")
        else:
            tweet_words_with_score = ["The input words list is:", "EMPTY", "The score is:", str(prediction)]

        processed_dataStream = dataStream.map(lambda rdd: rdd + "  SENTIMENT SCORE: " + str(prediction))
        
        return processed_dataStream



        
