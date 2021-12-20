""" THIS IS WHERE MACHINE LEARNING WILL BE APPLIED TO RETURN SCORE"""

# install pyspark if needed
from pyspark.ml.feature import HashingTF
from pyspark import SparkConf, SparkContext
from pyspark.ml.classification import NaiveBayes
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, regexp_replace

class AnalyzeDataFrames:

    def train_naive_bayes(processed_train_file,sc):
        # Boilerplate Spark stuff:
        #conf = SparkConf().setMaster("local").setAppName("SparkDecisionTree")
        #sc = SparkContext(conf = conf)
        spark = SparkSession(sc)

        # We read the processed data files
        # Currently using processed_training_tweets_SMALL.csv, but should try to use the larger file 
        train = spark.read.csv(processed_train_file, inferSchema=True, header=True)
        train = train.withColumn('words',split(regexp_replace(train["words"], '\[|\]',''),',').cast('array<string>'))

        # We now transform the words to a numerical number and keep track of the count
        hashTF = HashingTF(inputCol="words", outputCol="numerical")
        num_train= hashTF.transform(train).select('score', 'words', 'numerical')

        # Naive Bayes Training
        naive_bayes = NaiveBayes(labelCol = "score", featuresCol="numerical", smoothing=1.0, modelType="multinomial").fit(num_train)
        return naive_bayes
    
    def calculate_score(naive_bayes, dstr): 

        #spark = SparkSession(sc) 
        # test = spark.read.csv(dstr, inferSchema=True, header=True)
        test = dstr.withColumn('words',split(regexp_replace(dstr["words"], '\[|\]',''),',').cast('array<string>'))

        # We now transform the words to a numerical number and keep track of the count
        hashTF = HashingTF(inputCol="words", outputCol="numerical")
        test= hashTF.transform(test).select('score', 'words', 'numerical')

        return naive_bayes.transform(test).select("score").replace(1.0, 4.0)

    
        
