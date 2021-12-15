""" Execution script"""
from processing import ProcessTweets
from processing import ProcessDataframes


# get Spark dataframe from csv file
filename = 'testdata-manual-2009-06-14.csv'
df = ProcessDataframes.import_data_file(filename)
# see types -- can also use df.dtypes
df.printSchema()
df.show(10)

# distill to sentiment score and text columns and adjust column names
twit = ProcessDataframes.reduce_to_two_columns(df, '_c0', '_c5')
twit = ProcessDataframes.rename_spark_column(twit, '_c0', 'score')
twit = ProcessDataframes.rename_spark_column(twit, '_c5', 'tweet')
twit.show(10)
# shape of Spark dataframe
print(twit.toPandas().shape)
# length of Spark dataframe
numTweets = len(twit.toPandas())
print(numTweets)

# see sentiment breakdown
print(twit.groupby('score').count().toPandas())

# clean up tweet data
modified_twit = ProcessTweets.clean_up_tweets(twit)
modified_twit.show(10)

# split df by sentiment score
pos = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 4)
neut = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 2)
neg = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 0)
print(pos.toPandas().shape)
print(neut.toPandas().shape)
print(neg.toPandas().shape)