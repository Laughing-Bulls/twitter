""" Execution script"""
from processing import ProcessTweets
from processing import ProcessDataframes

if __name__ == '__main__':
    # get Spark dataframe from csv file
    filename = 'training.1600000.processed.noemoticon.csv'
    df = ProcessDataframes.import_data_file(filename)
    # see types -- can also use df.dtypes
    df.printSchema()
    df.show(5)

    # distill columns to sentiment score and text columns and adjust column names
    twit = ProcessDataframes.reduce_to_two_columns(df, '_c0', '_c5')
    twit = ProcessDataframes.rename_spark_column(twit, '_c0', 'score')
    twit = ProcessDataframes.rename_spark_column(twit, '_c5', 'tweet')
    twit.show(5)
    # shape of Spark dataframe
    print(twit.toPandas().shape)
    # length of Spark dataframe
    numTweets = len(twit.toPandas())
    print(numTweets)

    # see sentiment breakdown
    print(twit.groupby('score').count().toPandas())

    # clean up tweet data
    modified_twit = ProcessTweets.clean_up_tweets(twit)

    # split df by sentiment score
    pos = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 4)
    # neut = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 2)
    neg = ProcessDataframes.subset_of_spark_df(modified_twit, 'score', 0)
    print(pos.toPandas().shape)
    #print(neut.toPandas().shape)
    print(neg.toPandas().shape)

    # tokenize and stem tweets - adds 'words' column with tokenized array
    tokenized_twit = ProcessTweets.tokenize_tweets(modified_twit, 'tweet')
    panda_twit = ProcessDataframes.convert_tokenized_spark_to_pandas(tokenized_twit)
    panda_stem_twit = ProcessTweets.stem_tweet(panda_twit, 'words')
    panda_stem_twit.head()

    # save processed tweets in a csv file
    ProcessDataframes.export_pandas_dataframe_to_csv(panda_stem_twit, "processed_training_tweets.csv")

    # save processed tweets in database
    # ProcessDataframes.export_dataframe_to_mongodb(tokenized_twit)

    print("That's all, Folks!")
