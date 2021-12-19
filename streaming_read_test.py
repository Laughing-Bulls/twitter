# read test stream of data from newly generated csv files

# import sys
from pyspark import SparkContext
# from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

""" Create streaming of text every 3 seconds from csv files that create dynamically from streaming_write_test.py."""


def main():

    interval_time = 3

    sc = SparkContext.getOrCreate()
    # spark = SparkSession(sc)
    ssc = StreamingContext(sc, interval_time)
    print("Ready, and awaiting input files...")

    # Streaming executes files from existing directory every 3 seconds
    stream_data = ssc.textFileStream('/').map( lambda x: x.split(',') )

    stream_data.pprint()

    """
    counts = lines.flatMap(lambda line: line.split(",")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    """

    ssc.start()
    ssc.awaitTermination(50)
    ssc.stop()

    print("That's all, Folks!")


if __name__ == "__main__":
    main()
