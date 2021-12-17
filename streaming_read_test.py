# read test stream of data from newly generated csv files

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

""" Create streaming of text every 3 seconds from csv files that create dynamically from streaming_write_test.py."""


def main():

    interval_time = 3

    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, interval_time)
    print("Ready, and awaiting input files...")

    # Streaming executes files from existing directory every 3 seconds
    lines = ssc.textFileStream('/')
    counts = lines.flatMap(lambda line: line.split(",")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

    print("That's all, Folks!")

if __name__ == "__main__":
    main()
