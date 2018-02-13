# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 20:45:13 2018
@author: Anirban Das
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    #creating the spark context and the spark streaming context
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    #gettinh the zookeeper broker and topic information from command line
    zkQuorum, topic = sys.argv[1:]

    #creating the Streaming connection with Kafka
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    
    #print out 20 of the results
    counts.pprint(20)

    ssc.start()
    ssc.awaitTermination()

