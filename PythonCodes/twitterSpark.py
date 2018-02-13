# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 16:07:11 2018

@author: Anirban Das

REF: https://github.com/kaantas/kafka-twitter-spark-streaming/blob/master/kafka_twitter_spark_streaming.py
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json, sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: twitterSpark.py <zookeeper:port> <topic>", file=sys.stderr)
        exit(-1)
        
    #gettinh the zookeeper broker and topic information from command line
    zkQuorum, topic = sys.argv[1:]
    
	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")

	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

	#Create Kafka Stream to Consume Data Comes From Twitter Topic
	#localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, 'spark-streaming', {topic:1})
    
    #Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

	#Count the number of tweets per User
    #user_counts = parsed.map(lambda tweet: (tweet['user']['screen_name'], 1)).reduceByKey(lambda x,y: x + y)
    user_counts = parsed.map(lambda tweet: (tweet['text'],1)).reduceByKey(lambda x,y: x + y)
	#Print the User tweet counts
    user_counts.pprint(20)

	#Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()
