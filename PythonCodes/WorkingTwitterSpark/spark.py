"""
RUNNING PROGRAM;

1-Start Apache Kafka
./kafka/kafka_2.11-0.11.0.0/bin/kafka-server-start.sh ./kafka/kafka_2.11-0.11.0.0/config/server.properties

2-Run kafka_push_listener.py (Start Producer)
ipython >> run kafka_push_listener.py

3-Run kafka_twitter_spark_streaming.py (Start Consumer)
PYSPARK_PYTHON=python3 bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 ~/Documents/kafka_twitter_spark_streaming.py

"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def filter_out_unicode(x):
    """
    Pass in a list of (authors, hashtags) and return a list of hashtags that are not unicode
    """
    hashtags = []
    for hashtag in x[1]:
        try:
            hashtags.append(str(hashtag))
        except UnicodeEncodeError:
            pass
    return (x[0], hashtags)


def get_people_with_hashtags(tweet):
    """
    Returns (people, hashtags) if successful, otherwise returns empty tuple. All users
    except author have an @ sign appended to the front.
    """
    data = json.loads(tweet)
    try:
        hashtags = ["#" + hashtag["text"] for hashtag in data['entities']['hashtags']]
        # Tweets without hashtags are a waste of time
        if len(hashtags) == 0:
            return ()
        author = data['user']['screen_name']
        mentions = ["@" + user["screen_name"] for user in data['entities']['user_mentions']]
        people = mentions + [author]
        return (people, hashtags)
    except KeyError:
        return ()


def flatten(x):

    """
    Input:
    ([people],[hashtags]).
    Output:
    [(hashtag, (main_author_flag, {person})),...]

    """
    all_combinations = []
    people = x[0]
    hashtags = x[1]
    for person in people:
        for hashtag in hashtags:
            main_author_flag = 0 if "@" in person else 1
            all_combinations.append((hashtag, (main_author_flag, {person})))
    return all_combinations




if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")
    sc.setLogLevel("ERROR")
	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

	#Create Kafka Stream to Consume Data Comes From Twitter Topic
	#localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
# Returns ([people], [hashtags])
    lines = kafkaStream.map(lambda x: get_people_with_hashtags(x[1])).filter(lambda x: len(x)>0)
    # Filters out unicode hashtags

    hashtags = lines.map(filter_out_unicode)

    # Make all possible combinations --> (hashtag, (main_author, {person})), where main_author == 1

    # if it is the tweet author

    flat_hashtags = hashtags.flatMap(flatten)
    
    #Parse Twitter Data as json
    #parsed = kafkaStream.map(lambda v: json.loads(v))

    #parsed.pprint(1)

	#Count the number of tweets per User
    #user_counts = kafkaStream.map(lambda tweet: (tweet['user']['screen_name'], 1)).reduceByKey(lambda x,y: x + y)
    hash_tag_authors_and_counts = flat_hashtags.reduceByKey(lambda a, b: (a[0] + b[0], a[1] | b[1]))
	#Print the User tweet counts
    #user_counts.pprint()
    hash_tag_authors_and_counts.pprint(20)
	#Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()


