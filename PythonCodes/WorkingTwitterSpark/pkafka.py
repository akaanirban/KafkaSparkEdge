import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaConsumer, KafkaProducer


#TWITTER API CONFIGURATIONS
#consumer_key = 
#consumer_secret = 
#access_token = 
#access_secret = 

#TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):          
	def __init__(self):
		#localhost:9092 = Default Zookeeper Producer Host and Port Adresses
		#self.client = pykafka.KafkaClient("localhost:9092")
		
		#Get Producer that has topic name is Twitter
		#self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()
                self.producer = KafkaProducer(bootstrap_servers='localhost:9092')  


	def on_data(self, data):
		#Producer produces data for consumer
		#Data comes from Twitter
		#self.producer.produce(bytes(data, "ascii"))
                self.producer.send("twitter", data.encode('utf-8'))	
                print(data)
                return True
                                                                                                                                           
	def on_error(self, status):
		print(status)
		return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Game of Thrones hashtag (Tweets)
#twitter_stream.filter(track=['#GoTS7'])
twitter_stream.filter(track=['trump'])
