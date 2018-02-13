# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 14:59:30 2018

@author: Anirban Das
"""

# =============================================================================
# Create a file with TWITTER API CONFIGURATIONS
# give it name config.config and add followings:
#
# access_token:twitter_config.access_token
# access_secret:twitter_config.access_secret
# consumer_key:twitter_config.consumer_key
# consumer_secret:twitter_config.consumer_secret
# 
# =============================================================================
import sys, os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

class StdOutListener(StreamListener):
    def __init__(self, producer, topicname):
        self.producer = producer
        self.topicname = topicname
        
    def on_data(self, data):
        self.producer.send_messages(self.topicname, data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)


def main(sys_argv):
    bootstrap_servers = sys_argv[1]
    topicname = sys_argv[2]
    configfilename = sys_argv[3]
    if os.path.isfile(configfilename):
            #twitter config====================================================
            f = open('config.config', 'r')
            lines = f.readlines()
            config = list(map(lambda x: x.strip().split(':')[1], lines))
            access_token, access_token_secret, consumer_key, consumer_secret = config
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            
            #kafka config======================================================
            kafka = KafkaClient(bootstrap_servers)
            producer = SimpleProducer(kafka)
            KafkaListener = StdOutListener(producer, topicname)
            stream = Stream(auth, KafkaListener)
            stream.filter(track=['trump'], languages = ['en'])
    else:
        sys.exit('File {} doesnot exist in the folder path'.format(configfilename))

if __name__=="__main__":
    
    if len(sys.argv) < 4:
        sys.exit('Usage:python3 <filename.py> <bootstrap_server> <topicname> <configFilename>')
    
    main(sys.argv)
