# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 21:05:48 2018

@author: Anirban Das

Ref:https://github.com/dpkp/kafka-python/blob/master/example.py

"""

import random
import time, os
import threading
import multiprocessing, sys

from kafka import KafkaProducer

class Producer(threading.Thread):
    def __init__(self, bootstrap_servers, topicname, filename):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.bootstrap_servers = bootstrap_servers
        self.topicname = topicname
        self.filename = filename
        print('Created a dummy producer, will post to topic: {}'.format(self.topicname))

    def stop(self):
        self.stop_event.set()

    """get the stuff from the file mentioned
       take contents out, strip it , create a list of all lines, 
       send line to kafka on the topic mentioned in bytes format
    """
    def run(self):
        file = open(self.filename, "r", encoding='ISO-8859-1')
        contents = map(lambda x: x.strip(), file.readlines())
        nonEmptyContents = list(filter(None, contents))

        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        while not self.stop_event.is_set():
            tempString = bytes(random.choice(nonEmptyContents), encoding='ISO-8859-1')
            producer.send(self.topicname, tempString)
            print('Sent to kafka: {}'.format(tempString))
            time.sleep(.5) #sleep for 200 milliseconds
        producer.close()

def main(sys_argv):
    bootstrap_servers = sys_argv[1]
    topicname = sys_argv[2]
    filename = sys_argv[3]
    if os.path.isfile(filename):
        dummyProducer = Producer(bootstrap_servers, topicname, filename)
        dummyProducer.start()
        #time.sleep(100)
        try:
            #sleep for 100 seconds or get killed by Ctrl+C
            time.sleep(100)
            dummyProducer.join()
            dummyProducer.stop()
        except KeyboardInterrupt:
            dummyProducer.stop()
    else:
        sys.exit('File {} doesnot exist in the folder path'.format(filename))

    

if __name__ == '__main__':
  
    if len(sys.argv) < 4:
        sys.exit('Usage:python3 <filename.py> <bootstrap_server> <topicname> <textFilename>')
    
    main(sys.argv)

    


