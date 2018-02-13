### A container for deploying an Apache Kafka+Spark container on the edge

#### All python files are in `/PythonCodes`

#### Start the zookeeper and kafka server as usua;l

#### To start the dummy producer, first do `pip3 install kafka-python` to install the necessary python library, then just run 
```
python3 PythonCodes/dummyProducer.py localhost:9092 <topicname> <filename>
```

#### After having that done, run the spark streaming program as 
```
park-submit --jars PythonCodes/spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar PythonCodes/pythonSpark.py localhost:2181 <topicname>
```
or to redirect the output to a text file:
```
spark-submit --jars PythonCodes/spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar PythonCodes/pythonSpark.py
localhost:2181 <topicname> > output.txt 
```

#### TODO:
- [] Add the jar in the docker container
- [] Install `pip3` for python3 and `kafka-python` in docker
