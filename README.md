# Kafka-Spark-Streaming
This illustrates the usage of Kafka and Spark Streaming feature.
Event Messages are sent using Kafka and Received and processed using Spark-Streaming module.

There are two jupyter notebooks here.
KafkaSendMessage.ipynb -> Creates a kafka topic and starts sending the messages to kafka broker.

SparkReceiveKafkaStream.ipynb -> 1) Creates a spark stream object on the Kafka topic. 
                                 2) Groups the events(kafka messages) within a sliding window interval using reduceByWindow.
                                 3) Calls the proecessing function for this group of events.
                                 
 I have used, Kafka 2.11-0.11.0.2, spark 2.4.0, and jdk 8.0 , on windows.
 Initially tried to use jdk 11, but this caused issues while calling collect/count functions on the RDD.
 But, worked perfectly fine after downgrading the JDK to 8.0 .
 
 To use spark 2.4.0 on windows, I had to do the following tweeks - 
  1) download http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.0/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar to SPARK_HOME\jars directory. 
  2) add this jar to path using following command before getting the stream in the code-flow :
     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages <SPARK_HOME>\jars\spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar pyspark-shell'
