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
 
 To use pyspark 2.4.0 with Kafk, I had to do the following - 
  1) download http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.0/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar to SPARK_HOME\jars directory. 
  2) add this jar to path using following command before getting the stream in the code-flow :
     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages <SPARK_HOME>\jars\spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar pyspark-shell'
     
     
To use spark 2.4.0 on windows, had to do the following tweek - 
cd SPARK_HOME\python\lib
unzip pyspark.zip
edit pyspark\worker.py
comment following lines and zip back the pyspark dir.
resource library doesn't exist in windows, and this code is to check the memory relimits, so there is no harm if we comment this. 

  #import resource
  
  #set up memory limits
        #memory_limit_mb = int(os.environ.get('PYSPARK_EXECUTOR_MEMORY_MB', "-1"))
        #total_memory = resource.RLIMIT_AS
        #try:
        #   if memory_limit_mb > 0:
                #(soft_limit, hard_limit) = resource.getrlimit(total_memory)
                #msg = "Current mem limits: {0} of max {1}\n".format(soft_limit, hard_limit)
                #print(msg, file=sys.stderr)

                # convert to bytes
                #new_limit = memory_limit_mb * 1024 * 1024

                #if soft_limit == resource.RLIM_INFINITY or new_limit < soft_limit:
                #    msg = "Setting mem limits to {0} of max {1}\n".format(new_limit, new_limit)
                #    print(msg, file=sys.stderr)
                #    resource.setrlimit(total_memory, (new_limit, new_limit))

    #except (resource.error, OSError, ValueError) as e:
    #    # not all systems support resource limits, so warn instead of failing
    #    print("WARN: Failed to set memory limit: {0}\n".format(e), file=sys.stderr)

