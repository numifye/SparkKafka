# SparkKafka
Spark Login Anomaly Detector that consumes messages from input Kafka topic, performs wordcount, then outputs wordcounts to output Kafka topic.

Description: Spark Login Anomaly Detector that consumes messages from input Kafka topic, performs wordcount, then outputs wordcounts to output Kafka topic.


package = auditlog

class = LoginAnomalyDetector

Usage: auditlog.LoginAnomalyDetector <broker> <in-topic> <out-topic> <number-of-failed-login-attempts> <interval-in-ms> <window-size-in-ms>

Produce anomaly records at proper time as per threshold and duration. Windowing not required, but should still have correct command-line syntax for spark application which includes a window size.



Testing:

0. As any user, create the input and output Kafka topics:

$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic auditRecords 

$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic loginAnomalies

1. As the root user on the sandbox VM, install sshpass: 
yum install sshpass

2. In the first window on the sandbox VM, run the following command as the root user and keep the window open:

$ tail -f /var/log/audit/audit.log | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic auditRecords

3. In a second window on the sandbox VM, run the following commands as the root user and keep the window open:

$ while true

do

   sshpass -p wrongpass ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  user01@localhost
   
   sshpass -p wrongpass ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  user02@localhost 
   
   sleep 5

done

4. In a third window on the sandbox VM, run the following command as any user and keep the window open:

$ /opt/mapr/spark/spark-2.1.0/bin/spark-submit --master local[*] --class auditlog.LoginAnomalyDetector CS185-jar-with-dependencies.jar localhost:9092 auditRecords loginAnomalies 5 5000 30000      

5. In a fourth window on the sandbox VM, run the following command as any user and keep the window open:

$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic loginAnomalies 
