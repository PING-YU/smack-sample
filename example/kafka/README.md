# Kafka Example 

run 

$spark-submit --class com.imac.JavaKafkaWordCount --master local[2] --jars /opt/spark/lib/spark-examples-1.5.2-hadoop2.6.0.jar kafkaStreaming.jar 10.26.1.88:2181 test-consumer-group mytopic 1