package com.imac;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

public final class JavaKafkaWordCount {
	  public static void main(String[] args) {
	    if (args.length < 2) {
	      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>");
	      System.exit(1);
	    }

	    StreamingExamples.setStreamingLogLevels();

	    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(2000));
		int numThreads = Integer.parseInt(args[3]);
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
	    	    
	    messages.print();

	    // Start the computation
	    jssc.start();
	    jssc.awaitTermination();
	  }
	}