package com.cvapp.mrshen;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Consumer<String, String> consumer;
		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "cvapp:9092");
        properties.put("group.id", "0");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("testTopic"));
        
        while(true) {
        	ConsumerRecords<String, String>records = consumer.poll(100);
        	for(ConsumerRecord<String, String>record : records) {
        		System.out.printf("offset = %d, key = %s, value=%s\n", record.offset(), record.key(), record.value());
        	}
        }
	}

}
