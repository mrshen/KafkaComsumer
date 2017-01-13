package com.cvapp.mrshen;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


public class KafkaConsumer {

	private final static String TOPIC = "testTopic";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/1.json";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/2.json";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/3.json";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/4.json";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/5.json";
//	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/6.json";
	private final static String FILENAME = "/usr/local/flume-1.7.0/youmi_json/7.json";
	private static FileWriter fWriter = null;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		
		Properties properties = getProperties();
		Consumer<String, String> consumer;
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC));
        
        try {
        	fWriter = new FileWriter(FILENAME, true);
	        while(true) {
	        	ConsumerRecords<String, String>records = consumer.poll(1000);
	        	for(ConsumerRecord<String, String>record : records) {
	//        		System.out.printf("offset = %d, key = %s, value=%s\n", record.offset(), record.key(), record.value());
	        		String line = record.value();
	        		String[] fields = line.trim().split(";");
	        		String jfString = getJsonFormatString(fields);
	//        		System.out.println(jfString);
	        		fWriter.write(jfString + "\n");
	        	}
	        	fWriter.flush();
	        }
        } catch (IOException e) {
			// TODO: handle exception
        	e.printStackTrace();
		} finally {
        	System.out.println("finish.");
        	try {
		    	fWriter.flush();
		    	fWriter.close();
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
        }
	}
	
	public static Properties getProperties() {
		Properties properties = new Properties();
		
		properties.put("bootstrap.servers", "slave3:9092,slave4:9092,slave5:9092");
        properties.put("group.id", "0");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        return properties;
	}

	public static String getJsonFormatString(String[] fields) {
		String jString = "{";
		jString += "\"timestamp\":\"" + fields[0] + "\",";	// timestamp
		jString += "\"cid\":\"" + fill_nullString(fields[1]) + "\",";	// cid
		jString += "\"ip\":\"" + fill_nullString(fields[2]) + "\",";	// ip
		jString += "\"country\":\"" + fill_nullString(fields[3]) + "\",";	// country
		jString += "\"provn\":\"" + fill_nullString(fields[4]) + "\",";	// province
		jString += "\"cty\":\"" + fill_nullString(fields[5]) + "\",";	// city
		jString += "\"brd\":\"" + fill_nullString(fields[6]) + "\",";	// brd
		jString += "\"mod\":\"" + fill_nullString(fields[7]) + "\",";	// brd_mod
		jString += "\"apn\":\"" + fill_nullString(fields[11]) + "\",";	// apn
		jString += "\"app\":\"" + fill_nullString(fields[12]) + "\",";	// app
		jString += "\"mac\":\"" + fill_nullString(fields[13]) + "\",";	// mac
		jString += "\"ad\":\"" + fill_nullString(fields[14]) + "\",";	// ad
		jString += "\"sw\":" + fill_nullNumeric(fields[15]) + ",";	// sw
		jString += "\"sh\":" + fill_nullNumeric(fields[16]) + ",";	// sh
		jString += "\"tc\":\"" + fill_nullString(fields[18]) + "\",";	// tc, carried_name
		jString += "\"at\":" + fill_nullNumeric(fields[17]);	// at
		jString += "}";
		
		return jString;
	}
	
	public static String fill_nullString(String str) {
		if(str.isEmpty() || str.trim().isEmpty())
			return "Nil";
		else
			return str.trim();
	}
	
	public static String fill_nullNumeric(String str) {
		if(str.isEmpty() || str.trim().isEmpty())
			return "-1";
		else
			return str.trim();
	}
}
