package com.cloudurable.kafka;

import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.clients.consumer.Consumer;

import org.apache.kafka.common.serialization.LongDeserializer;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Properties;
//import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.*;
public class KafkaConsumerExample {

    private final static String TOPIC = "kafka_result_topic_project";

    private final static String BOOTSTRAP_SERVERS =

            "localhost:9092,localhost:9093,localhost:9094,172.52.110.31:9092,172.52.110.31:9093,172.52.110.31:5001";

      private static Consumer<Long, String> createConsumer() {

      final Properties props = new Properties();

      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,

                                  BOOTSTRAP_SERVERS);

      props.put(ConsumerConfig.GROUP_ID_CONFIG,

                                  "KafkaExampleConsumer");

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,

              LongDeserializer.class.getName());

      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,

              StringDeserializer.class.getName());

      // Create the consumer using props.

      final Consumer<Long, String> consumer =

                                  new KafkaConsumer<>(props);

      // Subscribe to the topic.

      consumer.subscribe(Collections.singletonList(TOPIC));

      return consumer;

  }

      static void runConsumer() throws InterruptedException {

        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true) {

            final ConsumerRecords<Long, String> consumerRecords =

                    consumer.poll(1000);

            if (consumerRecords.count()==0) {

                noRecordsCount++;

                if (noRecordsCount > giveUp) break;

                else continue;

            }

            consumerRecords.forEach(record -> {

                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",

                        record.key(), record.value(),

                        record.partition(), record.offset());
                /////////////////////////////
                
          
                String json_str=record.value().toString();
                
                try {
					JSONObject root = new JSONObject(json_str);
					//JSONArray projectArray=root.getJSONArray("project");
					//JSONArray event = root.getJSONArray("Event");

					//String p_code=(String) root.get("event");
					JSONObject event = root.getJSONObject("event"); 
					String p_code=event.getString("projectCode");
					String p_name=event.getString("projectName");
					int t_bid=event.getInt("totalBid");
					int e_duration=event.getInt("expectedDuration");
					String e_start=event.getString("expectedStartDate");
					String e_end=event.getString("expectedEndDate");
					
					System.out.println("*****"+e_end);
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                
                
                
                System.out.println(record.value().toString());
                
                
                
               /////////////////////////////////// 
            });
            
            
           
            consumer.commitAsync();

        }

        consumer.close();

        System.out.println("DONE");

    }

  public static void main(String... args) throws Exception {

      runConsumer();

  }

}
