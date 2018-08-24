package com.cloudurable.kafka;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.*;
import org.apache.kafka.clients.consumer.*;


import org.apache.kafka.clients.consumer.Consumer;

import org.apache.kafka.common.serialization.LongDeserializer;

import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;

import org.json.*;
public class KafkaConsumerExample {

	
	public static String p_code,p_name,e_start,e_end;
	public static boolean p_over;
	public static int t_bid,e_duration;
	
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
               
                
          
                String json_str=record.value().toString();
                
                try {
                	
                	/*Getting data from received KAFKA stream through SiddhiQL*/
                	
                	
					JSONObject root = new JSONObject(json_str);
					JSONObject event = root.getJSONObject("event"); 
					p_code=event.getString("projectCode");
					p_name=event.getString("projectName");
					t_bid=event.getInt("totalBid");
					 e_duration=event.getInt("expectedDuration");
					 e_start=event.getString("expectedStartDate");
					 e_end=event.getString("expectedEndDate");
					 p_over=event.getBoolean("over");
					
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                
                
                
                System.out.println(record.value().toString());
                
                //* if project is not over then update in hubspot*/
                if(p_over==false) {
                	
                	
                	
                }
                
                /* Fetch Hubspot API*/
                
                try {
                	 
            		HttpClient httpClient = new DefaultHttpClient();
            		HttpGet getRequest = new HttpGet(
            			"https://api.hubapi.com/deals/v1/deal/paged?hapikey=d583a2d1-b573-40f2-a4d3-56076b574cff&includeAssociations=true&limit=10&properties=closedate&properties=dealname");
            		getRequest.addHeader("accept", "application/json");

            		HttpResponse response = httpClient.execute(getRequest);

            		if (response.getStatusLine().getStatusCode() != 200) {
            			throw new RuntimeException("Failed : HTTP error code : "
            			   + response.getStatusLine().getStatusCode());
            		}

            		BufferedReader br = new BufferedReader(
                                     new InputStreamReader((response.getEntity().getContent())));
            		
            		try {
            			String output;
            			
            			output=br.readLine();
            			/*while ((output = br.readLine()) != null) {
            				System.out.println(output);
            			}*/
            			
            			
            			JSONObject myresponse = new JSONObject(output);
            			System.out.println(myresponse);
            			JSONArray deal_arr= myresponse.getJSONArray("deals");
            			for(int i=0;i<deal_arr.length();i++) {
            				JSONObject deal_1=deal_arr.getJSONObject(i);
            				JSONObject properties= deal_1.getJSONObject("properties");
            				JSONObject dealname= properties.getJSONObject("dealname");
            				String value=dealname.getString("value");
            				int deal_id = deal_1.getInt("dealId");
            			 System.out.println("Deal Name :"+value);
            			 
            				if(value.equals(p_name) && p_over==false) {
            					
            		
            				System.out.println("Going to hubspot");
            				
            				SimpleDateFormat sdfTime = new SimpleDateFormat("HH:mm:ss");

            			    Date now = new Date();

            			    String strTime = sdfTime.format(now);
            				
            				
            				String myDate = e_end+" "+strTime;
            				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            				Date date = sdf.parse(myDate);
            				long millis = date.getTime();
            			
            			
            			        JsonObject jsonobj=
            			        		Json.createObjectBuilder()
            			        		.add("properties",Json.createArrayBuilder()
            			        				.add(Json.createObjectBuilder()
            			        						.add("name", "closedate")
            			        						.add("value", millis)))
            			        		.build();
            			        
            			        
            			     
            			        
            			        
            			        
            			        DefaultHttpClient httpClient_2 = new DefaultHttpClient();
            					HttpPut putRequest = new HttpPut(
            						"https://api.hubapi.com/deals/v1/deal/"+deal_id+"?hapikey=d583a2d1-b573-40f2-a4d3-56076b574cff");

            					StringEntity input = new StringEntity(jsonobj.toString());
            					
            					
            				
            					input.setContentType("application/json");
            					putRequest.setEntity(input);

            					System.out.println(putRequest);
            					HttpResponse response_2 = httpClient_2.execute(putRequest);
            					System.out.println(response_2);
            					if (response_2.getStatusLine().getStatusCode() != 200) {
            						throw new RuntimeException("Failed : HTTP error code : "
            							+ response.getStatusLine().getStatusCode());
            				
            				
            				
            					}

            					System.out.println(jsonobj.toString());
            			        System.out.println("Successfully updated in Hubspot!");
            			       
            				
            			}
            				
            			//	System.out.println("*****"+deal_id+"*****"+value);
            				
            			}
            			
            			

            			
            			
            		} catch (JSONException e1) {
            			// TODO Auto-generated catch block
            			e1.printStackTrace();
            		} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		
            		

            		httpClient.getConnectionManager().shutdown();

            	  } catch (ClientProtocolException e) {
            	
            		e.printStackTrace();

            	  } catch (IOException e) {
            	
            		e.printStackTrace();
            	  }


       
              
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
