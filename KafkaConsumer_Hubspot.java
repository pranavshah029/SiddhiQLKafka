package com.cloudurable.kafka;

import org.apache.http.HttpResponse;




import java.util.Properties;
import java.sql.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.*;
import org.apache.kafka.clients.consumer.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.json.JsonSerializer;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import org.json.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class KafkaConsumer_Hubspot {



	
	
	public static String p_code,p_name,e_start,e_end,projectName,milestone,ActualCompletionDate,amount,initial_value,milestone_name;
	public static boolean p_over, isReadyForBilling;
	public static int t_bid,e_duration,project_id,milestone_id,milestone_bill_amount,deal_amount,initial_milestone_value,whizible_project_id,total_milestone_amount,total_deal_amount,hubspot_amount;
	
  // private final static String TOPIC = "Ezest-Whiziblem-connect-MileStone";
	private final static String TOPIC = "kafka_result_topic_project";
	//private final static String TOPIC = "kafka_result_topic_project_demo";
	//private final static String TOPIC = "kafka_topic";


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
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      


      // Create the consumer using props.

      final Consumer<Long, String> consumer =

                                  new KafkaConsumer<>(props);

      // Subscribe to the topic.

      consumer.subscribe(Collections.singletonList(TOPIC));

      return consumer;

  }

      @SuppressWarnings("resource")
	static void runConsumer(String hapikey,String WhizibleDatabaseConnection,String databaseName,String usernameforWhizible,String passwordforWhizible) throws InterruptedException {
    	  
    	  

        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 1000;   int noRecordsCount = 0;

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
					milestone_id=event.getInt("milestoneId");
					project_id=event.getInt("projectId");
					projectName=event.getString("projectName");
					milestone=event.getString("milestone");
					ActualCompletionDate=event.getString("ActualCompletionDate");
					milestone_bill_amount=event.getInt("billAmount");
					isReadyForBilling=event.getBoolean("isReadyForBilling");

					
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                
                
                
                System.out.println(record.value().toString());
                
                /* if project is not over then update in hubspot*/
             
                /* Fetch Hubspot API*/
                
                try {
                	@SuppressWarnings("deprecation")
					@Deprecated
				
					
					/////1st HTTP call of get all deals and extreact  deal id from it
            		HttpClient httpClient = new DefaultHttpClient();
            		HttpGet getRequest = new HttpGet(
            			"https://api.hubapi.com/deals/v1/deal/paged?hapikey="+hapikey+"&includeAssociations=true&limit=10&properties=closedate&properties=dealname&properties=amount");
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

            			
            			/*Parse JSON received from get all deals api*/
            			
            			JSONObject myresponse = new JSONObject(output);
            			System.out.println(myresponse);
            			JSONArray deal_arr= myresponse.getJSONArray("deals");
            			for(int i=0;i<deal_arr.length();i++) {
            				JSONObject deal_1=deal_arr.getJSONObject(i);
            				JSONObject properties= deal_1.getJSONObject("properties");
            				JSONObject dealname= properties.getJSONObject("dealname");
            				String value=dealname.getString("value");
            				int deal_id = deal_1.getInt("dealId");
            				
            				/*JSONObject amount= properties.getJSONObject("amount");
            				int deal_amount=amount.getInt("value");*/
           
            		//	 System.out.println("Deal Name :"+value);
            			 
            			 /*Match the deal name and find its deal id*/
            			 
            				if(value.equals(projectName) && isReadyForBilling==true) {
            					
            					//String input1 = "2018/11/12";
            					SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
            					Date date = format.parse(ActualCompletionDate);
            					Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
            					calendar.setTime(date);
            					int year = calendar.get(Calendar.YEAR);
            					//Date month_1 =calendar.getTime();
            					int month_1=calendar.get(Calendar.MONTH);
            					month_1++;
            					
            					String month = new SimpleDateFormat("MMM").format(calendar.getTime()).toLowerCase();
            					//System.out.println(calendar.getMonthOfYear());
            					
            					
            					///database connection
            					String connectionUrl = WhizibleDatabaseConnection +
            			                "databaseName="+databaseName+";user="+usernameforWhizible+";password="+passwordforWhizible;

            			        // Declare the JDBC objects.
            			        Connection con = null;
            			        Connection con_1=null;
            			        Connection con_2=null;
            			        Connection con_3=null;
            			        ResultSet rs = null;
            			        ResultSet rs_1 = null;
            			        ResultSet rs_2 = null;
            			        ResultSet rs_3=null;
            			        System.out.println("before datbase");
            			        try {
            			            // Establish the connection.
            			            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            			            con = DriverManager.getConnection(connectionUrl);
            			            con_1 = DriverManager.getConnection(connectionUrl);
            			            con_2 = DriverManager.getConnection(connectionUrl);
            			            con_3 = DriverManager.getConnection(connectionUrl);
            			            

            			            // Create and execute an SQL statement that returns some data.
        			            PreparedStatement ps_1 = con.prepareStatement("SELECT ProjectID FROM tbl_PM_Project where ProjectName='"+projectName+"'");
            			            rs = ps_1.executeQuery();
            			            // Iterate through the data in the result set and display it.
            			            	while(rs.next()) {
            			            		System.out.println("ProjectID is : "+rs.getInt(1));
                 			               whizible_project_id=rs.getInt(1);
                 			                System.out.println("In datbase");

            			            	}
            			                
            			            
            			            System.out.println("In datbase with id"+whizible_project_id);
        			                PreparedStatement ps_2=con_1.prepareStatement("select sum(BillAmount) from tbl_PM_Milestones where DATEPART(month,ActualCompletiondate)="+month_1+" and DATENAME(year,ActualCompletionDate)='"+year+"' and ProjectID="+whizible_project_id);
        			                
        			                PreparedStatement ps_4=con_3.prepareStatement("select MileStone,BillAmount from tbl_PM_Milestones where DATEPART(month,ActualCompletiondate)="+month_1+" and DATENAME(year,ActualCompletionDate)='"+year+"' and ProjectID="+whizible_project_id); 
        			                rs_3=ps_4.executeQuery();
        			                while(rs_3.next()) {
        			                	milestone_name=rs_3.getString(1);
        			                }
        			                
        			                rs_1=ps_2.executeQuery();
        			                
        			                PreparedStatement ps_3=con_2.prepareStatement("select sum(BillAmount) from tbl_PM_Milestones where ProjectID="+whizible_project_id);
        			                
        			                rs_2=ps_3.executeQuery();
        			                
        			                while(rs_1.next()) {
        			                	 total_milestone_amount=rs_1.getInt(1);
        			                }
        			                
        			                while(rs_2.next()) {
        			                	total_deal_amount=rs_2.getInt(1);
        			                }
        			                	
        			                	System.out.println("*****"+total_milestone_amount);
        			                	
        			                	
                    					
                        				/*Build JSON required for updating the deal*/
                        				//System.out.println("Initial amount is "+initial_milestone_value);
                			        JsonObject jsonobj=
                			        		Json.createObjectBuilder()
                			        		.add("properties",Json.createArrayBuilder()
                			        				.add(Json.createObjectBuilder()
                			        						.add("name","amount")
                			        						.add("value",total_deal_amount))
                			        				.add(Json.createObjectBuilder()
                			        						.add("name","n"+year+"_"+month+"_milestone")
                			        						.add("value",total_milestone_amount)))
                			        		.build();
                			        
                			        
                			     
                			        
                			        /*call update api in hubspot with specific dealid*/
                			        
                			        DefaultHttpClient httpClient_2 = new DefaultHttpClient();
                					HttpPut putRequest = new HttpPut(
                						"https://api.hubapi.com/deals/v1/deal/"+deal_id+"?hapikey="+hapikey);
                					StringEntity input = new StringEntity(jsonobj.toString());
                					putRequest.setEntity(input);
                					input.setContentType("application/json");
                					putRequest.setEntity(input);

                					System.out.println(putRequest);
                					HttpResponse response_2 = httpClient_2.execute(putRequest);
                					System.out.println(response_2);
                					if (response_2.getStatusLine().getStatusCode() != 200) {
                						
                						
                						
                						///////////Mailer Code
                						
                				 
                						final String username = "bill.gates1979366@gmail.com";
                						final String password = "p1979366";

                						Properties props = new Properties();
                						props.put("mail.smtp.auth", "true");
                						props.put("mail.smtp.starttls.enable", "true");
                						props.put("mail.smtp.host", "smtp.gmail.com");
                						props.put("mail.smtp.port", "587");

                						Session session = Session.getInstance(props,
                						  new javax.mail.Authenticator() {
                							protected PasswordAuthentication getPasswordAuthentication() {
                								return new PasswordAuthentication(username, password);
                							}
                						  });

                						try {

                							Message message = new MimeMessage(session);
                							message.setFrom(new InternetAddress("bill.gates1979366@gmail.com"));
                							message.setRecipients(Message.RecipientType.TO,
                								InternetAddress.parse("pranavshah029@gmail.com"));
                							message.setSubject("Testing Subject");
                							message.setText("Dear Mail Crawler,"
                								+ "\n\n No spam to my email, please!");

                							Transport.send(message);

                							System.out.println("Done");

                						} catch (MessagingException e) {
                							throw new RuntimeException(e);
                						}                				
                					}
                					
                					else {
                						
                						
                						String topicName_ezetweet = "Hubspot_Update";
                				        
                				        //Configure the Producer
                				        Properties configProperties = new Properties();
                				        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.52.110.31:9092");
                				        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
                				        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");

                				        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

                				        ObjectMapper objectMapper = new ObjectMapper();

                				      
                				        
                    			        JsonObject jsonobj_ezetweet=
                    			        		Json.createObjectBuilder()
                    			        		.add("event",Json.createArrayBuilder()
                    			        				.add(Json.createObjectBuilder()
                    			        						.add("ProjectName",projectName)
                    			        						.add("MileStone",milestone_name)
                    			        						.add("InitalAmount",total_milestone_amount)
                    			        						.add("UpdatedAmount",total_milestone_amount)))
                    			        				  .build();
                				        
                    			        //JsonNode jsonNode = objectMapper.valueToTree(jsonobj_ezetweet);
                    			        ProducerRecord rec = new ProducerRecord(topicName_ezetweet,jsonobj_ezetweet);
                				      System.out.println("Sending json to ezetweet *****************"+rec);
                				            producer.send(rec);
                				            
                				        
                				producer.close();
                						
                						
                					}
        			                	
        			                	
        			                	
        			             
            			        }

            			        // Handle any errors that may have occurred.
            			        catch (Exception e) {
            			            e.printStackTrace();
            			        }
            			        finally {
            			            if (rs != null) try { rs.close(); } catch(Exception e) {}
            			            if (con != null) try { con.close(); } catch(Exception e) {}
            			            if (rs_1 != null) try { rs.close(); } catch(Exception e) {}
            			            if (con_1 != null) try { con.close(); } catch(Exception e) {}
            			        }
            					
            				
            					
            				System.out.println("Going to hubspot for fetching intial milestones in particular month with dealid "+deal_id+" and deal name"+value);
            				
            				
            				/*Fetch initial milestone data*/
            			
            					//System.out.println(jsonobj.toString());
            			        System.out.println("Successfully updated in Hubspot!");
            			       
            				
            			}
            				else {
            					//System.out.println("Project not found in Hubspot");
            				}
            				
            			
            				
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
	  
	  
	  Properties prop = new Properties();
		InputStream input = null;
		String hapikey=null,WhizibleDatabaseConnection=null,databaseName=null,usernameforWhizible=null,passwordforWhizible=null;

		try {

			input = new FileInputStream("src/com/cloudurable/kafka/config.properties");
			//input =new FileInputStream("config1.properties");
			// load a properties file
			prop.load(input);

			// get the property value and print it out
		hapikey=prop.getProperty("hapikey");
		WhizibleDatabaseConnection=prop.getProperty("WhizibleDatabaseConnection");
		databaseName=prop.getProperty("databaseName");
		usernameforWhizible=prop.getProperty("usernameforWhizible");
		passwordforWhizible=prop.getProperty("passwordforWhizible");
		
			

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

      runConsumer(hapikey,WhizibleDatabaseConnection,databaseName,usernameforWhizible,passwordforWhizible);

  }

}
