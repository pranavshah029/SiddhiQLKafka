package demo_2_kafka.HubspotAPI_1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class ApacheHttpClientGet {

	public static void main(String[] args) {
	  try {

		DefaultHttpClient httpClient = new DefaultHttpClient();
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
			System.out.println("Output from Server .... \n");
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
			 System.out.println("Aaapli value :"+value);
			 
				if(value.equals("Deal2")) {
					
		
				System.out.println("Going to hubspot");
						
			
			
			        JsonObject jsonobj=
			        		Json.createObjectBuilder()
			        		.add("properties",Json.createArrayBuilder()
			        				.add(Json.createObjectBuilder()
			        						.add("name", "amount")
			        						.add("value", "500")))
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
			        
			       
				
			}
				
				System.out.println("*****"+deal_id+"*****"+value);
				
			}
			
			

			
			
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		

		httpClient.getConnectionManager().shutdown();

	  } catch (ClientProtocolException e) {
	
		e.printStackTrace();

	  } catch (IOException e) {
	
		e.printStackTrace();
	  }

	}

}
