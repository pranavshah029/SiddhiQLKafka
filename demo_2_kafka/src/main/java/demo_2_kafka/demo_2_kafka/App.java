package demo_2_kafka.demo_2_kafka;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        String definition = "@source(type='kafka',topic.list='kafka_topic', partition.no.list='0', threading.option='single.thread', group.id=\"group\",bootstrap.servers='localhost:9092') define stream SweetProductionStream (name string, amount double);";
        
        
        String definition_2="@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', partition.no='0') define stream LowProductionAlertStream (name string,amount double);";

        
        String query = "@info(name='query1') from SweetProductionStream[amount > 65.6] select * insert into LowProductionAlertStream;";
        
        SiddhiManager siddhiManager = new SiddhiManager();
        
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition +definition_2+ query);
        
      while(true) {
        
        executionPlanRuntime.addCallback("query1", new QueryCallback() {

			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				// TODO Auto-generated method stub
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				System.out.println("Working");
			}
        	
        });
       
       }
      
     

    }
 
    
}    
