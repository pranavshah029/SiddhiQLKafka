package demo_1.demo_1;

import java.awt.Event;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
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
        
        String definition = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume long);";
        
        String query = "@info(name = 'query1') from cseEventStream#window.timeBatch(500)  select symbol, sum(price) as price, sum(volume) as volume group by symbol insert into outputStream ;";
        
        SiddhiManager siddhiManager = new SiddhiManager();
        
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition + query);
        
        
        
        executionPlanRuntime.addCallback("query1", new QueryCallback() {

			@Override
			public void receive(long timeStamp, org.wso2.siddhi.core.event.Event[] inEvents,
					org.wso2.siddhi.core.event.Event[] removeEvents) {
				// TODO Auto-generated method stub
				EventPrinter.print(timeStamp,inEvents, removeEvents);
			}
        	
        });
       
        
        
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        System.out.println(executionPlanRuntime);
        try {
        	System.out.println("sending events...");
        	
			inputHandler.send(new Object[]{"ABC", 700f, 100l});
			 inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
		        inputHandler.send(new Object[]{"DEF", 700f, 100l});
		        inputHandler.send(new Object[]{"ABC", 700f, 100l});
		        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
		        inputHandler.send(new Object[]{"DEF", 700f, 100l});
		        inputHandler.send(new Object[]{"ABC", 700f, 100l});
		        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
		        inputHandler.send(new Object[]{"DEF", 700f, 100l});
		        
		        System.out.println("sent events...");
		          
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        executionPlanRuntime.shutdown();
        System.out.println("The end....");
    }
    
}
