package demo_kafka;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " + 
                " " +
                "@info(name = 'query1') " +
                "from StockEventStream#window.time(5 sec)  " +
                "select symbol, sum(price) as price, sum(volume) as volume " +
                "group by symbol " +
                "insert into AggregateStockStream ;";
        
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        
        siddhiAppRuntime.addCallback("AggregateStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        
      //Start SiddhiApp runtime
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 100f, 100L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 300L});
        inputHandler.send(new Object[]{"WSO2", 60f, 200L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 70f, 400L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 400L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 70f, 50L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 80f, 400L});
        inputHandler.send(new Object[]{"GOOG", 60f, 30L});
        Thread.sleep(1000);

        //Shutdown SiddhiApp runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi
        siddhiManager.shutdown();

    }
}
