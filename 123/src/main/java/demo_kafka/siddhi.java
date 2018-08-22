package demo_kafka;

public class siddhi {

	
	  String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " + 
              " " +
              "@info(name = 'query1') " +
              "from StockEventStream#window.time(5 sec)  " +
              "select symbol, sum(price) as price, sum(volume) as volume " +
              "group by symbol " +
              "insert into AggregateStockStream ;";

	  SiddhiManager siddhiManager = new SiddhiManager();
	  SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

}
