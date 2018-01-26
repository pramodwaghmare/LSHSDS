package flume.agent;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Scanner;

public class FlumeSource {
  public static void main(String[] args) throws Exception {
    MyRpcClientFacade client = new MyRpcClientFacade();
    // Initialize client with the remote Flume agent's host and port
    client.init("127.0.0.1", 41414);

    
    ArrayList<String> arrayRequest= new ArrayList<String>();
    Scanner s = new Scanner(new File("DATA/ECG200/ECG200_TEST"));
    while (s.hasNext()){
    	arrayRequest.add(s.next());
    }
    s.close();
    // Send 5 events to the remote Flume agent. That agent should be
    // configured to listen with an AvroSource.
    while(true){
    	String record=arrayRequest.get((int)(Math.random()*arrayRequest.size()));
        client.sendDataToFlume(record);
      //  System.out.println("Sending records: "+ecgrecord);
        try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    }
    

  // client.cleanUp();
  }
}

class MyRpcClientFacade {
  private RpcClient client;
  private String hostname;
  private int port;

  public void init(String hostname, int port) {
    // Setup the RPC connection
    this.hostname = hostname;
    this.port = port;
    this.client = RpcClientFactory.getDefaultInstance(hostname, port);
    // Use the following method to create a thrift client (instead of the above line):
    // this.client = RpcClientFactory.getThriftInstance(hostname, port);
  }

  public void sendDataToFlume(String data) {
    // Create a Flume Event object that encapsulates the sample data
    Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

    // Send the event
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      // clean up and recreate the client
      client.close();
      client = null;
      client = RpcClientFactory.getDefaultInstance(hostname, port);
      // Use the following method to create a thrift client (instead of the above line):
      // this.client = RpcClientFactory.getThriftInstance(hostname, port);
    }
  }

  public void cleanUp() {
    // Close the RPC connection
    client.close();
  }

}