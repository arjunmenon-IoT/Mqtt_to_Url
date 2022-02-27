package api;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttSubscriber  implements MqttCallback {

	 float value = 0;

	private static final String brokerUrl = "tcp://193.49.165.77"; // EMSE MQTT broker Ip.
	List <String> isalreadyprocessed =new ArrayList<String>();
	/** The topic. */
	
	private static final String topic_S424 = "emse/fayol/e4/S424/sensors/24a89ddc-23c8-4d9f-9f5e-cff4eba32fb5/metrics/TEMP";
	private static final String topic_S431H = "emse/fayol/e4/S431H/sensors/03f5ca58-aa70-47b3-980c-c8f486cac9ee/metrics/TEMP";
	private static final String topic_S423 = "emse/fayol/e4/S423/sensors/8aa60f58-fc6f-49e2-a53a-f5cc96bb9021/metrics/TEMP";
	private static final String topic_S421 = "emse/fayol/e4/S421/sensors/7b051d3a-8547-463d-9d28-a2d50c5098b4/metrics/TEMP";
	private static final String topic_s422 ="emse/fayol/e4/S422/sensors/88cb0522-478a-456c-b63b-9c402b5e03b2/metrics/TEMP";
	private static final String topic_s425 ="emse/fayol/e4/S425/sensors/70345659-3f50-49af-98e7-bbc93961df92/metrics/TEMP";
	private static final String topic_s431F ="emse/fayol/e4/S431F/sensors/f9538ac8-4fdb-4049-9ff6-ac4855e3bcc5/metrics/TEMP";
	private static final String topic_s405 ="emse/fayol/e4/S405/sensors/6bd134b6-339c-4168-9aeb-ae7d0f236851/metrics/TEMP";
	private static final String topic_s432  = "emse/fayol/e4/S432/sensors/140ade6c-4418-4d86-a14e-25b7db5ae83b/metrics/TEMP";
	private static final String topic_s416 = "emse/fayol/e4/S416/sensors/28bb16da-5d54-4882-9c2b-70c746586185/metrics/TEMP";
	public static  void main(String[] args) throws IOException {

		System.out.println("Subscriber running");
		//new MqttSubscriber().subscribe(topic_S424);
		MqttSubscriber subscribe =new MqttSubscriber();
		MqttSubscriber subscribe2 =new MqttSubscriber();
		MqttSubscriber subscribe3 =new MqttSubscriber();
		MqttSubscriber subscribe4 =new MqttSubscriber();
		MqttSubscriber subscribe5 =new MqttSubscriber();
		MqttSubscriber subscribe6 =new MqttSubscriber();
		MqttSubscriber subscribe7 =new MqttSubscriber();
		MqttSubscriber subscribe8 =new MqttSubscriber();
		MqttSubscriber subscribe9 =new MqttSubscriber();
		MqttSubscriber subscribe10 =new MqttSubscriber();



		
		subscribe.subscribe(topic_S424,"Raman");     // Topic with Client id
		System.out.println("S424 "+subscribe.getvalue());
		subscribe2.subscribe(topic_S431H,"Lakshmanan"); 
		System.out.println("S431H "+subscribe2.getvalue());
		subscribe3.subscribe(topic_S423,"Bhiman"); 
		System.out.println("S423 "+subscribe3.getvalue());
		subscribe4.subscribe(topic_S421,"Trump"); 
		System.out.println("S421 "+subscribe4.getvalue());
		subscribe5.subscribe(topic_s422,"Macroon"); 
		System.out.println("S422 "+subscribe5.getvalue());
		subscribe6.subscribe(topic_s425,"JeoBidan"); 
		System.out.println("S425 "+subscribe6.getvalue());
		subscribe7.subscribe(topic_s431F,"Putin"); 
		System.out.println("S431F "+subscribe7.getvalue());
		subscribe8.subscribe(topic_s405,"Fabien"); 
		System.out.println("S405 "+subscribe8.getvalue());
		subscribe9.subscribe(topic_s432,"Stalin"); 
		System.out.println("S432"+subscribe9.getvalue());
		subscribe10.subscribe(topic_s416,"krishan"); 
		System.out.println("S416 "+subscribe10.getvalue());

 
		do {

			

		

			URL url = new URL("http://localhost:8080/Demo/Temp?s431h="+subscribe2.getvalue()+"&s423="+subscribe3.getvalue()+"&s421="+subscribe4.getvalue()+"&s422="+subscribe5.getvalue()+"&s425="+subscribe6.getvalue()+"&s431f="+subscribe7.getvalue()+"&s405="+subscribe8.getvalue()+"&s424="+subscribe.getvalue()+"&s416="+subscribe10.getvalue()+"&s432="+subscribe9.getvalue()+"&s408=null&hall4nord=32.5");
			HttpURLConnection http = (HttpURLConnection)url.openConnection();
			http.setRequestMethod("PUT");
			http.setDoOutput(true);
			http.setRequestProperty("Content-Length", "0");

			
			
		}while (true);

	}







	

	public void subscribe(String topic,String number) {
		// logger file name and pattern to log
		MemoryPersistence persistence = new MemoryPersistence();
		
		try {

			MqttClient sampleClient = new MqttClient(brokerUrl, number, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);

			System.out.println("checking");
			System.out.println("Mqtt Connecting to broker: " + brokerUrl);

			sampleClient.connect(connOpts);
			System.out.println("Mqtt Connected");
			sampleClient.setCallback(this);
			sampleClient.subscribe(topic);
			 
	          
			System.out.println("Subscribed");
			System.out.println("Listening");

		} catch (MqttException me) {
			System.out.println(me);
		}
	}

	public float getvalue() {
		return value;
	}









	public void setvalue(float value) {
		this.value = value;
	}









	// Called when the client lost the connection to the broker
	public void connectionLost(Throwable arg0) {

	}

	// Called when a outgoing publish is complete
	public void deliveryComplete(IMqttDeliveryToken arg0) {

	}

	public void messageArrived(String topic, MqttMessage message) {
		
		
	
		this.value = Float.parseFloat(message.toString());	
		System.out.println(value + topic);	

	 
			
	}
	
	
	
	public static void wait(int ms) // delay method 
	{
	    try
	    {
	        Thread.sleep(ms);
	    }
	    catch(InterruptedException ex)
	    {
	        Thread.currentThread().interrupt();
	    }
	}

}
