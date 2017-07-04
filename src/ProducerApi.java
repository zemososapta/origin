import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApi {
	private Properties props;
	private Producer<String,String> prod;
	ProducerApi(){
	props= new Properties();
	//set server conf in the properties file
    props.put("bootstrap.servers", "localhost:9092");
    //Set acknowledgements for producer requests.      
    props.put("acks", "all");
    //If the request fails, the producer can automatically retry,
    props.put("retries", 0);
      
    //Specify buffer size in config
    props.put("batch.size", 16384);
      
    //Reduce the no of requests less than 0   
    props.put("linger.ms", 1);
      
    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
    props.put("buffer.memory", 33554432);
      
    props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
         
    props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
    prod=new KafkaProducer(props);
    prod.flush();
	}
	public void send(String topic,String user,String msg){
		prod.send(new ProducerRecord<String,String >(topic,user,msg));
	}
   
    

}
