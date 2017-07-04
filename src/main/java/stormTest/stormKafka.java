package stormTest;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;

//import backtype.storm.generated.DistributedRPC.Client;

import org.apache.storm.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
//import org.apache.storm.
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.*;
public class stormKafka {

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub
		Config config = new Config();
	      config.setDebug(true);
	      config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	      String zkConnString = "localhost:2181";
	      String topic = "myapp2";
	      BrokerHosts hosts = new ZkHosts(zkConnString);	      
	      SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,    
	         UUID.randomUUID().toString());
	      kafkaSpoutConfig.bufferSizeBytes = 8 * 1024 * 1024 * 4;
	      kafkaSpoutConfig.fetchSizeBytes =  8 * 1024 * 1024 * 4;
	      kafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;	     
	      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme((Scheme) new StringScheme());	  
	      TopologyBuilder builder = new TopologyBuilder();
	      builder.setSpout("kafka-spout", (IRichSpout) new KafkaSpout(kafkaSpoutConfig));
	      builder.setBolt("login-spitter", (IRichBolt) new loginBolt()).shuffleGrouping("kafka-spout");
	      builder.setBolt("login-counter", (IRichBolt) new loginBoltUtil()).shuffleGrouping("login-spitter");	         
	      builder.setBolt("items-spitter", (IRichBolt) new itemsplitterBolt()).shuffleGrouping("kafka-spout");	         
	      builder.setBolt("items-counter", (IRichBolt) new itemCountBolt()).shuffleGrouping("items-spitter");	         
	      builder.setBolt("items-counter-per-user", (IRichBolt) new itemPerUserBolt()).shuffleGrouping("items-spitter");
	      LocalCluster cluster = new LocalCluster();
	      System.setProperty("storm.jar", "/home/zemoso/apache-storm/apache-storm-1.1.0/lib/storm-core-1.1.0.jar");	      
	      cluster.submitTopology("KafkaStormSample2", config, builder.createTopology());    	      
	      Thread.sleep(150000);
	      cluster.killTopology("KafkaStormSample2");
          cluster.shutdown();   

	}

}
