package stormTest;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class NewBolt implements IRichBolt {
	Map<String, Integer> counters;
	private OutputCollector collector;

	public void cleanup() {
		// TODO Auto-generated method stub
		for(Map.Entry<String, Integer> entry:counters.entrySet()){
	         System.out.println(entry.getKey()+" : " + entry.getValue());
	      }
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str = input.getString(0);
	      
	      if(!counters.containsKey(str)){
	         counters.put(str, 1);
	      }else {
	         Integer c = counters.get(str) +1;
	         counters.put(str, c);
	      }
	   
	      collector.ack(input);
		
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.counters = new HashMap<String, Integer>();
	    this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
