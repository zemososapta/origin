package stormTest;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class loginBolt implements IRichBolt {
	private OutputCollector collector;
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String[] sentence=input.getString(0).split(" ");
		ArrayList<String> arr=new ArrayList<String>();
		for(String  word : sentence){
			//word.toLowerCase();
			//word.trim();
			arr.add(word);
		}
		if(arr.contains("logged") && arr.contains("in")){
			collector.emit(new Values(sentence[0]+" "+sentence[4]+" "+sentence[5]));
		}
		collector.ack(input);
		
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	      declarer.declare(new Fields("user"));

		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
