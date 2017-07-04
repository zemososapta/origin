package stormTest;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class itemsplitterBolt implements IRichBolt{
	OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String[] sentence=input.getString(0).split(" ");
		ArrayList<String> arr=new ArrayList<String>();
		for(String  word : sentence){
			//word.toLowerCase();
			//word.trim();
			arr.add(word);
		}
		if(arr.contains("selected")){
			collector.emit(new Values(sentence[0]+" "+sentence[3]));
		}
		collector.ack(input);
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("words"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
