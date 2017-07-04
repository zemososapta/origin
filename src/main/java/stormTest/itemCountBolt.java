package stormTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class itemCountBolt implements IRichBolt {
	HashMap<String,Long> map;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		map=new HashMap();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String[] sentence = input.getString(0).split(" ");
		String item=sentence[1];
		Long value=1L;
		if(map.containsKey(item)){
			value=map.get(item);
			map.put(item, value+1);
		}
		else{
			map.put(item, value);
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		BufferedWriter bw=null;
		FileWriter fw=null;		
		long maxValue;
		long minValue;
		try {
			String content="This is the content to write into file\n";			
			maxValue=0;
			minValue=99999999;			
			fw =new FileWriter("items_report.txt");
			bw =new BufferedWriter(fw);
			bw.write("Items sold:");
			for(Entry<String, Long> m:map.entrySet()){
				String item=m.getKey();
				Long value=m.getValue();
				bw.write(item+" : "+value);
				bw.write("\n");
				System.out.println(item+" : "+value);
				if(maxValue<value){
					maxValue=value;
				}
				if(minValue>value){
					minValue=value;
				}
			}
			for(Entry<String, Long> m:map.entrySet()){
				String item=m.getKey();
				Long value=m.getValue();
				if(value==maxValue){
					bw.write("Most Puchased item : "+item+" : "+value);
					bw.write("\n");
					System.out.println("Most Puchased Item : "+item+" : "+value);
				}
			}
			for(Entry<String, Long> m:map.entrySet()){
				String item=m.getKey();
				Long value=m.getValue();
				if(value==minValue){
					bw.write("Least Puchased item : "+item+" : "+value);
					System.out.println("Least Puchased Item : "+item+" : "+value);
				}
			}		
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			try {
				bw.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
