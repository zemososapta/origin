package stormTest;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class loginBoltUtil implements IRichBolt{
	HashMap<String,String> map;
	private OutputCollector collect;
	public void cleanup() {
		// TODO Auto-generated method stub
		
		BufferedWriter bw;
		FileWriter fw; 
		try {
			String content = "This is the content to write into file\n";
			fw =new FileWriter("login_report.txt");
			bw  = new BufferedWriter(fw);
			bw.write("Logged in users:");
			System.out.println("Done");
			map.forEach((k,v)-> {
				try {
					bw.write(k+" "+v);
					bw.write("\n");
					System.out.println("Done");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			bw.write("total no of users: "+map.size());
			bw.close();
			fw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
		System.out.println("Logged in users:");
		map.forEach((k,v)-> System.out.println(k+" "+v));
		System.out.println("total no of users: "+map.size());
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String[] sentences=input.getString(0).split(" ");
		map.put(sentences[0].toLowerCase(), sentences[1]+" "+sentences[2]);
		collect.ack(input);			
		
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collect) {
		// TODO Auto-generated method stub
		map=new HashMap();
		this.collect=collect;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
