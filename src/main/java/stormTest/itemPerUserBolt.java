package stormTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class itemPerUserBolt implements IRichBolt{
	Map<String,Map<String,Integer>> map;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		map=new HashMap();		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String,Integer> rec=new HashMap<String,Integer>();
		String[] sentence=input.getString(0).split(" ");
		String user=sentence[0];
		String item=sentence[1];
		if(map.containsKey(user)){
			rec=map.get(user);
			if(rec.containsKey(item)){
				int val=rec.get(item);
				rec.put(item, val+1);
			}
			else{
				rec.put(item, 1);
			}
			}
		else{
			rec.put(item, 1);
			map.put(user, rec);
		}
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		ArrayList<User> user=new ArrayList();
		map.forEach((k,v)->{
			String userID=k;
			Map<String,Integer> tempMap=v;
			int total=0;
			for (Map.Entry<String, Integer> entry : tempMap.entrySet()) {
				total+=entry.getValue();
			}
			User temp=new User(userID,total);
			user.add(temp);
		});
		Collections.sort(user, new Comparator<User>(){

			@Override
			public int compare(User user1, User user2) {
				// TODO Auto-generated method stub
				return user2.getCount()-user1.getCount();
			}
			
		});
		BufferedWriter bw;
		FileWriter fw; 
		try {
			String content = "This is the content to write into file\n";
			fw =new FileWriter("user_report.txt");
			bw  = new BufferedWriter(fw);
			bw.write("Most valued users:\n");
			System.out.println("Done");
			for(int i=0;i<10;i++){
				bw.write(user.get(i).getUserID()+" Items purchased: "+user.get(i).getCount()+"\n");
			}
			bw.write("Least valued users:");
			for(int i=0;i<10;i++){
				bw.write(user.get(user.size()-i-1).getUserID()+" Items purchased: "+user.get(user.size()-i-1).getCount()+"\n");
			}
			bw.close();
			fw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
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
