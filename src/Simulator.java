import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Simulator implements Runnable{
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Random user=new Random();
		int n=user.nextInt(1000)+1000;
		ExecutorService executor = Executors.newFixedThreadPool(n);
		for(int i=1;i<=n;i++){
			Runnable sim=new Simulator();
			executor.execute(sim);
		}
		executor.shutdown();
		

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Random wait=new Random();
		try {
			Thread.sleep(wait.nextInt(2000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ProducerApi prod=new ProducerApi();
		String user=new String("zemoso"+wait.nextInt(10000));
		prod.send("myapp2", user, user+" has logged in"+" "+new Timestamp(System.currentTimeMillis()));
		System.out.println(user+" has logged in "+new Timestamp(System.currentTimeMillis()));
		int n=wait.nextInt(100);
		while(n>0){
			n--;
			try {
				Thread.sleep(wait.nextInt(10000));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			prod.send("myapp2", user,user+" has selected item"+String.valueOf((wait.nextInt(7)+1))+" "+new Timestamp(System.currentTimeMillis()));
			System.out.println(user+" has selected item"+String.valueOf((wait.nextInt(7)+1))+" "+new Timestamp(System.currentTimeMillis()));
		}
		
		try {
			Thread.sleep(wait.nextInt(2000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(user+" has logged out "+new Timestamp(System.currentTimeMillis()));
	}

}
