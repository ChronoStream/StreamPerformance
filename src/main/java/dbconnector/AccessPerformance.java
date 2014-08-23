package dbconnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class AccessPerformance {

	static Random rand=new Random();
	static final int round=1000000;
	static final int maxInt=100000;
	static final int stringLength=20;

	public static Map<String, Integer> kvPairs = new HashMap<String, Integer>();
	
	public static void populate(){
		for(int i=0; i<round; ++i){
			kvPairs.put(RandomString.generateRandomString(stringLength), rand.nextInt(maxInt));
		}
	}
	
	public static void sumAggregate(){
		long count=0;
		for(String key : kvPairs.keySet()){
			count+=kvPairs.get(key);
		}
		System.out.println("count="+count);
	}
	
	public static void main(String[] args) {
		long startTime, endTime, elapsedTime;
		startTime=System.currentTimeMillis();
		populate();
		endTime=System.currentTimeMillis();
		elapsedTime=endTime-startTime;
		System.out.println("elapsed time="+elapsedTime+"ms");
		System.out.println("per tuple latency="+elapsedTime*1000.0/round+"ns");
		System.out.println("=============================");
		startTime=System.currentTimeMillis();
		populate();
		endTime=System.currentTimeMillis();
		elapsedTime=endTime-startTime;
		System.out.println("elapsed time="+elapsedTime+"ms");
		System.out.println("per tuple latency="+elapsedTime*1000.0/round+"ns");		
	}

}
