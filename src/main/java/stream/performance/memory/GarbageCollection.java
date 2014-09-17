package stream.performance.memory;

import java.util.HashMap;
import java.util.Map;

import stream.performance.toolkits.MemoryReport;
import stream.performance.toolkits.RandomString;

public class GarbageCollection {

	public static void testGC() throws InterruptedException{
		Map<String, String> mylist=new HashMap<String, String>();
		for(int i=0; i<1000000; ++i){
			mylist.put(RandomString.generateRandomString(20), RandomString.generateRandomString(20));
		}
//		System.gc();
//		Thread.sleep(1000);
		MemoryReport.reportStatus();
		System.out.println("=============");
		Map<String, String> mylist1=new HashMap<String, String>();
		for(int i=0; i<1000000; ++i){
			mylist1.put(RandomString.generateRandomString(20), RandomString.generateRandomString(20));
		}
//		System.gc();
		Thread.sleep(1000);
		MemoryReport.reportStatus();
//		System.gc();
		System.out.println(mylist.size());
	}
	
	public static void main(String[] args) throws InterruptedException {
		MemoryReport.reportStatus();
		for(int i=0; i<10; ++i){
		testGC();
		}
	}
}
