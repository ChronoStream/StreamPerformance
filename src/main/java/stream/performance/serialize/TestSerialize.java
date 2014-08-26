//package stream.performance.serialize;
//
//
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;
//
//import stream.performance.mytest.mymap.Mymap;
//
//public class TestSerialize {
//
//	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
//	static Random rnd = new Random();
//	static int stringLength=20;
//	static int tupleNum=2000000;
//	
//	static String randomString( int len ) 
//	{
//	   StringBuilder sb = new StringBuilder( len );
//	   for( int i = 0; i < len; i++ ) 
//	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
//	   return sb.toString();
//	}
//	
//	public static void main(String[] args) throws Exception {
//		Map<String, String> map = new HashMap<String, String>();
//		for(int i=0; i<tupleNum; ++i){
//			map.put(randomString(stringLength), randomString(stringLength));
//		}
//        Mymap.Builder mapmap=Mymap.newBuilder();
//        long beginTime=System.currentTimeMillis();
//        
////        FileOutputStream fos = new FileOutputStream("map.ser");
////        ObjectOutputStream oos = new ObjectOutputStream(fos);
////        oos.writeObject(map);
////        oos.close();
//        
//        for(String key : map.keySet()){
//        	mapmap.addMykey(key);
//            mapmap.addMyvalue(map.get(key));
//        }
//        FileOutputStream output = new FileOutputStream("map.ser");
//        mapmap.build().writeTo(output);
//        output.close();
//        
//        long endTime=System.currentTimeMillis();
//        System.out.println("elapsed time="+((endTime-beginTime)/1000.0)+"s");
//        System.out.println("total length="+(2*tupleNum*stringLength/1000.0)+"KB");
//        
////        FileInputStream fis = new FileInputStream("map.ser");
////        ObjectInputStream ois = new ObjectInputStream(fis);
////        Map anotherMap = (Map) ois.readObject();
////        ois.close();
////
////        System.out.println(anotherMap);
//	}
//
//}
