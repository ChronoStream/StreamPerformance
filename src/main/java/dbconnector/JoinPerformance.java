package dbconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class JoinPerformance {

	static Random rand=new Random();
	static final int stringLength=20;
	static final int round=1500000;
	static final int refround=100;
	static final int maxInt=100000;
	
	public static void sqliteJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String sql;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		statement.executeUpdate("create table mykeyvalue1(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < refround; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue1 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime=System.currentTimeMillis();
		sql = "select mykeyvalue1.mykey as resultkey from mykeyvalue0 inner join mykeyvalue1 on mykeyvalue0.mykey=mykeyvalue1.mykey";
		result = statement.executeQuery(sql);
		while(result.next()){
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();
	}
	
	public static void sqliteExternalJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String sql;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<refround; ++i){
			heapMap.put(RandomString.generateRandomString(stringLength), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime = System.currentTimeMillis();
		StringBuilder sb=new StringBuilder();
		for (String heapkey : heapMap.keySet()) {
			sb.append("'"+heapkey+"',");
		}
		sb.deleteCharAt(sb.length()-1);
		sql = "select mykeyvalue0.mykey as resultkey from mykeyvalue0 where mykeyvalue0.mykey in ("+sb.toString()+")";
		result = statement.executeQuery(sql);
		while (result.next()) {
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database array join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();		
	}
	
	
	public static void mysqlJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "yingjun";
		String sql;
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
//		statement.executeUpdate("drop table mykeyvalue0");
		statement.executeUpdate("drop table mykeyvalue1");
//		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int, index using hash (mykey)) engine=memory");
//		//insert
//		startTime=System.currentTimeMillis();
//		for (int i = 0; i < round; ++i) {
//			String key = generateRandomString();
//			Integer value = rand.nextInt(maxInt);
//			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
//			
//			statement.executeUpdate(sql);
//		}
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
//		//////////////////////////////////
		
		statement.executeUpdate("create table mykeyvalue1(mykey varchar("+String.valueOf(stringLength)+"), myvalue int, index using hash (mykey)) engine=memory");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < refround; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue1 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime=System.currentTimeMillis();
		sql = "select mykeyvalue1.mykey as resultkey from mykeyvalue0 inner join mykeyvalue1 on mykeyvalue0.mykey=mykeyvalue1.mykey";
		result = statement.executeQuery(sql);
		while(result.next()){
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();
	}
	
	public static void mysqlExternalJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "yingjun";
		String sql;
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
//		statement.executeUpdate("drop table mykeyvalue0");
//		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
//		//insert
//		startTime=System.currentTimeMillis();
//		for (int i = 0; i < round; ++i) {
//			String key = generateRandomString();
//			Integer value = rand.nextInt(maxInt);
//			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
//			
//			statement.executeUpdate(sql);
//		}
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
//		//////////////////////////////////
		
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<refround; ++i){
			heapMap.put(RandomString.generateRandomString(stringLength), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime = System.currentTimeMillis();
		StringBuilder sb=new StringBuilder();
		for (String heapkey : heapMap.keySet()) {
			sb.append("'"+heapkey+"',");
		}
		sb.deleteCharAt(sb.length()-1);
		sql = "select mykeyvalue0.mykey as resultkey from mykeyvalue0 where mykeyvalue0.mykey in ("+sb.toString()+")";
		result = statement.executeQuery(sql);
		while (result.next()) {
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database array join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();		
	}
}
