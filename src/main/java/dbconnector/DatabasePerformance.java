package dbconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DatabasePerformance {

	static Random rand=new Random();
	static final int stringLength=20;
	static final int round=10000;
	static final int maxInt=100000;
	static final int granularity=2000;
	
	public static void testDB() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "";
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		statement.executeUpdate("drop table mykeyvalue");
		statement.executeUpdate("create table mykeyvalue(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
		//testDatabaseInsert(statement);
		//******************************
		PreparedStatement pStatement=connect.prepareStatement("insert into mykeyvalue values(?, ?)");
		testBatchInsert(pStatement);
		pStatement.close();
		//******************************
		statement.close();
		connect.close();
	}
	
	public static void testSqlite() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		statement.executeUpdate("create table mykeyvalue(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//testDatabaseInsert(statement);
		//******************************
		PreparedStatement pStatement=connect.prepareStatement("insert into mykeyvalue values(?, ?)");
		testBatchInsert(pStatement);
		pStatement.close();
		//******************************
		statement.close();
		connect.close();
	}
	
	public static void testDatabaseInsert(Statement statement) throws SQLException{
		String sql;
		long startTime, endTime;
		double elapsedSeconds;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue values('" + key + "'," + value.toString() + ")";
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		System.out.println("per tuple latency="+((endTime-startTime)*1000.0/round)+"ns");
	}
	
	public static void testBatchInsert(PreparedStatement statement) throws SQLException{
		long startTime, endTime;
		double elapsedSeconds;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			statement.setString(1, key);
			statement.setInt(2, value);
			if (i % granularity == 0) {
				statement.addBatch();
				statement.executeBatch();
			}
		}
		
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		System.out.println("per tuple latency="+((endTime-startTime)*1000.0/round)+"ns");
	}	
	
	public static void testDatabaseQuery(Statement statement) throws SQLException{		
		String sql;
		long startTime, endTime;
		double elapsedSeconds;
		ResultSet result = null;
		//scan
		startTime=System.currentTimeMillis();
		sql = "select avg(myvalue) as mysum from mykeyvalue";
		result = statement.executeQuery(sql);
		int mysum = 0;
		while(result.next()){
			mysum=result.getInt("mysum");
			System.out.println("value="+mysum);
		}

		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database scan elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//retrieve scan
		startTime=System.currentTimeMillis();
		sql = "select myvalue from mykeyvalue";
		result = statement.executeQuery(sql);
		long mysum1 = 0;
		int count=0;
		while(result.next()){
			mysum1+=result.getInt("myvalue");
			count+=1;
		}
		System.out.println("count="+count+", value="+mysum1/count);

		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database retrieve scan elapsedTime="+elapsedSeconds+"s");
		result.close();
		
	}
	
	public static void testLib(){
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		long startTime, endTime;
		double elapsedSeconds;
		
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<round; ++i){
			heapMap.put(RandomString.generateRandomString(stringLength), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		System.out.println("per tuple latency="+((endTime-startTime)*1000.0/round)+"ns");
		
		final long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.out.println("real memory="+(stringLength+4)*round/1024/1024+"MB");
		System.out.println("used memory="+usedMem/1024/1024 +"MB");
		System.out.println("Max memory="+Runtime.getRuntime().maxMemory()/1024/1024+"MB");
		
		//scan
//		startTime=System.currentTimeMillis();
//		int sum=0;
//		for(Integer key : heapMap.values()){
//			sum+=key;
//		}		
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		
//		System.out.println("sum="+sum+", library scan elapsedTime="+elapsedSeconds+"s");
	}
	
	
	
	public static void main(String[] args) throws Exception{
		System.out.println("==========library======");
		testLib();
		System.out.println("==========database========");
		testDB();
		System.out.println("==========sqlite=========");
		testSqlite();
	}

}
