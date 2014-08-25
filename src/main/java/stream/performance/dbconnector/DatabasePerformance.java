package stream.performance.dbconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import stream.performance.toolkits.MemoryReport;
import stream.performance.toolkits.RandomString;

public class DatabasePerformance {

	static Random rand=new Random();
	static final int stringLength=20;
	static final int round=100000;
	static final int maxInt=10000;
	static final int granularity=20;
	
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
		//******************************
		PreparedStatement pStatement=connect.prepareStatement("insert into mykeyvalue values(?, ?)");
		testBatchInsert(pStatement);
		pStatement.close();
		//******************************
		PreparedStatement sumStatement=connect.prepareStatement("select avg(myvalue) as myavg from mykeyvalue");
		testDatabaseQuery1(sumStatement);
		sumStatement.close();
		//******************************
		PreparedStatement retrieveStatement=connect.prepareStatement("select myvalue from mykeyvalue");
		testDatabaseQuery2(retrieveStatement);
		retrieveStatement.close();
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
		//******************************
		PreparedStatement insertStatement=connect.prepareStatement("insert into mykeyvalue values(?, ?)");
		testBatchInsert(insertStatement);
		insertStatement.close();
		//******************************
		PreparedStatement sumStatement=connect.prepareStatement("select avg(myvalue) as myavg from mykeyvalue");
		testDatabaseQuery1(sumStatement);
		sumStatement.close();
		//******************************
		PreparedStatement retrieveStatement=connect.prepareStatement("select myvalue from mykeyvalue");
		testDatabaseQuery2(retrieveStatement);
		retrieveStatement.close();
		//******************************
		statement.close();
		connect.close();
	}
	
	public static void testDatabaseInsert(Statement statement) throws SQLException{
		String sql;
		long startTime, endTime;
		double elapsedMS;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue values('" + key + "'," + value.toString() + ")";
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database insert elapsedTime="+elapsedMS+"ms");
		System.out.println("per tuple latency="+(elapsedMS*1000.0/round)+"ns");
	}
	
	public static void testBatchInsert(PreparedStatement statement) throws SQLException{
		long startTime, endTime;
		double elapsedMS;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			statement.setString(1, key);
			statement.setInt(2, value);
			statement.addBatch();
			if (i % granularity == 0) {
				statement.executeBatch();
			}
		}
		statement.executeBatch();
		
		endTime=System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database insert elapsedTime="+elapsedMS+"ms");
		System.out.println("per tuple latency="+(elapsedMS*1000.0/round)+"ns");
	}	
	
	public static void testDatabaseQuery1(PreparedStatement statement) throws SQLException{		
		long startTime, endTime;
		double elapsedMS;
		ResultSet result = null;
		startTime=System.currentTimeMillis();
		result = statement.executeQuery();
		int average = 0;
		while(result.next()){
			average=result.getInt("myavg");
			System.out.println("the average value is "+average);
		}
		endTime=System.currentTimeMillis();
		elapsedMS=endTime-startTime;
		System.out.println("database average computation elapsedTime="+elapsedMS+"ms");
		result.close();
	}
	
	public static void testDatabaseQuery2(PreparedStatement statement) throws SQLException{
		long startTime, endTime;
		double elapsedMS;
		ResultSet result = null;
		startTime=System.currentTimeMillis();
		result = statement.executeQuery();
		long sum = 0;
		int count = 0;
		while (result.next()) {
			sum += result.getInt("myvalue");
			++count;
		}
		System.out.println("the average value is " + sum * 1.0 / count);
		endTime=System.currentTimeMillis();
		elapsedMS=endTime-startTime;
		System.out.println("database retrieve average computation elapsedTime="+elapsedMS+"ms");
		result.close();
	}
	
	public static void testLib(){
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		long startTime, endTime;
		double elapsedMS;
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<round; ++i){
			heapMap.put(RandomString.generateRandomString(stringLength), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedMS=endTime-startTime;

		System.out.println("library insert elapsedTime="+elapsedMS+"ms");
		System.out.println("per tuple latency="+(elapsedMS*1000/round)+"ns");
		
		System.out.println("real memory="+(stringLength+4)*round/1024/1024+"MB");
		MemoryReport.reportStatus();
		
		//scan
		startTime=System.currentTimeMillis();
		long sum = 0L;
		for(Integer key : heapMap.values()){
			sum+=key;
		}
		int average=(int) (sum/heapMap.size());
		endTime=System.currentTimeMillis();
		elapsedMS=endTime-startTime;
		System.out.println("average="+average+", heap average computation elapsedTime="+elapsedMS+"ms");
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
