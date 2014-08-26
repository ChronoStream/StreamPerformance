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

import stream.performance.toolkits.RandomString;

public class GetSetPerformance {

	static Random rand=new Random();
	static final int stringLength=20;
	static final int round=50000;
	static final int maxInt=10000;
	static final int granularity=20;
	
	static Connection connect = null;
	static Statement statement = null;
	static PreparedStatement insertStatement = null;
	static PreparedStatement selectStatement = null;
	
	public static void prepare() throws SQLException{
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "";
		connect = DriverManager.getConnection(url, user, password);
		connect.setAutoCommit(true);
		statement = connect.createStatement();
		statement.executeUpdate("create table mygetset(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
		insertStatement=connect.prepareStatement("insert into mygetset values(?, ?)");
		selectStatement=connect.prepareStatement("select * from mygetset");
		selectStatement.setFetchSize(500);
	}
	
	public static void cleanup() throws SQLException{
		statement.executeUpdate("drop table mygetset");
		insertStatement.close();
		statement.close();
		connect.close();
	}
	
	public static void testInsert() throws SQLException{
		long startTime, endTime;
		double elapsedMS;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = RandomString.generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			insertStatement.setString(1, key);
			insertStatement.setInt(2, value);
			insertStatement.addBatch();
			if (i % granularity == 0) {
				insertStatement.executeBatch();
			}
		}
		insertStatement.executeBatch();
		endTime=System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database insert elapsedTime="+elapsedMS+"ms");
		System.out.println("per tuple latency="+(elapsedMS*1000.0/round)+"us");
	}
	
	public static void testSelect() throws SQLException{
		long startTime, endTime;
		double elapsedMS;
		startTime=System.currentTimeMillis();
		ResultSet result=selectStatement.executeQuery();
		Map<String, Integer> mylist=new HashMap<String, Integer>();
		while(result.next()){
			mylist.put(result.getString("mykey"), result.getInt("myvalue"));
		}
		endTime=System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database insert elapsedTime="+elapsedMS+"ms");
		System.out.println("per tuple latency="+(elapsedMS*1000.0/mylist.size())+"us");
	}
	
	public static void main(String[] args) throws Exception{
		prepare();
		testInsert();
		testSelect();
		cleanup();
	}
}
