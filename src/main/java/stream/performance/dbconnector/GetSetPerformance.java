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

	static Random rand = new Random();
	static final int stringLength = 20;
	static final int round = 100000;
	static final int maxInt = 10000;
	static final int granularity = 1000;

	static Connection connect = null;
	static Statement statement = null;
	static PreparedStatement insertStatement = null;
	static PreparedStatement updateStatement = null;
	static PreparedStatement selectStatement = null;

	public static void prepare() throws SQLException {
		// connect = DriverManager.getConnection(
		// "jdbc:mysql://localhost:3306/testdb", "root", "");
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		// connect.setAutoCommit(true);
		statement = connect.createStatement();
		statement.executeUpdate("create table mygetset(mykey varchar("
				+ String.valueOf(stringLength)
				+ "), myvalue1 int, myvalue2 bigint, myvalue3 varchar("
				+ String.valueOf(stringLength) + "), myvalue4 int)");
		statement
				.executeUpdate("create index myindex on mygetset(myvalue1)");
		insertStatement = connect
				.prepareStatement("insert into mygetset values(?, ?, ?, ?, ?)");
		updateStatement = connect
				.prepareStatement("update mygetset set mykey=? where myvalue1=?");
		selectStatement = connect
				.prepareStatement("select mykey, myvalue1 from mygetset");
		selectStatement.setFetchSize(1000);
	}

	public static void cleanup() throws SQLException {
		statement.executeUpdate("drop table mygetset");
		insertStatement.close();
		statement.close();
		connect.close();
	}

	public static void setInsert() throws Exception {
		insertStatement.setString(1,
				RandomString.generateRandomString(stringLength));
		insertStatement.setInt(2, rand.nextInt(maxInt));
		insertStatement.setLong(3, rand.nextLong());
		insertStatement.setString(4,
				RandomString.generateRandomString(stringLength));
		insertStatement.setInt(5, rand.nextInt(maxInt));
		insertStatement.addBatch();
	}

	public static void executeInsert() throws Exception {
		insertStatement.executeBatch();
	}

	public static void testInsert() throws Exception {
		long startTime, endTime;
		double elapsedMS;
		// insert
		startTime = System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			setInsert();
			if (i % granularity == 0) {
				executeInsert();
			}
		}
		executeInsert();
		endTime = System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database insert elapsedTime=" + elapsedMS + "ms");
		System.out.println("insert per tuple latency=" + (elapsedMS * 1000.0 / round)
				+ "us");
	}

	public static void testUpdate() throws SQLException {
		long startTime, endTime;
		double elapsedMS;
		// insert
		startTime = System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			updateStatement.setString(1,
					RandomString.generateRandomString(stringLength));
			updateStatement.setInt(2, rand.nextInt(maxInt));
			updateStatement.addBatch();
			if (i % granularity == 0) {
				updateStatement.executeBatch();
			}
		}
		updateStatement.executeBatch();
		endTime = System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database update elapsedTime=" + elapsedMS + "ms");
		System.out.println("update per tuple latency=" + (elapsedMS * 1000.0 / round)
				+ "us");
	}

	public static void testSelect() throws SQLException {
		long startTime, endTime;
		double elapsedMS;
		startTime = System.currentTimeMillis();
		ResultSet result = selectStatement.executeQuery();
		Map<String, Integer> mylist = new HashMap<String, Integer>();
		int count = 0;
		while (result.next()) {
			mylist.put(result.getString(1), result.getInt(2));
			++count;
		}
		endTime = System.currentTimeMillis();
		elapsedMS = endTime - startTime;
		System.out.println("database select elapsedTime=" + elapsedMS + "ms");
		System.out.println("select per tuple latency="
				+ (elapsedMS * 1000.0 / mylist.size()) + "us");
		System.out.println("count=" + count);
	}

	public static void main(String[] args) throws Exception {
//		Thread.sleep(15000);
		prepare();
		testInsert();
		testUpdate();
//		testSelect();
		cleanup();
//		Thread.sleep(100000);
	}
}
