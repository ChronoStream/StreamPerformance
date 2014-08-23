package stream.performance.wordcount;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordcountStatelessMysql extends BaseRichBolt {

	private static final long serialVersionUID = 8408647992958409026L;
	private int _windowSize;
	private int _slidingStep;
	private OutputCollector _collector;

	Connection connect = null;
	Statement statement = null;
	ResultSet result = null;
	String sql;

	public WordcountStatelessMysql(int windowSize, int slidingStep) {
		_windowSize = windowSize;
		_slidingStep = slidingStep;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;

		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "yingjun";
		try {
			connect = DriverManager.getConnection(url, user, password);
			statement = connect.createStatement();
			statement.executeUpdate("drop table wordcount");
			statement
					.executeUpdate("create table wordcount(word varchar(20), count int) engine=memory");
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		long progress = input.getLong(1);
		int count = 1;
		sql = "select count from wordcount where word='" + word + "'";
		try {
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				count += rs.getInt(1);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		sql = "insert into wordcount values('" + word + "'," + count + ")";
		try {
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		_collector.emit(new Values(word, count, progress));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count", "progress"));
	}
}
