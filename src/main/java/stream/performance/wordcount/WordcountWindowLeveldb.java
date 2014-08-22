package stream.performance.wordcount;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordcountWindowLeveldb extends BaseRichBolt {

	private static final long serialVersionUID = 8408647992958409026L;
	private int _windowSize;
	private int _slidingStep;
	private DB database;
	private OutputCollector _collector;

	public WordcountWindowLeveldb(int windowSize, int slidingStep) {
		_windowSize = windowSize;
		_slidingStep = slidingStep;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		Options options = new Options();
		options.createIfMissing(true);
		try {
			database = factory.open(new File("wordcount"), options);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		long progress = input.getLong(1);
		int count = 1;
		String tmp="";
		if((tmp=asString(database.get(bytes(word))))!=null){
			count+=Integer.valueOf(tmp);
		}
		database.put(bytes(word), bytes(String.valueOf(count)));
		_collector.emit(new Values(word, count, progress));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count", "progress"));
	}
}
