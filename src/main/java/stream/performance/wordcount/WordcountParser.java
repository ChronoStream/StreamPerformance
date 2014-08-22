package stream.performance.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordcountParser extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		long progress = input.getLong(1);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
			if (word.length() < 20) {
				_collector.emit(new Values(word, progress));
			}
		}
		_collector.ack(input);
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "progress"));
	}

}
