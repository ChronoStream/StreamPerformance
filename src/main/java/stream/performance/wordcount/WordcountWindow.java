package stream.performance.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordcountWindow extends BaseRichBolt {

	private static final long serialVersionUID = 8408647992958409026L;
	private int _windowSize;
	private int _slidingStep;
	private HashMap<String, Integer> _wordcountMap;
	private OutputCollector _collector;

	public WordcountWindow(int windowSize, int slidingStep) {
		_windowSize = windowSize;
		_slidingStep = slidingStep;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_wordcountMap = new HashMap<String, Integer>();
		_collector = collector;
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		long progress = input.getLong(1);
		int count=1;
		if(_wordcountMap.containsKey(word)){
			count+=_wordcountMap.get(word);
		}
		_wordcountMap.put(word, count);
		_collector.emit(new Values(word, count, progress));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count", "progress"));
	}
}
