package stream.performance.wordcount;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
	private HashMap<String, Long> _wordWindow;
	private HashMap<String, Integer> _wordcountMap;
	private OutputCollector _collector;

	public WordcountWindow(int windowSize, int slidingStep) {
		_windowSize = windowSize;
		_slidingStep = slidingStep;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_wordcountMap = new HashMap<String, Integer>();
		_wordWindow = new HashMap<String, Long>();
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
		//remove expired tuples
		LinkedList<String> expiredWords=new LinkedList<String>();
		for (Iterator<Map.Entry<String, Long>> it = _wordWindow.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = it.next();
			if (progress-entry.getValue()>_windowSize) {
				expiredWords.add(entry.getKey());
				it.remove();
			}
		}
		for(String expiredWord : expiredWords){
			if(_wordcountMap.containsKey(word)){
				int expiredCount=_wordcountMap.get(expiredWord);
				if(expiredCount==1){
					_wordcountMap.remove(expiredWord);
				}else{
					_wordcountMap.put(expiredWord, expiredCount-1);
				}
			}
		}
		//add sentence to window
		_wordWindow.put(word, progress);
		//emit tuples
		for(String emitWord : _wordcountMap.keySet()){
			_collector.emit(new Values(emitWord, _wordcountMap.get(emitWord), progress));
		}
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count", "progress"));
	}
}
