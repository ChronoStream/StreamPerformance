package stream.performance.wordcount;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
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
	private DB _wordWindow;
	private DB _wordcountMap;
	private OutputCollector _collector;

	public WordcountWindowLeveldb(int windowSize, int slidingStep) {
		_windowSize = windowSize;
		_slidingStep = slidingStep;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		Options options = new Options();
		options.createIfMissing(true);
		try {
			_wordcountMap = factory.open(new File("wordcount"), options);
			_wordWindow = factory.open(new File("window"), options);
		} catch (IOException e) {
			e.printStackTrace();
		}
		_collector = collector;
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		long progress = input.getLong(1);
		int count = 1;
		String tmp="";
		if((tmp=asString(_wordcountMap.get(bytes(word))))!=null){
			count+=Integer.valueOf(tmp);
		}
		_wordcountMap.put(bytes(word), bytes(String.valueOf(count)));
		//remove expired tuples
		LinkedList<String> expiredWords =new LinkedList<String>();
		DBIterator wwIter=_wordWindow.iterator();
		wwIter.seekToFirst();
		while(wwIter.hasNext()){
			if(progress-Long.valueOf(asString(wwIter.peekNext().getValue()))>_windowSize){
				expiredWords.add(asString(wwIter.peekNext().getKey()));
				wwIter.remove();
			}
			wwIter.next();
		}
		for(String expiredWord : expiredWords){
			if((tmp=asString(_wordcountMap.get(bytes(word))))!=null){
				if(Integer.valueOf(tmp)==1){
					_wordcountMap.delete(bytes(expiredWord));
				}else{
					_wordcountMap.put(bytes(expiredWord), bytes(String.valueOf(Integer.valueOf(tmp)-1)));
				}
			}
		}
		//add sentence to window
		_wordWindow.put(bytes(word), bytes(String.valueOf(progress)));
		//emit tuples
		DBIterator wcIter=_wordcountMap.iterator();
		wcIter.seekToFirst();
		while(wcIter.hasNext()){
			String emitWord= asString(wcIter.peekNext().getKey());
			String emitCount = asString(wcIter.peekNext().getValue());
			_collector.emit(new Values(emitWord, Integer.valueOf(emitCount), progress));
			wcIter.next();
		}
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count", "progress"));
	}
}
