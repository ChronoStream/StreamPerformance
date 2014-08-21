package wordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class WordcountSource extends BaseRichSpout{

	private SpoutOutputCollector _collector;
	
	@Override
	public void nextTuple() {
		
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		_collector=collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
