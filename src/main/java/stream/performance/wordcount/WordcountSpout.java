package stream.performance.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordcountSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3299463820028469540L;
	SpoutOutputCollector _collector;
	BufferedReader _reader;
	String _inputfilename;
	Random rand = new Random();

	public WordcountSpout(String inputfilename) {
		_inputfilename = inputfilename;
	}

	public void nextTuple() {
		Utils.sleep(rand.nextInt(50));
		String line = null;
		try {
			line = _reader.readLine();
			if (line == null) {
				_reader.close();
				_reader = new BufferedReader(new FileReader(_inputfilename));
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		_collector.emit(new Values(line, System.currentTimeMillis()), "abc");
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {
			_reader = new BufferedReader(new FileReader(_inputfilename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence", "progress"));
	}

}
