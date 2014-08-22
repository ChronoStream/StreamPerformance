package stream.performance.wordcount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordcountSink extends BaseRichBolt {

	private static final long serialVersionUID = 4236870184495201624L;
	OutputCollector _collector;
	BufferedWriter _writer;
	String _outfilename;
	int totalCount = 0;
	long totalLatency = 0L;

	public WordcountSink(String filename) {
		_outfilename = filename;
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		int count = input.getInteger(1);
		long progress = input.getLong(2);
		long latency = System.currentTimeMillis() - progress;
		++totalCount;
		totalLatency += latency;
		if (totalCount % 1000 == 0) {
			System.out.println("average latency="+(totalLatency*1.0/totalCount)+"ms");
		}
		// try {
		// _writer.write("latency=" + latency + "ms.\n");
		// _writer.flush();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		_collector.ack(input);
	}

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try {
			_writer = new BufferedWriter(new FileWriter(_outfilename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

}
