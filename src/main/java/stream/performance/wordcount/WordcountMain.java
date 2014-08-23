package stream.performance.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordcountMain {

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			int numTasks = Integer.valueOf(args[1]);
			int numWorkers = Integer.valueOf(args[2]);
			int windowSize = Integer.valueOf(args[3]);
			int slidingStep = Integer.valueOf(args[4]);
			String inputFilename = args[5];
			String outFilename = args[6];
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("spout", new WordcountSpout(inputFilename));
			builder.setBolt("parser", new WordcountParser()).shuffleGrouping("spout");
			builder.setBolt("window", new WordcountWindow(windowSize, slidingStep)).fieldsGrouping("parser", new Fields("word"));
			builder.setBolt("sink", new WordcountSink(outFilename)).globalGrouping("window");

			Config conf = new Config();
			conf.setDebug(false);
			conf.setNumWorkers(numWorkers);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			int windowSize = 1000;
			int slidingStep = 10;
			String inputFilename = "tweets.txt";
			String outFilename = "output.txt";
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("spout", new WordcountSpout(inputFilename));
			builder.setBolt("parser", new WordcountParser()).shuffleGrouping("spout");
			builder.setBolt("window", new WordcountWindowLeveldb(windowSize, slidingStep)).fieldsGrouping("parser", new Fields("word"));
			builder.setBolt("sink", new WordcountSink(outFilename)).globalGrouping("window");

			Config conf = new Config();
			conf.setDebug(false);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", conf, builder.createTopology());
		}
	}
}
