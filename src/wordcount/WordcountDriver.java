package wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WordcountDriver {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("source", new WordcountSource(), 1);
		builder.setBolt("parser", new WordcountParser()).globalGrouping("source");
		builder.setBolt("query", new WordcountQuery()).globalGrouping("parser");
		builder.setBolt("sink", new WordcountSink()).globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}

}
