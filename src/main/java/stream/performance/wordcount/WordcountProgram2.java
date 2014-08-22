package stream.performance.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Random;

import backtype.storm.utils.Utils;

public class WordcountProgram2 {

	private static Random rand = new Random();
	private static BufferedReader _reader;
	private static HashMap<String, Integer> _wordcountMap = new HashMap<String, Integer>();
	private static int totalCount = 0;
	private static long totalLatency = 0L;

	public static void main(String[] args) throws Exception {
		_reader = new BufferedReader(new FileReader("tweets.txt"));
		while (true) {
			Utils.sleep(rand.nextInt(10));
			String line = null;
			long start_time = System.currentTimeMillis();
			line = _reader.readLine();
			if (line == null) {
				_reader.close();
				_reader = new BufferedReader(new FileReader("tweets.txt"));
				return;
			}
			String[] words = line.split(" ");
			for (String word : words) {
				word = word.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
				if (word.length() < 20) {
					int count = 1;
					if (_wordcountMap.containsKey(word)) {
						count += _wordcountMap.get(word);
					}
					_wordcountMap.put(word, count);
					long latency = System.currentTimeMillis() - start_time;
					++totalCount;
					totalLatency += latency;
					if (totalCount % 1000 == 0) {
						System.out.println("average latency="
								+ (totalLatency * 1.0 / totalCount) + "ms");
					}
				}
			}
		}
	}

}
