package stream.performance.nexmark;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import stream.performance.nexmark.InputTuple.AuctionTuple;
import stream.performance.nexmark.InputTuple.BidTuple;
import stream.performance.nexmark.InputTuple.PersonTuple;
import stream.performance.nexmark.InternalState.AuctionInfo;
import stream.performance.nexmark.InternalState.PersonInfo;
import stream.performance.toolkits.MemoryReport;

public class NEXMarkHeapMain {
	// record person information, infrequently updated.
	private static Map<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	private static Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	private static Map<String, LinkedList<Integer>> itemPrices = new HashMap<String, LinkedList<Integer>>();

	public static void populate(int totalCount) {
		TupleGenerator generator = new TupleGenerator();
		Random rand = new Random();
		// populate the stream. generate 50 persons and 50 auctions.
		for (int i = 0; i < 50; i++) {
			PersonTuple person = generator.generatePerson();
			personMap
					.put(String.valueOf(person.person_id), new PersonInfo(
							person.email, person.city, person.province,
							person.country));
		}
		for (int i = 0; i < 50; i++) {
			AuctionTuple auction = generator.generateAuction();
			auctionMap.put(String.valueOf(auction.auction_id), new AuctionInfo(
					String.valueOf(auction.seller_id), auction.begin_time,
					auction.end_time));
		}
		for (int count = 0; count < totalCount; ++count) {
			// now go into a loop generating bids and persons and so on
			// generating a person approximately 10th time will give is 10
			// items/person since we generate on average one bid per loop
			if (rand.nextInt(10) == 0) {
				PersonTuple person = generator.generatePerson();
				personMap.put(String.valueOf(person.person_id), new PersonInfo(
						person.email, person.city, person.province,
						person.country));
			}
			// want on average 1 item and 10 bids
			int numItems = rand.nextInt(3); // should average 1
			for (int i = 0; i < numItems; ++i) {
				AuctionTuple auction = generator.generateAuction();
				auctionMap.put(String.valueOf(auction.auction_id),
						new AuctionInfo(String.valueOf(auction.seller_id),
								auction.begin_time, auction.end_time));
			}
			int numBids = rand.nextInt(21) + 1; // should average 10
			for (int i = 0; i < numBids; ++i) {
				BidTuple bid = generator.generateBid();
				if (!itemPrices.containsKey(String.valueOf(bid.auction_id))) {
					itemPrices.put(String.valueOf(bid.auction_id),
							new LinkedList<Integer>());
				}
				itemPrices.get(String.valueOf(bid.auction_id)).add(bid.price);
			}
		}
	}

	public static void query() {
		Map<String, Integer> avgPrices = new HashMap<String, Integer>();
		for (String item : itemPrices.keySet()) {
			long sum = 0;
			for (Integer price : itemPrices.get(item)) {
				sum += price;
			}
			avgPrices.put(item, (int) (sum / itemPrices.size()));
		}
		Map<String, Integer> rankedPrices = new HashMap<String, Integer>();
		int count = 0;
		for (String key : avgPrices.keySet()) {
			rankedPrices.put(key, avgPrices.get(key));
			count += 1;
			if (count == 10) {
				break;
			}
		}
		Map<String, Integer> sellerPrice = new HashMap<String, Integer>();
		for (String item_id : rankedPrices.keySet()) {
			for (String auction_id : auctionMap.keySet()) {
				if (item_id == auction_id) {
					sellerPrice.put(auctionMap.get(auction_id).seller,
							rankedPrices.get(item_id));
				}
			}
		}

		for (String seller_id : sellerPrice.keySet()) {
			for (String person_id : personMap.keySet()) {
				if (seller_id == person_id) {
					System.out.println("seller=" + seller_id + ", average="
							+ sellerPrice.get(seller_id) + ", province="
							+ personMap.get(seller_id).province);
				}
			}
		}
	}

	public static void main(String[] args) {
		long start_time, end_time, elapsed_time;
		// ======================================
		start_time = System.currentTimeMillis();
		populate(5000);
		end_time = System.currentTimeMillis();
		elapsed_time = end_time - start_time;
		System.out.println("populate elapsed time=" + elapsed_time + "ms");
		MemoryReport.reportStatus();
		// ======================================
		start_time = System.currentTimeMillis();
		query();
		end_time = System.currentTimeMillis();
		elapsed_time = end_time - start_time;
		System.out.println("query elapsed time=" + elapsed_time + "ms");
		MemoryReport.reportStatus();
		// ======================================
		System.out.println("person state size=" + personMap.size());
		System.out.println("auction state size=" + auctionMap.size());
		System.out.println("item_price state size=" + itemPrices.size());

	}

}
