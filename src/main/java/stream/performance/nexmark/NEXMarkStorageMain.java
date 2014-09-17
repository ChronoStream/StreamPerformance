package stream.performance.nexmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import stream.performance.nexmark.common.InputTuple.AuctionTuple;
import stream.performance.nexmark.common.InputTuple.BidTuple;
import stream.performance.nexmark.common.InputTuple.PersonTuple;
import stream.performance.nexmark.common.TupleGenerator;
import stream.performance.toolkits.MemoryReport;

public class NEXMarkStorageMain {

	private static Connection connect = null;
	private static Statement statement = null;
	private static PreparedStatement personInsertion = null;
	private static PreparedStatement auctionInsertion = null;
	private static PreparedStatement itemInsertion = null;
	private static PreparedStatement personSelection = null;
	private static PreparedStatement auctionSelection = null;
	private static PreparedStatement itemSelection = null;

	public static void prepare() throws Exception {
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		statement
				.executeUpdate("create table persontable(person_id varchar(20), email varchar(20), city varchar(20), province varchar(20), country varchar(20))");
		statement
				.executeUpdate("create table auctiontable(auction_id varchar(20), seller varchar(20), begin_time bigint, end_time bigint)");
		statement
				.executeUpdate("create table itemtable(item_id varchar(20), price int)");
		personInsertion = connect
				.prepareStatement("insert into persontable values(?, ?, ?, ?, ?)");
		auctionInsertion = connect
				.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
		itemInsertion = connect
				.prepareStatement("insert into itemtable values(?, ?)");
		personSelection = connect.prepareStatement("select * from persontable");
		auctionSelection = connect.prepareStatement("select * from auctiontable");
		itemSelection = connect.prepareStatement("select * from itemtable");
	}

	public static void cleanup() throws Exception {
		statement.executeUpdate("drop table persontable");
		statement.executeUpdate("drop table auctiontable");
		statement.executeUpdate("drop table itemtable");
		statement.close();
		connect.close();
	}

	public static void populate(int totalCount) throws Exception {
		TupleGenerator generator = new TupleGenerator();
		Random rand = new Random();
		// populate the stream. generate 50 persons and 50 auctions.
		for (int i = 0; i < 50; i++) {
			PersonTuple person = generator.generatePerson();
			personInsertion.setString(1, String.valueOf(person.person_id));
			personInsertion.setString(2, person.email);
			personInsertion.setString(3, person.city);
			personInsertion.setString(4, person.province);
			personInsertion.setString(5, person.country);
			personInsertion.addBatch();
		}
		personInsertion.executeBatch();
		for (int i = 0; i < 50; i++) {
			AuctionTuple auction = generator.generateAuction();
			auctionInsertion.setString(1, String.valueOf(auction.auction_id));
			auctionInsertion.setString(2, String.valueOf(auction.seller_id));
			auctionInsertion.setLong(3, auction.begin_time);
			auctionInsertion.setLong(4, auction.end_time);
			auctionInsertion.addBatch();
		}
		auctionInsertion.executeBatch();
		for (int count = 0; count < totalCount; ++count) {
			// now go into a loop generating bids and persons and so on
			// generating a person approximately 10th time will give is 10
			// items/person since we generate on average one bid per loop
			if (rand.nextInt(10) == 0) {
				PersonTuple person = generator.generatePerson();
				personInsertion.setString(1, String.valueOf(person.person_id));
				personInsertion.setString(2, person.email);
				personInsertion.setString(3, person.city);
				personInsertion.setString(4, person.province);
				personInsertion.setString(5, person.country);
				personInsertion.addBatch();
			}
			personInsertion.executeBatch();
			// want on average 1 item and 10 bids
			int numItems = rand.nextInt(3); // should average 1
			for (int i = 0; i < numItems; ++i) {
				AuctionTuple auction = generator.generateAuction();
				auctionInsertion.setString(1,
						String.valueOf(auction.auction_id));
				auctionInsertion
						.setString(2, String.valueOf(auction.seller_id));
				auctionInsertion.setLong(3, auction.begin_time);
				auctionInsertion.setLong(4, auction.end_time);
				auctionInsertion.addBatch();
			}
			auctionInsertion.executeBatch();
			int numBids = rand.nextInt(21) + 1; // should average 10
			for (int i = 0; i < numBids; ++i) {
				BidTuple bid = generator.generateBid();
				itemInsertion.setString(1, String.valueOf(bid.auction_id));
				itemInsertion.setInt(2, bid.price);
				itemInsertion.addBatch();
			}
			itemInsertion.executeBatch();
		}
	}

	public static void query() throws Exception {
		Map<String, Integer> avgPrices = new HashMap<String, Integer>();
		statement.executeQuery("select * from table");
	}

	public static void main(String[] args) throws Exception {
		long start_time, end_time, elapsed_time;
		prepare();
		// ======================================
		start_time = System.currentTimeMillis();
		populate(200000);
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
		cleanup();
	}

}
