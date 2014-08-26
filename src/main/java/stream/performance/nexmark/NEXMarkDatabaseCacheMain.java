package stream.performance.nexmark;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import stream.performance.nexmark.InputTuple.AuctionTuple;
import stream.performance.nexmark.InputTuple.BidTuple;
import stream.performance.nexmark.InputTuple.PersonTuple;
import stream.performance.nexmark.InternalState.AuctionInfo;
import stream.performance.nexmark.InternalState.PersonInfo;
import stream.performance.structure.tableProtos.AuctionStructure;
import stream.performance.structure.tableProtos.AuctionTable;
import stream.performance.structure.tableProtos.ItemStructure;
import stream.performance.structure.tableProtos.ItemTable;
import stream.performance.structure.tableProtos.PersonStructure;
import stream.performance.structure.tableProtos.PersonTable;
import stream.performance.toolkits.MemoryReport;

public class NEXMarkDatabaseCacheMain {

	private static Connection connect = null;
	private static Statement statement = null;
	private static PreparedStatement personInsertion = null;
	private static PreparedStatement auctionInsertion = null;
	private static PreparedStatement itemInsertion = null;
	private static PreparedStatement itemAvgPriceQuery = null;

	// record person information, infrequently updated.
	private static Map<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	private static Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	private static Map<String, LinkedList<Integer>> itemPrices = new HashMap<String, LinkedList<Integer>>();

	public static void prepare() throws Exception {
		connect = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/testdb", "root", "");

		statement = connect.createStatement();
		statement.executeUpdate("drop table persontable");
		statement.executeUpdate("drop table auctiontable");
		statement.executeUpdate("drop table itemtable");
		statement
				.executeUpdate("create table persontable(person_id varchar(20), email varchar(50), city varchar(20), province varchar(20), country varchar(30)) engine=memory");
		statement
				.executeUpdate("create table auctiontable(auction_id varchar(20), seller varchar(20), begin_time bigint, end_time bigint) engine=memory");
		statement
				.executeUpdate("create table itemtable(item_id varchar(20), price int) engine=memory");
		personInsertion = connect
				.prepareStatement("insert into persontable values(?, ?, ?, ?, ?)");
		auctionInsertion = connect
				.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
		itemInsertion = connect
				.prepareStatement("insert into itemtable values(?, ?)");
		itemAvgPriceQuery = connect
				.prepareStatement("select item_id, avg(price) as avgprice from itemtable group by item_id order by avgprice desc limit 5");
	}

	public static void cleanup() throws Exception {
		// statement.executeUpdate("drop table persontable");
		// statement.executeUpdate("drop table auctiontable");
		// statement.executeUpdate("drop table itemtable");
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
			personMap
					.put(String.valueOf(person.person_id), new PersonInfo(
							person.email, person.city, person.province,
							person.country));
		}
		personInsertion.executeBatch();
		for (int i = 0; i < 50; i++) {
			AuctionTuple auction = generator.generateAuction();
			auctionInsertion.setString(1, String.valueOf(auction.auction_id));
			auctionInsertion.setString(2, String.valueOf(auction.seller_id));
			auctionInsertion.setLong(3, auction.begin_time);
			auctionInsertion.setLong(4, auction.end_time);
			auctionInsertion.addBatch();
			auctionMap.put(String.valueOf(auction.auction_id), new AuctionInfo(
					String.valueOf(auction.seller_id), auction.begin_time,
					auction.end_time));
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
				personMap.put(String.valueOf(person.person_id), new PersonInfo(
						person.email, person.city, person.province,
						person.country));
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
				auctionMap.put(String.valueOf(auction.auction_id),
						new AuctionInfo(String.valueOf(auction.seller_id),
								auction.begin_time, auction.end_time));
			}
			auctionInsertion.executeBatch();
			int numBids = rand.nextInt(21) + 1; // should average 10
			for (int i = 0; i < numBids; ++i) {
				BidTuple bid = generator.generateBid();
				itemInsertion.setString(1, String.valueOf(bid.auction_id));
				itemInsertion.setInt(2, bid.price);
				itemInsertion.addBatch();
				if (!itemPrices.containsKey(String.valueOf(bid.auction_id))) {
					itemPrices.put(String.valueOf(bid.auction_id),
							new LinkedList<Integer>());
				}
				itemPrices.get(String.valueOf(bid.auction_id)).add(bid.price);
			}
			itemInsertion.executeBatch();
		}
	}

	public static void query1() throws Exception {
		Map<String, Integer> avgPrices = new HashMap<String, Integer>();
		ResultSet result = statement
				.executeQuery("select item_id, avg(price) as avgprice from itemtable group by item_id order by avgprice desc limit 10");
		// ResultSet result = itemAvgPriceQuery.executeQuery();
		while (result.next()) {
			String item_id = result.getString("item_id");
			int average = result.getInt("avgprice");
			avgPrices.put(item_id, average);
			System.out.println("item_id=" + item_id + ", average=" + average);
		}
	}

	public static void query2() throws Exception {
		ResultSet result = statement
				.executeQuery("select avg(price) as avgPrice from ((select aucselltable.seller as seller, aucselltable.avgprice as price, persontable.province as province from "
						+ "((select auctiontable.seller, avgtable.avgprice from "
						+ "((select item_id, avg(price) as avgprice from itemtable group by item_id limit 10000) as avgtable) "
						+ "join "
						+ "auctiontable "
						+ "on auctiontable.auction_id=avgtable.item_id) as aucselltable) "
						+ "join "
						+ "persontable "
						+ "on aucselltable.seller=persontable.person_id) as provincePrice)");
		// ResultSet result = itemAvgPriceQuery.executeQuery();
		while (result.next()) {
			// String seller = result.getString("seller");
			int average = result.getInt("avgprice");
			// String province = result.getString("province");
			System.out.println("average=" + average);
		}
	}

	public static void query3() throws Exception {
		ResultSet result = statement
				.executeQuery("select auctiontable.seller as seller, avgtable.avgprice as price from "
						+ "((select item_id, avg(price) as avgprice from itemtable group by item_id limit 10000) as avgtable) "
						+ "join "
						+ "auctiontable "
						+ "on auctiontable.auction_id=avgtable.item_id");
		// ResultSet result = itemAvgPriceQuery.executeQuery();
		Map<String, Integer> sellerPrices = new HashMap<String, Integer>();
		while (result.next()) {
			sellerPrices
					.put(result.getString("seller"), result.getInt("price"));
		}
		System.out.println("seller price=" + sellerPrices.size());

		Map<String, Integer> provincePrices = new HashMap<String, Integer>();
		for (String seller_id : sellerPrices.keySet()) {
			for (String person_id : personMap.keySet()) {
				if (seller_id.equals(person_id)) {
					if (provincePrices.containsKey(personMap.get(seller_id))) {
						provincePrices.put(personMap.get(seller_id).province, provincePrices.get(personMap.get(seller_id).province)+sellerPrices.get(seller_id));
					} else {
						provincePrices.put(personMap.get(seller_id).province,
								sellerPrices.get(seller_id));
					}
				}
			}
		}
		System.out.println("province price=" + provincePrices.size());
		
		for(String province : provincePrices.keySet()){
			System.out.println("province="+province+", price="+provincePrices.get(province));
		}
	}

	public static void persistStructure() {
		PersonTable.Builder persontableBuilder = PersonTable.newBuilder();
		for (String key : personMap.keySet()) {
			PersonStructure.Builder personBuilder = PersonStructure
					.newBuilder().setPersonId(key)
					.setEmail(personMap.get(key).email)
					.setCity(personMap.get(key).city)
					.setProvince(personMap.get(key).province)
					.setCountry(personMap.get(key).country);
			persontableBuilder.addPersons(personBuilder);
		}
		try {
			FileOutputStream personOutput = new FileOutputStream(
					"persontable.dat");
			persontableBuilder.build().writeTo(personOutput);
		} catch (Exception e) {
			e.printStackTrace();
		}

		AuctionTable.Builder auctiontableBuilder = AuctionTable.newBuilder();
		for (String key : auctionMap.keySet()) {
			AuctionStructure.Builder auctionBuilder = AuctionStructure
					.newBuilder().setAuctionId(key)
					.setSeller(auctionMap.get(key).seller)
					.setBeginTime(auctionMap.get(key).begin_time)
					.setEndTime(auctionMap.get(key).end_time);
			auctiontableBuilder.addAuctions(auctionBuilder);
		}
		try {
			FileOutputStream auctionOutput = new FileOutputStream(
					"auctiontable.dat");
			auctiontableBuilder.build().writeTo(auctionOutput);
		} catch (Exception e) {
			e.printStackTrace();
		}

		ItemTable.Builder itemtableBuilder = ItemTable.newBuilder();
		for (String key : itemPrices.keySet()) {
			ItemStructure.Builder itemBuilder = ItemStructure.newBuilder()
					.setItemId(key);
			for (int price : itemPrices.get(key)) {
				itemBuilder.addPrice(price);
			}
			itemtableBuilder.addItems(itemBuilder);
		}
		try {
			FileOutputStream itemOutput = new FileOutputStream("itemtable.dat");
			itemtableBuilder.build().writeTo(itemOutput);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void retrieveStructure() throws FileNotFoundException,
			IOException {
		PersonTable persontable = PersonTable.parseFrom(new FileInputStream(
				"persontable.dat"));
		for (PersonStructure ps : persontable.getPersonsList()) {
			PersonInfo info = new PersonInfo(ps.getEmail(), ps.getCity(),
					ps.getProvince(), ps.getCountry());
			personMap.put(ps.getPersonId(), info);
		}

		AuctionTable auctiontable = AuctionTable.parseFrom(new FileInputStream(
				"auctiontable.dat"));
		for (AuctionStructure as : auctiontable.getAuctionsList()) {
			AuctionInfo info = new AuctionInfo(as.getAuctionId(),
					as.getBeginTime(), as.getEndTime());
			auctionMap.put(as.getAuctionId(), info);
		}

		ItemTable itemtable = ItemTable.parseFrom(new FileInputStream(
				"itemtable.dat"));
		for (ItemStructure is : itemtable.getItemsList()) {
			LinkedList<Integer> pricelist = new LinkedList<Integer>();
			for (int price : is.getPriceList()) {
				pricelist.add(price);
			}
			itemPrices.put(is.getItemId(), pricelist);
		}
	}

	public static void main(String[] args) throws Exception {
		long start_time, end_time, elapsed_time;
		prepare();
		// ======================================
		start_time = System.currentTimeMillis();
		populate(1000000);
		persistStructure();
//		retrieveStructure();
		end_time = System.currentTimeMillis();
		elapsed_time = end_time - start_time;
		System.out.println("populate elapsed time=" + elapsed_time + "ms");
		MemoryReport.reportStatus();

		// ======================================
		start_time = System.currentTimeMillis();
		query3();
		end_time = System.currentTimeMillis();
		elapsed_time = end_time - start_time;
		System.out.println("query elapsed time=" + elapsed_time + "ms");
		MemoryReport.reportStatus();
		// ======================================
		cleanup();
	}

}
