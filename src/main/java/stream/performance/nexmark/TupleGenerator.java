package stream.performance.nexmark;

import java.util.Random;

import stream.performance.nexmark.InputTuple.AuctionTuple;
import stream.performance.nexmark.InputTuple.BidTuple;
import stream.performance.nexmark.InputTuple.PersonTuple;
import stream.performance.relation.OpenAuctions;
import stream.performance.relation.PersonGen;
import stream.performance.relation.Persons;
import stream.performance.relation.SimpleCalendar;

public class TupleGenerator {
	// generate bids, items and persons
	private SimpleCalendar cal = new SimpleCalendar();
	// for managing person ids
	private Persons persons = new Persons();
	// for managing open auctions
	private OpenAuctions openAuctions = new OpenAuctions();
	// for generating values for person
	private PersonGen p = new PersonGen();
	
	private Random rand = new Random();
	
	PersonTuple generatePerson() {
		// schema: person_id, street_name, email, city, state, country
		cal.incrementTime();
		p.generateValues(); // person object is reusable now
		PersonTuple tuple=new PersonTuple();
		tuple.person_id=persons.getNewId();
		tuple.street_name=p.m_stName;
		tuple.email=p.m_stEmail;
		tuple.city=p.m_stCity;
		tuple.province=p.m_stProvince;
		tuple.country=p.m_stCountry;
		return tuple;
	}

	AuctionTuple generateAuction() {
		// schema: auction_id, seller_id, category_id, begin_time, end_time
		cal.incrementTime();
		AuctionTuple tuple=new AuctionTuple();
		tuple.auction_id=openAuctions.getNewId();
		tuple.seller_id=persons.getExistingId();
		tuple.category_id=rand.nextInt(1000);
		tuple.begin_time=cal.getTimeInSecs();
		tuple.end_time=tuple.begin_time+ 60 * 60 + rand.nextInt(24 * 60 * 60 * 2);
		return tuple;
	}

	BidTuple generateBid() {
		// schema: auction_id, date_time, person_id, price
		cal.incrementTime();
		BidTuple tuple=new BidTuple();
		tuple.auction_id=openAuctions.getExistingId();
		tuple.date_time=cal.getTimeInSecs();
		tuple.person_id=persons.getExistingId();
		tuple.price=openAuctions.getPrice();
		return tuple;
	}

}
