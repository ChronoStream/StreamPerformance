package stream.performance.nexmark;

public class InputTuple {
	static class PersonTuple{
		long person_id;
		String street_name;
		String email;
		String city;
		String province;
		String country;
	}
	
	static class AuctionTuple{
		long auction_id;
		long seller_id;
		int category_id;
		long begin_time;
		long end_time;
	}
	
	static class BidTuple{
		long auction_id;
		long date_time;
		long person_id;
		int price;
	}
}
