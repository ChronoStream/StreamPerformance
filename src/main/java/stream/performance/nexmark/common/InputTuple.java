package stream.performance.nexmark.common;

public class InputTuple {
	public static class PersonTuple{
		public long person_id;
		public String street_name;
		public String email;
		public String city;
		public String province;
		public String country;
	}
	
	public static class AuctionTuple{
		public long auction_id;
		public long seller_id;
		public int category_id;
		public long begin_time;
		public long end_time;
	}
	
	public static class BidTuple{
		public long auction_id;
		public long date_time;
		public long person_id;
		public int price;
	}
}
