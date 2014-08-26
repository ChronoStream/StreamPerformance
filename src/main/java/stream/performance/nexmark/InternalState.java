package stream.performance.nexmark;

import java.util.Comparator;
import java.util.Map;

public class InternalState {

	public static class PersonInfo {
		public PersonInfo(String email, String city, String province,
				String country) {
			this.email = email;
			this.city = city;
			this.province = province;
			this.country = country;
		}

		String email;
		String city;
		String province;
		String country;
	}

	public static class AuctionInfo {
		public AuctionInfo(String seller, long begin_time, long end_time) {
			this.seller = seller;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}

		String seller;
		long begin_time;
		long end_time;
	}

	public static class BuyerPrice {
		public BuyerPrice(String buyer_id, int price) {
			this.buyer_id = buyer_id;
			this.price = price;
		}

		public String buyer_id;
		public int price;
	}

	public static class ItemPrice {
		public ItemPrice(String item_id, int price) {
			this.item_id = item_id;
			this.price = price;
		}

		public String item_id;
		public int price;
	}

	public static class ValueComparator implements Comparator<String> {
		Map<String, Integer> base;

		public ValueComparator(Map<String, Integer> base) {
			this.base = base;
		}

		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return 1;
			} else {
				return -1;
			}
		}
	}
}
