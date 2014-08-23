package dbconnector;

import java.util.Random;

public class RandomString {
	static Random rand=new Random();
	static final String AB="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	
	public static String generateRandomString(int stringLength){
		StringBuilder sb=new StringBuilder(stringLength);
		for(int i=0; i<stringLength; ++i){
			sb.append(AB.charAt(rand.nextInt(AB.length())));
		}
		return sb.toString();
	}
}
