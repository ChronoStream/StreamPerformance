package stream.performance.offheap.test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import sun.misc.Unsafe;

public class Main {

	private static class HeapValue {
		public HeapValue(int intValue, double doubleValue, long longValue,
				long intValue2, double doubleValue2) {
			this.intValue = intValue;
			this.doubleValue = doubleValue;
			this.longValue = longValue;
			this.intValue2 = intValue2;
			this.doubleValue2 = doubleValue2;
		}

		public int intValue;
		public double doubleValue;
		public long longValue;
		public long intValue2;
		public double doubleValue2;
	}

	static final int AccessNum = 1000000;
	static final int ArraySize = 1000000;
	static final int IntSize = 4;
	static final int DoubleSize = 8;
	static final int LongSize = 8;
	static final int TotalSize = IntSize + DoubleSize + LongSize + IntSize
			+ DoubleSize;

	private static Unsafe unsafe;
	static {
		try {
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (Unsafe) field.get(null);
		} catch (Exception e) {
		}
	}

	public static void main(String[] args) {
		Random rand = new Random();
		int sum;
		// //////////////////////////////////////////////
		long startTime = System.currentTimeMillis();
		Map<Integer, HeapValue> heaps = new HashMap<Integer, HeapValue>();
		for (int i = 0; i < ArraySize; ++i) {
			heaps.put(i, new HeapValue(rand.nextInt(2000), rand.nextDouble(), rand
					.nextLong(), rand.nextInt(1000), rand.nextDouble()));
		}
		System.out.println("heap initiate elapsed time = "
				+ (System.currentTimeMillis() - startTime) + "ms");
		// //////////////////////////////////////////////
		startTime = System.currentTimeMillis();
		sum = 0;
		for (int i = 0; i < AccessNum; ++i) {
			sum += heaps.get(i).intValue;
			sum -= heaps.get(i).intValue2;
		}
		System.out.println("heap random access elapsed time = "
				+ (System.currentTimeMillis() - startTime) + "ms");
		System.out.println("sum=" + sum);
		// ///////////////////////////////////////////////

		// //////////////////////////////////////////////
		startTime = System.currentTimeMillis();
		Map<Integer, Long> offheaps = new HashMap<Integer, Long>();
		for (int i = 0; i < ArraySize; ++i) {
			Long address = unsafe.allocateMemory(TotalSize);
			unsafe.putInt(address, rand.nextInt(2000));
			unsafe.putDouble(address + IntSize, rand.nextDouble());
			unsafe.putLong(address + IntSize + DoubleSize, rand.nextLong());
			unsafe.putInt(address + IntSize + DoubleSize + LongSize,
					rand.nextInt(1000));
			unsafe.putDouble(address + IntSize + DoubleSize + LongSize
					+ IntSize, rand.nextDouble());
			offheaps.put(i, address);
		}
		System.out.println("offheap initiate elapsed time = "
				+ (System.currentTimeMillis() - startTime) + "ms");
		// ////////////////////////////////////////////////
		startTime = System.currentTimeMillis();
		sum = 0;
		for (int i = 0; i < AccessNum; ++i) {
			sum += unsafe.getInt(offheaps.get(i));
			sum -= unsafe.getInt(offheaps.get(i) + IntSize + DoubleSize
					+ LongSize);
		}
		System.out.println("offheap random access elapsed time = "
				+ (System.currentTimeMillis() - startTime) + "ms");
		System.out.println("sum=" + sum);
	}
}
