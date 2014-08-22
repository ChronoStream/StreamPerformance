package stream.performance.serialize;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import stream.performance.mytest.mymap.Mymap;



public class StreamCheckpoint {

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();
	static final int stringLength = 20;
	static final int tupleNum = 2000000;
	static final int dataSize = stringLength * tupleNum * 2;
	static HashMap<String, String> myState = new HashMap<String, String>();

	static Object ckptLock = new Object();

	static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	static void populate() {
		for (int i = 0; i < tupleNum; ++i) {
			myState.put(randomString(stringLength), randomString(stringLength));
		}
	}

	static void ProcessSyncStream() throws Exception {
		List<String> keys = new ArrayList<String>(myState.keySet());
		String randomKey = keys.get(rnd.nextInt(keys.size()));
		long beginTime, endTime;
		beginTime = System.currentTimeMillis();
		int count = 0;
		while (true) {
			synchronized (ckptLock) {
				Thread.sleep(1);
				myState.put(randomKey, randomString(stringLength));
			}
			++count;
			if (count % 2000 == 0) {
				System.out.println("count=" + count);
				endTime = System.currentTimeMillis();
				System.out.println("elapsed time="
						+ ((endTime - beginTime) / 1000.0) + "s");
				System.out.println("total length="
						+ (2 * tupleNum * stringLength / 1000.0) + "KB");
				beginTime = endTime;
			}
		}
	}

	public static class SyncCheckpoint implements Runnable {
		public void run() {
			try {
				while (true) {
					Thread.sleep(3000);
					System.out.println("begin ckpt...");
					synchronized (ckptLock) {
						Mymap.Builder mapmap = Mymap.newBuilder();
						for (String key : myState.keySet()) {
							mapmap.addMykey(key);
							mapmap.addMyvalue(myState.get(key));
						}
						FileOutputStream output = null;
						output = new FileOutputStream("map.ser");
						mapmap.build().writeTo(output);
						output.close();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static boolean isNormal = true;
	static boolean isCkptReady = false;

	public static class ProcessAsyncStream implements Runnable {
		public void run() {
			try {
				HashMap<String, String> myTempState = new HashMap<String, String>();
				List<String> keys = new ArrayList<String>(myState.keySet());
				String randomKey = keys.get(rnd.nextInt(keys.size()));
				long beginTime, endTime;
				beginTime = System.currentTimeMillis();
				int count = 0;
				while (true) {
					Thread.sleep(1);
					boolean notNormal = false;
					// synchronized (ckptLock) {
					if (isNormal) {
						myState.put(randomKey, randomString(stringLength));
					} else {
						notNormal = true;
					}
					// }
					if (notNormal) {
						myTempState.put(randomKey, randomString(stringLength));
						// synchronized (ckptLock) {
						// if (isCkptReady) {
						// System.out.println("ready!");
						// // merge here!
						// // for (String myTempKey : myTempState.keySet()) {
						// // myState.put(myTempKey,
						// myTempState.get(myTempKey));
						// // }
						// isCkptReady = false;
						// isNormal = true;
						// }
						// }
					}
					++count;
					if (count % 2000 == 0) {
						System.out.println("count=" + count);
						endTime = System.currentTimeMillis();
						System.out.println("elapsed time="
								+ ((endTime - beginTime) / 1000.0) + "s");
						System.out
								.println("total length="
										+ (2 * tupleNum * stringLength / 1000.0)
										+ "KB");
						beginTime = endTime;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class AsyncCheckpoint implements Runnable {
		public void run() {
			try {
				while (true) {
					Thread.sleep(3000);
					System.out.println("begin ckpt...");
					// synchronized (ckptLock) {
					// isNormal = false;
					// }
					Mymap.Builder mapmap = Mymap.newBuilder();
					for (String key : myState.keySet()) {
						mapmap.addMykey(key);
						mapmap.addMyvalue(myState.get(key));
					}
					FileOutputStream output;
					output = new FileOutputStream("map.ser");
					mapmap.build().writeTo(output);
					output.close();
					// synchronized (ckptLock) {
					// isCkptReady = true;
					// }
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		populate();
		(new Thread(new AsyncCheckpoint())).start();
		(new Thread(new ProcessAsyncStream())).start();;
	}

}
