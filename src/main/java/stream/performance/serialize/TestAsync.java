package stream.performance.serialize;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class TestAsync {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newCachedThreadPool();
        FutureTask<String> future = new FutureTask<String>(new Callable<String>() {
            public String call() throws Exception{
                try {
                    Thread.sleep(5* 1000);
                    return "Hello Welcome!";
                }
                catch(Exception e) {
                    throw new Exception("Callable terminated with Exception!");
                }
            }
        });
        executor.execute(future);
        long t = System.currentTimeMillis();
        try {
            String result = future.get();
            System.err.println("result is " + result + ", time is " + (System.currentTimeMillis() - t));
        } catch (InterruptedException e) {
            future.cancel(true);
            System.err.println("Interrupte time is " + (System.currentTimeMillis() - t));
        } catch (ExecutionException e) {
            future.cancel(true);
            System.err.println("Throw Exception time is " + (System.currentTimeMillis() - t));
//        } catch (TimeoutException e) {
//            future.cancel(true);
//            System.err.println("Timeout time is " + (System.currentTimeMillis() - t));
        } finally {
            executor.shutdown();
        }

    }
}