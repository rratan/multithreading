package com.rratan.io;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Driver {

    static AtomicInteger ops = new AtomicInteger();
    static CountDownLatch latch = new CountDownLatch(3);
    private static final int MAX_OPS = 200;

    public static void main(String[] args) throws Exception {
        BufferedWriter mw = new BufferedWriter(System.getProperty("user.dir")+ "/test_files/test.txt");

        ExecutorService ex =  Executors.newFixedThreadPool(3);
        long start1 = System.currentTimeMillis();
        ex.execute(new WriterThread(mw));
        ex.execute(new WriterThread(mw));
        ex.execute(new WriterThread(mw));

        latch.await();
        mw.close();
        long start2 = System.currentTimeMillis();
        System.out.println("Time taken: "+ (start2-start1));

        ex.shutdown();
    }


    public static class WriterThread implements Runnable{

        MultiThreadWriter mw;

        public WriterThread(MultiThreadWriter mw) {
            this.mw = mw;
        }


        @Override
        public void run() {
            while (ops.getAndIncrement() < MAX_OPS){
                mw.save(Thread.currentThread().toString() + " saving data");
                try {
                    Thread.sleep(200);
                }catch (InterruptedException e){
                   e.printStackTrace();
                }
            }
            latch.countDown();
        }
    }


}
