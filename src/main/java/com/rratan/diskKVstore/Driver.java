package com.rratan.diskKVstore;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Driver {

    public static final AtomicInteger ops = new AtomicInteger();
    public static final int MAX_ALLOWED = 100;
    public static void  main(String[] args){
        DiskKVStore map = new DiskKVStore();
        Thread t1 = new Thread(new LowLatnecy(map,"abc"));
        Thread t2 = new Thread(new LowLatnecy(map,"bce"));


        Thread t3 = new Thread(new LowLatencyWriter(map,"abc"));
        Thread t4 = new Thread(new LowLatencyWriter(map,"bce"));

        t1.start();
        t2.start();

        t3.start();

        t4.start();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static class LowLatnecy implements Runnable {
        DiskKVStore map;
        String val ;
        public LowLatnecy( DiskKVStore map,String val ){
            this.map = map;
            this.val = val;
        }

        @Override
        public void run() {
            while(ops.getAndIncrement() < MAX_ALLOWED){
                System.out.println(Thread.currentThread().toString() +" "+ val + " "+ map.getValue(val) );
                try{
                    Thread.sleep(500);
                }catch (InterruptedException e){

                }


            }
        }
    }


    public static class LowLatencyWriter implements Runnable {
        DiskKVStore map;
        String val ;
        Random rr = new Random();
        public LowLatencyWriter( DiskKVStore map,String val ){
            this.map = map;
            this.val = val;
        }

        @Override
        public void run() {
            while(ops.getAndIncrement() < MAX_ALLOWED){

                String val1 = String.valueOf(rr.nextInt()%100 );
                System.out.println("Write"+ Thread.currentThread().toString() + val +" "+ val1);
                map.add(val ,val1);
                try{
                    Thread.sleep(1000);
                }catch (InterruptedException e){

                }

            }
        }
    }
}

