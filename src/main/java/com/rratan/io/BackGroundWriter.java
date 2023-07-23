package com.rratan.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackGroundWriter extends Thread{
    private final OutputStream os;
    private AtomicBoolean bl = new AtomicBoolean(true);
    private final ConcurrentLinkedDeque<BufferedWriter.LocalBuffer> waitQueue;
    private final ConcurrentLinkedDeque<BufferedWriter.LocalBuffer> availableBuffer;

    public BackGroundWriter(OutputStream os, ConcurrentLinkedDeque<BufferedWriter.LocalBuffer> waitQueue,ConcurrentLinkedDeque<BufferedWriter.LocalBuffer> availableBuffer){
        this.os = os;
        this.waitQueue = waitQueue;
        this.availableBuffer = availableBuffer;
        System.out.println("Starting write");
    }

    @Override
    public void run() {
        while (bl.get() || waitQueue.size()>0){

            try{
            BufferedWriter.LocalBuffer qe = waitQueue.poll();
            if(qe!=null){
                System.out.println("Doing write");
                    qe.buffer.writeTo(os);
                    qe.buffer.reset();
                    availableBuffer.add(qe);
                    this.os.flush();
            }
            Thread.sleep(10);
        }catch (InterruptedException | IOException e){
                e.printStackTrace();
            }
    }
    }



    public void close() throws InterruptedException {
        bl.set(false);
        this.join();
        try{
            System.out.println("Closing writer thread with size"+waitQueue.size());
            this.os.close();

        }catch (Exception e){
            e.printStackTrace();
        }


    }
}


