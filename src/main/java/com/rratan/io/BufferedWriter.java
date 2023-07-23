package com.rratan.io;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class BufferedWriter implements MultiThreadWriter,AutoCloseable {

    private int MAX_BUFFER_SIZE = 100;
    private final OutputStream os;

    private final ConcurrentLinkedDeque<LocalBuffer> waitQueue = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<LocalBuffer> availableBuffer = new ConcurrentLinkedDeque<>();
    private final ConcurrentHashMap<Thread, LocalBuffer> threadLocal = new ConcurrentHashMap<>();
    private final BackGroundWriter writerThread;

    public BufferedWriter(String fileName) throws IOException {
        this.os = new BufferedOutputStream(new FileOutputStream(fileName));
        this.writerThread = new BackGroundWriter(os,waitQueue,availableBuffer);
        this.writerThread.start();
//        this.writerThread.join();
    }

    public static class LocalBuffer {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(buffer);

    }
    @Override
    public void save(String data) {
        LocalBuffer lf = threadLocal.computeIfAbsent(Thread.currentThread(), (k) -> {
            return getBuffer();
        });
        try {

            addToBuffer(data, lf.writer);
            System.out.println("Writing data "+ lf.buffer.size());
            if (lf.buffer.size() > MAX_BUFFER_SIZE) {
                waitQueue.add(lf);
                threadLocal.put(Thread.currentThread(), getBuffer());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addToBuffer(String data, OutputStreamWriter buffer ) throws IOException {
        buffer.write(data + '\n');
        buffer.flush();
    }


    public LocalBuffer getBuffer() {
        LocalBuffer free = availableBuffer.poll();
        if(free == null){
            free = new LocalBuffer();
        }
        return free;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        for(LocalBuffer pending: threadLocal.values()) {
            waitQueue.add(pending);
        }

        writerThread.close();

    }
}

