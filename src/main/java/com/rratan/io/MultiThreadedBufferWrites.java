package com.rratan.io;

import java.io.*;

public class MultiThreadedBufferWrites implements MultiThreadWriter ,Closeable{
    private final OutputStream outSteam;
    private final ThreadLocal<LocalBuffer> tr = ThreadLocal.withInitial(()->{
      return   new LocalBuffer();
    });

    public MultiThreadedBufferWrites(String fileName) throws IOException {
        System.out.println("Instantiating MultiThreadedBuffer Writes writer with file:"+fileName);
        this.outSteam = new BufferedOutputStream(new FileOutputStream(fileName));
    }

    @Override
    public void close() throws IOException {
        if(this.outSteam !=null){
            this.outSteam.close();
        }
    }


    public static class LocalBuffer {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(buffer);
    }

    public void save(String data) {
//        System.out.println("Writing data:"+data);
        ByteArrayOutputStream buffer = tr.get().buffer;
        OutputStreamWriter writer = tr.get().writer;
        try{
            addToBuffer(data, writer);
            synchronized (this.outSteam){
                buffer.writeTo(this.outSteam);
                buffer.reset();
//                System.out.println("Write complete :"+data);
                this.outSteam.flush();
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public void addToBuffer(String data, OutputStreamWriter buffer ) throws IOException {
        buffer.write(data + '\n');

        buffer.flush();
    }
}
