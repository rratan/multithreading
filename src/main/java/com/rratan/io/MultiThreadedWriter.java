package com.rratan.io;

import java.io.*;

public class MultiThreadedWriter implements MultiThreadWriter,Closeable {

    private final OutputStream outSteam;

    public MultiThreadedWriter(String fileName) throws IOException {
        System.out.println("Instantiating MultiThreadedWriter Writes writer with file:" + fileName);
        this.outSteam = new BufferedOutputStream(new FileOutputStream(fileName));
    }

    @Override
    public void close() throws IOException {
        if (this.outSteam != null) {
            this.outSteam.close();
        }
    }


    public void save(String data) {
        synchronized (this.outSteam) {
            try {
                this.outSteam.write(data.getBytes());

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}