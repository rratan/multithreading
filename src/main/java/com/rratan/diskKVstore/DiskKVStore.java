package com.rratan.diskKVstore;

import java.io.*;
import java.nio.file.Path;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class DiskKVStore  {

    /*
    Features:
    1. Add a key to a file .
    2. Get the value of key , if not exists return null.
    3. Multiple thread access
     */


    private final String BASE_STORE_PATH;
    private ConcurrentHashMap<String , KVFile> mp;
    private UnixFileUtility fs;


    public DiskKVStore() {
        this(System.getProperty("user.dir")+ "/test_files");
    }


    public  DiskKVStore(String path) {
        System.out.println("Creating KV store with basepath" + path);
        this.BASE_STORE_PATH = path;
        this.fs = new UnixFileUtility(path) ;
        this.mp = new ConcurrentHashMap<>() ;
        start();
    }



    public void start(){
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        ex.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                compaction();
            }
        },0,1, TimeUnit.SECONDS);

    }


    /*
        Check the local disk FS and
     */
    public String  getValue(String key) {
        String fileName = getFileName(key);
        System.out.println("File name for get "+ fileName);
        KVFile kvObj = mp.getOrDefault(fileName , null);
        if(kvObj == null){
            return null;
        }
        String val = fs.getValue(kvObj, key);

        return val;
    }


    /*

     */
    public boolean add(String key, String value) {

        String fileName = getFileName(key);
        KVFile kvObj = mp.computeIfAbsent(fileName , (k)->{
           KVFile obj = new KVFile(k);
           return obj;
        });

        return  fs.addKV(kvObj , getKVasStr(key,value));
    }


    public String getKVasStr(String key, String value) {
        return key + "," + value;
    }

    public String getFileName(String key) {
        return "kv_"+ String.valueOf(key.hashCode() % 100);
    }


    public void compaction() {
        Set<String> fe = mp.keySet();
        for( String f : fe){
            KVFile s = mp.get(f);
            synchronized (s){
                runCompaction(s);
            }
        }
    }

    public void runCompaction(KVFile f) {

        synchronized (f){
            System.out.println("Running compaction for file "+ f.getFileName());
            HashMap<String, String > cm = new HashMap<>();
            try(BufferedReader br = new BufferedReader(new FileReader(f.getFileName()))){
                String line;
                String key, value;
                while ((line = br.readLine())!=null){
                    key = line.split(":")[1].split(",")[0];
                    cm.put(key, line);
                }
            }catch (IOException e){

            }
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(f.getFileName()))) {
                for (String line : cm.values()) {
                    writer.write(line);
                    writer.newLine();
                }
                writer.flush();
            }catch (IOException e){

            }
        }
    }

}
