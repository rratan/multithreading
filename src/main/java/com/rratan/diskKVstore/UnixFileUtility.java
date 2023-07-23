package com.rratan.diskKVstore;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Set;
import java.util.stream.Collectors;


public class UnixFileUtility {

    private String filePath;

    public UnixFileUtility() {
        throw  new RuntimeException("filePath is missing for UnixFileUtility");
    }

    public UnixFileUtility(String filePath) {
        this.filePath = filePath;
    }


    public  List<Path> getAllFiles()  {
        Path p = Paths.get(this.filePath);
        List<Path> fileList = null;
        try{
            fileList = Files.list(p).filter(myFiles -> !Files.isDirectory(myFiles)).collect(Collectors.toList());
        }catch (NoSuchFileException e){
            System.out.println(String.format("File path %s does not exist", this.filePath));
        }catch (IOException e){
            e.printStackTrace();
        }
        return fileList;
    }

    public boolean addKV(KVFile file, String line) {
        boolean flag = true;
        synchronized (file){
            try(FileWriter fw = new FileWriter(Paths.get(filePath,file.getFileName()).toFile()); BufferedWriter writer = new BufferedWriter(fw)){
                writer.write(System.currentTimeMillis() + ":"+file);
                writer.newLine();
                writer.flush();
            }catch (Exception e){
                flag = false;
                e.printStackTrace();
            }
        }
        return flag;
    }


    public String getValue(KVFile file , String key) {
        String line;
        synchronized(file){
            try(FileReader fr = new FileReader(Paths.get(filePath,file.getFileName()).toFile()); BufferedReader reader = new BufferedReader(fr)){
            while((line = reader.readLine())!=null){
                 String[] splitted = line.split(",");
                if(key.compareTo(splitted[0]) == 0){
                    return splitted[1];
                }
            }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return null;
    }

}
