package com.datagen.Output;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by shiva on 2/25/18.
 */
public class OutputGenerator {
    //List<String> fileNames;
    // int numOfPartitions;
    final String directory = "./";
    String fileName;
    Map<String, List<String>> parameters;
    BufferedOutputStream stream;

    public OutputGenerator(Map<String, List<String>> parameters) {
        this.parameters = parameters;
        setUniqueFileName();
    }

    private void setUniqueFileName() {
        String fname = parameters.get("file-name").get(0);
        if (fname != null && !fname.contains(".txt")) {
            fname = fname + ".txt";
        }
        fileName = (fname != null) ? directory + fname : directory + "tempFile.txt";
        File f = new File(fileName);
        int i = 1;
        String newFname = fileName;
        while (f.exists()) {
            String[] splits = fileName.split(".txt");
            newFname = splits[0] + "_" + i + ".txt";
            f = new File(newFname);
            i++;
        }
        fileName = newFname;
        try {
            f.createNewFile();
            stream = new BufferedOutputStream(new FileOutputStream(fileName));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public void write(String record) {
        try {
            stream.write(record.getBytes());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void close() {
        try {
            stream.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
