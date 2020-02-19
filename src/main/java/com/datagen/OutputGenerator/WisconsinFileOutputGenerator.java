package com.datagen.OutputGenerator;

import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.Schema.Schema;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public class WisconsinFileOutputGenerator extends AWisconsinOutputGenerator {

    public WisconsinFileOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        super(schema, generators);
        initiate();
    }

    @Override
    public void write(int threadId, List<String> batchOfRecords, int batchIndex) {
        for (String record : batchOfRecords) {
            try {
                threadToFileOutputStream.get(threadId).write(record.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void closeWriter(int threadId) {
        try {
            threadToFileOutputStream.get(threadId).close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void initiate() {
        super.initiate();
        //TODO: use schema file name for this purpose
        String filePrefix = schema.getFileName().split(".adm")[0];
        final String fileName = (filePrefix != null) ? directory + filePrefix : directory + "tempFile";
        File f = new File(fileName);
        IntStream.range(0, schema.getNumOfPartitions()).forEach(readerId -> {
            String execFilename = fileName + "p_" + readerId + ".adm";
            File execFile = new File(execFilename);
            try {
                execFile.createNewFile();
                threadToFileOutputStream.put(readerId, new BufferedOutputStream(new FileOutputStream(execFilename)));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }
}
