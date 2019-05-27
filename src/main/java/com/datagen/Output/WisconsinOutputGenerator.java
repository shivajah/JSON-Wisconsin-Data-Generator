package com.datagen.Output;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Generators.wisconsingenerators.WisconsinGenerator;
import com.datagen.Generators.wisconsingenerators.WisconsinStringGenerator;
import com.datagen.schema.Schema;

import javafx.util.Pair;

/**
 * Created by shiva on 3/10/18.
 */
public class WisconsinOutputGenerator {
    private Schema schema;
    private List<WisconsinGenerator> generators;
    final String directory = "./";
    private String fileName;
    private Map<Integer, BufferedOutputStream> streams;
    private long maxRecordLength;
    private ExecutorService executorService;
    private Map<Integer, Pair<Integer, Integer>> executorsToStartAndEnd;
    private int numOfExecutors;
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    public double totalFileSize;

    public WisconsinOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        numOfExecutors = schema.getNumOfPartitions();
        initExecutors();
        this.schema = schema;
        this.generators = generators;
        streams = new HashMap<>();
        setUniqueFileName();
        maxRecordLength = 0;
        totalFileSize = 0;

        executorsToStartAndEnd = new HashMap<>();
        initReaderToStartAndEnd();

    }

    private void initExecutors() {
        executorService = Executors.newFixedThreadPool(numOfExecutors);
    }

    private void shutDownExecutors() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }
            executorService.shutdownNow();
            System.out.println("Shutdown complete!");
        }
    }

    private void initReaderToStartAndEnd() {
        int length = schema.getCardinality() / numOfExecutors;
        int oneToLastEnd = -1;
        for (int i = 0; i < numOfExecutors; i++) {
            int start = length * i;

            Pair<Integer, Integer> p;
            if (i == numOfExecutors - 1) {
                p = new Pair<>(start, schema.getCardinality());
            } else {
                p = new Pair<>(start, start + length);
            }
            executorsToStartAndEnd.put(i, p);
        }

    }

    public void execute() {
        IntStream.range(0, numOfExecutors).forEach(readerId -> {
            executorService.submit(() -> {

                //
                System.out.println("start index: " + executorsToStartAndEnd.get(readerId).getKey() + " end index: "
                        + executorsToStartAndEnd.get(readerId).getValue());

                int recordCount = 0;
                long totalRecordLength = 0;
                long minRecordLength = Long.MAX_VALUE;
                long startTime = System.currentTimeMillis();
                long seconds;
                long minutes;
                long hours;
                long duration;

//                for (int id = executorsToStartAndEnd.get(readerId).getKey(); id <= executorsToStartAndEnd.get(readerId)
//                        .getValue(); id++) {
//                    if (schema.getFileSize() < 0 || (totalFileSize/(double)1024/(double)1024) > schema.getFileSize()  ) {
//                        break;
//                    }
                int id= 0;
                while((totalFileSize/(double)1024/(double)1024) < schema.getFileSize()) {
                    id++;
                    if (id % 10000 == 0) {
                        duration = System.currentTimeMillis() - startTime;

                        seconds = (duration / 1000) % 60;
                        minutes = ((duration / (1000 * 60)) % 60);
                        hours = ((duration / (1000 * 60 * 60)) % 24);

                        System.out.println("Processed " + id + ": it took " + hours + ":" + minutes + ":" + seconds
                                + "  so far. ");
                    }
                    long size = 0;
                    String record = "{";

                    for (int i = 0; i < schema.getFields().size(); i++) {

                        //                        if (i == schema.getFields().size() - 1) {
                        //                            System.out.println("Last field");
                        //                        }
                        boolean comma = record.equalsIgnoreCase("{") ? false : true;
                        Object nullOrMissing = generators.get(i).next(id);
                        if ((nullOrMissing instanceof Long && (long) nullOrMissing == Long.MAX_VALUE)
                                || (nullOrMissing instanceof String && ((String) nullOrMissing).isEmpty())) {
                            continue;
                        }
                        if (comma) {
                            record = record + ", ";
                        }
                        if (nullOrMissing == null) {
                            record = record + "\"" + schema.getFields().get(i).getName() + "\":" + null;
                        } else {
                            try {
                                Object val = nullOrMissing;
                                // Object val = generators.get(i).next(id);
                                record = record + "\"" + schema.getFields().get(i).getName() + "\":";

                                if (generators.get(i).getDataType() == DataType.BINARY) {
                                    record = record + "hex(\"" + val + "\")";
                                    size += ((String) val).length();
                                } else if (generators.get(i).getDataType() == DataType.STRING) {
                                    record = record + "\"" + val + "\"";
                                    size += ((String) val).length();
                                } else if (generators.get(i).getDataType() == DataType.INTEGER) {
                                    record = record + val;
                                    size += schema.getFields().get(i).getSizeInBytes(schema.getCardinality());;
                                }

                                //                            if (val instanceof String)
                                //                                record = record + "\"" + val + "\"";
                                //                            else
                                //                                record = record + val;
                                // size += schema.getFields().get(i).getSizeInBytes(schema.getCardinality());
                            } catch (Exception e) {
                                System.err.println(e);
                            }
                        }

                    }
                    record = record + "}\n";
                    //System.out.println(record);
                    write(record, readerId);
                    totalFileSize += size;
                    if (size > maxRecordLength) {
                        maxRecordLength = size;
                    } else if (size < minRecordLength) {
                        minRecordLength = size;
                    }
                    totalRecordLength += size;
                    //                    if (id == 5) {
                    //                        System.out.println(maxRecordLength);
                    //                    }
                    recordCount++;
                }

                duration = System.currentTimeMillis() - startTime;

                seconds = (duration / 1000) % 60;
                minutes = ((duration / (1000 * 60)) % 60);
                hours = ((duration / (1000 * 60 * 60)) % 24);

                System.out.println("Processed " + recordCount + ": it took " + hours + ":" + minutes + ":" + seconds
                        + "  so far. ");

                System.out.println(
                        "Record Generation Done. recordCount: " + recordCount + " minRecordLength: " + minRecordLength
                                + " maxRecordLength: " + maxRecordLength + " totalRecordLength: " + totalRecordLength+
                                " totalFileSize: "+ (totalFileSize/(double)1024/(double)1024)+" (MB)"+
                                 " avg record length: " + decFormat.format((double) totalRecordLength / recordCount));

                // Calculate the string length
                for (int i = 0; i < schema.getFields().size(); i++) {
                    if (generators.get(i).getDataType() == DataType.STRING) {
                        long length = ((WisconsinStringGenerator) generators.get(i)).getHexStringLength();
                        // Variable Length?
                        if (length > 0) {
                            ((WisconsinStringGenerator) generators.get(i)).printAvgHexStringLength();
                            ((WisconsinStringGenerator) generators.get(i)).closeGammaDistributionFile();
                        }
                    }

                }

                close(readerId);
            });
        });
        shutDownExecutors();
    }

    public long getMaxRecordLength() {
        return maxRecordLength;
    }

    private void setUniqueFileName() {
        String fname = schema.getFileName();
        if (fname != null && !fname.contains(".adm")) {
            fname = fname + ".adm";
        }
        fileName = (fname != null) ? directory + fname : directory + "tempFile.adm";
        File f = new File(fileName);
        int i = 1;
        String newFname = fileName;
        while (f.exists()) {
            String[] splits = fileName.split(".adm");
            newFname = splits[0] + "_" + i + ".adm";
            f = new File(newFname);
            i++;
        }
        fileName = newFname;
        IntStream.range(0, numOfExecutors).forEach(readerId -> {
            String[] splits = fileName.split(".adm");
            String execFilename = splits[0] + "p_" + readerId + ".adm";
            File execFile = new File(execFilename);
            try {
                execFile.createNewFile();
                streams.put(readerId, new BufferedOutputStream(new FileOutputStream(execFilename)));
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        });

    }

    public void write(String record, int execId) {

        try {
            streams.get(execId).write(record.getBytes());
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public void close(int execId) {
        try {
            streams.get(execId).close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
