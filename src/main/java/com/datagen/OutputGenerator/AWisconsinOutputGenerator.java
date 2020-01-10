package com.datagen.OutputGenerator;

import java.io.BufferedOutputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.datagen.Utils.Utils;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.FieldGenerators.WisconsinStringGenerator;
import com.datagen.Schema.Schema;

/**
 * Created by shiva on 3/10/18.
 */
public abstract class AWisconsinOutputGenerator {

    private static final Logger LOGGER = LogManager.getRootLogger();
    protected Schema schema;
    private List<WisconsinGenerator> generators;
    final String directory = "./";
    protected Map<Integer, BufferedOutputStream> threadToFileOutputStream;
    private static long maxRecordLength;
    protected ExecutorService executorService;
    //A map from executor sequence id to a map for start and end row index
    private Map<Integer, Pair<Integer, Integer>> executorsToStartAndEnd;
    private int numOfExecutors;
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    //TODO: totalFileSize should be calculated from the record size to know the exact start and end row index(if the file size is deciding where we finish instead of number of records
    public double totalFileSize;


    public AWisconsinOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        this.numOfExecutors = schema.getNumOfPartitions();
        this.schema = schema;
        this.generators = generators;
        this.threadToFileOutputStream = new HashMap<>();
        this.maxRecordLength = 0;
        this.totalFileSize = 0;
        this.executorsToStartAndEnd = new HashMap<>();
        initiate();

    }

    private void initExecutors() {
        executorService = Executors.newFixedThreadPool(numOfExecutors);
    }

    private void initReaderToStartAndEnd() {
        int length = schema.getCardinality() / numOfExecutors;
        int start;
        for (int i = 0; i < numOfExecutors; i++) {
            start = length * i;
            Pair<Integer, Integer> p = i == numOfExecutors - 1 ? new Pair<>(start, schema.getCardinality()): new Pair<>(start, start + length);
            executorsToStartAndEnd.put(i, p);
        }
    }

    public void execute() {
        IntStream.range(0, numOfExecutors).forEach(readerId -> {
            executorService.submit(() -> {
                int batchIndex = 0; // batch1, batch2,...
                int batchSize = (int) schema.getBatchSize(); // Maximum number of records in a batch
                int recordCount = 0;
                long totalRecordLength = 0;
                long minRecordLength = Long.MAX_VALUE;
                long startTime = System.currentTimeMillis();
                List<String> batchOfRecords = new LinkedList<>();

                LOGGER.info("start index: " + executorsToStartAndEnd.get(readerId).getKey() + " end index: "
                        + executorsToStartAndEnd.get(readerId).getValue());

                for (int id = executorsToStartAndEnd.get(readerId).getKey(); id <= executorsToStartAndEnd.get(readerId)
                        .getValue(); id++) {

                    if (id % batchSize == 0){
                        LOGGER.info("Thread "+ readerId +" Created " + id  + " records in "
                                + Utils.getPrintableTimeDifference(startTime, System.currentTimeMillis()));
                    }
                    long currentRecordsize = 0;
                    String record = "{";

                    for (int i = 0; i < schema.getFields().size(); i++) {
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
                                record = record + "\"" + schema.getFields().get(i).getName() + "\":";
                                if (generators.get(i).getDataType() == DataType.BINARY) {
                                    record = record + "hex(\"" + val + "\")";
                                    currentRecordsize += ((String) val).length();
                                } else if (generators.get(i).getDataType() == DataType.STRING) {
                                    record = record + "\"" + val + "\"";
                                    currentRecordsize += ((String) val).length();
                                } else if (generators.get(i).getDataType() == DataType.INTEGER) {
                                    record = record + val;
                                    currentRecordsize += schema.getFields().get(i).getSizeInBytes(schema.getCardinality());
                                }
                            } catch (Exception e) {
                                LOGGER.error(e);
                            }
                        }

                    }
                    record = record + "}\n";
                    if (batchOfRecords.size() >= schema.getBatchSize()){
                        // Empty the batch
                        write(readerId, batchOfRecords, batchIndex);
                        batchIndex++;
                        batchOfRecords.clear();
                    }
                    batchOfRecords.add(record);

                    totalFileSize += currentRecordsize;
                    if (currentRecordsize > maxRecordLength) {
                        maxRecordLength = currentRecordsize;
                    } else if (currentRecordsize < minRecordLength) {
                        minRecordLength = currentRecordsize;
                    }
                    totalRecordLength += currentRecordsize;
                    recordCount++;
                }
                if (batchOfRecords.size() > 0) {
                    write(readerId, batchOfRecords, batchIndex);
                }
                closeWriter(readerId);

                // Stats info logging
                LOGGER.info("Thread "+ readerId +" Processed " + recordCount + " in " + Utils.getPrintableTimeDifference(startTime, System.currentTimeMillis()));
                LOGGER.info(
                        "Record Generation Done. recordCount: " + recordCount + " minRecordLength: " + minRecordLength
                                + " maxRecordLength: " + maxRecordLength + " totalRecordLength: " + totalRecordLength+
                                " totalFileSize: "+ (totalFileSize/(double)1024/(double)1024)+" (MB)"+
                                 " avg record length: " + decFormat.format((double) totalRecordLength / recordCount));

                // Calculate and print string length on average
                LOGGER.info(printStringLengthInfo());
            });
        });
        shutDownExecutors();
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

    //Stats Printing
    private String printStringLengthInfo() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < schema.getFields().size(); i++) {
            if (generators.get(i).getDataType() == DataType.STRING) {
                long length = ((WisconsinStringGenerator) generators.get(i)).getHexStringLength();
                // Variable Length?
                if (length > 0) {
                    strBuilder.append(((WisconsinStringGenerator) generators.get(i)).getAvgHexStringLength() + "\n");
                    ((WisconsinStringGenerator) generators.get(i)).closeGammaDistributionFile();
                }
            }
        }
        return strBuilder.toString();
    }


    // Abstract Methods
   public abstract void write(int threadId, List<String> batchOfRecords, int batchIndex);
   public abstract void closeWriter(int threadID);
   public void initiate(){
       initExecutors();
       initReaderToStartAndEnd();
   };

}
