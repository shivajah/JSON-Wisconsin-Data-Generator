/*Copyright (c) 2020 Shiva Jahangiri

        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
*/
package com.datagen.OutputGenerator;

import java.io.BufferedOutputStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.datagen.Server;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.datagen.Utils.Utils;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.FieldGenerators.AWisconsinGenerator;
import com.datagen.FieldGenerators.WisconsinStringGenerator;
import com.datagen.Schema.Schema;

public abstract class AWisconsinOutputGenerator {

    private static final Logger LOGGER = LogManager.getRootLogger();
    protected Schema schema;
    private List<AWisconsinGenerator> generators;
    final String directory = "./";
    protected Map<Integer, BufferedOutputStream> threadToFileOutputStream;
    private static long maxRecordLength;
    protected ExecutorService executorService;
    //A map from executor sequence id to a map for start and end row index
    private Map<Integer, Pair<Long, Long>> executorsToStartAndEnd;
    private int partitions;
    private int partition;
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    //TODO: totalFileSize should be calculated from the record size to know the exact start and end row index(if the file size is deciding where we finish instead of number of records
    public double totalFileSize;
    public Map<Long, Integer> sizeToCount;


    public AWisconsinOutputGenerator(Schema schema, List<AWisconsinGenerator> generators) {
        this.partitions = Integer.valueOf(Server.JSONDataGenConfiguration.get(Server.PARTITIONS_NAME));
        this.partition = Integer.valueOf(Server.JSONDataGenConfiguration.get(Server.PARTITION_NAME));
        this.schema = schema;
        this.generators = generators;
        this.threadToFileOutputStream = new HashMap<>();
        this.maxRecordLength = 0;
        this.totalFileSize = 0;
        this.executorsToStartAndEnd = new HashMap<>();
        this.sizeToCount = new HashMap<>();
    }

    private void initExecutors() {
        // if partition == -1, then all partitions should get created (in parallel)
        if (partition < 0) {
            executorService = Executors.newFixedThreadPool(partitions);
        } else { // Otherwise, only the mentioned partition will be generated using one thread
            executorService = Executors.newFixedThreadPool(1);
        }
    }

    private void initReaderToStartAndEnd() {
        long cardinality =  Long.valueOf(Server.JSONDataGenConfiguration.get(Server.CARDINALITY_NAME));
        long length = cardinality / partitions;
        long start;
        for (int i = 0; i < partitions; i++) {
            start = Math.max(1,length * i);
            Pair<Long, Long> p = i == partitions - 1 ? new Pair<>(start, cardinality):
                    new Pair<>(start, start + length);
            executorsToStartAndEnd.put(i, p);
        }
    }

    public void execute() {
        if (partition < 0) { // All partitions
            IntStream.range(0, partitions).forEach(readerId -> {
                executorService.submit(() -> {
                    executorTask(readerId);
                });
            });
        } else { //only one partition
            executorService.submit(()->executorTask(partition));
        }
            shutDownExecutors();

    }
    private void executorTask(int readerId) {
        int batchIndex = 0; // batch1, batch2,...
        int batchSize = Integer.valueOf(Server.JSONDataGenConfiguration.get(Server.BATCHSIZE_NAME)); // Maximum number of records in a batch
        long fileSize = Long.valueOf(Server.JSONDataGenConfiguration.get(Server.FILESIZE_NAME));
        int recordCount = 0;
        long totalRecordLength = 0;
        long minRecordLength = Long.MAX_VALUE;
        long startTime = System.currentTimeMillis();
        List<String> batchOfRecords = new LinkedList<>();

        LOGGER.info("start index: " + executorsToStartAndEnd.get(readerId).getKey() + " end index: "
                + executorsToStartAndEnd.get(readerId).getValue());

        for (long id = executorsToStartAndEnd.get(readerId).getKey(); id <= executorsToStartAndEnd.get(readerId)
                .getValue(); id++) {
            if (fileSize > 0 && totalFileSize >= fileSize * 1024 * 1024){
                break;
            }
            if (id % batchSize == 0){
                LOGGER.info("Thread "+ readerId +" Created " + id  + " records in "
                        + Utils.getPrintableTimeDifference(startTime, System.currentTimeMillis()));
            }
            long currentRecordsize = 0;
            String record = "{";

            boolean allFieldsNull= true;
            for (int i = 0; i < schema.getFields().size(); i++) {

                Object val = generators.get(i).next(id);
                 if (val instanceof String &&  val == "#MISSING") {
                    continue;
                } else {
                    try {
                        boolean comma = record.equalsIgnoreCase("{") ? false : true;
                        if (comma) {
                            record = record + ", ";
                        }
                        allFieldsNull = false;
                        record = record + "\"" + schema.getFields().get(i).getName() + "\":";
                        if (generators.get(i).getDataType() == DataType.BINARY) {
                            record = record + "hex(\"" + val + "\")";
                            currentRecordsize += ((String) val).length();
                        } else if (generators.get(i).getDataType() == DataType.STRING) {
                            record = record + "\"" + val + "\"";
                            currentRecordsize += ((String) val).length();
                        } else if (generators.get(i).getDataType() == DataType.INTEGER) {
                            record = record + val;
                            currentRecordsize += 8;
                        }
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                }
            }
            record = record + "}\n";
            if (allFieldsNull)
                continue;
            if (batchOfRecords.size() >= batchSize){
                // Empty the batch
                write(readerId, batchOfRecords, batchIndex);
                batchIndex++;
                batchOfRecords.clear();
            }
            batchOfRecords.add(record);

            if (!sizeToCount.containsKey(currentRecordsize)){
                sizeToCount.put(currentRecordsize,0);
            }
            int count = sizeToCount.get(currentRecordsize);
            sizeToCount.put(currentRecordsize, count+1);

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
        LOGGER.info(Arrays.asList(sizeToCount));

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
