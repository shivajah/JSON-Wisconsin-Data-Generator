package com.datagen.Schema;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by shiva on 3/1/18.
 */
public class Schema {

    private List<Field> fields;
    private long cardinality;
    private String fileName;
    private int numOfPartitions;
    private long prime;
    private long generator;
    private long batchSize;
    private long fileSize;

    public Schema(){
        this.fields = new LinkedList<Field>();
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) { this.cardinality = cardinality; }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public long getPrime() {
        return prime;
    }

    public void setPrime(long prime) {
        this.prime = prime;
    }

    public long getGenerator() {
        return generator;
    }

    public void setGenerator(long generator) { this.generator = generator; }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getBatchSize() { return batchSize; }

    public void setBatchSize(long batchSize) { this.batchSize = batchSize; }
}
