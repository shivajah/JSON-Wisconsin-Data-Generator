package com.datagen.Schema;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by shiva on 3/1/18.
 */
public class Schema {

    private List<Field> fields;
    private long prime;
    private long generator;


    public Schema(){
        this.fields = new LinkedList<Field>();
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
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
}
