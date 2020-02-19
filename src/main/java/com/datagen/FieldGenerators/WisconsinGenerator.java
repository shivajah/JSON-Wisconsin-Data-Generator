package com.datagen.FieldGenerators;

import java.util.Random;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Schema.Schema;
import com.datagen.Server;

/**
 * Created by shiva on 3/7/18.
 */
public abstract class WisconsinGenerator {
    Schema schema;
    int fieldId;
    Random rand;
    long nextNull;
    long nextMissing;
    DataType generatorType;

    public WisconsinGenerator() {
    }

    public WisconsinGenerator(Schema schema, int fieldId) {
        this.schema = schema;
        this.fieldId = fieldId;
        rand = new Random();
        nextNull = nextNull(0);
        nextMissing = nextMissing(0);
    }

    public abstract Object next(long i);

    public long rand(long seed, long cardinality) {
        do {
            seed = (schema.getGenerator() * seed) % schema.getPrime();
        } while (seed > cardinality);
        return (seed);
    }

    public long nextNull(int i) {
        long val = nextNullOrMissing(i, schema.getFields().get(fieldId).getNulls());
        decreaseNulls();
        return val;
    }

    public long nextMissing(int i) {
        long val = nextNullOrMissing(i, schema.getFields().get(fieldId).getMissings());
        decreaseMissings();
        return val;
    }

    private long nextNullOrMissing(int i, int numRemnullOrMissing) {
        long cardinality = Long.valueOf(Server.couchbaseConfiguration.get(Server.CARDINALITY_NAME));
        if (numRemnullOrMissing <= 0) {
            return cardinality * 2;
        }
        long max = cardinality / numRemnullOrMissing;
        if (i > max && max * 2 <= cardinality- 1)
            max = max * 2;
        return rand.nextLong()/max + i;
    }

    private void decreaseNulls() {
        schema.getFields().get(fieldId).setNulls(schema.getFields().get(fieldId).getNulls() - 1);
    }

    private void decreaseMissings() {
        schema.getFields().get(fieldId).setMissings(schema.getFields().get(fieldId).getMissings() - 1);
    }

    public DataType getDataType() {
        return generatorType;
    }

}