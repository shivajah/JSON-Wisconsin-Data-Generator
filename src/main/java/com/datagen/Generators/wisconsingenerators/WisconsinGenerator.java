package com.datagen.Generators.wisconsingenerators;

import java.util.Random;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.schema.Schema;
import com.datagen.schema.Domain.Domain;

/**
 * Created by shiva on 3/7/18.
 */
public abstract class WisconsinGenerator {
    Schema schema;
    int fieldId;
    Random rand;
    int nextNull;
    int nextMissing;
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

    public int getNumberOfValuesInDomain() {
        int numberOfValuesInDomain;
        Domain domain = schema.getFields().get(fieldId).getDomain();
        if (domain.getType() == Domain.Type.RANGE) {
            numberOfValuesInDomain = Integer.parseInt(domain.getRange().getTo())
                    - Integer.parseInt(domain.getRange().getFrom()) + 1;
        } else {
            numberOfValuesInDomain = domain.getValues().size();
        }
        if (numberOfValuesInDomain <= 0) {
            throw new IllegalArgumentException("Domain has no value to assign.");
        }
        return numberOfValuesInDomain;
    }

    public abstract Object next(long i);

    public long rand(long seed, long cardinality) {
        do {
            seed = (schema.getGenerator() * seed) % schema.getPrime();
        } while (seed > cardinality);
        return (seed);
    }

    public int nextNull(int i) {
        int val = nextNullOrMissing(i, schema.getFields().get(fieldId).getNulls());
        decreaseNulls();
        return val;
    }

    public int nextMissing(int i) {
        int val = nextNullOrMissing(i, schema.getFields().get(fieldId).getMissings());
        decreaseMissings();
        return val;
    }

    private int nextNullOrMissing(int i, int numRemnullOrMissing) {
        if (numRemnullOrMissing <= 0) {
            return schema.getCardinality() * 2;
        }
        int max = schema.getCardinality() / numRemnullOrMissing;
        if (i > max && max * 2 <= schema.getCardinality() - 1)
            max = max * 2;
        return rand.nextInt(max + 1 - i) + i;
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