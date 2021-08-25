package com.datagen.FieldGenerators;

import com.datagen.Constants.DataTypes;
import com.datagen.Schema.Schema;

import java.util.Random;

public abstract class AWisconsinGenerator {
    Schema schema;
    int fieldId;
    Random rand;
    DataTypes.DataType generatorType;
    double nullStartRang;
    double nullEndRange;
    double missingStartRange;
    double missingEndRange;

    public enum ValueType{
        NULL,
        MISSING,
        VALUE
    }

    public AWisconsinGenerator(Schema schema, int fieldId) {
        this.schema = schema;
        this.fieldId = fieldId;
        this.rand = new Random();
        setUpNullAndMissingRanges();
    }

    private void setUpNullAndMissingRanges() {
        this.nullStartRang=-1;
        this.nullEndRange=-1;
        this.missingStartRange=-1;
        this.missingEndRange=-1;
        if (schema.getFields().get(fieldId).isNullable()) {//NULL
            this.nullStartRang = 0;
            this.nullEndRange = schema.getFields().get(fieldId).getNulls();
            if (schema.getFields().get(fieldId).isOptional()) { //NULL & MISSING
                this.missingStartRange = nullEndRange;
                this.missingEndRange = nullEndRange + schema.getFields().get(fieldId).getMissings();
            }
        }
        else if (schema.getFields().get(fieldId).isOptional()) {// MISSING
            this.missingStartRange = 0;
            this.missingEndRange = schema.getFields().get(fieldId).getMissings();
        }

    }
    public abstract Object next(long i);
    public long rand(long seed, long cardinality) {
        do {
            seed = (schema.getGenerator() * seed) % schema.getPrime();
        } while (seed > cardinality);
        return (seed);
    }

    public ValueType getValueType() {
        float nextEvent = rand.nextFloat();
        if (nextEvent >= nullStartRang && nextEvent< nullEndRange ){
            return ValueType.NULL;
        } else if (nextEvent >= missingStartRange && nextEvent < missingEndRange){
            return ValueType.MISSING;
        }
        return ValueType.VALUE;
    }

    public DataTypes.DataType getDataType() {
        return generatorType;
    }

}
