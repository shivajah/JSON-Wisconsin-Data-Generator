package com.datagen.Parser;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.Order;
import com.datagen.Schema.Field;
import com.datagen.Schema.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class Parser {
    public List<Schema> parseWisconsinConfigFile(String configFile) throws Exception {
        List<SchemaForParser> schemas = new LinkedList<>();
        List<Schema> finalSchemas = new LinkedList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File(configFile);
        try {
            schemas = objectMapper.readValue(file, new TypeReference<List<SchemaForParser>>() {
            });
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
        for (SchemaForParser sp : schemas) {
            finalSchemas.add(generateSchema(sp));
        }
        return finalSchemas;
    }

    private Schema generateSchema(SchemaForParser sp) throws Exception {
        Schema schema = new Schema();
        List<Field> fields = new LinkedList<>();
        schema.setCardinality(sp.dataset.cardinality);
        schema.setFileName(sp.file.name);
        schema.setNumOfPartitions(sp.file.partitions);
        schema.setFileSize(sp.dataset.fileSize);
        schema.setBatchSize(sp.dataset.batchSize);
        Field field;
        for (PField f : sp.fields) {
            field = new Field();
            field.setName(f.name);
            field.setDeclared(f.declared);
            field.setNullable(f.nullable);
            field.setNulls(f.nulls);
            field.setOptional(f.optional);
            field.setMissings(f.missings);
            field.setRange(f.range);
            field.setOdd(f.odd);
            field.setEven(f.even);
            field.setStringLength(f.length);
            field.setVariableLength(f.variableLength);
            field.setWord(f.word);
            field.setNormalDistribution(f.normalDistribution);
            field.setStandardDeviation(f.standardDeviation);
            field.setShape(f.shape);
            field.setScale(f.scale);
            field.setBernoulliDistribution(f.bernoulliDistribution);
            field.setProbLargeRecord(f.probLargeRecord);
            field.setMinSizeSmall(f.minSizeSmall);
            field.setMaxSizeSmall(f.maxSizeSmall);
            field.setMinSizeLarge(f.minSizeLarge);
            field.setMaxSizeLarge(f.maxSizeLarge);
            Order.order order = (f.order.equalsIgnoreCase("random") ? Order.order.RANDOM
                    : (f.order.equalsIgnoreCase("sequential") ? Order.order.SEQUENTIAL : null));
            field.setOrder(order);
            DataTypes.DataType dataType;

            if (f.type.equalsIgnoreCase("string")) {
                dataType = DataTypes.DataType.STRING;
            } else if (f.type.equalsIgnoreCase("integer")) {
                dataType = DataTypes.DataType.INTEGER;
            } else if (f.type.equalsIgnoreCase("binary")) {
                dataType = DataTypes.DataType.BINARY;
            } else {
               throw new Exception("Unknown Data Type");
            }

            field.setType(dataType);
            fields.add(field);
        }
        schema.setFields(fields);
        setPrimeAndGenerator(schema);
        return schema;
    }


    private void setPrimeAndGenerator(Schema schema) {
        long cardinality = schema.getCardinality();
        long generator;
        long prime;
        if (cardinality <= Math.pow(10, 1)) {
            prime = 11;
            generator = 2;
        } else if (cardinality <= Math.pow(10, 2)) {
            prime = 101;
            generator = 7;
        } else if (cardinality <= Math.pow(10, 3)) {
            prime = 1009;
            generator = 26;

        } else if (cardinality <= Math.pow(10, 4)) {
            prime = 10007;
            generator = 59;

        } else if (cardinality <= Math.pow(10, 5)) {
            prime = 100003;
            generator = 242;

        } else if (cardinality <= Math.pow(10, 6)) {
            prime = 1000003;
            generator = 568;
        } else if (cardinality <= Math.pow(10, 7)) {
            prime = 10000019;
            generator = 1792;
        } else if (cardinality <= Math.pow(10, 8)) {
            prime = 100000007;
            generator = 5649;
        } else if (cardinality <= Math.pow(10, 9)) {
            prime = 2147483647;
            generator = 16807;
        } else {
            throw new IllegalArgumentException("Cannot generate rows more than 10^9");
        }
        schema.setGenerator(generator);
        schema.setPrime(prime);
    }
}
