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
package com.datagen.Parser;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;
import com.datagen.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class Parser {
    public Schema parseWisconsinConfigFile(String configFile) throws Exception {
        List<PField> fields = new LinkedList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File(configFile);
        try {
            fields = objectMapper.readValue(file, new TypeReference<List<PField>>() {
            });
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }

        return generateSchema(fields);
    }

    private Schema generateSchema(List<PField> pfields) throws Exception {
        List<Field> fields = new LinkedList<>();
        Field field;
        for (PField f : pfields) {
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
            field.setGammaDistribution(f.gammaDistribution);
            field.setStandardDeviation(f.standardDeviation);
            field.setShape(f.shape);
            field.setScale(f.scale);
            field.setBernoulliDistribution(f.bernoulliDistribution);
            field.setProbLargeRecord(f.probLargeRecord);
            field.setMinSizeSmall(f.minSizeSmall);
            field.setMaxSizeSmall(f.maxSizeSmall);
            field.setMinSizeLarge(f.minSizeLarge);
            field.setMaxSizeLarge(f.maxSizeLarge);
            field.setZipfDistribution(f.zipfDistribution);
            field.setZipfMinSize(f.zipfMinSize);
            field.setZipfMaxSize(f.zipfMaxSize);
            field.setZipfSkew(f.zipfSkew);
            field.setNumberOfDupicatesOfNegOne(f.numberOfDupicatesOfNegOne);
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
        Schema schema = new Schema();
        schema.setFields(fields);
        setPrimeAndGenerator(schema);
        return schema;
    }


    private void setPrimeAndGenerator(Schema schema) {
        long cardinality = Long.valueOf(Server.couchbaseConfiguration.get("cardinality"));
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
