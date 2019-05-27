package com.datagen.Generators;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.schema.Field;
import com.datagen.schema.Domain.Range;

/**
 * Created by shiva on 2/23/18.
 */
public class NumberGenerator extends IGenerator {
    Map<String, List<String>> parameters;

    public long getMin() {
        return Long.MIN_VALUE;
    }

    public long getMax() {
        return Long.MAX_VALUE;
    }

    public NumberGenerator(Map<String, List<String>> parameters, int index) {
        this.parameters = parameters;
        String to = parameters.get("to").get(index);
        String from = parameters.get("from").get(index);
        Range range = new Range();
        range.setFrom((from.equalsIgnoreCase("null")) ? String.valueOf(getMin()) : from);
        range.setTo((to.equalsIgnoreCase("null")) ? String.valueOf(getMax()) : to);
        this.domain.setRange(range);
        generatorType = DataType.INTEGER;
    }

    public NumberGenerator(Field field) {
        super(field);
        generatorType = DataType.INTEGER;
    }

    public long nextNumber() {
        return Long.parseLong(next());
    }

    public String getNextRandom() {
        String val = "";
        long nextInSeq = Long.parseLong(getNextValueInSequence());
        int domainLength = getNumberOfValuesInDomain();
        //hash
        try {
            val = String.valueOf(hash(nextInSeq, domainLength));
        } catch (Exception e) {
        } ;
        return val;
    }

    private long getClosestPrimeNumber(long val) {
        boolean found = false;
        while (!found) {
            BigInteger valB = BigInteger.valueOf(val);
            if (valB.isProbablePrime(1)) {
                found = true;
            } else {
                if (val % 2 == 0) {
                    valB = BigInteger.valueOf(val + 1);
                } else {
                    valB = BigInteger.valueOf(val + 2);
                }
            }
            val = valB.longValue();
        }
        return val;
    }

    @Override
    protected String getNextValInRange() {
        return String.valueOf(Long.parseLong(domain.getRange().getFrom()) + index);
    }

}
