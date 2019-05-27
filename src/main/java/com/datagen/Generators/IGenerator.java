package com.datagen.Generators;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Domain.Domain;

/**
 * Created by shiva on 2/24/18.
 */
public abstract class IGenerator {
    Field field;
    Domain domain;
    Order.order order;
    boolean isFirstCall;
    int index;
    List<String> smallRandoms;
    DataType generatorType;

    abstract String getNextValInRange();

    abstract String getNextRandom();

    public IGenerator() {
    }

    public IGenerator(Field field) {
        this.field = field;
        this.order = field.getOrder();
        this.domain = field.getDomain();
        this.index = 0;
        isFirstCall = true;
        this.smallRandoms = new LinkedList<>();
    }

    public String next() {
        String val = "";
        if (order == Order.order.SEQUENTIAL) {
            return getNextValueInSequence();

        } else {//Random
            if (getNumberOfValuesInDomain() < 1000) {
                if (isFirstCall) {
                    for (int i = 0; i < getNumberOfValuesInDomain(); i++) {
                        smallRandoms.add(getNextValueInSequence());
                    }
                    Collections.shuffle(smallRandoms);
                    index = 0;
                    isFirstCall = false;
                }
                if (index >= getNumberOfValuesInDomain()) {
                    index = 0;
                }
                val = smallRandoms.get(index);
                index++;

            } else {
                try {
                    val = getNextRandom();
                } catch (Exception e) {
                    System.out.println("md5 is not implemented.");
                }
            }
        }
        return val;
    }

    public String getNextValueInSequence() {
        String val;
        if (domain.getType() == Domain.Type.VALUE) {
            if (index >= domain.getValues().size()) {
                index = 0;
            }
            val = domain.getValues().get(index);
            index++;
        } else {//Range
            if (index > Long.parseLong(domain.getRange().getTo())) {
                index = 0;
            }
            val = getNextValInRange();
            index++;
        }
        return val;
    }

    protected long hash(long i, int partitions) throws java.security.NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(toBytes(i));
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        return bigInt.mod(new BigInteger(1, toBytes(partitions))).abs().longValue();
    }

    protected byte[] toBytes(long ii) {
        // int i = ii.intValue();
        byte[] result = new byte[4];

        result[0] = (byte) (ii >> 24);
        result[1] = (byte) (ii >> 16);
        result[2] = (byte) (ii >> 8);
        result[3] = (byte) (ii /*>> 0*/);

        return result;
    }

    public int getNumberOfValuesInDomain() {
        int numberOfValuesInDomain;
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

    public DataType getDataType() {
        return generatorType;
    }
}
