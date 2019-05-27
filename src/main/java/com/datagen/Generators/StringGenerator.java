package com.datagen.Generators;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Domain.Domain;

/**
 * Created by shiva on 2/24/18.
 */
public class StringGenerator extends IGenerator {
    private int length = 100;
    private char[] min = new char[length];
    private char[] max = new char[length];
    private String currentVal;

    public StringGenerator() {
        setMinAndMax();
        generatorType = DataType.STRING;
    }

    public StringGenerator(int length) {
        this.length = length;
        min = new char[length];
        max = new char[length];
        setMinAndMax();
        generatorType = DataType.STRING;
    }

    public StringGenerator(Field field) {
        this.field = field;
        this.domain = field.getDomain();
        //TODO
        //this.length = field.getLength();
        if (field.getDomain().getType() == Domain.Type.RANGE) {
            this.min = field.getDomain().getRange().getFrom().toCharArray();
            this.max = field.getDomain().getRange().getTo().toCharArray();
        }
        generatorType = DataType.STRING;
    }

    private void setMinAndMax() {
        for (int i = 0; i < length; i++) {
            min[i] = (i < 7) ? 'A' : 'X';
            max[i] = (i < 7) ? 'Z' : 'X';
        }
    }

    public String nextString() {
        String val = currentVal;
        resetIndex();
        if (field.getDomain().getType() == Domain.Type.VALUE) {
            if (field.getOrder() == Order.order.SEQUENTIAL) {
                val = field.getDomain().getValues().get(index);
            }
        }

        index++;
        return val;
    }

    private void resetIndex() {
        if (field.getDomain().getType() == Domain.Type.VALUE && index >= field.getDomain().getValues().size()
                || field.getDomain().getType() == Domain.Type.RANGE
                        && currentVal.equalsIgnoreCase(field.getDomain().getRange().getTo()))
            index = 0;
    }

    public String next(int seed) {
        char[] temp = { 'A', 'A', 'A', 'A', 'A', 'A', 'A' };
        char[] field = min.clone();
        int i, j, rem, cnt;
        i = 6;
        cnt = 0;
        while (seed > 0 && i >= 0) {
            rem = seed % 26;
            temp[i] = (char) ('A' + rem);
            seed = seed / 26;
            i--;
            cnt++;
        }
        if (i < 0)
            i = 0;
        for (j = 0; j < cnt; j++, i++) {
            field[j] = temp[i];
        }

        return String.valueOf(field);
    }

    private void addOne() {
    };

    @Override
    protected String getNextValInRange() {
        String val = currentVal;
        addOne();//adds one to currentVal
        return val;
    }

    @Override
    String getNextRandom() {
        return "Rows larger than 1000 are still under construction.";
    }
}
