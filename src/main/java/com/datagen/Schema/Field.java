package com.datagen.Schema;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.Order;

/**
 * Created by shiva on 3/1/18.
 */
public class Field {
    private Schema schema;
    private DataTypes.DataType type;
    private String name;
    private boolean declared;
    private boolean optional;
    private boolean nullable;
    private int nulls;
    private int missings;
    private Order.order order;
    private int stringLength;
    private int range;
    private boolean even;
    private boolean odd;
    private boolean variableLength;
    // true: normal distribution, false: gamma distribution
    private boolean normalDistribution;
    // For the gamma distribution
    private double shape = 1.5;
    private double scale = 1.5;
    // generate word?
    private boolean word;
    // standard deviation for the variable-length field
    private double standardDeviation;
    // Bernoulli distribution with prob = largeRecordProb.
    private boolean bernoulliDistribution;
    private double probLargeRecord = 0.5;
    private int minSizeSmall = 100;
    private int maxSizeSmall = 32 * 1024 - 1;
    private int minSizeLarge = 32 * 1024;
    private int maxSizeLarge = 5 * 32 * 1024;


    public long getSizeInBytes(long cardinality) {
        if (range > 0) {
            cardinality = range;
        }
        if (type == DataTypes.DataType.INTEGER) {
            if (cardinality <= 127 && cardinality >= -128) {//tinyint
                return 1; //Byte
            } else if (cardinality <= 32767 && cardinality >= -32768) {//smallint
                return 2;
            } else if (cardinality <= 2147483647 && cardinality >= -2147483648) {//int
                return 4;
            } else {//bigint
                return 8;
            }
        } else if (type == DataTypes.DataType.STRING || type == DataTypes.DataType.BINARY) {
            return stringLength * Character.BYTES;
        }
        return 0;
    }

    public int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }

    public DataTypes.DataType getType() {
        return type;
    }

    public void setType(DataTypes.DataType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDeclared() {
        return declared;
    }

    public void setDeclared(boolean declared) {
        this.declared = declared;
    }

    public Order.order getOrder() {
        return order;
    }

    public void setOrder(Order.order order) {
        this.order = order;
    }

    public int getStringLength() {
        return stringLength;
    }

    public void setStringLength(int stringLength) {
        if (stringLength != 0)
            this.stringLength = stringLength;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getNulls() {
        return nulls;
    }

    public void setNulls(int nulls) {
        this.nulls = nulls;
    }

    public int getMissings() {
        return missings;
    }

    public void setMissings(int missings) {
        this.missings = missings;
    }

    public boolean isEven() {
        return even;
    }

    public void setEven(boolean even) {
        this.even = even;
    }

    public boolean isOdd() {
        return odd;
    }

    public void setOdd(boolean odd) {
        this.odd = odd;
    }

    public boolean isVariableLength() {
        return variableLength;
    }

    public void setVariableLength(boolean variableLength) {
        this.variableLength = variableLength;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    public boolean isWord() {
        return word;
    }

    public void setWord(boolean word) {
        this.word = word;
    }

    public boolean isNormalDistribution() {
        return normalDistribution;
    }

    public void setNormalDistribution(boolean normalDistribution) {
        this.normalDistribution = normalDistribution;
    }

    public double getShape() {
        return shape;
    }

    public void setShape(double shape) {
        this.shape = shape;
    }

    public double getScale() {
        return scale;
    }

    public void setScale(double scale) {
        this.scale = scale;
    }

    public boolean isBernoulliDistribution() {
        return bernoulliDistribution;
    }

    public void setBernoulliDistribution(boolean bernoulliDistribution) {
        this.bernoulliDistribution = bernoulliDistribution;
    }

    public double getProbLargeRecord() {
        return probLargeRecord;
    }

    public void setProbLargeRecord(double probLargeRecord) {
        this.probLargeRecord = probLargeRecord;
    }

    public int getMinSizeSmall() {
        return minSizeSmall;
    }

    public void setMinSizeSmall(int minSizeSmall) {
        this.minSizeSmall = minSizeSmall;
    }

    public int getMinSizeLarge() {
        return minSizeLarge;
    }

    public void setMinSizeLarge(int minSizeLarge) {
        this.minSizeLarge = minSizeLarge;
    }

    public int getMaxSizeSmall() {
        return maxSizeSmall;
    }

    public void setMaxSizeSmall(int maxSizeSmall) {
        this.maxSizeSmall = maxSizeSmall;
    }

    public int getMaxSizeLarge() {
        return maxSizeLarge;
    }

    public void setMaxSizeLarge(int maxSizeLarge) {
        this.maxSizeLarge = maxSizeLarge;
    }
}
