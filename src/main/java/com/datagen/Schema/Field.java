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
package com.datagen.schema;

import com.datagen.Constants.DataTypes;
import com.datagen.Constants.Order;

public class Field {
    private DataTypes.DataType type;
    private String name;
    private boolean optional;
    private boolean nullable;
    private double nulls;
    private double missings;
    private Order.order order;
    private int stringLength;
    private int range;
    private boolean even;
    private boolean odd;
    private boolean variableLength;
    // true: normal distribution, false: gamma distribution
    private boolean normalDistribution;
    private boolean gammaDistribution;
    //For normal distribution
    private double mean = 1;
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
    private boolean zipfDistribution =false;
    private int zipfMinSize=0;
    private int zipfMaxSize=30720;
    private double zipfSkew =2;

    public boolean isZipfDistribution() {
        return zipfDistribution;
    }
    public int getZipfMinSize(){
        return zipfMinSize;
    }
    public int getZipfMaxSize(){
        return zipfMaxSize;
    }

    public double getZipfSkew(){
        return zipfSkew;
    }

    public int getRange() {
        return range;
    }


    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
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

    public double getNulls() {
        return nulls;
    }

    public void setNulls(double nulls) {
        this.nulls = nulls;
    }

    public double getMissings() {
        return missings;
    }

    public void setMissings(double missings) {
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
    public boolean isGammaDistribution() {
        return gammaDistribution;
    }

    public void setNormalDistribution(boolean normalDistribution) {
        this.normalDistribution = normalDistribution;
    }

    public void setGammaDistribution(boolean gammaDistribution) {
        this.gammaDistribution = gammaDistribution;
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
    public void setZipfDistribution(boolean zipfDistribution) {
        this.zipfDistribution = zipfDistribution;
    }

    public void setZipfMinSize(int zipfMinSize) {
        this.zipfMinSize = zipfMinSize;
    }

    public void setZipfMaxSize(int zipfMaxSize) {
        this.zipfMaxSize = zipfMaxSize;
    }

    public void setZipfSkew(double zipfSkew) {
        this.zipfSkew = zipfSkew;
    }

}
