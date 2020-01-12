package com.datagen.Parser;

/**
 * Created by shiva on 3/10/18.
 */
public class PField {
    public String name;
    public String type;
    public String order;
    public int length;
    public int range;
    public boolean declared;
    public boolean optional;
    public int missings;
    public boolean nullable;
    public int nulls;
    public boolean even;
    public boolean odd;
    public boolean variableLength = false;
    // Generate word in case of a string?
    public boolean word = false;
    public double standardDeviation = 0.0;
    // normal distribution? Or gamma distribution?
    public boolean normalDistribution = true;
    // shape and scale in the gamma distribution
    public double shape = 1.5;
    public double scale = 1.5;

    // Bernoulli distribution with prob = largeRecordProb.
    public boolean bernoulliDistribution;
    public double probLargeRecord = 0.5;
    public int minSizeSmall = 100;
    public int maxSizeSmall = 32 * 1024 - 1;
    public int minSizeLarge = 32 * 1024;
    public int maxSizeLarge = 5 * 32 * 1024;
}
