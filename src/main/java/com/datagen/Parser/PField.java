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

public class PField {
    public String name;
    public String type;
    public String order;
    public int length;
    public int range;
    public boolean optional;
    public double missings;
    public boolean nullable;
    public double nulls;
    public boolean even;
    public boolean odd;
    public boolean variableLength = false;
    // Generate word in case of a string?
    public boolean word = false;
    public double standardDeviation = 0.0;
    // normal distribution? Or gamma distribution?
    public boolean normalDistribution = false;
    public boolean gammaDistribution = false;
    // normal distribution mean
    public double mean=1;
    // shape and scale in the gamma distribution
    public double shape = 1.5;
    public double scale = 1.5;

    // Bernoulli distribution with prob = largeRecordProb.
    public boolean bernoulliDistribution;
    public double probLargeRecord = 0.05;
    public int minSizeSmall = 187;
    public int maxSizeSmall = 987;
    public int minSizeLarge = 17787;
    public int maxSizeLarge = 19787;

    public boolean zipfDistribution =false;
    public int zipfMinSize=0;
    public int zipfMaxSize=30720;
    public double zipfSkew =2;
}
