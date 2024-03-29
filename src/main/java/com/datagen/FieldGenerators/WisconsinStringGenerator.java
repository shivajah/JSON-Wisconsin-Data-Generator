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

package com.datagen.FieldGenerators;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.Stack;

import com.datagen.Server;
import org.apache.commons.math3.distribution.GammaDistribution;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.Constants.StringWordList;
import com.datagen.Schema.Field;
import com.datagen.Schema.Schema;

public class WisconsinStringGenerator extends AWisconsinGenerator {
    private static final char[] VALUES = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
            'F' };
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");

    Random r;
    StringWordList wordList;
    StringBuilder builder;
    Set<Integer> wordsSet;
    int wordListSize;
    long hexStringLength;
    int maxHexStringLength;
    int minHexStringLength;
    int hexStringCount;
    char[] returnHexCharValues;
    GammaDistribution gd;
    Field f;
    ZipfGenerator zipfGenerator;

    // To write the rawdata of gamma distribution.
    BufferedWriter writer = null;
    String fileName = "gamma_distribution_rawdata.txt";

    public WisconsinStringGenerator(Schema schema, int fieldId) throws IOException {
        super(schema, fieldId);
        generatorType = DataType.STRING;
        r = new Random(fieldId);
        wordList = StringWordList.getInstance();
        builder = new StringBuilder();
        wordsSet = new LinkedHashSet<>();
        wordListSize = wordList.getWordList().size();
        hexStringLength = 0;
        // Just default size
        returnHexCharValues = new char[10];
        maxHexStringLength = 0;
        minHexStringLength = Integer.MAX_VALUE;
        hexStringCount = 0;
        f = schema.getFields().get(fieldId);
        if (f.isVariableLength() && !f.isWord() && !f.isNormalDistribution()) {
            gd = new GammaDistribution(f.getShape(), f.getScale());
            writer = new BufferedWriter(new FileWriter(fileName));
        }
        if (f.isZipfDistribution()) {
            if (f.isWord()) {
                int bound = Math.min(wordListSize, f.getRange());
                zipfGenerator = new ZipfGenerator(bound, f.getZipfSkew());
            } else {
                int rangeSize = f.getZipfMaxSize() - f.getZipfMinSize();
                assert (rangeSize > 0);
                zipfGenerator = new ZipfGenerator(rangeSize + 1, f.getZipfSkew());
            }
        }
    }

    public String next(long seed) {
        ValueType type = getValueType();
        if ( type ==ValueType.NULL){
            return "\"null\"";
        } else if (type ==ValueType.MISSING){
            return "#MISSING";
        }
        if (f.getOrder() != null && f.getOrder() == Order.order.RANDOM) {
            seed = rand(seed, Long.valueOf(Server.JSONDataGenConfiguration.get(Server.CARDINALITY_NAME)));
        }
        if (f.getRange() > 0) {
            seed = seed % f.getRange();
        }

        if (f.isVariableLength()) {
            if (f.isWord()) {
                // Variable length word generation
                // For this case, the length is regarded as the average word count.
                return generateRandomWords(f.getStringLength(), f.getStandardDeviation());
            } else if (f.isNormalDistribution()) {
                // Variable length hex char string generation
                // For this case, the length is regarded as the average length.
                //                return generateRandomHexChars();
                return generateRandomHexCharsGaussian(f.getStringLength(), f.getStandardDeviation());
            } else if (f.isBernoulliDistribution()) {
                // Bernoulli Distribution: with some probability the record is large
                if (r.nextFloat() < f.getProbLargeRecord()) {
                    return generateRandomHexCharsUniformDist(f.getMinSizeLarge(), f.getMaxSizeLarge());
                }
                return generateRandomHexCharsUniformDist(f.getMinSizeSmall(), f.getMaxSizeSmall());
            } else if (f.isZipfDistribution()) {
                return generateRandomHexCharsZipfianDist(f.getZipfMinSize());
            }
            else {
                // Gamma distribution case...
                // Variable length hex char string generation
                // For this case, the length is regarded as the average length.
                //                return generateRandomHexChars();
                String result= generateRandomHexCharsGamma(f.getStringLength());
                return result;
            }
        }
        else {//Fixed Length Records
            if (f.isWord()) {
                if (f.isZipfDistribution()) {
                    //Zipf distribution is for join skew not field length.
                    int rank = zipfGenerator.next();
                    String word = wordList.getWordList().get(rank);
                    return addPaddingX(word, f.isVariableLength());
                } else {//uniform
                    int rank = r.nextInt(wordListSize);
                    if (f.getRange() > 0) {
                        rank = rank %f.getRange();
                    }
                    return addPaddingX(wordList.getWordList().get(rank), f.isVariableLength());

                }
            } else {
                Stack<Character> temp = new Stack<>();
                char[] field = { 'A', 'A', 'A', 'A', 'A', 'A', 'A' };
                if (seed == 0) {
                    long rem = seed % 26;
                    temp.push((char) ('A' + rem));
                }
                while (seed > 0) {
                    long rem = seed % 26;
                    temp.push((char) ('A' + rem));
                    seed = seed / 26;
                }
                int i = 0;
                while (!temp.empty() && i < field.length) {
                    field[i] = temp.pop();
                    i++;
                }
                return addPaddingX(String.valueOf(field), f.isVariableLength());
            }
        }
    }

    private String addPaddingX(String value, boolean variableLength) {
        int stLen = f.getStringLength();
        int diff = Math.max(0,stLen - value.length());
        if (variableLength) {
            diff = r.nextInt(diff);
        }
        for (int i = 0; i < diff; i++) {
            value = value + 'X';
        }
        return value;
    }

    private String generateRandomWords(int wordCount, double standardDeviation) {
        builder.delete(0, builder.length());
        wordsSet.clear();

        // Based on the Gaussian distribution, we get the actual word count.
        int actualWordCount = (int) (r.nextGaussian() * standardDeviation + wordCount);
        // At least it should be one.
        actualWordCount = Math.max(actualWordCount, 1);
        // At maximum, it cannot be greater than (stringLength + (stringLength - 1)) to make the distribution symmetric.
        //
        //    x = (stringLength - 1)                  x = (stringLength - 1)
        // 1 ------------------------ stringLength --------------------------- (stringLength + (stringLength - 1))
        // since the left range is between 1 and the given stringLength.
        actualWordCount = Math.min(actualWordCount, 2 * actualWordCount - 1);

        int idx;
        for (int i = 0; i < actualWordCount; i++) {
            // Find a random number that is not in the set yet.
            while (true) {
                idx = r.nextInt(wordListSize);
                if (!wordsSet.contains(idx)) {
                    wordsSet.add(idx);
                    builder.append(wordList.getWordList().get(idx));
                    if (i != (actualWordCount - 1)) {
                        builder.append(" ");
                    }
                    break;
                }
            }
        }
        return builder.toString();
    }

    private String generateRandomHexString(int length) {
        if (length >= maxHexStringLength) {
            maxHexStringLength = length;
            returnHexCharValues = new char[maxHexStringLength];
        }
        if (length < minHexStringLength) {
            minHexStringLength = length;
        }
        char nextHex;
        for (int i = 0; i < length; i++) {
            nextHex = VALUES[r.nextInt(VALUES.length)];
            returnHexCharValues[i] = nextHex;
        }
        return String.valueOf(returnHexCharValues, 0, length);
    }

    private String generateRandomHexChars(int length) {
        hexStringLength += length;
        hexStringCount++;
        return generateRandomHexString(length);
    }

    private String  generateRandomHexCharsGaussian(int stringAvgLength, double standardDeviation) {
        // Based on the Gaussian distribution, we get the actual word count.
        int stringLength = (int) (r.nextGaussian() * standardDeviation + stringAvgLength);
        // At least it should be one.
        stringLength = Math.max(stringLength, 1);
        // At maximum, it cannot be greater than (stringLength + (stringLength - 1)) to make the distribution symmetric.
        //
        //    x = (stringLength - 1)                  x = (stringLength - 1)
        // 1 ------------------------ stringLength --------------------------- (stringLength + (stringLength - 1))
        // since the left range is between 1 and the given stringLength.
        stringLength = Math.min(stringLength, 2 * stringLength - 1);
        return generateRandomHexChars(stringLength);
    }

    private String generateRandomHexCharsGamma(int multiplier) {
        // Based on the gamma distribution...
        // At least the actual string length should be one.
        int stringLength = (int) (gd.sample() * multiplier) + 1;
        //System.out.print(stringLength+",");
        try {
            writer.write(Integer.toString(stringLength) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return generateRandomHexChars(stringLength);
    }

    private String generateRandomHexCharsUniformDist(int minSize, int maxSize) {
        int rangeSize = maxSize - minSize;
        assert(rangeSize > 0);
        int stringLength = r.nextInt(rangeSize + 1) + minSize;
        return generateRandomHexChars(stringLength);
    }

    private String generateRandomHexCharsZipfianDist(int minSize) {
        int rank = zipfGenerator.next(); // a number in the range of max-min
        int stringLength =  rank + minSize;
        return  generateRandomHexChars(stringLength);
    }

    public long getHexStringLength() {
        return hexStringLength;
    }

    public String getAvgHexStringLength() {
        if (hexStringLength > 0 && hexStringCount > 0) {
            return ("Hex-string-count: " + hexStringCount + " Total length(KB): " + (double)hexStringLength/1024 + " Max: "
                    + maxHexStringLength + " Min: " + minHexStringLength + " Avg: "
                    + decFormat.format((double) hexStringLength / hexStringCount) + " (KB): "
                    + decFormat.format((double) hexStringLength / hexStringCount / 1024));
        }
        return "";
    }

    public void closeGammaDistributionFile() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
