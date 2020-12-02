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

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;
import com.datagen.Server;
import org.apache.commons.math3.distribution.GammaDistribution;

import java.util.Random;

public class WisconsinNumberGenerator extends WisconsinGenerator {
    GammaDistribution  gd;
    Random r;
    Field field;
    int numberOfDuplicatesOfNegOne;
    int currentNumberOfNegOnes;

    public WisconsinNumberGenerator(Schema schema, int fieldId) {
        super(schema, fieldId);
        generatorType = DataType.INTEGER;
         field = schema.getFields().get(fieldId);
        gd = new GammaDistribution(field.getShape(), field.getScale());
        r= new Random(fieldId);
        numberOfDuplicatesOfNegOne = field.getNumberOfDupicatesOfNegOne();
        currentNumberOfNegOnes = 0;
    }

    @Override
    public Object next(long seed) {
        if (currentNumberOfNegOnes < numberOfDuplicatesOfNegOne) {
            currentNumberOfNegOnes++;
            return -1;
        }

        long result;
        long cardinality = Long.valueOf(Server.couchbaseConfiguration.get(Server.CARDINALITY_NAME));
        // Use cardinality and Jim Gray's algo
        if (field.getOrder() == Order.order.RANDOM) {
            result = rand(seed, cardinality);
        }
        else
            result = seed;
        if (field.getRange() > 0) {
            result = result % field.getRange();
        }
        if (field.isEven()) {
            result *= 2;
        }
        if (field.isOdd()) {
            result = result * 2 + 1;
        }

        if (field.isGammaDistribution()) {

            long range = field.getRange() > 0? field.getRange():cardinality;
            double sample = gd.sample();
            result = (int)(( sample * result)%range);
            System.out.print(result+",");
        } else if (field.isNormalDistribution()) {
            long range = field.getRange() > 0 ? field.getRange():cardinality;
            double stdev = field.getStandardDeviation() >= 1 ? field.getStandardDeviation():
                    Math.ceil(field.getStandardDeviation()*range);
            result = (int)(r.nextGaussian() * stdev +(range/2));
            return result;

        }
        return result;
    }

}
