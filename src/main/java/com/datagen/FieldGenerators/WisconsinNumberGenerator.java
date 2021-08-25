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
import com.datagen.Schema.Field;
import com.datagen.Schema.Schema;
import com.datagen.Server;
import org.apache.commons.math3.distribution.GammaDistribution;

import java.util.Random;

public class WisconsinNumberGenerator extends AWisconsinGenerator {
    GammaDistribution  gd;
    Random r;
    Field field;

    public WisconsinNumberGenerator(Schema schema, int fieldId) {
        super(schema, fieldId);
        generatorType = DataType.INTEGER;
        field = schema.getFields().get(fieldId);
        gd = new GammaDistribution(field.getShape(), field.getScale());
        r= new Random(fieldId);
    }

    @Override
    public Object next(long seed) {

        long result;
        long cardinality = Long.valueOf(Server.JSONDataGenConfiguration.get(Server.CARDINALITY_NAME));
        ValueType type=getValueType();
        if (type == ValueType.NULL){
            return "\"null\"";
        } else if ( type == ValueType.MISSING){
            return "#MISSING";
        }
        // Use cardinality and Jim Gray's algo
        if (field.getOrder() != null &&field.getOrder() == Order.order.RANDOM) {
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

        } else if (field.isNormalDistribution()) {
            double mean = field.getMean() > 0? field.getMean():cardinality/2;
            double stdev = field.getStandardDeviation();
            result = (int)(r.nextGaussian() * stdev +mean);
            return result;

        }
        return result;
    }

}
