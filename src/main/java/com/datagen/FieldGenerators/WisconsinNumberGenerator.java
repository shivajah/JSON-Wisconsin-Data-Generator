package com.datagen.FieldGenerators;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.Schema.Field;
import com.datagen.Schema.Schema;
import org.apache.commons.math3.distribution.GammaDistribution;

public class WisconsinNumberGenerator extends WisconsinGenerator {
    GammaDistribution  gd;
    Field field;
    public WisconsinNumberGenerator(Schema schema, int fieldId) {
        super(schema, fieldId);
        generatorType = DataType.INTEGER;
         field = schema.getFields().get(fieldId);
        gd = new GammaDistribution(field.getShape(), field.getScale());
    }

    @Override
    public Object next(long seed) {
        long result;
        // Use cardinality and Jim Gray's algo
        if (field.getOrder() == Order.order.RANDOM)
            result = rand(seed, schema.getCardinality());
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

        if (!field.isNormalDistribution()) {

            long range = field.getRange() > 0? field.getRange():schema.getCardinality();
            double sample = gd.sample();
            result = (int)(( sample * result)%range);
            System.out.print(result+",");
        }
        return result;
    }

}
