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
        if (nextNull < seed) {
            nextNull = nextNull(Math.toIntExact(seed));
            schema.getFields().get(fieldId).setNulls(field.getNulls() + 1);
        }
        if (nextMissing < seed) {
            nextMissing = nextMissing(Math.toIntExact(seed));
            schema.getFields().get(fieldId).setMissings(field.getMissings() + 1);
        }

        if (nextNull <= nextMissing && nextNull == seed) {
            nextNull = nextNull(Math.toIntExact(seed) + 1);
            return null;
        }
        if (nextMissing <= nextNull && nextMissing == seed) {
            nextMissing = nextMissing(Math.toIntExact(seed) + 1);
            return Long.MAX_VALUE;
        }
        long result;

        //if(field.getDomain()== null){//no domain provided==> use cardinality and Jim Gray's algo
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

            int range = field.getRange() > 0? field.getRange():schema.getCardinality();
            //System.out.println("range "+range);
            double sample = gd.sample();
            //System.out.println("sample " + sample);
            result = (int)(( sample * result)%range);
            System.out.print(result+",");
        }
        //System.out.print(result+",");
        return result;
    }

}
