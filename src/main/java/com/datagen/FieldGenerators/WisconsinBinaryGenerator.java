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

import java.util.Random;
import java.util.Stack;

import com.datagen.Constants.DataTypes.DataType;
import com.datagen.Constants.Order;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;
import com.datagen.Server;
public class WisconsinBinaryGenerator extends AWisconsinGenerator {
    private static final char[] VALUES = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    private static final int HEXRANGE = 15;
    Random r;

    public WisconsinBinaryGenerator(Schema schema, int fieldId) {
        super(schema, fieldId);
        r = new Random(fieldId);
        generatorType = DataType.BINARY;
    }

    public String next(long seed) {
        Field f = schema.getFields().get(fieldId);
        if (getValueType() ==ValueType.NULL){
            return "\"null\"";
        } else if (getValueType() ==ValueType.NULL){
            return "#MISSING";
        }
        if (f.getOrder() != null && f.getOrder() == Order.order.RANDOM) {
            seed = rand(seed, Long.valueOf(Server.JSONDataGenConfiguration.get(Server.CARDINALITY_NAME)));
        }
        if (f.getRange() > 0) {
            seed = seed % f.getRange();
        }
        Stack<Character> temp = new Stack<>();

        r.setSeed(seed);
        char nextHex;
        int length = schema.getFields().get(fieldId).getStringLength();
        for (int i = 0; i < length; i++) {
            nextHex = VALUES[r.nextInt(HEXRANGE)];
            temp.push(nextHex);
        }

        char[] returnCharValues = new char[length];
        int index = 0;
        while (!temp.isEmpty()) {
            returnCharValues[index] = temp.pop();
            index++;
        }

        return String.valueOf(returnCharValues);

    }
}
