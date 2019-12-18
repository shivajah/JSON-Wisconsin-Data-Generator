package com.datagen;

import java.util.LinkedList;
import java.util.List;

import com.datagen.Constants.DataTypes;
import com.datagen.Generators.wisconsingenerators.WisconsinBinaryGenerator;
import com.datagen.Generators.wisconsingenerators.WisconsinGenerator;
import com.datagen.Generators.wisconsingenerators.WisconsinNumberGenerator;
import com.datagen.Generators.wisconsingenerators.WisconsinStringGenerator;
import com.datagen.Output.WisconsinOutputGenerator;
import com.datagen.Parser.Parser;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;

/**
 * Created by shiva on 2/21/18.
 */

public class Server {
    public static final String wisconsin = "WISCONSIN";
    public static String algo = wisconsin;

    public static void main(String[] args) throws Exception {
        String configFile = "/Users/shiva/workspace/datagen/src/main/java/com/datagen/configs/wisconsin_1GB_std_zero_fixedLength_noBigObject.json";
        if (args.length > 0) {
            configFile = args[1];
            System.out.println(
                   "A config file is provided. The generator will use the provided config file:" + configFile);
        } else {
            System.out.println(
                   "No config file is provided. The generator will use the default config file:" + configFile);
        }

        if (algo.equalsIgnoreCase(wisconsin)) {
            Parser parser = new Parser();
            List<Schema> schemas = parser.parseWisconsinConfigFile(configFile);
            for (Schema schema : schemas) {
                List<WisconsinGenerator> generators = new LinkedList<>();
                int fId = 0;
                WisconsinGenerator wg;
                for (Field field : schema.getFields()) {
                    if (field.getDomain() == null) {
                        //use id
                        if (field.getType() == DataTypes.DataType.INTEGER) {
                            wg = new WisconsinNumberGenerator(schema, fId);
                            generators.add(wg);
                        } else if (field.getType() == DataTypes.DataType.STRING) {
                            wg = new WisconsinStringGenerator(schema, fId);
                            generators.add(wg);
                        } else if (field.getType() == DataTypes.DataType.BINARY) {
                            wg = new WisconsinBinaryGenerator(schema, fId);
                            generators.add(wg);
                        } else {
                            throw new IllegalArgumentException(
                                    "Invalid data type has been entered.Currently we only support String and Integer.");
                        }
                    }
                    fId++;
                }
                WisconsinOutputGenerator outputGenerators = new WisconsinOutputGenerator(schema, generators);
                outputGenerators.execute();
                System.out.println("maxRecordSize : " + outputGenerators.getMaxRecordLength() + " bytes");
            }
        }
    }
}
