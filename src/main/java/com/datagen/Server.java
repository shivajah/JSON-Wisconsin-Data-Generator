package com.datagen;

import java.util.LinkedList;
import java.util.List;

import com.datagen.Constants.DataTypes;
import com.datagen.FieldGenerators.WisconsinBinaryGenerator;
import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.FieldGenerators.WisconsinNumberGenerator;
import com.datagen.FieldGenerators.WisconsinStringGenerator;
import com.datagen.OutputGenerator.AWisconsinOutputGenerator;
import com.datagen.OutputGenerator.WisconsinCouchbaseLoadOutputGenerator;
import com.datagen.OutputGenerator.WisconsinFileOutputGenerator;
import com.datagen.Parser.Parser;
import com.datagen.Schema.Field;
import com.datagen.Schema.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by shiva on 2/21/18.
 */

public class Server {

    private static final Logger LOGGER = LogManager.getRootLogger();


    public static void main(String[] args) throws Exception {

        //TODO: Have a default workload. Also have a properties file to read the workload from.
        if (args.length < 1) {
            LOGGER.error("Workload needs to be specified. Exiting.");
            return;
        }

        Parser parser = new Parser();
        List<Schema> schemas = parser.parseWisconsinConfigFile(args[1]);//args[1] is the workload name
        for (Schema schema : schemas) {
            List<WisconsinGenerator> generators = new LinkedList<>();
            int fieldId = 0;
            WisconsinGenerator wg;
            for (Field field : schema.getFields()) {
                    //use field Id
                    if (field.getType() == DataTypes.DataType.INTEGER) {
                        wg = new WisconsinNumberGenerator(schema, fieldId);
                        generators.add(wg);
                    } else if (field.getType() == DataTypes.DataType.STRING) {
                        wg = new WisconsinStringGenerator(schema, fieldId);
                        generators.add(wg);
                    } else if (field.getType() == DataTypes.DataType.BINARY) {
                        wg = new WisconsinBinaryGenerator(schema, fieldId);
                        generators.add(wg);
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid data type has been entered.Currently we only support String and Integer.");
                    }
                fieldId++;
            }
            AWisconsinOutputGenerator outputGenerators = new WisconsinCouchbaseLoadOutputGenerator(schema, generators);
            outputGenerators.execute();
        }
    }
}
