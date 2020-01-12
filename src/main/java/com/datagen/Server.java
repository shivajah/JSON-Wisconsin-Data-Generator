package com.datagen;

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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by shiva on 2/21/18.
 */

public class Server {

    private static final Logger LOGGER = LogManager.getRootLogger();
    public static final String workloadsFolder = "src/main/java/com/datagen/Workloads/";
    public static void main(String[] args) throws Exception {

        Map<String, String> commandLineCfg = processCommandLineConfig(args);
        String workload = workloadsFolder + (commandLineCfg.containsKey("workload")? commandLineCfg.get("workload"):
                "wisconsin_1GB_std_zero_fixedLength_noBigObject.json");

        Parser parser = new Parser();
        List<Schema> schemas = parser.parseWisconsinConfigFile(workload);
        for (Schema schema : schemas) {
            List<WisconsinGenerator> generators = new LinkedList<>();
            int fieldId = 0;
            WisconsinGenerator wg;
            for (Field field : schema.getFields()) {
                    //use field Id
                    if (field.getType() == DataTypes.DataType.INTEGER) {
                        wg = new WisconsinNumberGenerator(schema, fieldId);
                    } else if (field.getType() == DataTypes.DataType.STRING) {
                        wg = new WisconsinStringGenerator(schema, fieldId);
                    } else if (field.getType() == DataTypes.DataType.BINARY) {
                        wg = new WisconsinBinaryGenerator(schema, fieldId);
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid data type has been entered.Currently we only support String and Integer.");
                    }
                generators.add(wg);
                fieldId++;
            }

            // Default outputwriter is filewriter.
            AWisconsinOutputGenerator outputGenerators = new WisconsinFileOutputGenerator(schema, generators);
            if (commandLineCfg.containsKey("writer")) {
                String writer = commandLineCfg.get("writer");
                if (writer.equalsIgnoreCase("couchbase")) {
                    outputGenerators = new WisconsinCouchbaseLoadOutputGenerator(schema, generators);
                }
            }
            outputGenerators.execute();
        }
    }

    private static Map<String, String> processCommandLineConfig(String[] args) {
        Map<String, String> commandLineConfig = new HashMap<>();
        if (args != null) {
            for (String arg: args) {
                if (arg.contains("=")) {
                    commandLineConfig.put(arg.substring(0, arg.indexOf("=")).toLowerCase(),
                            arg.substring(arg.indexOf("=")+1));
                }
            }
        }
        return commandLineConfig;
    }
}
