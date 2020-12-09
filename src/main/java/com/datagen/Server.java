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
package com.datagen;

import com.datagen.Constants.DataTypes;
import com.datagen.FieldGenerators.WisconsinBinaryGenerator;
import com.datagen.FieldGenerators.AWisconsinGenerator;
import com.datagen.FieldGenerators.WisconsinNumberGenerator;
import com.datagen.FieldGenerators.WisconsinStringGenerator;
import com.datagen.OutputGenerator.AWisconsinOutputGenerator;
import com.datagen.OutputGenerator.WisconsinAsterixDBLoadOutputGenerator;
import com.datagen.OutputGenerator.WisconsinFileOutputGenerator;
import com.datagen.Parser.Parser;
import com.datagen.schema.Field;
import com.datagen.schema.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

public class Server {

    private static Map<String, String> env = System.getenv();
    private static String DATAGEN_HOME = env.get("DATAGEN_HOME");
    public static String workloadsFolder = DATAGEN_HOME!=null?DATAGEN_HOME+"/Workloads/": "Workloads/";
    private static final Logger LOGGER = LogManager.getRootLogger();

    // Configuration file name
    private static final String PROPERTIES_FILE_NAME = "wisconsin_datagen.properties";
    private static final String PROPERTIES_FILE_PATH_FIELD_NAME = "propertiesfilepath";


//    // Properties field names -  Dataset
    public static final String WORKLOAD_NAME = "workload";
    public static final String FILESIZE_NAME = "filesize";
    public static final String CARDINALITY_NAME = "cardinality";
    public static final String BATCHSIZE_NAME = "batchsize";
    public static final String PARTITIONS_NAME = "partitions";
    public static final String PARTITION_NAME = "partition";
    public static final String FILEOUTPUT_NAME = "fileoutput";
    public static final String WRITER_NAME = "writer";
    public static final String ASTERIXDB_LOAD_PORT="AsterixDBLoadPort";
    public static final String HOST_NAME_FIELD_NAME = "hostname";
    public static final Map<String, String> JSONDataGenConfiguration = new HashMap<>();


    static {
        JSONDataGenConfiguration.put(PROPERTIES_FILE_PATH_FIELD_NAME,PROPERTIES_FILE_NAME);
    }


    public static void main(String[] args) throws Exception {
        Map<String, String> commandLineCfg = processCommandLineConfig(args);
        processAndSetConfiguration(commandLineCfg);

        String workload = workloadsFolder + (JSONDataGenConfiguration.containsKey(WORKLOAD_NAME)? JSONDataGenConfiguration.get(
                "workload"):
                "Default.json");

        Parser parser = new Parser();
        Schema schema = parser.parseWisconsinConfigFile(workload);
        List<AWisconsinGenerator> generators = new LinkedList<>();
        int fieldId = 0;
        AWisconsinGenerator wg;
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
        AWisconsinOutputGenerator outputGenerators = null;
            String writer = JSONDataGenConfiguration.get(WRITER_NAME);
            if (writer != null && writer.equalsIgnoreCase("asterixdb")){
                outputGenerators = new WisconsinAsterixDBLoadOutputGenerator(schema, generators);
            }
            else {
                outputGenerators = new WisconsinFileOutputGenerator(schema, generators);
            }
            if(outputGenerators!=null) {
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

    private static void processAndSetConfiguration(Map<String, String> cmdlineConfig) {
        // Command line configurations
        String propertiesFilePath = null;

        // Get the properties file if it was provided in command line arguments
        for (Map.Entry<String, String> entry : cmdlineConfig.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(PROPERTIES_FILE_PATH_FIELD_NAME)) {
                propertiesFilePath = entry.getValue();
                break;
            }
        }

        // Properties file configurations
        Map<String, String> propertiesFileConfig = readPropertiesFileConfiguration(propertiesFilePath);

        // Make sure the command line configs override the properties file configs
        JSONDataGenConfiguration.putAll(propertiesFileConfig);
        JSONDataGenConfiguration.putAll(cmdlineConfig);
    }

    /**
     * Reads the configuration from the properties file. Uses the default properties file if path for properties file
     * is not provided.
     *
     * @param propertiesFilePath properties file path that is provided by the user
     * @return a map containing the read configuration
     */
    private static Map<String, String> readPropertiesFileConfiguration(String propertiesFilePath) {
        Map<String, String> configs = new HashMap<>();
        boolean isPropertiesFilePathProvided = false;

        // User provided file path
        if (propertiesFilePath != null) {
            isPropertiesFilePathProvided = true;
            LOGGER.info("Loaded properties file: " + propertiesFilePath);
        } else {
            // No file provided, use default
            LOGGER.info("No properties file provided, using default properties file");
        }

        try (InputStream inputStream = isPropertiesFilePathProvided ?
                new FileInputStream(Paths.get(propertiesFilePath).toFile()) :
                WisconsinFileOutputGenerator.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME)) {

            Properties properties = new Properties();

            // Load the properties from the configuration file
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                // This should never happen, default configuration file exists in resources
                LOGGER.error("Properties configuration file not found.");
            }
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                configs.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
                System.out.println(entry.getKey().toString().toLowerCase()+":"+entry.getValue().toString()+"\n");
            }
        } catch (FileNotFoundException ex) {
            LOGGER.error("Configuration file not found. " + ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("Failed to read configuration file. " + ex.getMessage());
        }

        return configs;
    }
}
