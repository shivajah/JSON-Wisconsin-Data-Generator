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
import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.FieldGenerators.WisconsinNumberGenerator;
import com.datagen.FieldGenerators.WisconsinStringGenerator;
import com.datagen.OutputGenerator.AWisconsinOutputGenerator;
import com.datagen.OutputGenerator.WisconsinAsterixDBLoadOutputGenerator;
import com.datagen.OutputGenerator.WisconsinCouchbaseLoadOutputGenerator;
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
    private static String DATAGEN_HOME = env.get("DATAGEN_HOME") == null ? "/Users/shiva/workspace/datagen_afterCB/":env.get(
            "DATAGEN_HOME");
    public static String workloadsFolder = DATAGEN_HOME+"/Workloads/";
    private static final Logger LOGGER = LogManager.getRootLogger();

    // Configuration file name
    private static final String PROPERTIES_FILE_NAME = "wisconsin_datagen.properties";

    // Couchbase cluster and bucket configs default values
    private static final String HOST_NAME_DEFAULT = "localhost";
    public static final int PORT_DEFAULT = -9999; // Let the SDK use its own default if not provided
    private static final String USER_NAME_DEFAULT = "Administrator";
    private static final String PASSWORD_DEFAULT = "pass123";
    private static final String BUCKET_NAME_DEFAULT = "sample";

    // Dataset default values
    private static final String WORKLOAD_NAME_DEFUALT = "wisconsin_1GB_std_zero_fixedLength_noBigObject_3kbEachRecord.json";
    private static final int FILESIZE_DEAFULT = -1; // If <0 program finishes when cardinality is reached, otherwise faster_reached(filesize,cardinality) will br the terminator of the program.
    private static final int BATCHSIZE_DEFAULT = 1000;
    private static final int PARTITIONS_DEFAULT = 1;
    private static final int PARTITION_DEFAULT = -1;
    private static final String FILEOUTPUT_NAME_DEFAULT = "output.adm";
    private static final long CARDINALITY_DEFAULT = 999;
    private static final String WRITER_NAME_DEFAULT = "file";
    private static final int ASTERIXDB_LOAD_PORT_DEFAULT=10001;

    // Configurable members default values
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 30000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;
    private static final int FAILURE_MAXIMUM_RETRIES_DEFAULT = 10;
    // Properties field names -  Couchbase Loader

    private static final String PROPERTIES_FILE_PATH_FIELD_NAME = "propertiesfilepath";
    public static final String HOST_NAME_FIELD_NAME = "hostname";
    public static final String PORT_FIELD_NAME = "port";
    public static final String USER_NAME_FIELD_NAME = "username";
    public static final String PASSWORD_FIELD_NAME = "password";
    public static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    public static final String KV_ENDPOINTS_FIELD_NAME = "kvendpoints";
    public static final String KV_TIMEOUT_FIELD_NAME = "kvtimeout";
    public static final String FAILURE_RETRY_DELAY_FIELD_NAME = "failureretrydelay";
    public static final String FAILURE_MAXIMUM_RETRIES_FIELD_NAME = "failuremaximumretries";
    public static final Map<String, String> couchbaseConfiguration = new HashMap<>();

    // Properties field names -  Dataset
    public static final String WORKLOAD_NAME = "workload";
    public static final String FILESIZE_NAME = "filesize";
    public static final String CARDINALITY_NAME = "cardinality";
    public static final String BATCHSIZE_NAME = "batchsize";
    public static final String PARTITIONS_NAME = "partitions";
    public static final String PARTITION_NAME = "partition";
    public static final String FILEOUTPUT_NAME = "fileoutput";
    public static final String WRITER_NAME = "writer";
    public static final String ASTERIXDB_LOAD_PORT="AsterixDBLoadPort";


    static {
        couchbaseConfiguration.put(HOST_NAME_FIELD_NAME, HOST_NAME_DEFAULT);
        couchbaseConfiguration.put(PORT_FIELD_NAME, String.valueOf(PORT_DEFAULT));
        couchbaseConfiguration.put(USER_NAME_FIELD_NAME, USER_NAME_DEFAULT);
        couchbaseConfiguration.put(PASSWORD_FIELD_NAME, PASSWORD_DEFAULT);
        couchbaseConfiguration.put(BUCKET_NAME_FIELD_NAME, BUCKET_NAME_DEFAULT);
        couchbaseConfiguration.put(KV_ENDPOINTS_FIELD_NAME, String.valueOf(KV_ENDPOINTS_DEFAULT));
        couchbaseConfiguration.put(KV_TIMEOUT_FIELD_NAME, String.valueOf(KV_TIMEOUT_DEFAULT));
        couchbaseConfiguration.put(FAILURE_RETRY_DELAY_FIELD_NAME, String.valueOf(FAILURE_RETRY_DELAY_DEFAULT));
        couchbaseConfiguration.put(FAILURE_MAXIMUM_RETRIES_FIELD_NAME, String.valueOf(FAILURE_MAXIMUM_RETRIES_DEFAULT));

        //Dataset
        couchbaseConfiguration.put(WORKLOAD_NAME, WORKLOAD_NAME_DEFUALT);
        couchbaseConfiguration.put(FILESIZE_NAME, String.valueOf(FILESIZE_DEAFULT));
        couchbaseConfiguration.put(BATCHSIZE_NAME, String.valueOf(BATCHSIZE_DEFAULT));
        couchbaseConfiguration.put(PARTITIONS_NAME, String.valueOf(PARTITIONS_DEFAULT));
        couchbaseConfiguration.put(PARTITION_NAME, String.valueOf(PARTITION_DEFAULT));
        couchbaseConfiguration.put(FILEOUTPUT_NAME, FILEOUTPUT_NAME_DEFAULT);
        couchbaseConfiguration.put(CARDINALITY_NAME, String.valueOf(CARDINALITY_DEFAULT));
        couchbaseConfiguration.put(WRITER_NAME, WRITER_NAME_DEFAULT);
        couchbaseConfiguration.put(ASTERIXDB_LOAD_PORT, String.valueOf(ASTERIXDB_LOAD_PORT_DEFAULT));
    }


    public static void main(String[] args) throws Exception {
        Map<String, String> commandLineCfg = processCommandLineConfig(args);
        processAndSetConfiguration(commandLineCfg);

        String workload = workloadsFolder + (couchbaseConfiguration.containsKey("workload")? couchbaseConfiguration.get("workload"):
                "wisconsin_1GB_std_zero_fixedLength_noBigObject_1p23kbEachRecord.json");

        Parser parser = new Parser();
        Schema schema = parser.parseWisconsinConfigFile(workload);
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
        AWisconsinOutputGenerator outputGenerators = null;
            String writer = couchbaseConfiguration.get("writer");
            if (writer != null && writer.equalsIgnoreCase("couchbase")) {
                outputGenerators = new WisconsinCouchbaseLoadOutputGenerator(schema, generators);
            }
            else if (writer != null && writer.equalsIgnoreCase("asterixdb")){
                outputGenerators = new WisconsinAsterixDBLoadOutputGenerator(schema, generators);
            }
            else {
                outputGenerators = new WisconsinFileOutputGenerator(schema, generators);
            }
        outputGenerators.execute();
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
        couchbaseConfiguration.putAll(propertiesFileConfig);
        couchbaseConfiguration.putAll(cmdlineConfig);
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
                WisconsinCouchbaseLoadOutputGenerator.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME)) {

            Properties properties = new Properties();

            // Load the properties from the configuration file
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                // This should never happen, default configuration file exists in resources
                LOGGER.error("Properties configuration file not found");
            }

            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                configs.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
            }
        } catch (FileNotFoundException ex) {
            LOGGER.error("Configuration file not found. " + ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("Failed to read configuration file. " + ex.getMessage());
        }

        return configs;
    }
}
