package com.datagen.OutputGenerator;

import com.couchbase.client.core.env.KeyValueServiceConfig;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.datagen.FieldGenerators.WisconsinGenerator;
import com.datagen.Schema.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WisconsinCouchbaseLoadOutputGenerator extends AWisconsinOutputGenerator {

    private Map<String,String> commandLineConfig;

    private BucketConfiguration bucketConfiguration;
    private Cluster cluster;

    private static final Logger LOGGER = LogManager.getRootLogger();

    // Configuration file name
    private static final String PROPERTIES_FILE_NAME = "wisconsin_datagen.properties";

    // Couchbase cluster and bucket configs default values
    private static final String HOST_NAME_DEFAULT = "localhost";
    private static final int PORT_DEFAULT = -9999; // Let the SDK use its own default if not provided
    private static final String USER_NAME_DEFAULT = "Administrator";
    private static final String PASSWORD_DEFAULT = "pass123";
    private static final String BUCKET_NAME_DEFAULT = "sample";

    // Configurable members default values
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 30000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;
    private static final int FAILURE_MAXIMUM_RETRIES_DEFAULT = 10;
    // Properties field names
    private static final String PROPERTIES_FILE_PATH_FIELD_NAME = "propertiesfilepath";
    private static final String HOST_NAME_FIELD_NAME = "hostname";
    private static final String PORT_FIELD_NAME = "port";
    private static final String USER_NAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    private static final String KV_ENDPOINTS_FIELD_NAME = "kvendpoints";
    private static final String KV_TIMEOUT_FIELD_NAME = "kvtimeout";
    private static final String FAILURE_RETRY_DELAY_FIELD_NAME = "failureretrydelay";
    private static final String FAILURE_MAXIMUM_RETRIES_FIELD_NAME = "failuremaximumretries";
    private static final Map<String, String> couchbaseConfiguration = new HashMap<>();

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
    }


    public WisconsinCouchbaseLoadOutputGenerator(Schema schema, List<WisconsinGenerator> generators, Map<String, String> commandLineConfig) {
        super(schema, generators);
        this.commandLineConfig = commandLineConfig;
        initiate();
    }

    public void initiate(){
        super.initiate();
        processAndSetConfiguration(commandLineConfig);
        cluster = createAndAuthenticateCluster();
        Bucket bucket = openBucket(cluster);
        setUpBucketConfiguration(bucket);
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
    private static Cluster createAndAuthenticateCluster() {
        String hostname = couchbaseConfiguration.get(HOST_NAME_FIELD_NAME);
        String username = couchbaseConfiguration.get(USER_NAME_FIELD_NAME);
        String password = couchbaseConfiguration.get(PASSWORD_FIELD_NAME);
        int port = Integer.valueOf(couchbaseConfiguration.get(PORT_FIELD_NAME));
        int kvEndpoints = Integer.valueOf(couchbaseConfiguration.get(KV_ENDPOINTS_FIELD_NAME));
        int kvTimeout = Integer.valueOf(couchbaseConfiguration.get(KV_TIMEOUT_FIELD_NAME));

        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder().kvTimeout(kvTimeout)
                .keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)).continuousKeepAliveEnabled(false);

        // Use the provided port if supplied
        if (port != PORT_DEFAULT) {
            builder.bootstrapHttpDirectPort(port);
        }

        // Build the environment
        LOGGER.info("Provided properties: " + couchbaseConfiguration.toString());
        CouchbaseEnvironment environment = builder.build();

        String address = "hostname: " + hostname + " port: " + environment.bootstrapHttpDirectPort();
        LOGGER.info("Creating and authenticating cluster on: " + address);
        Cluster cluster = CouchbaseCluster.create(environment, hostname);
        cluster.authenticate(username, password);
        LOGGER.info("Cluster created and authenticated successfully on: " + address);

        return cluster;
    }

    /**
     * Opens the bucket.
     *
     * @param cluster Cluster which the bucket is created on.
     * @return Returns the bucket to upsert the data to.
     */
    private static Bucket openBucket(Cluster cluster) {
        String bucketName = couchbaseConfiguration.get(BUCKET_NAME_FIELD_NAME);

        LOGGER.info("opening bucket " + bucketName);
        Bucket bucket = cluster.openBucket(bucketName);
        LOGGER.info(bucketName + " bucket opened");

        return bucket;
    }


    private void setUpBucketConfiguration(Bucket bucket) {
        int failureRetryDelay = Integer.valueOf(couchbaseConfiguration.get(FAILURE_RETRY_DELAY_FIELD_NAME));
        int failureMaximumRetries = Integer.valueOf(couchbaseConfiguration.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME));
        bucketConfiguration = new BucketConfiguration(bucket, failureRetryDelay, failureMaximumRetries);
    }


    @Override
    public void write(int threadId, List<String> batchOfRecords, int batchIndex) {
        List<JsonDocument> jsonDocuments = new LinkedList<>();

        for (int i = 0; i < batchOfRecords.size(); i++) {
              JsonObject jsonObject = JsonObject.fromJson(batchOfRecords.get(i));
                jsonDocuments.add(JsonDocument.create(threadId+ "_" + batchIndex + "_" + i, jsonObject));
        }
        Observable.from(jsonDocuments).flatMap(
                (final JsonDocument docToInsert) -> bucketConfiguration.getBucket().async().upsert(docToInsert)
                        .retryWhen(RetryBuilder.anyOf(Exception.class).delay(Delay
                                .fixed(bucketConfiguration.getFailureRetryDelay(), TimeUnit.MILLISECONDS))
                                .max(bucketConfiguration.getFailureMaximumRetries()).build()))
                .onErrorReturn(throwable -> {
                    LOGGER.error(throwable.getMessage());
                    return null;
                }).toBlocking().last();

    }

    @Override
    public void closeWriter(int threadID) {
        cluster.disconnect();
    }

}
