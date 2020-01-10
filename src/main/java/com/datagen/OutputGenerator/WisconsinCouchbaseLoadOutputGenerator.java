package com.datagen.OutputGenerator;

import com.couchbase.client.core.env.KeyValueServiceConfig;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WisconsinCouchbaseLoadOutputGenerator extends AWisconsinOutputGenerator {

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
    private static final String BUCKET_NAME_DEFAULT = "Test";
    private static final boolean IS_DELETE_BUCKET_IF_EXISTS_DEFAULT = true;
    private static final int MEMORY_QUOTA_DEFAULT = 4096; // In megabytes

    // Configurable members default values
    private static final int BATCH_LIMIT_DEFAULT = 10000; // Threshold to reach before batch upserting
    private static final double SCALE_FACTOR_DEFAULT = 1;
    private static final int PARTITIONS_DEFAULT = 2;
    private static final int PARTITION_DEFAULT = -1;
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 30000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;
    private static final int FAILURE_MAXIMUM_RETRIES_DEFAULT = 10;
    // Properties field names
    private static final String HOST_NAME_FIELD_NAME = "hostname";
    private static final String PORT_FIELD_NAME = "port";
    private static final String USER_NAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    private static final String IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME = "isdeleteifbucketexists";
    private static final String MEMORY_QUOTA_FIELD_NAME = "memoryquota";
    private static final String BATCH_LIMIT_FIELD_NAME = "batchlimit";
    private static final String PARTITIONS_FIELD_NAME = "partitions";
    private static final String PARTITION_FIELD_NAME = "partition";
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
        couchbaseConfiguration.put(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME, String.valueOf(IS_DELETE_BUCKET_IF_EXISTS_DEFAULT));
        couchbaseConfiguration.put(MEMORY_QUOTA_FIELD_NAME, String.valueOf(MEMORY_QUOTA_DEFAULT));
        couchbaseConfiguration.put(BATCH_LIMIT_FIELD_NAME, String.valueOf(BATCH_LIMIT_DEFAULT));
        couchbaseConfiguration.put(PARTITIONS_FIELD_NAME, String.valueOf(PARTITIONS_DEFAULT));
        couchbaseConfiguration.put(PARTITION_FIELD_NAME, String.valueOf(PARTITION_DEFAULT));
        couchbaseConfiguration.put(KV_ENDPOINTS_FIELD_NAME, String.valueOf(KV_ENDPOINTS_DEFAULT));
        couchbaseConfiguration.put(KV_TIMEOUT_FIELD_NAME, String.valueOf(KV_TIMEOUT_DEFAULT));
        couchbaseConfiguration.put(FAILURE_RETRY_DELAY_FIELD_NAME, String.valueOf(FAILURE_RETRY_DELAY_DEFAULT));
        couchbaseConfiguration.put(FAILURE_MAXIMUM_RETRIES_FIELD_NAME, String.valueOf(FAILURE_MAXIMUM_RETRIES_DEFAULT));
    }


    public WisconsinCouchbaseLoadOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        super(schema, generators);
    }

    public void initiate(){
        super.initiate();
        cluster = createAndAuthenticateCluster();
        Bucket bucket = openBucket(cluster);
        setUpBucketConfiguration(bucket);
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
        int batchLimit = Integer.valueOf(couchbaseConfiguration.get(BATCH_LIMIT_FIELD_NAME));
        int failureRetryDelay = Integer.valueOf(couchbaseConfiguration.get(FAILURE_RETRY_DELAY_FIELD_NAME));
        int failureMaximumRetries = Integer.valueOf(couchbaseConfiguration.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME));
        bucketConfiguration = new BucketConfiguration(bucket, batchLimit, failureRetryDelay, failureMaximumRetries);
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
