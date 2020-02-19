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
import com.datagen.Server;
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


    private static final Logger LOGGER = LogManager.getRootLogger();
    private BucketConfiguration bucketConfiguration;
    private Cluster cluster;



    public WisconsinCouchbaseLoadOutputGenerator(Schema schema, List<WisconsinGenerator> generators) {
        super(schema, generators);
        initiate();
    }

    public void initiate(){
        super.initiate();
        cluster = createAndAuthenticateCluster();
        Bucket bucket = openBucket(cluster);
        setUpBucketConfiguration(bucket);
    }


    private static Cluster createAndAuthenticateCluster() {
        String hostname = Server.couchbaseConfiguration.get(Server.HOST_NAME_FIELD_NAME);
        String username = Server.couchbaseConfiguration.get(Server.USER_NAME_FIELD_NAME);
        String password = Server.couchbaseConfiguration.get(Server.PASSWORD_FIELD_NAME);
        int port = Integer.valueOf(Server.couchbaseConfiguration.get(Server.PORT_FIELD_NAME));
        int kvEndpoints = Integer.valueOf(Server.couchbaseConfiguration.get(Server.KV_ENDPOINTS_FIELD_NAME));
        int kvTimeout = Integer.valueOf(Server.couchbaseConfiguration.get(Server.KV_TIMEOUT_FIELD_NAME));

        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder().kvTimeout(kvTimeout)
                .keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)).continuousKeepAliveEnabled(false);

        // Use the provided port if supplied
        if (port != Server.PORT_DEFAULT) {
            builder.bootstrapHttpDirectPort(port);
        }

        // Build the environment
        LOGGER.info("Provided properties: " + Server.couchbaseConfiguration.toString());
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
        String bucketName = Server.couchbaseConfiguration.get(Server.BUCKET_NAME_FIELD_NAME);

        LOGGER.info("opening bucket " + bucketName);
        Bucket bucket = cluster.openBucket(bucketName);
        LOGGER.info(bucketName + " bucket opened");

        return bucket;
    }


    private void setUpBucketConfiguration(Bucket bucket) {
        int failureRetryDelay = Integer.valueOf(Server.couchbaseConfiguration.get(Server.FAILURE_RETRY_DELAY_FIELD_NAME));
        int failureMaximumRetries = Integer.valueOf(Server.couchbaseConfiguration.get(Server.FAILURE_MAXIMUM_RETRIES_FIELD_NAME));
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
