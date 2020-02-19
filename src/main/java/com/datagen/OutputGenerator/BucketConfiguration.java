/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.datagen.OutputGenerator;

import com.couchbase.client.java.Bucket;

public class BucketConfiguration {
    private Bucket bucket;
    private int failureRetryDelay;
    private int failureMaximumRetries;

    BucketConfiguration(Bucket bucket, int failureRetryDelay, int failureMaximumRetries) {
        this.bucket = bucket;
        this.failureRetryDelay = failureRetryDelay;
        this.failureMaximumRetries = failureMaximumRetries;
    }

    public Bucket getBucket() {
        return bucket;
    }

    public int getFailureRetryDelay() {
        return failureRetryDelay;
    }

    public int getFailureMaximumRetries() {
        return failureMaximumRetries;
    }
}
