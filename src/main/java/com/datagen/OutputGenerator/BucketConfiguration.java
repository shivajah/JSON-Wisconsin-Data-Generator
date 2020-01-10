/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.datagen.OutputGenerator;

import com.couchbase.client.java.Bucket;

public class BucketConfiguration {
    private Bucket bucket;
    private int batchLimit;
    private int failureRetryDelay;
    private int failureMaximumRetries;

    BucketConfiguration(Bucket bucket, int batchLimit, int failureRetryDelay, int failureMaximumRetries) {
        this.bucket = bucket;
        this.batchLimit = batchLimit;
        this.failureRetryDelay = failureRetryDelay;
        this.failureMaximumRetries = failureMaximumRetries;
    }

    public Bucket getBucket() {
        return bucket;
    }

    public void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }

    public int getBatchLimit() {
        return batchLimit;
    }

    public void setBatchLimit(int batchLimit) {
        this.batchLimit = batchLimit;
    }

    public int getFailureRetryDelay() {
        return failureRetryDelay;
    }

    public void setFailureRetryDelay(int failureRetryDelay) {
        this.failureRetryDelay = failureRetryDelay;
    }

    public int getFailureMaximumRetries() {
        return failureMaximumRetries;
    }

    public void setFailureMaximumRetries(int failureMaximumRetries) {
        this.failureMaximumRetries = failureMaximumRetries;
    }
}
