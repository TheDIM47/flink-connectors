package com.juliasoft.flink.mongodb;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB Sink
 */
public class MongoDBSink extends RichSinkFunction<Document> implements Serializable {
    public static class EmptyCallback implements SingleResultCallback<Void>, Serializable {
        public static final EmptyCallback INSTANCE = new EmptyCallback();
        @Override
        public void onResult(Void result, Throwable t) {
            if (t != null) {
                log.error(t.getMessage(), t);
            }
        }
    }

    public MongoDBSink(String connectionString, String databaseName, String collectionName,
                       SingleResultCallback<Void> resultCallback) {
        Preconditions.checkNotNull(connectionString, "Connection string must not be null!");
        Preconditions.checkNotNull(databaseName, "You must provide database name!");
        Preconditions.checkNotNull(collectionName, "You must provide collection name!");
        Preconditions.checkNotNull(resultCallback, "You must provide callback function!");
        this.connectionString = connectionString;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.resultCallback = resultCallback;
    }

    public MongoDBSink(String connectionString, String databaseName, String collectionName) {
        this(connectionString, databaseName, collectionName, EmptyCallback.INSTANCE);
    }

    @Override
    public void invoke(org.bson.Document value) throws Exception {
        mongoCollection.insertOne(value, resultCallback);
    }

    public void invokeMany(List<Document> values) throws Exception {
        mongoCollection.insertMany(values, resultCallback);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create(connectionString);
        mongoDatabase = mongoClient.getDatabase(databaseName);
        if (!collectionExists(mongoDatabase, collectionName)) {
            mongoDatabase.createCollection(collectionName, EmptyCallback.INSTANCE);
        }
        mongoCollection = mongoDatabase.getCollection(collectionName);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
        super.close();
    }

    private boolean collectionExists(MongoDatabase db, String coll) throws InterruptedException {
        final boolean[] result = new boolean[1];
        final CountDownLatch latch = new CountDownLatch(1);
        db.listCollections()
                .forEach(document -> {
                    if (!result[0]) {
                        result[0] = document != null && document.get("name") != null && coll.equalsIgnoreCase(document.getString("name"));
                    }
                }, (o, t) -> {
                    if (t != null) log.error(t.getMessage(), t);
                    latch.countDown();
                });
        latch.await(5, TimeUnit.SECONDS);
        return result[0];
    }

    private transient MongoCollection mongoCollection;
    private transient MongoDatabase mongoDatabase;
    private transient MongoClient mongoClient;

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final SingleResultCallback<Void> resultCallback;

    private static final Logger log = LoggerFactory.getLogger(MongoDBSink.class);
}
