package com.juliasoft.flink.mongodb;

import com.mongodb.async.SingleResultCallback;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB Batch Sink implementation (with delay in mills)
 */
public class MongoDBBatchSink extends MongoDBSink {
    public MongoDBBatchSink(String connectionString, String databaseName, String collectionName, SingleResultCallback<Void> resultCallback, long mills) {
        super(connectionString, databaseName, collectionName, resultCallback);
        this.mills = mills;
    }

    public MongoDBBatchSink(String connectionString, String databaseName, String collectionName, long mills) {
        super(connectionString, databaseName, collectionName);
        this.mills = mills;
    }

    @Override
    public void invoke(Document value) throws Exception {
        data.add(value);
        if (updateCondition()) {
            invokeMany(data);
            data = new ArrayList<>();
            lastTime = System.currentTimeMillis();
        }
    }

    private boolean updateCondition() {
        return System.currentTimeMillis() - lastTime >= mills;
    }

    private long lastTime = System.currentTimeMillis();
    private final long mills;
    private List<Document> data = new ArrayList<>();
}
