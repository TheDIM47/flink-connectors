package com.juliasoft.flink.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.util.Map;

/**
 * RabbitMQ RichSinkFunction implementation
 * Inspired by https://github.com/apache/flink/tree/master/flink-streaming-connectors/flink-connector-rabbitmq
 * @see open(...) for implementation details
 */
public class RabbitMQSink<T> extends RichSinkFunction<T> {
    public RabbitMQSink(RMQConnectionFactoryBuilder fb, SerializationSchema<T> schema) {
        this.fb = fb;
        this.schema = schema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        final ConnectionFactory f = fb.build();
        connection = f.newConnection();
        channel = connection.createChannel();
        if (channel == null) {
            throw new RuntimeException("None of RabbitMQ channels are available");
        }

        // will use exchange if exchange name is set
        final boolean usingExchange = (exchangeName != null && !exchangeName.isEmpty());
        if (usingExchange) {
            if (declarePassiveExchange) {
                channel.exchangeDeclarePassive(exchangeName);
            } else {
                channel.exchangeDeclare(exchangeName, exchangeType, durableExchange, autoDeleteExchange, exclusiveExchange, exchangeArgs);
            }
        }

        if (queueName == null || queueName.isEmpty()) {
            AMQP.Queue.DeclareOk result = channel.queueDeclare();
            queueName = result.getQueue();
        } else if (declarePassiveQueue) {
            channel.queueDeclarePassive(queueName);
        } else {
            channel.queueDeclare(queueName, durableQueue, exclusiveQueue, autoDeleteQueue, queueArgs);
        }

        // bind if we use exchange
        if (usingExchange) {
            channel.queueBind(queueName, exchangeName, routingKey, bindArgs);
        }

        // if exchange name not set - empty string will be used instead
        if (exchangeName == null || exchangeName.isEmpty()) {
            exchangeName = "";
        }

        // if routing key not set - queue name will be used instead
        if (routingKey == null || routingKey.isEmpty()) {
            routingKey = queueName;
        }
    }

    @Override
    public void invoke(T value) throws Exception {
        final byte[] msg = schema.serialize(value);
        channel.basicPublish(exchangeName, routingKey, null, msg);
    }

    @Override
    public void close() throws Exception {
        if (channel != null && channel.isOpen()) channel.close();
        if (connection != null && connection.isOpen()) connection.close();
        super.close();
    }

    public RabbitMQSink<T> withDeclarePassiveExchange(boolean declarePassiveExchange) {
        this.declarePassiveExchange = declarePassiveExchange;
        return this;
    }

    public RabbitMQSink<T> withExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
        return this;
    }

    public RabbitMQSink<T> withExchangeType(String exchangeType) {
        this.exchangeType = exchangeType;
        return this;
    }

    public RabbitMQSink<T> withDurableExchange(boolean durableExchange) {
        this.durableExchange = durableExchange;
        return this;
    }

    public RabbitMQSink<T> withExclusiveExchange(boolean exclusiveExchange) {
        this.exclusiveExchange = exclusiveExchange;
        return this;
    }

    public RabbitMQSink<T> withAutoDeleteExchange(boolean autoDeleteExchange) {
        this.autoDeleteExchange = autoDeleteExchange;
        return this;
    }

    public RabbitMQSink<T> withExchangeArgs(Map<String, Object> exchangeArgs) {
        this.exchangeArgs = exchangeArgs;
        return this;
    }

    public RabbitMQSink<T> withRoutingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    public RabbitMQSink<T> withBindArgs(Map<String, Object> bindArgs) {
        this.bindArgs = bindArgs;
        return this;
    }

    public RabbitMQSink<T> withDeclarePassiveQueue(boolean declarePassiveQueue) {
        this.declarePassiveQueue = declarePassiveQueue;
        return this;
    }

    public RabbitMQSink<T> withQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public RabbitMQSink<T> withDurableQueue(boolean durableQueue) {
        this.durableQueue = durableQueue;
        return this;
    }

    public RabbitMQSink<T> withExclusiveQueue(boolean exclusiveQueue) {
        this.exclusiveQueue = exclusiveQueue;
        return this;
    }

    public RabbitMQSink<T> withAutoDeleteQueue(boolean autoDeleteQueue) {
        this.autoDeleteQueue = autoDeleteQueue;
        return this;
    }

    public RabbitMQSink<T> withQueueArgs(Map<String, Object> queueArgs) {
        this.queueArgs = queueArgs;
        return this;
    }

    private transient Connection connection;
    private transient Channel channel;
    //
    private final RMQConnectionFactoryBuilder fb;
    private final SerializationSchema<T> schema;
    // exchange
    private boolean declarePassiveExchange = false;
    private String exchangeName;
    private String exchangeType;
    private boolean durableExchange = false;
    private boolean exclusiveExchange = false;
    private boolean autoDeleteExchange = false;
    private Map<String, Object> exchangeArgs = null;
    // routing
    private String routingKey;
    private Map<String, Object> bindArgs = null;
    // queue
    private boolean declarePassiveQueue = false;
    private String queueName;
    private boolean durableQueue = false;
    private boolean exclusiveQueue = false;
    private boolean autoDeleteQueue = false;
    private Map<String, Object> queueArgs = null;
}
