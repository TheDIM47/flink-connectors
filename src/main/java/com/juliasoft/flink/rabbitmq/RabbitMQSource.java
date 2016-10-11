package com.juliasoft.flink.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * RabbitMQ MultipleIdsMessageAcknowledgingSourceBase implementation
 * Inspired by https://github.com/apache/flink/tree/master/flink-streaming-connectors/flink-connector-rabbitmq
 *
 * @see open(...) for implementation details
 */
public class RabbitMQSource<T> extends MultipleIdsMessageAcknowledgingSourceBase<T, String, Long> implements ResultTypeQueryable<T> {

    public RabbitMQSource(RMQConnectionFactoryBuilder fb, boolean useCorrelationId, DeserializationSchema<T> schema, TypeInformation<String> idTypeInfo) {
        super(idTypeInfo);
        this.fb = fb;
        this.useCorrelationId = useCorrelationId;
        this.schema = schema;
    }

    public RabbitMQSource(RMQConnectionFactoryBuilder fb, DeserializationSchema<T> schema, TypeInformation<String> idTypeInfo) {
        this(fb, false, schema, idTypeInfo);
    }

    public RabbitMQSource(RMQConnectionFactoryBuilder fb, boolean useCorrelationId, DeserializationSchema<T> schema) {
        super(String.class);
        this.fb = fb;
        this.useCorrelationId = useCorrelationId;
        this.schema = schema;
    }

    public RabbitMQSource(RMQConnectionFactoryBuilder fb, DeserializationSchema<T> schema) {
        this(fb, false, schema);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

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

        consumer = new QueueingConsumer(channel);

        final RuntimeContext runtimeContext = getRuntimeContext();
        if (runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
            autoAck = false;
            // enables transaction mode
            channel.txSelect();
        } else {
            autoAck = true;
        }

        channel.basicConsume(queueName, autoAck, consumer);

        running = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (running) {
            final QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            synchronized (ctx.getCheckpointLock()) {
                final T result = schema.deserialize(delivery.getBody());
                if (schema.isEndOfStream(result)) {
                    break;
                }
                if (!autoAck) {
                    final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                    if (useCorrelationId) {
                        final String correlationId = delivery.getProperties().getCorrelationId();
                        Preconditions.checkNotNull(correlationId, MSG_CORRELATION_ERROR);
                        if (!addId(correlationId)) {
                            // we have already processed this message
                            continue;
                        }
                    }
                    sessionIds.add(deliveryTag);
                }
                ctx.collect(result);
            } // synchronized
        } // while
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    protected void acknowledgeSessionIDs(List<Long> longs) {
        try {
            for (long id : longs) {
                channel.basicAck(id, false);
            }
            channel.txCommit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        if (channel != null && channel.isOpen()) channel.close();
        if (connection != null && connection.isOpen()) connection.close();
        super.close();
    }

    public RabbitMQSource<T> withDeclarePassiveExchange(boolean declarePassiveExchange) {
        this.declarePassiveExchange = declarePassiveExchange;
        return this;
    }

    public RabbitMQSource<T> withExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
        return this;
    }

    public RabbitMQSource<T> withExchangeType(String exchangeType) {
        this.exchangeType = exchangeType;
        return this;
    }

    public RabbitMQSource<T> withDurableExchange(boolean durableExchange) {
        this.durableExchange = durableExchange;
        return this;
    }

    public RabbitMQSource<T> withExclusiveExchange(boolean exclusiveExchange) {
        this.exclusiveExchange = exclusiveExchange;
        return this;
    }

    public RabbitMQSource<T> withAutoDeleteExchange(boolean autoDeleteExchange) {
        this.autoDeleteExchange = autoDeleteExchange;
        return this;
    }

    public RabbitMQSource<T> withExchangeArgs(Map<String, Object> exchangeArgs) {
        this.exchangeArgs = exchangeArgs;
        return this;
    }

    public RabbitMQSource<T> withRoutingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    public RabbitMQSource<T> withBindArgs(Map<String, Object> bindArgs) {
        this.bindArgs = bindArgs;
        return this;
    }

    public RabbitMQSource<T> withDeclarePassiveQueue(boolean declarePassiveQueue) {
        this.declarePassiveQueue = declarePassiveQueue;
        return this;
    }

    public RabbitMQSource<T> withQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public RabbitMQSource<T> withDurableQueue(boolean durableQueue) {
        this.durableQueue = durableQueue;
        return this;
    }

    public RabbitMQSource<T> withExclusiveQueue(boolean exclusiveQueue) {
        this.exclusiveQueue = exclusiveQueue;
        return this;
    }

    public RabbitMQSource<T> withAutoDeleteQueue(boolean autoDeleteQueue) {
        this.autoDeleteQueue = autoDeleteQueue;
        return this;
    }

    public RabbitMQSource<T> withQueueArgs(Map<String, Object> queueArgs) {
        this.queueArgs = queueArgs;
        return this;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public RabbitMQSource<T> withAutoAck() {
        this.autoAck = true;
        return this;
    }

    private final RMQConnectionFactoryBuilder fb;
    private final boolean useCorrelationId;
    private final DeserializationSchema<T> schema;

    private transient Connection connection;
    private transient Channel channel;
    private transient QueueingConsumer consumer;

    private transient boolean autoAck = true;
    private transient volatile boolean running;

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

    public static final String MSG_CORRELATION_ERROR = "RabbitMQ source was instantiated " +
            "with usesCorrelationId set to true but a message was received with " +
            "correlation id set to null!";
}
