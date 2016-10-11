package com.juliasoft.flink.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * RabbitMQ ConnectionFactory builder for RabbitMQSink and RabbitMQSource
 * Inspired by https://github.com/apache/flink/tree/master/flink-streaming-connectors/flink-connector-rabbitmq
 */
public class RMQConnectionFactoryBuilder implements Serializable {

    // factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
    public static RMQConnectionFactoryBuilder fromUri(String uri) throws URISyntaxException {
        final URI u = new URI(uri);
        final RMQConnectionFactoryBuilder builder = new RMQConnectionFactoryBuilder();
        builder.host = u.getHost();
        builder.port = u.getPort();
        if (u.getUserInfo() != null) {
            String[] split = u.getUserInfo().split(":");
            if (split.length > 1) {
                builder.password = split[1];
            }
            builder.username = split[0];
        }
        builder.virtualHost = u.getPath() == null || u.getPath().isEmpty() ? "/" : u.getPath();
        return builder;
    }

    public static RMQConnectionFactoryBuilder fromProps(String host, Integer port, String virtualHost, String username, String password) {
        return new RMQConnectionFactoryBuilder()
                .withHost(host)
                .withPort(port)
                .withVirtualHost(virtualHost)
                .withUsername(username)
                .withPassword(password);
    }

    public static RMQConnectionFactoryBuilder fromDefaults(String host, Integer port, String virtualHost) {
        return fromProps(host, port, virtualHost, "guest", "guest");
    }

    public static RMQConnectionFactoryBuilder fromDefaults(String host, Integer port) {
        return fromDefaults(host, port, "/");
    }

    public static RMQConnectionFactoryBuilder fromDefaults() {
        return fromDefaults("localhost", 5672, "/");
    }

    public RMQConnectionFactoryBuilder test() {
        Preconditions.checkNotNull(host, "Host must not be empty!");
        Preconditions.checkElementIndex(port, 65535, "Port must be positive integer!");
        Preconditions.checkNotNull(virtualHost, "Virtual host must not be empty!");
        Preconditions.checkNotNull(username, "User name must not be empty!");
        Preconditions.checkNotNull(password, "Password must not be empty!");
        return this;
    }

    public ConnectionFactory build() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.host);
        factory.setPort(this.port);
        factory.setVirtualHost(this.virtualHost);
        factory.setUsername(this.username);
        factory.setPassword(this.password);
        if (this.automaticRecovery != null) {
            factory.setAutomaticRecoveryEnabled(this.automaticRecovery);
        }
        if (this.connectionTimeout != null) {
            factory.setConnectionTimeout(this.connectionTimeout);
        }
        if (this.networkRecoveryInterval != null) {
            factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
        }
        if (this.requestedHeartbeat != null) {
            factory.setRequestedHeartbeat(this.requestedHeartbeat);
        }
        if (this.topologyRecovery != null) {
            factory.setTopologyRecoveryEnabled(this.topologyRecovery);
        }
        if (this.requestedChannelMax != null) {
            factory.setRequestedChannelMax(this.requestedChannelMax);
        }
        if (this.requestedFrameMax != null) {
            factory.setRequestedFrameMax(this.requestedFrameMax);
        }
        return factory;
    }

    public RMQConnectionFactoryBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public RMQConnectionFactoryBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public RMQConnectionFactoryBuilder withVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    public RMQConnectionFactoryBuilder withUsername(String username) {
        this.username = username;
        return this;
    }

    public RMQConnectionFactoryBuilder withPassword(String password) {
        this.password = password;
        return this;
    }

    public RMQConnectionFactoryBuilder withNetworkRecoveryInterval(Integer networkRecoveryInterval) {
        this.networkRecoveryInterval = networkRecoveryInterval;
        return this;
    }

    public RMQConnectionFactoryBuilder withAutomaticRecovery(Boolean automaticRecovery) {
        this.automaticRecovery = automaticRecovery;
        return this;
    }

    public RMQConnectionFactoryBuilder withTopologyRecovery(Boolean topologyRecovery) {
        this.topologyRecovery = topologyRecovery;
        return this;
    }

    public RMQConnectionFactoryBuilder withConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public RMQConnectionFactoryBuilder withRequestedChannelMax(Integer requestedChannelMax) {
        this.requestedChannelMax = requestedChannelMax;
        return this;
    }

    public RMQConnectionFactoryBuilder withRequestedFrameMax(Integer requestedFrameMax) {
        this.requestedFrameMax = requestedFrameMax;
        return this;
    }

    public RMQConnectionFactoryBuilder withRequestedHeartbeat(Integer requestedHeartbeat) {
        this.requestedHeartbeat = requestedHeartbeat;
        return this;
    }

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;

    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;

    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;
}
