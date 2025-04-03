package org.opencadc.tap.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Configuration for Kafka connections
 * Handles producer and consumer configurations
 * 
 * @author stvoutsin
 */
public class KafkaConfig {
    private static final Logger log = Logger.getLogger(KafkaConfig.class);

    private final String bootstrapServer;
    private final String queryTopic;
    private final String statusTopic;
    private final String deleteTopic;
    private final String username;
    private final String password;
    private final boolean useSasl;

    /**
     * Create a Kafka configuration
     * 
     * @param bootstrapServer Kafka bootstrap server addresses
     * @param queryTopic      Topic for query requests
     * @param statusTopic     Topic for status updates
     * @param deleteTopic     Topic for job deletion events
     */
    public KafkaConfig(String bootstrapServer, String queryTopic, String statusTopic, String deleteTopic) {
        this(bootstrapServer, queryTopic, statusTopic, deleteTopic, null, null);
    }

    /**
     * Create a Kafka configuration with authentication
     * 
     * @param bootstrapServer Kafka bootstrap server addresses
     * @param queryTopic      Topic for query requests
     * @param statusTopic     Topic for status updates
     * @param deleteTopic     Topic for job deletion events
     * @param username        SASL username (optional)
     * @param password        SASL password (optional)
     */
    public KafkaConfig(String bootstrapServer, String queryTopic, String statusTopic, String deleteTopic,
            String username, String password) {
        this.bootstrapServer = bootstrapServer;
        this.queryTopic = queryTopic;
        this.statusTopic = statusTopic;
        this.deleteTopic = deleteTopic;
        this.username = username;
        this.password = password;
        this.useSasl = (username != null && !username.isEmpty() &&
                password != null && !password.isEmpty());

        if (useSasl) {
            log.debug("Kafka SASL authentication enabled with SCRAM-SHA-512, username: " + username);
        } else {
            log.debug("Kafka SASL authentication disabled - no credentials provided");
        }
    }

    /**
     * Create a producer
     */
    public Producer<String, String> createProducer() {
        log.debug("Creating Kafka JSON producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 16 * 1024 * 1024);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1 * 1024 * 1024);

        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 60000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        if (useSasl) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "SCRAM-SHA-512");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                            "username=\"" + username + "\" " +
                            "password=\"" + password + "\";");
        }

        log.debug("Creating KafkaProducer instance");

        return new KafkaProducer<>(props);
    }

    /**
     * Create consumer properties for the Kafka consumer
     * 
     */
    public Properties createConsumerProperties(String groupId) {
        log.debug("Creating consumer properties for group ID: " + groupId);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        if (useSasl) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "SCRAM-SHA-512");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                            "username=\"" + username + "\" " +
                            "password=\"" + password + "\";");
        }

        return props;
    }

    /**
     * Get the topic name for query run requests
     */
    public String getQueryTopic() {
        return queryTopic;
    }

    /**
     * Get the topic name for status updates
     */
    public String getStatusTopic() {
        return statusTopic;
    }

    /**
     * Get the topic name for job deletion events
     */
    public String getDeleteTopic() {
        return deleteTopic;
    }

    /**
     * Get the bootstrap server addresses
     */
    public String getBootstrapServer() {
        return bootstrapServer;
    }
}