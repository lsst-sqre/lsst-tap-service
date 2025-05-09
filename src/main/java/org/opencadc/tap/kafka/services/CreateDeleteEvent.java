package org.opencadc.tap.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.KafkaConfig;
import org.opencadc.tap.kafka.models.JobDelete;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for submitting job deletion events to Kafka
 * 
 * @author stvoutsin
 */
public class CreateDeleteEvent implements AutoCloseable {
    private static final Logger log = Logger.getLogger(CreateDeleteEvent.class);

    private final Producer<String, String> producer;
    private final String deleteTopic;
    private final int timeoutSeconds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new job deletion event service
     * 
     * @param kafkaConfig Kafka configuration
     */
    public CreateDeleteEvent(KafkaConfig kafkaConfig) {
        this(kafkaConfig, 30);
    }

    /**
     * Create a new job deletion event service
     * 
     * @param kafkaConfig    Kafka configuration
     * @param timeoutSeconds Timeout for Kafka operations
     */
    public CreateDeleteEvent(KafkaConfig kafkaConfig, int timeoutSeconds) {
        log.info("Initializing CreateDeleteEvent with timeout: " + timeoutSeconds + " seconds");

        this.producer = kafkaConfig.createProducer();
        this.deleteTopic = kafkaConfig.getDeleteTopic();
        this.timeoutSeconds = timeoutSeconds;

        if (deleteTopic == null || deleteTopic.isEmpty()) {
            throw new IllegalArgumentException("Kafka delete topic cannot be null or empty");
        }

        log.info("CreateDeleteEvent initialized");

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Submit a job deletion request with owner information
     * 
     * @param executionID   Execution ID to delete
     * @param ownerID Owner identifier
     * @param jobID      Specific job identifier
     * @return Execution ID for the submitted deletion request
     * @throws ExecutionException   if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     */
    public String submitDeletion(String executionID, String ownerID, String jobID)
            throws ExecutionException, InterruptedException {

        if (closed.get()) {
            throw new IllegalStateException("CreateDeleteEvent has been closed");
        }

        if (executionID == null || executionID.isEmpty()) {
            throw new IllegalArgumentException("executionID cannot be null or empty");
        }

        if (jobID == null || jobID.isEmpty()) {
            throw new IllegalArgumentException("jobID cannot be null or empty");
        }

        try {
            log.info("Creating job delete event for executionID: " + executionID);

            JobDelete jobDelete = JobDelete.newBuilder()
                    .setExecutionID(executionID)
                    .setOwnerID(ownerID)
                    .setJobID(jobID)
                    .build();

            String jsonString = jobDelete.toJsonString();
            log.debug("JSON message: " + jsonString);

            log.info("Sending job delete event to topic: " + deleteTopic + " with executionID: " + executionID);

            ProducerRecord<String, String> record = new ProducerRecord<>(deleteTopic, executionID, jsonString);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(timeoutSeconds, TimeUnit.SECONDS);

            log.info("JSON job delete sent successfully to " + metadata.topic() +
                    " [partition=" + metadata.partition() +
                    ", offset=" + metadata.offset() + "]");

            return executionID;

        } catch (TimeoutException e) {
            log.error("Timeout sending job delete request", e);
            throw new ExecutionException("Timeout sending job delete request", e);
        } catch (ExecutionException e) {
            log.error("Error sending job delete request", e);
            throw e;
        }
    }

    /**
     * Close the Kafka producer
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (producer != null) {
                try {
                    producer.flush();
                    producer.close(Duration.ofSeconds(30));
                    log.info("Kafka producer closed successfully");
                } catch (Exception e) {
                    log.warn("Error closing Kafka producer", e);
                }
            }
        }
    }
}