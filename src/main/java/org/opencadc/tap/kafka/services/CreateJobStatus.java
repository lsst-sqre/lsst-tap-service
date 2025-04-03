package org.opencadc.tap.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.KafkaConfig;
import org.opencadc.tap.kafka.models.JobStatus;
import org.opencadc.tap.kafka.models.JobStatus.ExecutionStatus;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for submitting job run events to Kafka
 */
public class CreateJobStatus implements AutoCloseable {
    private static final Logger log = Logger.getLogger(CreateJobStatus.class);
    
    private final Producer<String, String> producer;
    private final String statusTopic;
    private final int timeoutSeconds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new Job Status service
     * @param kafkaConfig
     */
    public CreateJobStatus (KafkaConfig kafkaConfig) {
        this(kafkaConfig, 30);
    }
    
    /**
     * Create a new Job Status service
     * 
     * @param kafkaConfig Kafka configuration
     * @param timeoutSeconds Timeout for Kafka operations
     */
    public CreateJobStatus(KafkaConfig kafkaConfig, int timeoutSeconds) {
        log.info("Initializing CreateJobStatus with timeout: " + timeoutSeconds + " seconds");

        this.producer = kafkaConfig.createProducer();
        this.statusTopic = kafkaConfig.getStatusTopic();
        this.timeoutSeconds = timeoutSeconds;
        
        if (statusTopic == null || statusTopic.isEmpty()) {
            throw new IllegalArgumentException("Kafka status topic cannot be null or empty");
        }
        
        log.info("CreateJobStatus initialized");
        
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Send job status update
     * 
     * @param jobID Specific job identifier
     * @param status Status for the query
     * @return Job ID for the submitted query
     * @throws ExecutionException if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     */
    public String submitQuery(String jobID, ExecutionStatus status) 
            throws ExecutionException, InterruptedException {
        
        if (closed.get()) {
            throw new IllegalStateException("CreateJobEvent has been closed");
        }
        
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null or empty");
        }

        if (jobID == null || jobID.trim().isEmpty()) {
            throw new IllegalArgumentException("JobID cannot be null or empty");
        }

        try {
            log.info("Creating job run for job ID: " + jobID);
            
            JobStatus jobStatus = JobStatus.newBuilder()
                .setJobID(jobID)
                .setStatus(status)
                .build();
            
            String jsonString = jobStatus.toJsonString();
            
            log.debug("JSON message: " + jsonString);
            log.info("Sending job status event to topic: " + statusTopic + " with jobID: " + jobID);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(statusTopic, jobID, jsonString);
            
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(timeoutSeconds, TimeUnit.SECONDS);
            
            log.info("JSON job run sent successfully to " + metadata.topic() + 
                    " [partition=" + metadata.partition() + 
                    ", offset=" + metadata.offset() + "]");
            
            return jobID;
            
        } catch (TimeoutException e) {
            log.error("Timeout sending job run request", e);
            throw new ExecutionException("Timeout sending job run request", e);
        } catch (ExecutionException e) {
            log.error("Error sending job run request", e);
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
                    log.info("Closing Kafka producer...");
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