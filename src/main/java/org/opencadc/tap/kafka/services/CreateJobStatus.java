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
 * Service for submitting job status events to Kafka
 * Used mainly for testing and development, since actual job status updates will
 * come from the workers.
 * 
 * @author stvoutsin
 */
public class CreateJobStatus implements AutoCloseable {
    private static final Logger log = Logger.getLogger(CreateJobStatus.class);

    private final Producer<String, String> producer;
    private final String statusTopic;
    private final int timeoutSeconds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new Job Status service
     * 
     * @param kafkaConfig
     */
    public CreateJobStatus(KafkaConfig kafkaConfig) {
        this(kafkaConfig, 30);
    }

    /**
     * Create a new Job Status service
     * 
     * @param kafkaConfig    Kafka configuration
     * @param timeoutSeconds Timeout for Kafka operations
     */
    public CreateJobStatus(KafkaConfig kafkaConfig, int timeoutSeconds) {
        log.debug("Initializing CreateJobStatus with timeout: " + timeoutSeconds + " seconds");

        this.producer = kafkaConfig.createProducer();
        this.statusTopic = kafkaConfig.getStatusTopic();
        this.timeoutSeconds = timeoutSeconds;

        if (statusTopic == null || statusTopic.isEmpty()) {
            throw new IllegalArgumentException("Kafka status topic cannot be null or empty");
        }

        log.debug("CreateJobStatus initialized");

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Send job status update
     * 
     * @param jobID  Specific job identifier
     * @param status Status for the query
     * @return Job ID for the submitted query
     * @throws ExecutionException   if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     */
    public String submitQuery(String jobID, ExecutionStatus status)
            throws ExecutionException, InterruptedException {

        if (closed.get()) {
            throw new IllegalStateException("CreateJobEvent has been closed");
        }

        if (status == null) {
            throw new IllegalArgumentException("status cannot be null or empty");
        }

        if (jobID == null || jobID.trim().isEmpty()) {
            throw new IllegalArgumentException("jobID cannot be null or empty");
        }

        try {
            log.debug("Creating job run for job ID: " + jobID);

            JobStatus jobStatus = JobStatus.newBuilder()
                    .setJobID(jobID)
                    .setStatus(status)
                    .setTimestampNow()
                    .build();

            return submitJobStatus(jobStatus);

        } catch (TimeoutException e) {
            log.error("Timeout sending job run request", e);
            throw new ExecutionException("Timeout sending job run request", e);
        }
    }

    /**
     * Submit a complete job status to Kafka
     * 
     * @param jobStatus The job status to submit
     * @return Job ID for the submitted job status
     * @throws ExecutionException   if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     * @throws TimeoutException     if the operation times out
     */
    public String submitJobStatus(JobStatus jobStatus)
            throws ExecutionException, InterruptedException, TimeoutException {

        if (closed.get()) {
            throw new IllegalStateException("CreateJobEvent has been closed");
        }

        if (jobStatus == null) {
            throw new IllegalArgumentException("jobStatus cannot be null");
        }

        if (jobStatus.getJobID() == null || jobStatus.getJobID().trim().isEmpty()) {
            throw new IllegalArgumentException("jobStatus.jobID cannot be null or empty");
        }

        if (jobStatus.getStatus() == null) {
            throw new IllegalArgumentException("jobStatus.status cannot be null");
        }

        if (jobStatus.getTimestamp() == null) {
            jobStatus = JobStatus.newBuilder()
                    .setJobID(jobStatus.getJobID())
                    .setStatus(jobStatus.getStatus())
                    .setTimestampNow()
                    .setExecutionID(jobStatus.getExecutionID())
                    .setQueryInfo(jobStatus.getQueryInfo())
                    .setResultInfo(jobStatus.getResultInfo())
                    .setErrorInfo(jobStatus.getErrorInfo())
                    .setMetadata(jobStatus.getMetadata())
                    .build();
        }

        String jsonString = jobStatus.toJsonString();

        log.debug("JSON message: " + jsonString);
        log.debug("Sending job status event to topic: " + statusTopic + " with jobID: " + jobStatus.getJobID());

        ProducerRecord<String, String> record = new ProducerRecord<>(statusTopic, jobStatus.getJobID(), jsonString);

        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(timeoutSeconds, TimeUnit.SECONDS);

        log.debug("JSON job status sent successfully to " + metadata.topic() +
                " [partition=" + metadata.partition() +
                ", offset=" + metadata.offset() + "]");

        return jobStatus.getJobID();
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
                    log.debug("Kafka producer closed successfully");
                } catch (Exception e) {
                    log.warn("Error closing Kafka producer", e);
                }
            }
        }
    }
}
