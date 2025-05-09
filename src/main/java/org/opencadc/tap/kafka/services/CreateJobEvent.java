package org.opencadc.tap.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.KafkaConfig;
import org.opencadc.tap.kafka.models.JobRun;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat;
import org.opencadc.tap.kafka.models.JobRun.UploadTable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for submitting job run events to Kafka
 * 
 * @author stvoutsin
 */
public class CreateJobEvent implements AutoCloseable {
    private static final Logger log = Logger.getLogger(CreateJobEvent.class);

    private final Producer<String, String> producer;
    private final String queryTopic;
    private final int timeoutSeconds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new job event service
     * 
     * @param kafkaConfig
     */
    public CreateJobEvent(KafkaConfig kafkaConfig) {
        this(kafkaConfig, 30);
    }

    /**
     * Create a new job event service
     * 
     * @param kafkaConfig    Kafka configuration
     * @param timeoutSeconds Timeout for Kafka operations
     */
    public CreateJobEvent(KafkaConfig kafkaConfig, int timeoutSeconds) {
        log.info("Initializing CreateJobEvent with timeout: " + timeoutSeconds + " seconds");

        this.producer = kafkaConfig.createProducer();
        this.queryTopic = kafkaConfig.getQueryTopic();
        this.timeoutSeconds = timeoutSeconds;

        if (queryTopic == null || queryTopic.isEmpty()) {
            throw new IllegalArgumentException("Kafka query topic cannot be null or empty");
        }

        log.info("CreateJobEvent initialized");

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Submit a query to be executed with a specific job ID
     * 
     * @param query             SQL query to execute
     * @param jobID             Specific job identifier
     * @param resultDestination Result destination for the query
     * @param resultFormat      Optional custom result format
     * @return Job ID for the submitted query
     * @throws ExecutionException   if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     */
    public String submitQuery(String query, String jobID, String resultDestination, ResultFormat resultFormat)
            throws ExecutionException, InterruptedException {

        return submitQuery(query, jobID, resultDestination, null, resultFormat, null, null, null, null);
    }

    /**
     * Submit a query to be executed with optional parameters
     * 
     * @param query             SQL query to execute
     * @param jobID             Specific job identifier
     * @param resultDestination Result destination for the query
     * @param resultLocation    Optional custom result location
     * @param resultFormat      Optional custom result format
     * @param ownerID           Owner identifier
     * @param database          Optional database to query
     * @param maxrec            Optional maximum number of records
     * @return Job ID for the submitted query
     * @throws ExecutionException   if sending to Kafka fails
     * @throws InterruptedException if the operation is interrupted
     */
    public String submitQuery(String query, String jobID, String resultDestination, String resultLocation,
            ResultFormat resultFormat, String ownerID, String database, Integer maxrec, 
            List<UploadTable> uploadTables)
            throws ExecutionException, InterruptedException {

        if (closed.get()) {
            throw new IllegalStateException("CreateJobEvent has been closed");
        }

        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }

        if (resultFormat == null) {
            throw new IllegalArgumentException("ResultFormat cannot be null or empty");
        }

        if (resultDestination == null || resultDestination.trim().isEmpty()) {
            throw new IllegalArgumentException("ResultDestination cannot be null or empty");
        }

        if (jobID == null || jobID.trim().isEmpty()) {
            throw new IllegalArgumentException("JobID cannot be null or empty");
        }

        try {
            log.info("Creating job run event for jobID: " + jobID);

            JobRun jobRun = JobRun.newBuilder()
                    .setJobID(jobID)
                    .setQuery(query)
                    .setMaxrec(maxrec)
                    .setOwnerID(ownerID)
                    .setResultDestination(resultDestination)
                    .setResultLocation(resultLocation)
                    .setResultFormat(resultFormat)
                    .setDatabase(database)
                    .setUploadTables(uploadTables)
                    .build();

            String jsonString = jobRun.toJsonString();
            log.debug("JSON message: " + jsonString);

            log.info("Sending job run event to topic: " + queryTopic + " with jobID: " + jobID);

            ProducerRecord<String, String> record = new ProducerRecord<>(queryTopic, jobID, jsonString);

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