package org.opencadc.tap.kafka.services;

import org.apache.log4j.Logger;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.URI;
import org.json.JSONException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opencadc.tap.kafka.models.JobStatus;
import org.opencadc.tap.kafka.KafkaConfig;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;

/**
 * Consumer for job status updates from Kafka
 * 
 * @author stvoutsin
 */
public class ReadJobStatus implements AutoCloseable {
    private static final Logger log = Logger.getLogger(ReadJobStatus.class);

    private final KafkaConfig kafkaConfig;
    private final String groupId; // Not sure if this is needed or not
    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<String> subscribedJobIds = ConcurrentHashMap.newKeySet();
    private JobUpdater jobUpdater;

    /**
     * Interface for status update listeners
     */
    public interface StatusListener {
        void onStatusUpdate(JobStatus status);
    }

    private final List<StatusListener> statusListeners = new ArrayList<>();

    /**
     * Create a new consumer for job status updates
     */
    public ReadJobStatus(KafkaConfig kafkaConfig, String groupId) {
        log.debug("Initializing ReadJobStatus with group ID: " + groupId);
        this.kafkaConfig = kafkaConfig;
        this.groupId = groupId;

        Properties props = kafkaConfig.createConsumerProperties(groupId);
        this.consumer = new KafkaConsumer<>(props);
        this.executor = Executors.newSingleThreadExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        log.debug("ReadJobStatus initialized successfully");

        // Initialize JobUpdater - Perhaps this should be imported from elsewhere?
        jobUpdater = (JobUpdater) new PostgresJobPersistence();
    }

    /**
     * Subscribe to status updates for a specific job
     */
    public void subscribeToJob(String jobId) {
        subscribedJobIds.add(jobId);
        log.debug("Subscribed to status updates for job: " + jobId);
    }

    /**
     * Unsubscribe from status updates for a job
     */
    public void unsubscribeFromJob(String jobId) {
        subscribedJobIds.remove(jobId);
        log.debug("Unsubscribed from status updates for job: " + jobId);
    }

    /**
     * Add a listener for status updates
     */
    public synchronized void addStatusListener(StatusListener listener) {
        statusListeners.add(listener);
        log.debug("Status listener added, current count: " + statusListeners.size());
    }

    /**
     * Remove a status listener
     */
    public synchronized void removeStatusListener(StatusListener listener) {
        statusListeners.remove(listener);
        log.debug("Status listener removed, current count: " + statusListeners.size());
    }

    /**
     * Start consuming status updates
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.debug("Starting to consume messages from topic: " + kafkaConfig.getStatusTopic());
            consumer.subscribe(Collections.singletonList(kafkaConfig.getStatusTopic()));
            executor.submit(this::consumeStatusUpdates);
            log.debug("Started consuming status updates from topic: " + kafkaConfig.getStatusTopic());
        } else {
            log.debug("Consumer already running - start request ignored");
        }
    }

    /**
     * Stop consuming status updates
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.debug("Stopping consumer");
            consumer.wakeup();
            log.debug("Stopped consuming status updates");
        } else {
            log.debug("Consumer already stopped, stop request ignored");
        }
    }

    /**
     * Convert ISO timestamp string to Date
     */
    private Date parseIsoTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }

        try {
            Instant instant = Instant.parse(timestamp);
            return Date.from(instant);
        } catch (DateTimeParseException e) {
            log.warn("Error parsing ISO timestamp: " + timestamp, e);
            return null;
        }
    }

    /**
     * Main consumer loop
     * 
     */
    private void consumeStatusUpdates() {
        log.debug("Job Status update consumer loop started");
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    log.info("Received " + records.count() + " records");
                }

                for (ConsumerRecord<String, String> record : records) {
                    String jsonString = record.value();

                    try {
                        JobStatus status = JobStatus.fromJson(jsonString);
                        if (status == null) {
                            log.warn("Received null status from Kafka");
                            continue;
                        }
                        if (status.getJobID() == null) {
                            log.warn("Received status with null job ID");
                            continue;
                        }

                        ExecutionPhase previousPhase = jobUpdater.getPhase(status.getJobID());
                        ExecutionPhase newPhase = JobStatus.ExecutionStatus.toExecutionPhase(status.getStatus());

                        Date timestamp = parseIsoTimestamp(status.getTimestamp());
                        if (timestamp == null) {
                            timestamp = new Date();
                        }

                        List<Result> diagnostics = getJobMetadata(status);

                        ErrorSummary errorSummary = getErrorInfo(status);
                        if (errorSummary != null) {
                            jobUpdater.setPhase(status.getJobID(), previousPhase, newPhase, errorSummary, new Date());
                        } else {
                            jobUpdater.setPhase(status.getJobID(), previousPhase, newPhase, diagnostics, new Date());
                        }

                        log.info("Received status update for job " + status.getJobID() + ": " + status.getStatus());

                        synchronized (this) {
                            for (StatusListener listener : statusListeners) {
                                try {
                                    listener.onStatusUpdate(status);
                                } catch (Exception e) {
                                    log.error("Error notifying listener", e);
                                }
                            }
                        }

                        if (isTerminalStatus(status.getStatus())) {
                            log.debug(
                                    "Job " + status.getJobID() + " reached terminal status: " + status.getStatus());
                            unsubscribeFromJob(status.getJobID());
                        }

                    } catch (JSONException e) {
                        log.error("Error parsing JSON message: " + jsonString, e);
                    } catch (Exception e) {
                        log.error("Unexpected error processing message: " + jsonString, e);
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore, we expect this
            if (running.get()) {
                log.error("Unexpected wakeup exception", e);
            }
        } catch (Exception e) {
            log.error("Error consuming status updates", e);
        } finally {
            try {
                consumer.close();
                log.debug("Consumer closed in consumer loop");
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
        }
    }

    /**
     * Get job error information if present
     */
    private ErrorSummary getErrorInfo(JobStatus status) {
        if (status == null || status.getJobID() == null) {
            return null;
        }

        try {
            if (status.getStatus() == JobStatus.ExecutionStatus.ERROR &&
                    status.getErrorInfo() != null &&
                    status.getErrorInfo().getErrorMessage() != null) {

                String errorMessage = status.getErrorInfo().getErrorMessage();
                String errorCode = status.getErrorInfo().getErrorCode();

                // This is a temporary workaround
                ErrorType errorType = ErrorType.FATAL;
                ErrorSummary errorSummary = new ErrorSummary(errorMessage, errorType);
                return errorSummary;
            }
        } catch (Exception e) {
            log.error("Error updating error info for job: " + status.getJobID(), e);
        }
        return null;
    }

    /**
     * Get job metadata with additional information
     */
    private List<Result> getJobMetadata(JobStatus status) {
        List<Result> metadata = new ArrayList<>();

        if (status == null || status.getJobID() == null) {
            return metadata;
        }

        try {

            if (status.getResultInfo() != null) {
                if (status.getResultInfo().getTotalRows() != null) {
                    metadata.add(new Result("rowcount", URI.create("final:" + status.getResultInfo().getTotalRows())));
                }
                Result res = new Result("result", new URI(status.getResultInfo().getResultLocation()));
                metadata.add(res);
            }

            if (status.getExecutionID() != null) {
                // What do we do with the executionID?
                // status.getExecutionID());
                metadata.add(new Result("executionID", URI.create(status.getExecutionID())));
            }

            /*
             * if (status.getQueryInfo() != null) {
             * JSONObject queryInfo = new JSONObject();
             * 
             * if (status.getQueryInfo().getDuration() != null) {
             * queryInfo.put("duration", status.getQueryInfo().getDuration());
             * }
             * if (status.getQueryInfo().getTotalChunks() != null) {
             * queryInfo.put("totalChunks", status.getQueryInfo().getTotalChunks());
             * }
             * if (status.getQueryInfo().getCompletedChunks() != null) {
             * queryInfo.put("completedChunks", status.getQueryInfo().getCompletedChunks());
             * }
             * if (status.getQueryInfo().getEstimatedTimeRemaining() != null) {
             * queryInfo.put("estimatedTimeRemaining",
             * status.getQueryInfo().getEstimatedTimeRemaining());
             * }
             * 
             * if (!queryInfo.isEmpty()) {
             * metadata.put("queryInfo", queryInfo);
             * }
             * }
             */

            return metadata;
        } catch (Exception e) {
            log.error("Error updating metadata for job: " + status.getJobID(), e);
        }

        return metadata;
    }

    /**
     * Check if a status is terminal
     */
    private boolean isTerminalStatus(JobStatus.ExecutionStatus status) {
        return status == JobStatus.ExecutionStatus.COMPLETED ||
                status == JobStatus.ExecutionStatus.ERROR ||
                status == JobStatus.ExecutionStatus.ABORTED ||
                status == JobStatus.ExecutionStatus.DELETED;
    }

    /**
     * Close the consumer
     */
    @Override
    public void close() {
        log.debug("Closing ReadJobStatus...");
        stop();

        try {
            log.debug("Shutting down executor...");
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in the specified time, forcing shutdown...");
                executor.shutdownNow();
            }
            log.debug("Executor shut down successfully");
        } catch (InterruptedException e) {
            log.warn("Executor shutdown interrupted, forcing immediate shutdown");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            log.debug("Closing Kafka consumer...");
            consumer.close(Duration.ofSeconds(5));
            log.debug("Kafka consumer closed successfully");
        } catch (Exception e) {
            log.warn("Error closing Kafka consumer", e);
        }

        log.debug("ReadJobStatus closed successfully");
    }
}