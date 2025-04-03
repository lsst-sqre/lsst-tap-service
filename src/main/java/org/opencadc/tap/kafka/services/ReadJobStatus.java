package org.opencadc.tap.kafka.services;


import org.opencadc.tap.kafka.models.JobStatus;
import org.opencadc.tap.kafka.KafkaConfig;
import org.apache.log4j.Logger;
import java.time.Duration;
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
import org.json.JSONException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;

/**
 * Consumer for job status updates from Kafka
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
        log.info("Initializing ReadJobStatus with group ID: " + groupId);
        this.kafkaConfig = kafkaConfig;
        this.groupId = groupId;
        
        Properties props = kafkaConfig.createConsumerProperties(groupId);
        this.consumer = new KafkaConsumer<>(props);
        this.executor = Executors.newSingleThreadExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        log.info("ReadJobStatus initialized successfully");

        // Initialize JobUpdater - Perhaps this should be imported from elsewhere?
        jobUpdater = (JobUpdater) new PostgresJobPersistence();

    }
    
    /**
     * Subscribe to status updates for a specific job
     */
    public void subscribeToJob(String jobId) {
        subscribedJobIds.add(jobId);
        log.info("Subscribed to status updates for job: " + jobId);
    }
    
    /**
     * Unsubscribe from status updates for a job
     */
    public void unsubscribeFromJob(String jobId) {
        subscribedJobIds.remove(jobId);
        log.info("Unsubscribed from status updates for job: " + jobId);
    }
    
    /**
     * Add a listener for status updates
     */
    public synchronized void addStatusListener(StatusListener listener) {
        statusListeners.add(listener);
        log.info("Status listener added, current count: " + statusListeners.size());
    }
    
    /**
     * Remove a status listener
     */
    public synchronized void removeStatusListener(StatusListener listener) {
        statusListeners.remove(listener);
        log.info("Status listener removed, current count: " + statusListeners.size());
    }
    
    /**
     * Start consuming status updates
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting to consume messages from topic: " + kafkaConfig.getStatusTopic());
            consumer.subscribe(Collections.singletonList(kafkaConfig.getStatusTopic()));
            executor.submit(this::consumeStatusUpdates);
            log.info("Started consuming status updates from topic: " + kafkaConfig.getStatusTopic());
        } else {
            log.info("Consumer already running - start request ignored");
        }
    }
    
    /**
     * Stop consuming status updates
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping consumer");
            consumer.wakeup();
            log.info("Stopped consuming status updates");
        } else {
            log.info("Consumer already stopped - stop request ignored");
        }
    }
    
    /**
     * Main consumer loop
     */
    private void consumeStatusUpdates() {
        log.info("Job Status update consumer loop started");
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
                        } if (status.getJobID() == null) {
                            log.warn("Received status with null job ID");
                            continue;
                        }
                        ExecutionPhase previousStatus = jobUpdater.getPhase(status.getJobID());
                        
                        // TODO: Pass in the ExecutionPhase from the status - This will need a mappping between the ExecutionStatus (kafka event) and ExecutionPhase (uws)
                        jobUpdater.setPhase(status.getJobID(),previousStatus, ExecutionPhase.COMPLETED, new Date());

                        
                        if (status != null && subscribedJobIds.contains(status.getJobID())) {
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
                                log.info("Job " + status.getJobID() + " reached terminal status: " + status.getStatus());
                                unsubscribeFromJob(status.getJobID());
                            }
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
                log.info("Consumer closed in consumer loop");
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
        }
    }
    
    /**
     * Check if a status is terminal (no more updates expected)
     */
    private boolean isTerminalStatus(JobStatus.ExecutionStatus status) {
        return status == JobStatus.ExecutionStatus.COMPLETED || 
               status == JobStatus.ExecutionStatus.ERROR || 
               status == JobStatus.ExecutionStatus.ABORTED;
    }
    
    /**
     * Close the consumer
     */
    @Override
    public void close() {
        log.info("Closing ReadJobStatus...");
        stop();
        
        try {
            log.info("Shutting down executor...");
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in the specified time, forcing shutdown...");
                executor.shutdownNow();
            }
            log.info("Executor shut down successfully");
        } catch (InterruptedException e) {
            log.warn("Executor shutdown interrupted, forcing immediate shutdown");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        try {
            log.info("Closing Kafka consumer...");
            consumer.close(Duration.ofSeconds(5));
            log.info("Kafka consumer closed successfully");
        } catch (Exception e) {
            log.warn("Error closing Kafka consumer", e);
        }
        
        log.info("ReadJobStatus closed successfully");
    }
}