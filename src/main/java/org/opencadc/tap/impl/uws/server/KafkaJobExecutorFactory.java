package org.opencadc.tap.impl.uws.server;

import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.services.CreateDeleteEvent;
import org.opencadc.tap.kafka.services.CreateJobEvent;

import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobUpdater;

import org.opencadc.tap.impl.context.WebAppContext;

/**
 * Factory class to create and configure KafkaJobExecutor instances.
 * 
 * @author stvoutsin
 */
public class KafkaJobExecutorFactory {

    private static final Logger log = Logger.getLogger(KafkaJobExecutorFactory.class);
    private static final String CONFIG_BUCKET_URL = "gcs_bucket_url";
    private static final String CONFIG_BUCKET = "gcs_bucket";
    private static final String CONFIG_DATABASE = "database";

    /**
     * Create a new KafkaJobExecutor instance with the given parameters.
     * 
     * @param jobUpdater     The JobUpdater implementation
     * @param jobRunnerClass The JobRunner implementation class
     * @param jobPersistence The JobPersistence implementation
     * @return A KafkaJobExecutor
     */
    public static KafkaJobExecutor createExecutor(JobUpdater jobUpdater, Class jobRunnerClass,
            JobPersistence jobPersistence) {
        log.debug("Creating KafkaJobExecutor with jobRunnerClass: " + jobRunnerClass.getName());

        CreateJobEvent createJobEventService = getJobEventService();
        CreateDeleteEvent deleteJobEventService = getJobDeleteService();

        String bucketURL = getBucketURL();
        String bucket = getBucket();
        String databaseString = getDatabase();

        if (createJobEventService == null) {
            log.warn("CreateJobEvent service not available!");
        }

        return new KafkaJobExecutor(jobUpdater, jobRunnerClass, jobPersistence, createJobEventService, deleteJobEventService, bucketURL,
                bucket, databaseString);
    }

    /**
     * Get the Kafka job create event service from the web application context
     * 
     * @return The CreateJobEvent service or null if not available
     */
    private static CreateJobEvent getJobEventService() {
        try {
            log.debug("Retrieving Kafka services from WebAppContext...");

            Object service = WebAppContext.getContextAttribute("jobProducer");
            if (service != null && service instanceof CreateJobEvent) {
                log.debug("Successfully retrieved job producer service from WebAppContext");
                return (CreateJobEvent) service;
            } else {
                log.warn("Job producer service not found in WebAppContext or is of wrong type");
            }
        } catch (Exception e) {
            log.error("Error retrieving Kafka services from WebAppContext", e);
        }
        return null;
    }

    /**
     * Get the Kafka job delete event service from the web application context
     * 
     * @return The CreateDeleteEvent service or null if not available
     */

    private static CreateDeleteEvent getJobDeleteService() {
        try {
            log.debug("Retrieving Kafka services from WebAppContext...");

            Object service = WebAppContext.getContextAttribute("jobDeleteProducer");
            if (service != null && service instanceof CreateDeleteEvent) {
                log.debug("Successfully retrieved job delete producer service from WebAppContext");
                return (CreateDeleteEvent) service;
            } else {
                log.warn("Job delete producer service not found in WebAppContext or is of wrong type");
            }
        } catch (Exception e) {
            log.error("Error retrieving Kafka services from WebAppContext", e);
        }
        return null;
    }


    /**
     * Get the bucket URL from configuration
     * 
     * @return The configured bucket URL or a default
     */
    private static String getBucketURL() {
        String bucketURL = System.getenv("GCS_BUCKET_URL");

        if (bucketURL == null || bucketURL.isEmpty()) {
            bucketURL = System.getProperty(CONFIG_BUCKET_URL);
        }

        if (bucketURL == null || bucketURL.isEmpty()) {
            throw new IllegalStateException("GCS_BUCKET_URL not set in environment variables or system properties");
        }

        return bucketURL;
    }

    /**
     * Get the bucket name from configuration
     * 
     * @return
     */
    private static String getBucket() {
        String bucket = System.getenv("GCS_BUCKET");

        if (bucket == null || bucket.isEmpty()) {
            bucket = System.getProperty(CONFIG_BUCKET);
        }

        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalStateException("GCS_BUCKET not set in environment variables or system properties");
        }

        return bucket;
    }

    /**
     * Get the database name from configuration
     * 
     * @return The configured database name or a default
     */
    private static String getDatabase() {
        String database = System.getenv("DATABASE");

        if (database == null || database.isEmpty()) {
            database = System.getProperty(CONFIG_DATABASE);
        }

        if (database == null || database.isEmpty()) {
            throw new IllegalStateException("DATABASE not set in environment variables or system properties");
        }

        return database;
    }

}