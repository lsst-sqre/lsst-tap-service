package org.opencadc.tap.kafka;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.log4j.Logger;
import org.opencadc.tap.impl.context.WebAppContext;
import org.opencadc.tap.kafka.services.CreateDeleteEvent;
import org.opencadc.tap.kafka.services.CreateJobEvent;
import org.opencadc.tap.kafka.services.JobStatusListener;
import org.opencadc.tap.kafka.services.ReadJobStatus;

/**
 * Servlet context listener to initialize Kafka connections and services
 * 
 * @author stvoutsin
 */
@WebListener
public class KafkaContextListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(KafkaContextListener.class);

    private KafkaConfig kafkaConfig;
    private CreateJobEvent createJobEventService;
    private ReadJobStatus readJobStatusService;
    private CreateDeleteEvent createDeleteEventService;
    private String groupId = "tap";

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        log.info("Initializing Kafka configuration");

        ServletContext context = sce.getServletContext();
        WebAppContext.setServletContext(context);

        try {
            String bootstrapServer = getConfigValue("KAFKA_BOOTSTRAP_SERVER", "kafka.bootstrap.server",
                    "sasquatch-dev-kafka-1.lsst.cloud:9094");

            String queryTopic = getConfigValue("KAFKA_QUERY_TOPIC", "kafka.query.topic", "lsst.tap.job-run");
            String statusTopic = getConfigValue("KAFKA_STATUS_TOPIC", "kafka.status.topic", "lsst.tap.job-status");
            String deleteTopic = getConfigValue("KAFKA_DELETE_TOPIC", "kafka.delete.topic", "lsst.tap.job-delete");

            String username = getConfigValue("KAFKA_USERNAME", "kafka.username", "tap");
            String password = getConfigValue("KAFKA_PASSWORD", "kafka.password", "");

            log.debug("Creating Kafka configuration with bootstrap server: " + bootstrapServer +
                    ", query topic: " + queryTopic +
                    ", status topic: " + statusTopic +
                    ", delete topic: " + deleteTopic);

            kafkaConfig = new KafkaConfig(bootstrapServer, queryTopic, statusTopic, deleteTopic, username, password);

            log.debug("Initializing job event producer...");
            createJobEventService = new CreateJobEvent(kafkaConfig);

            log.debug("Initializing job delete producer...");
            createDeleteEventService = new CreateDeleteEvent(kafkaConfig);

            String applicationName = context.getServletContextName();
            if (applicationName == null || applicationName.isEmpty()) {
                applicationName = "tap-service";
            }

            log.debug("Initializing job status consumer with group ID: " + groupId);
            readJobStatusService = new ReadJobStatus(kafkaConfig, groupId);
            readJobStatusService.subscribeToJob("*");
            log.debug("Subscribed to all job status updates (*)");
            readJobStatusService.start();
            log.debug("Job status consumer started with group ID: " + groupId);

            JobStatusListener jobStatusListener = new JobStatusListener();
            readJobStatusService.addStatusListener(jobStatusListener);
            log.debug("Registered job status listener");

            context.setAttribute("kafkaConfig", kafkaConfig);
            context.setAttribute("jobProducer", createJobEventService);
            context.setAttribute("statusConsumer", readJobStatusService);
            context.setAttribute("jobDeleteProducer", createDeleteEventService);

            log.debug("Kafka services stored in servlet context");

        } catch (Exception e) {
            log.error("Failed to initialize Kafka services", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.debug("Shutting down Kafka connections...");

        if (readJobStatusService != null) {
            try {
                log.debug("Closing job status consumer");
                readJobStatusService.close();
                log.debug("Job status consumer closed successfully");
            } catch (Exception e) {
                log.warn("Error closing status consumer", e);
            }
        }

        if (createJobEventService != null) {
            try {
                log.debug("Closing job event producer");
                createJobEventService.close();
                log.debug("Job event producer closed successfully");
            } catch (Exception e) {
                log.warn("Error closing job event service", e);
            }
        }

        log.info("Kafka connections shut down");
    }

    /**
     * Get configuration value from env, system or default
     */
    private String getConfigValue(String envVar, String sysProp, String defaultVal) {
        String val = System.getenv(envVar);

        if (val == null || val.isEmpty()) {
            val = System.getProperty(sysProp);
        }

        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }
}