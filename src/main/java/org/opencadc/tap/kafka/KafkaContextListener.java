package org.opencadc.tap.kafka;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.log4j.Logger;
import org.opencadc.tap.impl.context.WebAppContext;
import org.opencadc.tap.kafka.services.CreateJobEvent;
import org.opencadc.tap.kafka.services.CreateJobStatus;
import org.opencadc.tap.kafka.services.LoggingStatusListener;
import org.opencadc.tap.kafka.services.ReadJobStatus;

/**
 * Servlet context listener to initialize Kafka connections and services
 */
@WebListener
public class KafkaContextListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(KafkaContextListener.class);
    
    private KafkaConfig kafkaConfig;
    private CreateJobEvent createJobEventService;
    private ReadJobStatus readJobStatusService;
    private CreateJobStatus createJobStatusService;

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
            
            String username = getConfigValue("KAFKA_USERNAME", "kafka.username", "tap");
            String password = getConfigValue("KAFKA_PASSWORD", "kafka.password", "");
            
            log.info("Creating Kafka configuration with bootstrap server: " + bootstrapServer + 
                     ", query topic: " + queryTopic +
                     ", status topic: " + statusTopic);
            
            kafkaConfig = new KafkaConfig(bootstrapServer, queryTopic, statusTopic, username, password);
            
            // JobRun event producer
            log.info("Initializing job event producer...");
            createJobEventService = new CreateJobEvent(kafkaConfig);
            log.info("Job event producer initialized successfully");
            
           // JobStatus event producer
            log.info("Initializing job status event producer...");
            createJobStatusService = new CreateJobStatus(kafkaConfig);

            // JobStatus consumer
            String applicationName = context.getServletContextName();
            if (applicationName == null || applicationName.isEmpty()) {
                applicationName = "tap-service";
            }

            String groupId = applicationName + "-status-consumer-" + System.currentTimeMillis();
            
            log.info("Initializing job status consumer with group ID: " + groupId);
            readJobStatusService = new ReadJobStatus(kafkaConfig, groupId);
            readJobStatusService.subscribeToJob("*");
            log.info("Subscribed to all job status updates (*)");
            readJobStatusService.start();
            log.info("Job status consumer started with group ID: " + groupId);

            // Not sure if this is the right approach
            LoggingStatusListener loggingListener = new LoggingStatusListener();
            readJobStatusService.addStatusListener(loggingListener);
            log.info("Registered logging status listener");

            
            // Store contexts
            context.setAttribute("kafkaConfig", kafkaConfig);
            context.setAttribute("jobProducer", createJobEventService);
            context.setAttribute("jobStatusProducer", createJobStatusService);
            context.setAttribute("statusConsumer", readJobStatusService);

            log.info("Kafka services stored in servlet context");

        } catch (Exception e) {
        
            log.error("Failed to initialize Kafka services", e);
        
        }
    }
    
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("Shutting down Kafka connections...");
        
        if (readJobStatusService != null) {
            try {
                log.info("Closing job status consumer...");
                readJobStatusService.close();
                log.info("Job status consumer closed successfully");
            } catch (Exception e) {
                log.warn("Error closing status consumer", e);
            }
        }
        
        if (createJobEventService != null) {
            try {
                log.info("Closing job event producer...");
                createJobEventService.close();
                log.info("Job event producer closed successfully");
            } catch (Exception e) {
                log.warn("Error closing job event service", e);
            }
        }
        
        log.info("Kafka connections shut down");
    }
    
    /**
     * Get configuration value from environment, system property, or default
     */
    private String getConfigValue(String envVar, String sysProp, String defaultVal) {
        String val = System.getenv(envVar);
        
        if (val == null || val.isEmpty()) {
            val = System.getProperty(sysProp);
        }
        
        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }
}