package org.opencadc.tap.kafka.services;

import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.models.JobStatus;

/**
 * Simple listener to log all job status updates
 * 
 */
public class LoggingStatusListener implements ReadJobStatus.StatusListener {
    private static final Logger log = Logger.getLogger(LoggingStatusListener.class);
    
    @Override
    public void onStatusUpdate(JobStatus status) {
        log.debug("Status Update Received: " + status.toString());
    }
}