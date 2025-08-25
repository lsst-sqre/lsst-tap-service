package org.opencadc.tap.impl.logging;

import ca.nrc.cadc.date.DateUtil;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.util.Date;

/**
 * Simple structured logger for TAP service operations.
 * Outputs JSON with timestamp, jobID, username, and message.
 * 
 * @author stvoutsin
 */
public class TAPLogger {

    private final Logger logger;

    public TAPLogger(Class<?> clazz) {
        this.logger = Logger.getLogger(clazz);
    }

    /**
     * Log a message with job context
     * 
     * @param jobID    The job identifier
     * @param username The user (can be null)
     * @param message  The log message
     */
    public void log(String jobID, String username, String message) {
        String logEntry = createLogEntry(jobID, username, message);
        logger.info(logEntry);
    }

    /**
     * Log a message with job context (no username)
     * 
     * @param jobID The job identifier
     * @param message The log message
     */
    public void log(String jobID, String message) {
        log(jobID, null, message);
    }

    /**
     * Create a JSON log entry
     */
    private String createLogEntry(String jobID, String username, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        DateFormat format = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
        Date date = new Date(System.currentTimeMillis());
        sb.append("\"@timestamp\":\"").append(format.format(date)).append("\",");

        sb.append("\"service\":{\"name\":\"tap\"},");

        sb.append("\"thread\":{\"name\":\"").append(Thread.currentThread().getName()).append("\"},");

        sb.append("\"log\":{\"level\":\"info\"},");

        if (jobID != null) {
            sb.append("\"jobID\":\"").append(sanitize(jobID)).append("\",");
        }

        if (username != null) {
            sb.append("\"user\":\"").append(sanitize(username)).append("\",");
        }

        if (message != null) {
            sb.append("\"message\":\"").append(sanitize(message)).append("\"");
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * Sanitize string values for JSON
     */
    private String sanitize(String input) {
        if (input == null)
            return "";
        return input.replaceAll("\"", "'").replaceAll("\\s+", " ").trim();
    }
}