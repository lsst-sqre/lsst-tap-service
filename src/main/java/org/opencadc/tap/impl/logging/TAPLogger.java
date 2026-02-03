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
        String logEntry = new LogEntryBuilder()
                .jobID(jobID)
                .user(username)
                .message(message)
                .build();
        logger.info(logEntry);
    }

    /**
     * Log a message with job context (no username)
     *
     * @param jobID   The job identifier
     * @param message The log message
     */
    public void log(String jobID, String message) {
        log(jobID, null, message);
    }

    /**
     * Log job completion with metrics
     *
     * @param jobID    The job identifier
     * @param username The user (can be null)
     * @param phase    The final execution phase
     * @param duration Duration in milliseconds (can be null)
     * @param rowCount Number of rows returned (can be null)
     */
    public void logJobComplete(String jobID, String username, String phase, Long duration, Long rowCount) {
        String logEntry = new LogEntryBuilder()
                .jobID(jobID)
                .user(username)
                .phase(phase)
                .duration(duration)
                .rowCount(rowCount)
                .message("Job finished")
                .build();
        logger.info(logEntry);
    }

    /**
     * Log job error
     *
     * @param jobID        The job identifier
     * @param username     The user (can be null)
     * @param errorMessage The error message
     */
    public void logError(String jobID, String username, String errorMessage) {
        String logEntry = new LogEntryBuilder()
                .level("error")
                .jobID(jobID)
                .user(username)
                .message(errorMessage)
                .build();
        logger.error(logEntry);
    }

    /**
     * Log a warning
     *
     * @param jobID   The job identifier
     * @param username The user (can be null)
     * @param message The warning message
     */
    public void logWarn(String jobID, String username, String message) {
        String logEntry = new LogEntryBuilder()
                .level("warn")
                .jobID(jobID)
                .user(username)
                .message(message)
                .build();
        logger.warn(logEntry);
    }

    /**
     * Log phase transition
     *
     * @param jobID         The job identifier
     * @param username      The user (can be null)
     * @param previousPhase The previous phase
     * @param newPhase      The new phase
     */
    public void logPhaseTransition(String jobID, String username, String previousPhase, String newPhase) {
        String logEntry = new LogEntryBuilder()
                .jobID(jobID)
                .user(username)
                .previousPhase(previousPhase)
                .phase(newPhase)
                .message("Phase transition")
                .build();
        logger.info(logEntry);
    }

    /**
     * Log an upload operation
     *
     * @param filename The filename being uploaded
     * @param message  The log message
     */
    public void logUpload(String filename, String message) {
        String logEntry = new LogEntryBuilder()
                .filename(filename)
                .message(message)
                .build();
        logger.info(logEntry);
    }

    /**
     * Log upload completion with metrics
     *
     * @param filename The filename uploaded
     * @param duration Duration in milliseconds
     * @param rowCount Number of rows (can be null)
     * @param message  The log message
     */
    public void logUploadComplete(String filename, Long duration, Long rowCount, String message) {
        String logEntry = new LogEntryBuilder()
                .filename(filename)
                .duration(duration)
                .rowCount(rowCount)
                .message(message)
                .build();
        logger.info(logEntry);
    }

    /**
     * Log upload error
     *
     * @param filename     The filename (can be null)
     * @param errorMessage The error message
     */
    public void logUploadError(String filename, String errorMessage) {
        String logEntry = new LogEntryBuilder()
                .level("error")
                .filename(filename)
                .message(errorMessage)
                .build();
        logger.error(logEntry);
    }

    /**
     * Log upload warning
     *
     * @param filename The filename (can be null)
     * @param message  The warning message
     */
    public void logUploadWarn(String filename, String message) {
        String logEntry = new LogEntryBuilder()
                .level("warn")
                .filename(filename)
                .message(message)
                .build();
        logger.warn(logEntry);
    }

    /**
     * Builder for structured log entries
     */
    public static class LogEntryBuilder {
        private String level = "info";
        private String jobID;
        private String user;
        private String message;
        private String phase;
        private String previousPhase;
        private Long duration;
        private Long rowCount;
        private String filename;
        private Integer fieldCount;

        public LogEntryBuilder level(String level) {
            this.level = level;
            return this;
        }

        public LogEntryBuilder jobID(String jobID) {
            this.jobID = jobID;
            return this;
        }

        public LogEntryBuilder user(String user) {
            this.user = user;
            return this;
        }

        public LogEntryBuilder message(String message) {
            this.message = message;
            return this;
        }

        public LogEntryBuilder phase(String phase) {
            this.phase = phase;
            return this;
        }

        public LogEntryBuilder previousPhase(String previousPhase) {
            this.previousPhase = previousPhase;
            return this;
        }

        public LogEntryBuilder duration(Long duration) {
            this.duration = duration;
            return this;
        }

        public LogEntryBuilder rowCount(Long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public LogEntryBuilder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public LogEntryBuilder fieldCount(Integer fieldCount) {
            this.fieldCount = fieldCount;
            return this;
        }

        public String build() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");

            DateFormat format = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
            Date date = new Date(System.currentTimeMillis());
            sb.append("\"@timestamp\":\"").append(format.format(date)).append("\",");

            sb.append("\"service\":{\"name\":\"tap\"},");

            sb.append("\"thread\":{\"name\":\"").append(Thread.currentThread().getName()).append("\"},");

            sb.append("\"log\":{\"level\":\"").append(level).append("\"},");

            if (jobID != null) {
                sb.append("\"jobID\":\"").append(sanitize(jobID)).append("\",");
            }

            if (user != null) {
                sb.append("\"user\":\"").append(sanitize(user)).append("\",");
            }

            if (previousPhase != null) {
                sb.append("\"previousPhase\":\"").append(sanitize(previousPhase)).append("\",");
            }

            if (phase != null) {
                sb.append("\"phase\":\"").append(sanitize(phase)).append("\",");
            }

            if (duration != null) {
                sb.append("\"duration\":").append(duration).append(",");
            }

            if (rowCount != null) {
                sb.append("\"rowCount\":").append(rowCount).append(",");
            }

            if (filename != null) {
                sb.append("\"filename\":\"").append(sanitize(filename)).append("\",");
            }

            if (fieldCount != null) {
                sb.append("\"fieldCount\":").append(fieldCount).append(",");
            }

            if (message != null) {
                sb.append("\"message\":\"").append(sanitize(message)).append("\"");
            }

            // Remove trailing comma if message was null
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.setLength(sb.length() - 1);
            }

            sb.append("}");
            return sb.toString();
        }

        private String sanitize(String input) {
            if (input == null)
                return "";
            return input.replaceAll("\"", "'").replaceAll("\\s+", " ").trim();
        }
    }
}