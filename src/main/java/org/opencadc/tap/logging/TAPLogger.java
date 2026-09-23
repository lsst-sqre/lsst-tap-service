package org.opencadc.tap.logging;

import ca.nrc.cadc.date.DateUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Structured logger for TAP job execution. Each entry is one line of JSON so
 * Google Cloud Logging parses it as jsonPayload. Messages for a job start
 * with [jobID], and every job ends with a job.finished, job.failed or
 * job.aborted entry.
 *
 * @author stvoutsin
 */
public class TAPLogger {

    public static final String MODE_SYNC = "sync";
    public static final String MODE_ASYNC = "async";

    public static final String REASON_USER = "user";
    public static final String REASON_SYNC_TIMEOUT = "sync_timeout";
    public static final String REASON_BACKEND = "backend";

    public static final String RESPONSE_RESULTS = "results";
    public static final String RESPONSE_ERROR = "error";
    public static final String RESPONSE_ABORTED = "aborted";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Logger logger;

    public TAPLogger(Class<?> clazz) {
        this.logger = Logger.getLogger(clazz);
    }

    public void jobStarted(String jobID, String username, String mode) {
        ObjectNode entry = entry("job.started", jobID, username);
        entry.put("mode", mode);
        info(entry, jobID, capitalize(mode) + " job started" + by(username));
    }

    /**
     * A sync request for a job that is already running, e.g. a client
     * retrying after a 503.
     */
    public void jobResumed(String jobID, String username, String phase) {
        ObjectNode entry = entry("job.resumed", jobID, username);
        entry.put("mode", MODE_SYNC);
        entry.put("resumed", true);
        entry.put("jobPhase", phase);
        info(entry, jobID, "Sync job resumed in " + phase + by(username));
    }

    /**
     * @param prepareMs time from the start of the job to submission
     * @param maxrec    row limit, only when set by the user (can be null)
     */
    public void jobSubmitted(String jobID, String username, String mode, long prepareMs,
            int uploadTables, Integer maxrec) {
        ObjectNode entry = entry("job.submitted", jobID, username);
        entry.put("mode", mode);
        entry.put("prepareMs", prepareMs);
        List<String> details = new ArrayList<>();
        if (uploadTables > 0) {
            entry.put("uploadTables", uploadTables);
            details.add(uploadTables + (uploadTables == 1 ? " upload table" : " upload tables"));
        }
        if (maxrec != null) {
            entry.put("maxrec", maxrec);
            details.add("maxrec " + maxrec);
        }
        info(entry, jobID, "Submitted to Kafka after " + formatMillis(prepareMs) + details(details));
    }

    public void jobPhase(String jobID, String username, String previousPhase, String newPhase) {
        ObjectNode entry = entry("job.phase", jobID, username);
        entry.put("previousJobPhase", previousPhase);
        entry.put("jobPhase", newPhase);
        info(entry, jobID, previousPhase + " -> " + newPhase);
    }

    public void jobFinished(String jobID, String username, String phase, Outcome outcome) {
        Outcome o = outcome != null ? outcome : new Outcome();
        ObjectNode entry = entry("job.finished", jobID, username);
        entry.put("jobPhase", phase);
        o.addTo(entry);

        StringBuilder message = new StringBuilder(phase);
        if (o.rowCount != null) {
            message.append(", ").append(o.rowCount).append(o.rowCount == 1 ? " row" : " rows");
        }
        if (o.executionMs != null) {
            message.append(" in ").append(formatMillis(o.executionMs));
        }
        List<String> details = new ArrayList<>();
        if (o.queryMs != null) {
            details.add("query " + formatMillis(o.queryMs));
        }
        if (o.completedChunks != null && o.totalChunks != null) {
            details.add(o.completedChunks + "/" + o.totalChunks + " chunks");
        }
        if (o.bytesProcessed != null) {
            details.add(formatBytes(o.bytesProcessed) + " processed");
        }
        if (Boolean.TRUE.equals(o.cached)) {
            details.add("cached");
        }
        info(entry, jobID, message + details(details));
    }

    /**
     * Logged as a warning, since a job usually fails because of the query
     * (bad ADQL, timeout). Service faults are logged with logError.
     */
    public void jobFailed(String jobID, String username, String errorMessage, Outcome outcome) {
        Outcome o = outcome != null ? outcome : new Outcome();
        ObjectNode entry = entry("job.failed", jobID, username);
        entry.put("jobPhase", "ERROR");
        o.addTo(entry);
        String message = "ERROR: " + (errorMessage != null ? errorMessage : "Job failed")
                + (o.errorCode != null ? " (code " + o.errorCode + ")" : "");
        warn(entry, jobID, message);
    }

    /**
     * User aborts are logged as info, sync timeouts and backend aborts as
     * warnings.
     */
    public void jobAborted(String jobID, String username, String reason, Outcome outcome) {
        Outcome o = outcome != null ? outcome : new Outcome();
        ObjectNode entry = entry("job.aborted", jobID, username);
        entry.put("jobPhase", "ABORTED");
        entry.put("reason", reason);
        o.addTo(entry);
        String after = o.executionMs != null ? " after " + formatMillis(o.executionMs) : "";
        if (REASON_SYNC_TIMEOUT.equals(reason)) {
            warn(entry, jobID, "Sync timeout" + after + ", job aborted");
        } else if (REASON_BACKEND.equals(reason)) {
            warn(entry, jobID, "ABORTED by backend" + after);
        } else {
            info(entry, jobID, "ABORTED by user" + after);
        }
    }

    /**
     * @param response RESPONSE_RESULTS, RESPONSE_ERROR or RESPONSE_ABORTED
     * @param bytes    size of the results sent (can be null)
     * @param totalMs  time since the sync request started
     */
    public void jobDelivered(String jobID, String username, String response, Long bytes, long totalMs) {
        ObjectNode entry = entry("job.delivered", jobID, username);
        entry.put("mode", MODE_SYNC);
        entry.put("response", response);
        entry.put("totalMs", totalMs);
        String message;
        if (RESPONSE_ERROR.equals(response)) {
            message = "Error sent to client";
        } else if (RESPONSE_ABORTED.equals(response)) {
            message = "Abort notice sent to client";
        } else {
            message = "Results sent to client";
        }
        if (bytes != null) {
            entry.put("responseBytes", bytes);
            message += ", " + formatBytes(bytes);
        }
        info(entry, jobID, message + ", " + formatMillis(totalMs) + " total");
    }

    public void logInfo(String jobID, String username, String message) {
        info(entry(null, jobID, username), jobID, message);
    }

    public void logError(String jobID, String username, String message) {
        logError(jobID, username, message, null);
    }

    public void logError(String jobID, String username, String message, Throwable error) {
        ObjectNode entry = entry(null, jobID, username);
        addStackTrace(entry, error);
        logger.error(finish(entry, "error", jobID, message));
    }

    public void logWarn(String jobID, String username, String message) {
        logWarn(jobID, username, message, null);
    }

    public void logWarn(String jobID, String username, String message, Throwable error) {
        ObjectNode entry = entry(null, jobID, username);
        addStackTrace(entry, error);
        warn(entry, jobID, message);
    }

    public void uploadFinished(String jobID, String filename, Long rowCount, long uploadMs) {
        ObjectNode entry = entry("upload.finished", jobID, null);
        entry.put("filename", filename);
        entry.put("uploadMs", uploadMs);
        String message = "Uploaded " + filename;
        if (rowCount != null) {
            entry.put("rowCount", rowCount);
            message += ", " + rowCount + (rowCount == 1 ? " row" : " rows");
        }
        info(entry, jobID, message + " in " + formatMillis(uploadMs));
    }

    /**
     * Timings and backend details for the entry that records how a job ended.
     */
    public static class Outcome {
        private String executionID;
        private Long executionMs;
        private Long queryMs;
        private Long rowCount;
        private Integer completedChunks;
        private Integer totalChunks;
        private Long bytesProcessed;
        private Long bytesBilled;
        private Boolean cached;
        private String errorCode;

        public Outcome executionID(String executionID) {
            this.executionID = executionID;
            return this;
        }

        public Outcome executionMs(Long executionMs) {
            this.executionMs = executionMs;
            return this;
        }

        public Outcome queryMs(Long queryMs) {
            this.queryMs = queryMs;
            return this;
        }

        public Outcome rowCount(Long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Outcome chunks(Integer completed, Integer total) {
            this.completedChunks = completed;
            this.totalChunks = total;
            return this;
        }

        public Outcome bytes(Long processed, Long billed, Boolean cached) {
            this.bytesProcessed = processed;
            this.bytesBilled = billed;
            this.cached = cached;
            return this;
        }

        public Outcome errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        private void addTo(ObjectNode entry) {
            putIfSet(entry, "executionID", executionID);
            putIfSet(entry, "executionMs", executionMs);
            putIfSet(entry, "queryMs", queryMs);
            putIfSet(entry, "rowCount", rowCount);
            putIfSet(entry, "completedChunks", completedChunks);
            putIfSet(entry, "totalChunks", totalChunks);
            putIfSet(entry, "bytesProcessed", bytesProcessed);
            putIfSet(entry, "bytesBilled", bytesBilled);
            putIfSet(entry, "cached", cached);
            putIfSet(entry, "errorCode", errorCode);
        }
    }

    private ObjectNode entry(String event, String jobID, String username) {
        DateFormat format = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
        ObjectNode entry = MAPPER.createObjectNode();
        entry.put("@timestamp", format.format(new Date()));
        entry.putObject("service").put("name", "tap");
        entry.putObject("thread").put("name", Thread.currentThread().getName());
        putIfSet(entry, "event", event);
        putIfSet(entry, "jobID", jobID);
        putIfSet(entry, "user", username);
        return entry;
    }

    private void info(ObjectNode entry, String jobID, String message) {
        logger.info(finish(entry, "info", jobID, message));
    }

    private void warn(ObjectNode entry, String jobID, String message) {
        logger.warn(finish(entry, "warn", jobID, message));
    }

    private static String finish(ObjectNode entry, String level, String jobID, String message) {
        entry.putObject("log").put("level", level);
        // Google Cloud Logging reads the entry severity from this field
        entry.put("severity", "warn".equals(level) ? "WARNING" : level.toUpperCase());
        if (message != null) {
            entry.put("message", jobID != null ? "[" + jobID + "] " + message : message);
        }
        return entry.toString();
    }

    private static void addStackTrace(ObjectNode entry, Throwable error) {
        if (error != null) {
            StringWriter trace = new StringWriter();
            error.printStackTrace(new PrintWriter(trace));
            entry.put("stack_trace", trace.toString().trim());
        }
    }

    private static void putIfSet(ObjectNode entry, String name, Object value) {
        if (value instanceof String) {
            entry.put(name, (String) value);
        } else if (value instanceof Long) {
            entry.put(name, (Long) value);
        } else if (value instanceof Integer) {
            entry.put(name, (Integer) value);
        } else if (value instanceof Boolean) {
            entry.put(name, (Boolean) value);
        }
    }

    private static String by(String username) {
        return username != null ? " by " + username : "";
    }

    private static String details(List<String> details) {
        return details.isEmpty() ? "" : " (" + String.join(", ", details) + ")";
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    static String formatMillis(long millis) {
        if (millis < 1000) {
            return millis + "ms";
        }
        return String.format("%.1fs", millis / 1000.0);
    }

    static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = { "KB", "MB", "GB", "TB" };
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format("%.1f %s", value, units[unit]);
    }
}
