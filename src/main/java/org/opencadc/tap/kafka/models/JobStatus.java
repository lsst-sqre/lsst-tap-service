package org.opencadc.tap.kafka.models;

import org.json.JSONArray;
import org.json.JSONObject;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.nrc.cadc.uws.ExecutionPhase;

/**
 * Model for job status updates
 * 
 * @author stvoutsin
 */
public class JobStatus {

    public enum ExecutionStatus {
        QUEUED, EXECUTING, COMPLETED, ERROR, ABORTED, DELETED;

        private static final Map<String, ExecutionStatus> STRING_TO_STATUS = new HashMap<>();

        // Mapping between Kafka ExecutionStatus and UWS ExecutionPhase
        private static final Map<JobStatus.ExecutionStatus, ExecutionPhase> STATUS_PHASE_MAP = new HashMap<>();

        static {
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.QUEUED, ExecutionPhase.QUEUED);
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.EXECUTING, ExecutionPhase.EXECUTING);
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.COMPLETED, ExecutionPhase.COMPLETED);
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.ERROR, ExecutionPhase.ERROR);
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.ABORTED, ExecutionPhase.ABORTED);
            STATUS_PHASE_MAP.put(JobStatus.ExecutionStatus.DELETED, ExecutionPhase.ARCHIVED);
        }

        static {
            for (ExecutionStatus status : values()) {
                STRING_TO_STATUS.put(status.name().toUpperCase(), status);
            }
        }

        public static ExecutionStatus fromString(String statusStr) {
            if (statusStr == null || statusStr.trim().isEmpty()) {
                return null;
            }

            try {
                return valueOf(statusStr);
            } catch (IllegalArgumentException e) {
                return STRING_TO_STATUS.get(statusStr.toUpperCase());
            }
        }

        public boolean isTerminal() {
            return this == COMPLETED || this == ERROR || this == ABORTED || this == DELETED;
        }

        public static boolean isTerminal(ExecutionStatus status) {
            return status != null && status.isTerminal();
        }

        public static ExecutionPhase toExecutionPhase(ExecutionStatus status) {
            return STATUS_PHASE_MAP.get(status);
        }
    }

    private String jobID;
    private String executionID;
    private Long timestamp;
    private ExecutionStatus status;
    private QueryInfo queryInfo;
    private ResultInfo resultInfo;
    private ErrorInfo errorInfo;
    private Metadata metadata;

    /**
     * Default constructor
     */
    public JobStatus() {
    }

    /**
     * Constructor with required fields
     */
    public JobStatus(String jobID, ExecutionStatus status, Long timestamp) {
        this.jobID = jobID;
        this.status = status;
        this.timestamp = timestamp;
    }

    /**
     * Create a JobStatus from a JSON string
     */
    public static JobStatus fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        JobStatus jobStatus = new JobStatus();

        if (json.has("jobID")) {
            jobStatus.setJobID(json.getString("jobID"));
        }

        if (json.has("executionID") && !json.isNull("executionID")) {
            jobStatus.setExecutionID(json.getString("executionID"));
        }

        if (json.has("timestamp") && !json.isNull("timestamp")) {
            jobStatus.setTimestamp(json.getLong("timestamp"));
        }

        if (json.has("status")) {
            String statusStr = json.getString("status");
            try {
                jobStatus.setStatus(ExecutionStatus.valueOf(statusStr));
            } catch (IllegalArgumentException e) {
                jobStatus.setStatus(ExecutionStatus.QUEUED);
            }
        }

        if (json.has("queryInfo") && !json.isNull("queryInfo")) {
            jobStatus.setQueryInfo(QueryInfo.fromJson(json.getJSONObject("queryInfo")));
        }

        if (json.has("resultInfo") && !json.isNull("resultInfo")) {
            jobStatus.setResultInfo(ResultInfo.fromJson(json.getJSONObject("resultInfo")));
        }
    
        if (json.has("errorInfo") && !json.isNull("errorInfo")) {
            jobStatus.setErrorInfo(ErrorInfo.fromJson(json.getJSONObject("errorInfo")));
        }
    
        if (json.has("metadata") && !json.isNull("metadata")) {
            jobStatus.setMetadata(Metadata.fromJson(json.getJSONObject("metadata")));
        }

        return jobStatus;
    }

    /**
     * Convert to JSON
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("jobID", jobID);
        json.put("status", status.name());

        if (timestamp != null) {
            json.put("timestamp", timestamp);
        }

        if (executionID != null) {
            json.put("executionID", executionID);
        }

        if (queryInfo != null) {
            json.put("queryInfo", queryInfo.toJson());
        }

        if (resultInfo != null) {
            json.put("resultInfo", resultInfo.toJson());
        }

        if (errorInfo != null) {
            json.put("errorInfo", errorInfo.toJson());
        }

        if (metadata != null) {
            json.put("metadata", metadata.toJson());
        }

        return json;
    }

    /**
     * Convert to JSON
     */
    public String toJsonString() {
        return toJson().toString();
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getExecutionID() {
        return executionID;
    }

    public void setExecutionID(String executionID) {
        this.executionID = executionID;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Get current timestamp with millisecond precision as Long
     * 
     * @return Current time as milliseconds since epoch
     */
    public static Long getCurrentTimestamp() {
        return Instant.now().toEpochMilli();
    }

    /**
     * Format an Instant to a date-time string with millisecond precision
     * 
     * @param instant The instant to format
     * @return Formatted string with millisecond precision
     */
    public static String formatTimestampWithMillis(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.MILLIS));
    }

    /**
     * Convert millisecond timestamp to formatted string
     * 
     * @param timestamp The timestamp in milliseconds
     * @return Formatted string with millisecond precision
     */
    public static String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return null;
        }
        return formatTimestampWithMillis(Instant.ofEpochMilli(timestamp));
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public QueryInfo getQueryInfo() {
        return queryInfo;
    }

    public void setQueryInfo(QueryInfo queryInfo) {
        this.queryInfo = queryInfo;
    }

    public ResultInfo getResultInfo() {
        return resultInfo;
    }

    public void setResultInfo(ResultInfo resultInfo) {
        this.resultInfo = resultInfo;
    }

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(ErrorInfo errorInfo) {
        this.errorInfo = errorInfo;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Create Builder instance for JobStatus objects
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class creating JobStatus instances
     */
    public static class Builder {
        private String jobID;
        private String executionID;
        private Long timestamp;
        private ExecutionStatus status;
        private QueryInfo queryInfo;
        private ResultInfo resultInfo;
        private ErrorInfo errorInfo;
        private Metadata metadata;

        private Builder() {
            this.timestamp = getCurrentTimestamp();
        }

        public Builder setJobID(String jobID) {
            this.jobID = jobID;
            return this;
        }

        public Builder setExecutionID(String executionID) {
            this.executionID = executionID;
            return this;
        }

        public Builder setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setStatus(ExecutionStatus status) {
            this.status = status;
            return this;
        }

        public Builder setQueryInfo(QueryInfo queryInfo) {
            this.queryInfo = queryInfo;
            return this;
        }

        public Builder setResultInfo(ResultInfo resultInfo) {
            this.resultInfo = resultInfo;
            return this;
        }

        public Builder setErrorInfo(ErrorInfo errorInfo) {
            this.errorInfo = errorInfo;
            return this;
        }

        public Builder setMetadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setTimestampNow() {
            this.timestamp = getCurrentTimestamp();
            return this;
        }

        public JobStatus build() {
            JobStatus jobStatus = new JobStatus(jobID, status, timestamp);
            jobStatus.setExecutionID(executionID);
            jobStatus.setQueryInfo(queryInfo);
            jobStatus.setResultInfo(resultInfo);
            jobStatus.setErrorInfo(errorInfo);
            jobStatus.setMetadata(metadata);
            return jobStatus;
        }
    }

    @Override
    public String toString() {
        return "JobStatus{" +
                "jobID='" + jobID + '\'' +
                ", executionID='" + executionID + '\'' +
                ", timestamp=" + timestamp +
                ", status=" + status +
                ", queryInfo=" + queryInfo +
                ", resultInfo=" + resultInfo +
                ", errorInfo=" + errorInfo +
                ", metadata=" + metadata +
                '}';
    }

    /**
     * QueryInfo inner class
     */
    public static class QueryInfo {
        private Long startTime;
        private Long endTime;
        private Integer duration;
        private Integer totalChunks;
        private Integer completedChunks;
        private Integer estimatedTimeRemaining;

        public QueryInfo() {
        }

        public static QueryInfo fromJson(JSONObject json) {
            QueryInfo queryInfo = new QueryInfo();

            if (json.has("startTime") && !json.isNull("startTime")) {
                queryInfo.setStartTime(json.getLong("startTime"));
            }

            if (json.has("endTime") && !json.isNull("endTime")) {
                queryInfo.setEndTime(json.getLong("endTime"));
            }

            if (json.has("duration") && !json.isNull("duration")) {
                queryInfo.setDuration(json.getInt("duration"));
            }

            if (json.has("totalChunks") && !json.isNull("totalChunks")) {
                queryInfo.setTotalChunks(json.getInt("totalChunks"));
            }

            if (json.has("completedChunks") && !json.isNull("completedChunks")) {
                queryInfo.setCompletedChunks(json.getInt("completedChunks"));
            }

            if (json.has("estimatedTimeRemaining") && !json.isNull("estimatedTimeRemaining")) {
                queryInfo.setEstimatedTimeRemaining(json.getInt("estimatedTimeRemaining"));
            }

            return queryInfo;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            if (startTime != null) {
                json.put("startTime", startTime);
            }

            if (endTime != null) {
                json.put("endTime", endTime);
            }

            if (duration != null) {
                json.put("duration", duration);
            }

            if (totalChunks != null) {
                json.put("totalChunks", totalChunks);
            }

            if (completedChunks != null) {
                json.put("completedChunks", completedChunks);
            }

            if (estimatedTimeRemaining != null) {
                json.put("estimatedTimeRemaining", estimatedTimeRemaining);
            }

            return json;
        }

        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public Integer getDuration() {
            return duration;
        }

        public void setDuration(Integer duration) {
            this.duration = duration;
        }

        public Integer getTotalChunks() {
            return totalChunks;
        }

        public void setTotalChunks(Integer totalChunks) {
            this.totalChunks = totalChunks;
        }

        public Integer getCompletedChunks() {
            return completedChunks;
        }

        public void setCompletedChunks(Integer completedChunks) {
            this.completedChunks = completedChunks;
        }

        public Integer getEstimatedTimeRemaining() {
            return estimatedTimeRemaining;
        }

        public void setEstimatedTimeRemaining(Integer estimatedTimeRemaining) {
            this.estimatedTimeRemaining = estimatedTimeRemaining;
        }

        @Override
        public String toString() {
            return "QueryInfo{" +
                    "startTime='" + startTime + '\'' +
                    ", endTime='" + endTime + '\'' +
                    ", duration=" + duration +
                    ", totalChunks=" + totalChunks +
                    ", completedChunks=" + completedChunks +
                    ", estimatedTimeRemaining=" + estimatedTimeRemaining +
                    '}';
        }
    }

    /**
     * ResultInfo inner class
     */
    public static class ResultInfo {
        private Integer totalRows;
        private String resultLocation;
        private Format format;

        public ResultInfo() {
        }

        public static ResultInfo fromJson(JSONObject json) {
            ResultInfo resultInfo = new ResultInfo();

            if (json.has("totalRows")) {
                resultInfo.setTotalRows(json.getInt("totalRows"));
            }

            if (json.has("resultLocation")) {
                resultInfo.setResultLocation(json.getString("resultLocation"));
            }

            if (json.has("format")) {
                resultInfo.setFormat(Format.fromJson(json.getJSONObject("format")));
            }

            return resultInfo;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            if (totalRows != null) {
                json.put("totalRows", totalRows);
            }

            if (resultLocation != null) {
                json.put("resultLocation", resultLocation);
            }

            if (format != null) {
                json.put("format", format.toJson());
            }

            return json;
        }

        public Integer getTotalRows() {
            return totalRows;
        }

        public void setTotalRows(Integer totalRows) {
            this.totalRows = totalRows;
        }

        public String getResultLocation() {
            return resultLocation;
        }

        public void setResultLocation(String resultLocation) {
            this.resultLocation = resultLocation;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        @Override
        public String toString() {
            return "ResultInfo{" +
                    "totalRows=" + totalRows +
                    ", resultLocation='" + resultLocation + '\'' +
                    ", format=" + format +
                    '}';
        }

        /**
         * Format inner class
         */
        public static class Format {
            private String type;
            private String serialization;

            public Format() {
            }

            public static Format fromJson(JSONObject json) {
                Format format = new Format();

                if (json.has("type")) {
                    format.setType(json.getString("type"));
                }

                if (json.has("serialization")) {
                    format.setSerialization(json.getString("serialization"));
                }

                return format;
            }

            public JSONObject toJson() {
                JSONObject json = new JSONObject();

                if (type != null) {
                    json.put("type", type);
                }

                if (serialization != null) {
                    json.put("serialization", serialization);
                }

                return json;
            }

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }

            public String getSerialization() {
                return serialization;
            }

            public void setSerialization(String serialization) {
                this.serialization = serialization;
            }

            @Override
            public String toString() {
                return "Format{" +
                        "type='" + type + '\'' +
                        ", serialization='" + serialization + '\'' +
                        '}';
            }
        }
    }

    /**
     * ErrorInfo inner class
     */
    public static class ErrorInfo {
        private String errorCode;
        private String errorMessage;
        private String stackTrace;

        public ErrorInfo() {
        }

        public static ErrorInfo fromJson(JSONObject json) {
            ErrorInfo errorInfo = new ErrorInfo();

            if (json.has("errorCode")) {
                errorInfo.setErrorCode(json.getString("errorCode"));
            }

            if (json.has("errorMessage")) {
                errorInfo.setErrorMessage(json.getString("errorMessage"));
            }

            if (json.has("stackTrace")) {
                errorInfo.setStackTrace(json.getString("stackTrace"));
            }

            return errorInfo;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            if (errorCode != null) {
                json.put("errorCode", errorCode);
            }

            if (errorMessage != null) {
                json.put("errorMessage", errorMessage);
            }

            if (stackTrace != null) {
                json.put("stackTrace", stackTrace);
            }

            return json;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getStackTrace() {
            return stackTrace;
        }

        public void setStackTrace(String stackTrace) {
            this.stackTrace = stackTrace;
        }

        @Override
        public String toString() {
            return "ErrorInfo{" +
                    "errorCode='" + errorCode + '\'' +
                    ", errorMessage='" + errorMessage + '\'' +
                    ", stackTrace='" + stackTrace + '\'' +
                    '}';
        }
    }

    /**
     * Metadata inner class
     */
    public static class Metadata {
        private String query;
        private String database;
        private List<String> userTables;

        public Metadata() {
            this.userTables = new ArrayList<>();
        }

        public static Metadata fromJson(JSONObject json) {
            Metadata metadata = new Metadata();

            if (json.has("query")) {
                metadata.setQuery(json.getString("query"));
            }

            if (json.has("database")) {
                metadata.setDatabase(json.getString("database"));
            }

            if (json.has("userTables")) {
                JSONArray tablesArray = json.getJSONArray("userTables");
                List<String> tables = new ArrayList<>();
                for (int i = 0; i < tablesArray.length(); i++) {
                    tables.add(tablesArray.getString(i));
                }
                metadata.setUserTables(tables);
            }

            return metadata;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            if (query != null) {
                json.put("query", query);
            }

            if (database != null) {
                json.put("database", database);
            }

            if (userTables != null && !userTables.isEmpty()) {
                json.put("userTables", new JSONArray(userTables));
            }

            return json;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public List<String> getUserTables() {
            return userTables;
        }

        public void setUserTables(List<String> userTables) {
            this.userTables = userTables;
        }

        public void addUserTable(String tableName) {
            if (this.userTables == null) {
                this.userTables = new ArrayList<>();
            }
            this.userTables.add(tableName);
        }

        @Override
        public String toString() {
            return "Metadata{" +
                    "query='" + query + '\'' +
                    ", database='" + database + '\'' +
                    ", userTables=" + userTables +
                    '}';
        }
    }
}