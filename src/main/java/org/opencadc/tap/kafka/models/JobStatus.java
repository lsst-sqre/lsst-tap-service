package org.opencadc.tap.kafka.models;

import org.json.JSONObject;

/**
 * Model for job status updates
 */
public class JobStatus {
    
    public enum ExecutionStatus {
        PENDING, QUEUED, EXECUTING, COMPLETED, ERROR, ABORTED
    }
    
    private String jobID;
    private ExecutionStatus status;
   
    /**
     * Default constructor
     */
    public JobStatus() { }
    
    /**
     * Constructor with required fields
     */
    public JobStatus(String jobID, ExecutionStatus status) {
        this.jobID = jobID;
        this.status = status;
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
        
        if (json.has("status")) {
            String statusStr = json.getString("status");
            try {
                jobStatus.setStatus(ExecutionStatus.valueOf(statusStr));
            } catch (IllegalArgumentException e) {
                jobStatus.setStatus(ExecutionStatus.PENDING);
            }
        }
        
        return jobStatus;
    }
    
    /**
     * Convert to JSON object
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("jobID", jobID);
        json.put("status", status.name());
        return json;
    }
    
    /**
     * Convert to JSON string
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

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
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
        private ExecutionStatus status;
        
        private Builder() {}
        
        public Builder setJobID(String jobID) {
            this.jobID = jobID;
            return this;
        }
        
        public Builder setStatus(ExecutionStatus status) {
            this.status = status;
            return this;
        }
        
        public JobStatus build() {
            return new JobStatus(jobID, status);
        }
    }

    @Override
    public String toString() {
        return "JobStatus{" +
                "jobID='" + jobID + '\'' +
                ", status=" + status +
                '}';
    }
}