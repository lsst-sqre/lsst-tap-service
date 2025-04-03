package org.opencadc.tap.kafka.models;

import org.json.JSONObject;

/**
 * Model class representing a job deletion request.
 * 
 * @author stvoutsin
 */
public class JobDelete {

    private String executionID;
    private String ownerID;
    private String jobID;

    /**
     * Default constructor
     */
    public JobDelete() {
    }

    /**
     * Constructor with required fields
     */
    public JobDelete(String executionID, String jobID) {
        this.executionID = executionID;
        this.jobID = jobID;
    }

    /**
     * Constructor with all fields
     */
    public JobDelete(String executionID, String ownerID, String jobID) {
        this.executionID = executionID;
        this.ownerID = ownerID;
        this.jobID = jobID;
    }

    /**
     * Create a JobDelete from JSON
     */
    public static JobDelete fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        JobDelete jobDelete = new JobDelete();

        if (json.has("executionID")) {
            jobDelete.setExecutionID(json.getString("executionID"));
        }

        if (json.has("ownerID")) {
            jobDelete.setOwnerID(json.getString("ownerID"));
        }

        if (json.has("jobID")) {
            jobDelete.setJobID(json.getString("jobID"));
        }
        return jobDelete;
    }

    /**
     * Convert to JSON
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        json.put("executionID", executionID);

        json.put("jobID", jobID);

        if (ownerID != null) {
            json.put("ownerID", ownerID);
        }

        return json;
    }

    /**
     * Convert to JSON string
     */
    public String toJsonString() {
        return toJson().toString();
    }

    public String getExecutionID() {
        return executionID;
    }

    public void setExecutionID(String executionID) {
        this.executionID = executionID;
    }

    public String getOwnerID() {
        return ownerID;
    }

    public void setOwnerID(String ownerID) {
        this.ownerID = ownerID;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    /**
     * Create a new Builder instance for building JobDelete objects
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder pattern for creating JobDelete instances
     */
    public static class Builder {
        private String executionID;
        private String ownerID;
        private String jobID;

        private Builder() {
        }

        public Builder setExecutionID(String executionID) {
            this.executionID = executionID;
            return this;
        }

        public Builder setOwnerID(String ownerID) {
            this.ownerID = ownerID;
            return this;
        }

        public Builder setJobID(String jobID) {
            this.jobID = jobID;
            return this;
        }

        public JobDelete build() {
            JobDelete jobDelete = new JobDelete(executionID, jobID);
            jobDelete.setOwnerID(ownerID);
            return jobDelete;
        }
    }

    @Override
    public String toString() {
        return "JobDelete{" +
                "executionID='" + executionID + '\'' +
                ", jobID='" + jobID + '\'' +
                ", ownerID='" + ownerID + '\'' +
                '}';
    }
}