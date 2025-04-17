package org.opencadc.tap.kafka.models;

import org.json.JSONObject;

/**
 * Model class representing a job deletion request.
 * 
 * @author stvoutsin
 */
public class JobDelete {

    private String jobID;
    private String ownerID;

    /**
     * Default constructor
     */
    public JobDelete() {
    }

    /**
     * Constructor with required fields
     */
    public JobDelete(String jobID) {
        this.jobID = jobID;
    }

    /**
     * Constructor with all fields
     */
    public JobDelete(String jobID, String ownerID) {
        this.jobID = jobID;
        this.ownerID = ownerID;
    }

    /**
     * Create a JobDelete from JSON
     */
    public static JobDelete fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        JobDelete jobDelete = new JobDelete();

        if (json.has("jobID")) {
            jobDelete.setJobID(json.getString("jobID"));
        }

        if (json.has("ownerID")) {
            jobDelete.setOwnerID(json.getString("ownerID"));
        }

        return jobDelete;
    }

    /**
     * Convert to JSON
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

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

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getOwnerID() {
        return ownerID;
    }

    public void setOwnerID(String ownerID) {
        this.ownerID = ownerID;
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
        private String jobID;
        private String ownerID;

        private Builder() {
        }

        public Builder setJobID(String jobID) {
            this.jobID = jobID;
            return this;
        }

        public Builder setOwnerID(String ownerID) {
            this.ownerID = ownerID;
            return this;
        }

        public JobDelete build() {
            JobDelete jobDelete = new JobDelete(jobID);
            jobDelete.setOwnerID(ownerID);
            return jobDelete;
        }
    }

    @Override
    public String toString() {
        return "JobDelete{" +
                "jobID='" + jobID + '\'' +
                ", ownerID='" + ownerID + '\'' +
                '}';
    }
}