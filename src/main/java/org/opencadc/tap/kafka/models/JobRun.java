package org.opencadc.tap.kafka.models;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Model class representing a UWS Job Run request.
 * 
 * @author stvoutsin
 */
public class JobRun {

    private String jobID;
    private String query;
    private String database;
    private String ownerID;
    private String resultDestination;
    private String resultLocation;
    private ResultFormat resultFormat;
    private UploadTable uploadTable;
    private Integer timeout;

    /**
     * Default constructor
     */
    public JobRun() {
    }

    /**
     * Constructor with required fields
     */
    public JobRun(String jobID, String query, String ownerID, String resultDestination, ResultFormat resultFormat) {
        this.jobID = jobID;
        this.query = query;
        this.ownerID = ownerID;
        this.resultDestination = resultDestination;
        this.resultFormat = resultFormat;
    }

    /**
     * Create a JobRun from JSON
     */
    public static JobRun fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        JobRun jobRun = new JobRun();

        if (json.has("jobID")) {
            jobRun.setJobID(json.getString("jobID"));
        }

        if (json.has("query")) {
            jobRun.setQuery(json.getString("query"));
        }

        if (json.has("database")) {
            jobRun.setDatabase(json.getString("database"));
        }

        if (json.has("ownerID")) {
            jobRun.setOwnerID(json.getString("ownerID"));
        }

        if (json.has("resultDestination")) {
            jobRun.setResultDestination(json.getString("resultDestination"));
        }

        if (json.has("resultLocation")) {
            jobRun.setResultLocation(json.getString("resultLocation"));
        }

        if (json.has("resultFormat")) {
            jobRun.setResultFormat(ResultFormat.fromJson(json.getJSONObject("resultFormat")));
        }

        if (json.has("uploadTable")) {
            jobRun.setUploadTable(UploadTable.fromJson(json.getJSONObject("uploadTable")));
        }

        if (json.has("timeout")) {
            jobRun.setTimeout(json.getInt("timeout"));
        }

        return jobRun;
    }

    /**
     * Convert to JSON
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        json.put("jobID", jobID);
        json.put("query", query);
        json.put("ownerID", ownerID);
        json.put("resultDestination", resultDestination);
        json.put("resultFormat", resultFormat.toJson());

        if (database != null) {
            json.put("database", database);
        }

        if (resultLocation != null) {
            json.put("resultLocation", resultLocation);
        }

        if (uploadTable != null) {
            json.put("uploadTable", uploadTable.toJson());
        }

        if (timeout != null) {
            json.put("timeout", timeout);
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

    public String getOwnerID() {
        return ownerID;
    }

    public void setOwnerID(String ownerID) {
        this.ownerID = ownerID;
    }

    public String getResultDestination() {
        return resultDestination;
    }

    public void setResultDestination(String resultDestination) {
        this.resultDestination = resultDestination;
    }

    public String getResultLocation() {
        return resultLocation;
    }

    public void setResultLocation(String resultLocation) {
        this.resultLocation = resultLocation;
    }

    public ResultFormat getResultFormat() {
        return resultFormat;
    }

    public void setResultFormat(ResultFormat resultFormat) {
        this.resultFormat = resultFormat;
    }

    public UploadTable getUploadTable() {
        return uploadTable;
    }

    public void setUploadTable(UploadTable uploadTable) {
        this.uploadTable = uploadTable;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    /**
     * Create a new Builder instance for building JobRun objects
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder pattern for creating JobRun instances
     */
    public static class Builder {
        private String jobID;
        private String query;
        private String database;
        private String ownerID;
        private String resultDestination;
        private String resultLocation;
        private ResultFormat resultFormat;
        private UploadTable uploadTable;
        private Integer timeout;

        private Builder() {
        }

        public Builder setJobID(String jobID) {
            this.jobID = jobID;
            return this;
        }

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setOwnerID(String ownerID) {
            this.ownerID = ownerID;
            return this;
        }

        public Builder setResultDestination(String resultDestination) {
            this.resultDestination = resultDestination;
            return this;
        }

        public Builder setResultLocation(String resultLocation) {
            this.resultLocation = resultLocation;
            return this;
        }

        public Builder setResultFormat(ResultFormat resultFormat) {
            this.resultFormat = resultFormat;
            return this;
        }

        public Builder setUploadTable(UploadTable uploadTable) {
            this.uploadTable = uploadTable;
            return this;
        }

        public Builder setTimeout(Integer timeout) {
            this.timeout = timeout;
            return this;
        }

        public JobRun build() {
            JobRun jobRun = new JobRun(jobID, query, ownerID, resultDestination, resultFormat);
            jobRun.setDatabase(database);
            jobRun.setResultLocation(resultLocation);
            jobRun.setUploadTable(uploadTable);
            jobRun.setTimeout(timeout);
            return jobRun;
        }
    }

    @Override
    public String toString() {
        return "JobRun{" +
                "jobID='" + jobID + '\'' +
                ", query='" + query + '\'' +
                ", database='" + database + '\'' +
                ", ownerID='" + ownerID + '\'' +
                ", resultDestination='" + resultDestination + '\'' +
                ", resultLocation='" + resultLocation + '\'' +
                ", resultFormat=" + resultFormat +
                ", uploadTable=" + uploadTable +
                ", timeout=" + timeout +
                '}';
    }

    /**
     * Nested class for ResultFormat
     */
    public static class ResultFormat {
        private Format format;
        private Envelope envelope;
        private List<ColumnType> columnTypes;
        private String baseUrl;

        public ResultFormat() {
        }

        public ResultFormat(Format format, Envelope envelope, List<ColumnType> columnTypes, String baseUrl) {
            this.format = format;
            this.envelope = envelope;
            this.columnTypes = columnTypes;
            this.baseUrl = baseUrl;
            
        }
        public ResultFormat(Format format, Envelope envelope, List<ColumnType> columnTypes) {
            this(format, envelope, columnTypes, null);
        }
        
        public static ResultFormat fromJson(JSONObject json) {
            ResultFormat resultFormat = new ResultFormat();

            if (json.has("format")) {
                resultFormat.setFormat(Format.fromJson(json.getJSONObject("format")));
            }

            if (json.has("envelope")) {
                JSONObject envelopeJson = json.getJSONObject("envelope");
                resultFormat.setEnvelope(new Envelope(
                        envelopeJson.getString("header"),
                        envelopeJson.getString("footer"),
                        envelopeJson.optString("footerOverflow")));
            }

            if (json.has("columnTypes")) {
                JSONArray columnTypesJson = json.getJSONArray("columnTypes");
                List<ColumnType> columnTypes = new ArrayList<>();
                for (int i = 0; i < columnTypesJson.length(); i++) {
                    columnTypes.add(ColumnType.fromJson(columnTypesJson.getJSONObject(i)));
                }
                resultFormat.setColumnTypes(columnTypes);
            }

            if (json.has("baseUrl")) {
                resultFormat.setBaseUrl(json.getString("baseUrl"));
            }

            return resultFormat;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            json.put("format", format.toJson());
            json.put("envelope", new JSONObject()
                    .put("header", envelope.getHeader())
                    .put("footer", envelope.getFooter())
                    .put("footerOverflow", envelope.getFooterOverflow()));

            JSONArray columnTypesArray = new JSONArray();
            for (ColumnType columnType : columnTypes) {
                columnTypesArray.put(columnType.toJson());
            }
            json.put("columnTypes", columnTypesArray);

            if (baseUrl != null) {
                json.put("baseUrl", baseUrl);
            }

            return json;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        public Envelope getEnvelope() {
            return envelope;
        }

        public void setEnvelope(Envelope envelope) {
            this.envelope = envelope;
        }

        public List<ColumnType> getColumnTypes() {
            return columnTypes;
        }

        public void setColumnTypes(List<ColumnType> columnTypes) {
            this.columnTypes = columnTypes;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        @Override
        public String toString() {
            return "ResultFormat{" +
                    "format=" + format +
                    ", envelope=" + envelope +
                    ", columnTypes=" + columnTypes +
                    ", baseUrl='" + baseUrl + '\'' +
                    '}';
        }

        /**
         * Format within ResultFormat
         */
        public static class Format {
            private String type;
            private String serialization;

            public Format() {
            }

            public Format(String type, String serialization) {
                this.type = type;
                this.serialization = serialization;
            }

            public static Format fromJson(JSONObject json) {
                return new Format(
                        json.getString("type"),
                        json.getString("serialization"));
            }

            public JSONObject toJson() {
                return new JSONObject()
                        .put("type", type)
                        .put("serialization", serialization);
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

        /**
         * Envelope within ResultFormat
         */
        public static class Envelope {
            private String header;
            private String footerOverflow;
            private String footer;

            public Envelope() {
            }

            public Envelope(String header, String footer, String footerOverflow) {
                this.header = header;
                this.footerOverflow = footerOverflow;
                this.footer = footer;
            }

            public String getHeader() {
                return header;
            }

            public void setHeader(String header) {
                this.header = header;
            }

            public String getFooterOverflow() {
                return footerOverflow;
            }

            public void setFooterOverflow(String footerOverflow) {
                this.footerOverflow = footerOverflow;
            }

            public String getFooter() {
                return footer;
            }

            public void setFooter(String footer) {
                this.footer = footer;
            }

            @Override
            public String toString() {
                return "Envelope{" +
                        "header='" + header + '\'' +
                        ", footer='" + footer + '\'' +
                        ", footerOverflow='" + footerOverflow + '\'' +
                        '}';
            }
        }

        /**
         * ColumnType within ResultFormat
         */
        public static class ColumnType {
            private String name;
            private String datatype;
            private String arraysize;
            private Boolean requiresUrlRewrite;

            public ColumnType() {
            }

            public ColumnType(String name, String datatype) {
                this.name = name;
                this.datatype = datatype;
            }

            public static ColumnType fromJson(JSONObject json) {
                ColumnType columnType = new ColumnType(
                        json.getString("name"),
                        json.getString("datatype"));

                if (json.has("arraysize")) {
                    columnType.setArraysize(json.getString("arraysize"));
                }

                if (json.has("requiresUrlRewrite")) {
                    columnType.setRequiresUrlRewrite(json.getBoolean("requiresUrlRewrite"));
                }

                return columnType;
            }

            public JSONObject toJson() {
                JSONObject json = new JSONObject()
                        .put("name", name)
                        .put("datatype", datatype);

                if (arraysize != null) {
                    json.put("arraysize", arraysize);
                }

                if (requiresUrlRewrite != null) {
                    json.put("requiresUrlRewrite", requiresUrlRewrite);
                }

                return json;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getDatatype() {
                return datatype;
            }

            public void setDatatype(String datatype) {
                this.datatype = datatype;
            }

            public String getArraysize() {
                return arraysize;
            }

            public void setArraysize(String arraysize) {
                this.arraysize = arraysize;
            }

            public Boolean getRequiresUrlRewrite() {
                return requiresUrlRewrite;
            }

            public void setRequiresUrlRewrite(Boolean requiresUrlRewrite) {
                this.requiresUrlRewrite = requiresUrlRewrite;
            }

            @Override
            public String toString() {
                return "ColumnType{" +
                        "name='" + name + '\'' +
                        ", datatype='" + datatype + '\'' +
                        ", arraysize='" + arraysize + '\'' +
                        ", requiresUrlRewrite=" + requiresUrlRewrite +
                        '}';
            }

        }
    }

    /**
     * UploadTable
     */
    public static class UploadTable {
        private String tableName;
        private String sourceUrl;

        public UploadTable() {
        }

        public UploadTable(String tableName, String sourceUrl) {
            this.tableName = tableName;
            this.sourceUrl = sourceUrl;
        }

        public static UploadTable fromJson(JSONObject json) {
            UploadTable uploadTable = new UploadTable();

            if (json.has("tableName")) {
                uploadTable.setTableName(json.getString("tableName"));
            }

            if (json.has("sourceUrl")) {
                uploadTable.setSourceUrl(json.getString("sourceUrl"));
            }

            return uploadTable;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            if (tableName != null) {
                json.put("tableName", tableName);
            }

            if (sourceUrl != null) {
                json.put("sourceUrl", sourceUrl);
            }

            return json;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getSourceUrl() {
            return sourceUrl;
        }

        public void setSourceUrl(String sourceUrl) {
            this.sourceUrl = sourceUrl;
        }

        @Override
        public String toString() {
            return "UploadTable{" +
                    "tableName='" + tableName + '\'' +
                    ", sourceUrl='" + sourceUrl + '\'' +
                    '}';
        }
    }
}
