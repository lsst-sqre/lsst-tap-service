package org.opencadc.tap.ws;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opencadc.tap.kafka.models.JobStatus;
import org.opencadc.tap.kafka.models.JobStatus.ExecutionStatus;
import org.opencadc.tap.kafka.models.JobStatus.QueryInfo;
import org.opencadc.tap.kafka.models.JobStatus.ResultInfo;
import org.opencadc.tap.kafka.models.JobStatus.ErrorInfo;
import org.opencadc.tap.kafka.models.JobStatus.Metadata;
import org.opencadc.tap.kafka.services.CreateJobStatus;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class JobUpdateService extends HttpServlet {
    private static final Logger log = Logger.getLogger(JobUpdateService.class);

    private CreateJobStatus createJobStatusService;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        log.info("Initializing JobUpdateService");
        ServletContext context = config.getServletContext();

        createJobStatusService = (CreateJobStatus) context.getAttribute("jobStatusProducer");

        if (createJobStatusService == null) {
            log.error("Job Producer service not found in servlet context");
            throw new ServletException("Job Producer service not properly initialized");
        }

        log.info("JobUpdateService initialized successfully");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        log.info("Received GET request");

        try {
            String jobID = getRequiredParameter(request, "jobid");
            String statusStr = getRequiredParameter(request, "status");
            
            ExecutionStatus status = ExecutionStatus.fromString(statusStr);
            if (status == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
                        "Invalid status value. Accepted values: " + String.join(", ", getValidStatusValues()));
                return;
            }
            
            JobStatus jobStatus = buildJobStatusFromParams(request, jobID, status);
            
            String resultJobId = createJobStatusService.submitJobStatus(jobStatus);
            log.info("Job status submitted successfully with job ID: " + resultJobId);

            sendSuccessResponse(response, resultJobId);

        } catch (RequiredParameterException e) {
            log.warn(e.getMessage());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Error submitting job status to Kafka", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Failed to submit job status: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing job status", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "An unexpected error occurred while processing the request: " + e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        log.info("Received POST request");
        
        String contentType = request.getContentType();
        
        if (contentType != null && contentType.contains("application/json")) {
            processJsonRequest(request, response);
        } else {
            log.info("Processing POST request with form parameters");
            doGet(request, response);
        }
    }
    
    /**
     * Process a request with JSON body
     */
    private void processJsonRequest(HttpServletRequest request, HttpServletResponse response) 
    throws IOException {
    try {
        String requestBody;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()))) {
            requestBody = reader.lines().collect(Collectors.joining("\n"));
        }
        
        if (requestBody == null || requestBody.trim().isEmpty()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Empty request body");
            return;
        }
            
            log.info("Processing JSON request");
            
            JSONObject jsonObj = new JSONObject(requestBody);
            
            if (!jsonObj.has("jobID")) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required field: jobID");
                return;
            }
            
            if (!jsonObj.has("status")) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required field: status");
                return;
            }
            
            JobStatus jobStatus = JobStatus.fromJson(requestBody);
            
            if (jobStatus.getTimestamp() == null) {
                jobStatus.setTimestamp(JobStatus.getCurrentTimestamp());
            }
            
            String resultJobId = createJobStatusService.submitJobStatus(jobStatus);
            log.info("Job status submitted successfully with job ID: " + resultJobId);
            
            sendSuccessResponse(response, resultJobId);
            
        } catch (JSONException e) {
            log.error("Error parsing JSON request", e);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid JSON format: " + e.getMessage());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Error submitting job status to Kafka", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Failed to submit job status: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing job status", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "An unexpected error occurred while processing the request: " + e.getMessage());
        }
    }
    
    /**
     * Build a JobStatus object from query parameters
     */
    private JobStatus buildJobStatusFromParams(HttpServletRequest request, String jobID, ExecutionStatus status) {
        JobStatus.Builder builder = JobStatus.newBuilder()
        .setJobID(jobID)
        .setStatus(status);

        String timestampParam = request.getParameter("timestamp");
        if (timestampParam != null && !timestampParam.trim().isEmpty()) {
            try {
                Long timestamp = Long.parseLong(timestampParam);
                builder.setTimestamp(timestamp);
            } catch (NumberFormatException e) {
                log.warn("Invalid timestamp format: " + timestampParam + ". Using current time instead.", e);
                builder.setTimestampNow();
            }
        } else {
            builder.setTimestampNow();
        }
        
        String executionID = request.getParameter("executionID");
        if (executionID != null && !executionID.trim().isEmpty()) {
            builder.setExecutionID(executionID);
        }
        
        QueryInfo queryInfo = buildQueryInfoFromParams(request);
        if (queryInfo != null) {
            builder.setQueryInfo(queryInfo);
        }
        
        ResultInfo resultInfo = buildResultInfoFromParams(request);
        if (resultInfo != null) {
            builder.setResultInfo(resultInfo);
        }
        
        ErrorInfo errorInfo = buildErrorInfoFromParams(request);
        if (errorInfo != null) {
            builder.setErrorInfo(errorInfo);
        }
        
        Metadata metadata = buildMetadataFromParams(request);
        if (metadata != null) {
            builder.setMetadata(metadata);
        }
        
        return builder.build();
    }
    
    /**
     * Build QueryInfo from request parameters
     */
    private QueryInfo buildQueryInfoFromParams(HttpServletRequest request) {
        QueryInfo queryInfo = new QueryInfo();
        boolean hasData = false;
        
        String startTime = request.getParameter("queryInfo.startTime");
        String endTime = request.getParameter("queryInfo.endTime");

        if (startTime != null && !startTime.trim().isEmpty()) {
            try {
                queryInfo.setStartTime(Long.parseLong(startTime));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid startTime format: " + startTime, e);
            }
        }

        if (endTime != null && !endTime.trim().isEmpty()) {
            try {
                queryInfo.setEndTime(Long.parseLong(endTime));
            } catch (NumberFormatException e) {
                log.warn("Invalid endTime format: " + endTime, e);
            }

        }
        
        String durationStr = request.getParameter("queryInfo.duration");
        if (durationStr != null && !durationStr.trim().isEmpty()) {
            try {
                queryInfo.setDuration(Integer.parseInt(durationStr));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid duration value: " + durationStr);
            }
        }
        
        String totalChunksStr = request.getParameter("queryInfo.totalChunks");
        if (totalChunksStr != null && !totalChunksStr.trim().isEmpty()) {
            try {
                queryInfo.setTotalChunks(Integer.parseInt(totalChunksStr));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid totalChunks value: " + totalChunksStr);
            }
        }
        
        String completedChunksStr = request.getParameter("queryInfo.completedChunks");
        if (completedChunksStr != null && !completedChunksStr.trim().isEmpty()) {
            try {
                queryInfo.setCompletedChunks(Integer.parseInt(completedChunksStr));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid completedChunks value: " + completedChunksStr);
            }
        }
        
        String estimatedTimeRemainingStr = request.getParameter("queryInfo.estimatedTimeRemaining");
        if (estimatedTimeRemainingStr != null && !estimatedTimeRemainingStr.trim().isEmpty()) {
            try {
                queryInfo.setEstimatedTimeRemaining(Integer.parseInt(estimatedTimeRemainingStr));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid estimatedTimeRemaining value: " + estimatedTimeRemainingStr);
            }
        }
        
        return hasData ? queryInfo : null;
    }
    
    /**
     * Build ResultInfo from request parameters
     */
    private ResultInfo buildResultInfoFromParams(HttpServletRequest request) {
        ResultInfo resultInfo = new ResultInfo();
        boolean hasData = false;
        
        String totalRowsStr = request.getParameter("resultInfo.totalRows");
        if (totalRowsStr != null && !totalRowsStr.trim().isEmpty()) {
            try {
                resultInfo.setTotalRows(Integer.parseInt(totalRowsStr));
                hasData = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid totalRows value: " + totalRowsStr);
            }
        }
        
        String resultLocation = request.getParameter("resultInfo.resultLocation");
        if (resultLocation != null && !resultLocation.trim().isEmpty()) {
            resultInfo.setResultLocation(resultLocation);
            hasData = true;
        }
        
        String formatType = request.getParameter("resultInfo.format.type");
        String formatSerialization = request.getParameter("resultInfo.format.serialization");
        
        if ((formatType != null && !formatType.trim().isEmpty()) || 
            (formatSerialization != null && !formatSerialization.trim().isEmpty())) {
            ResultInfo.Format format = new ResultInfo.Format();
            
            if (formatType != null && !formatType.trim().isEmpty()) {
                format.setType(formatType);
            }
            
            if (formatSerialization != null && !formatSerialization.trim().isEmpty()) {
                format.setSerialization(formatSerialization);
            }
            
            resultInfo.setFormat(format);
            hasData = true;
        }
        
        return hasData ? resultInfo : null;
    }
    
    /**
     * Build ErrorInfo from request parameters
     */
    private ErrorInfo buildErrorInfoFromParams(HttpServletRequest request) {
        ErrorInfo errorInfo = new ErrorInfo();
        boolean hasData = false;
        
        String errorCode = request.getParameter("errorInfo.errorCode");
        if (errorCode != null && !errorCode.trim().isEmpty()) {
            errorInfo.setErrorCode(errorCode);
            hasData = true;
        }
        
        String errorMessage = request.getParameter("errorInfo.errorMessage");
        if (errorMessage != null && !errorMessage.trim().isEmpty()) {
            errorInfo.setErrorMessage(errorMessage);
            hasData = true;
        }
        
        String stackTrace = request.getParameter("errorInfo.stackTrace");
        if (stackTrace != null && !stackTrace.trim().isEmpty()) {
            errorInfo.setStackTrace(stackTrace);
            hasData = true;
        }
        
        return hasData ? errorInfo : null;
    }
    
    /**
     * Build Metadata from request parameters
     */
    private Metadata buildMetadataFromParams(HttpServletRequest request) {
        Metadata metadata = new Metadata();
        boolean hasData = false;
        
        String query = request.getParameter("metadata.query");
        if (query != null && !query.trim().isEmpty()) {
            metadata.setQuery(query);
            hasData = true;
        }
        
        String database = request.getParameter("metadata.database");
        if (database != null && !database.trim().isEmpty()) {
            metadata.setDatabase(database);
            hasData = true;
        }
        
        String userTablesStr = request.getParameter("metadata.userTables");
        if (userTablesStr != null && !userTablesStr.trim().isEmpty()) {
            String[] tables = userTablesStr.split(",");
            List<String> tableList = new ArrayList<>();
            
            for (String table : tables) {
                String trimmed = table.trim();
                if (!trimmed.isEmpty()) {
                    tableList.add(trimmed);
                }
            }
            
            if (!tableList.isEmpty()) {
                metadata.setUserTables(tableList);
                hasData = true;
            }
        }
        
        return hasData ? metadata : null;
    }
    
    /**
     * Get a required parameter or throw an exception
     */
    private String getRequiredParameter(HttpServletRequest request, String paramName) 
            throws RequiredParameterException {
        String value = request.getParameter(paramName);
        if (value == null || value.trim().isEmpty()) {
            throw new RequiredParameterException("Missing required parameter: " + paramName);
        }
        return value;
    }
    
    /**
     * Get list of valid status values for error messages
     */
    private List<String> getValidStatusValues() {
        List<String> values = new ArrayList<>();
        for (ExecutionStatus status : ExecutionStatus.values()) {
            values.add(status.name());
        }
        return values;
    }
    
    /**
     * Send a success response
     */
    private void sendSuccessResponse(HttpServletResponse response, String jobId) throws IOException {
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        out.println("{");
        out.println("  \"status\": \"success\",");
        out.println("  \"jobId\": \"" + jobId + "\",");
        out.println("  \"message\": \"Job status submitted to Kafka successfully\"");
        out.println("}");
    }
    
    /**
     * Exception for missing required parameters
     */
    private static class RequiredParameterException extends Exception {
        public RequiredParameterException(String message) {
            super(message);
        }
    }

    /**
     * Cleans up resources when the servlet is being destroyed.
     */
    @Override
    public void destroy() {
        log.info("JobUpdateService is being destroyed");
        super.destroy();
    }
}