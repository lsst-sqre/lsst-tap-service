package org.opencadc.tap.kafka.services;

import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.tap.PluginFactoryImpl;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.tap.TableWriter;
import org.apache.log4j.Logger;
import org.opencadc.tap.impl.logging.TAPLogger;
import org.opencadc.tap.impl.uws.server.KafkaJobExecutor;
import org.opencadc.tap.kafka.models.JobStatus;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Job update listener for updating UWS jobs based on Kafka events.
 * 
 * @author stvoutsin
 */
public class JobStatusListener implements ReadJobStatus.StatusListener {
    private static final Logger log = Logger.getLogger(JobStatusListener.class);
    private static final TAPLogger tapLog = new TAPLogger(KafkaJobExecutor.class);

    // We should probably move this elsewhere
    private static final String baseURL = System.getProperty("base_url");
    private static final String pathPrefix = System.getProperty("path_prefix");

    private final JobPersistence jobPersist;
    private final JobUpdater jobUpdater;

    public JobStatusListener() {
        this.jobPersist = new PostgresJobPersistence();
        this.jobUpdater = (JobUpdater) jobPersist;
    }

    @Override
    public void onStatusUpdate(JobStatus status) {
        log.debug("Job Status Update Received: " + status.toString());

        try {
            if (status == null || status.getJobID() == null) {
                log.warn("Received null status or status with null job ID");
                return;
            }

            Job job = jobPersist.get(status.getJobID());
            jobPersist.getDetails(job);

            // Log user
            String username = job.getOwnerID() != null ? job.getOwnerID() : "";

            ExecutionPhase previousPhase = jobUpdater.getPhase(status.getJobID());
            ExecutionPhase newPhase = JobStatus.ExecutionStatus.toExecutionPhase(status.getStatus());

            // Check if previous phase was ABORTED
            if (previousPhase == ExecutionPhase.ABORTED) {
                log.info("Job " + status.getJobID() + " is already ABORTED. Ignoring update to " + newPhase);
                return;
            }

            // Now update with additional metadata
            JobInfo jobInfo = getJobInfo(status, job);
            List<Result> diagnostics = getJobMetadata(status, job);
            ErrorSummary errorSummary = getErrorInfo(status, job);

            job.setExecutionPhase(newPhase);
            job.setOwnerID(username);

            if (errorSummary != null) {
                job.setErrorSummary(errorSummary);
            }

            if (jobInfo != null) {
                // job.setJobInfo(jobInfo);
                // TODO: Add the jobInfo once pyvo can handle it properly
            }

            if (diagnostics != null && !diagnostics.isEmpty()) {
                job.setResultsList(diagnostics);
            }

            // Set the end time if the job is in a terminal state
            if (isTerminalStatus(status.getStatus())) {
                job.setEndTime(new Date());
            }

            jobPersist.put(job);

            tapLog.log(job.getID(), username, "Query update event received. Updated phase for job " + status.getJobID() + ": " + previousPhase + " -> " + newPhase);

            if (isTerminalStatus(status.getStatus())) {
                tapLog.log(job.getID(), username, "Job finished with phase: " + newPhase);
            }
        } catch (Exception e) {
            log.error("Error processing status update for job ID: " +
                    (status != null ? status.getJobID() : "unknown"), e);
        }
    }

    /**
     * Get job error information if present
     */
    private ErrorSummary getErrorInfo(JobStatus status, Job job) {
        if (status == null || status.getJobID() == null) {
            return null;
        }

        try {
            if (status.getStatus() == JobStatus.ExecutionStatus.ERROR &&
                    status.getErrorInfo() != null &&
                    status.getErrorInfo().getErrorMessage() != null) {

                String errorMessage = status.getErrorInfo().getErrorMessage();
                String errorCode = status.getErrorInfo().getErrorCode();

                // This is a temporary workaround
                ErrorType errorType = ErrorType.FATAL;


                URL errorURL = createErrorDocument(job, errorMessage, status);
                
                if (errorURL != null) {
                    return new ErrorSummary(errorMessage, errorType, errorURL);
                } else {
                    return new ErrorSummary(errorMessage, errorType);
                }
            }
        } catch (Exception e) {
            log.error("Error updating error info for job: " + status.getJobID(), e);
        }
        return null;
    }

    /**
     * Create error document and upload to result store
     * 
     * @param job
     * @param errorMessage
     * @param status
     * @return URL of the error document or null if creation failed
     */
    private URL createErrorDocument(Job job, String errorMessage, JobStatus status) {
        try {
            log.debug("creating TableWriter for error...");
            
            PluginFactoryImpl pfac = new PluginFactoryImpl(job);
            ResultStore rs = pfac.getResultStore();
            
            TableWriter ewriter = pfac.getErrorWriter();
            
            String exceptionMessage = "Job execution failed: " + errorMessage;
            Exception errorException = new RuntimeException(exceptionMessage);
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ewriter.write(errorException, bos);
            String filename = "error_" + job.getID() + "." + ewriter.getExtension();
            
            rs.setJob(job);
            rs.setFilename(filename);
            rs.setContentType(ewriter.getContentType());
            URL errorURL = rs.put(errorException, ewriter);
            
            log.debug("Error URL: " + errorURL);
            return errorURL;
            
        } catch (Exception e) {
            log.error("Failed to create error document for job: " + job.getID(), e);
            return null;
        }
    }

    /**
     * Get job information, mainly used for progress information at this time.
     * 
     * @param status
     * @param job
     * @return
     */
    private JobInfo getJobInfo(JobStatus status, Job job) {
        String pctComplete = null;
        String content = "";
        String contentType = "text/xml";
        Boolean valid = true;

        int completedChunks = 0;
        int totalChunks = 0;
        try {
            if (status.getQueryInfo() != null) {
                if (status.getQueryInfo().getCompletedChunks() != null
                        && status.getQueryInfo().getTotalChunks() != null) {
                    completedChunks = status.getQueryInfo().getCompletedChunks();
                    totalChunks = status.getQueryInfo().getTotalChunks();
                    if (totalChunks > 0) {
                        pctComplete = String.format("%.0f", (completedChunks / (double) totalChunks) * 100);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error calculating progress for job: " + status.getJobID(), e);
            return null;
        }

        if (pctComplete == null) {
            log.debug("Job " + status.getJobID() + " has no progress information available.");
            return null;
        }

        try {
            StringBuilder xmlBuilder = new StringBuilder();
            xmlBuilder.append("<pct_complete>").append(pctComplete).append("</pct_complete>\n");
            xmlBuilder.append("<tap_chunks_processed>").append(completedChunks).append("</tap_chunks_processed>\n");
            xmlBuilder.append("<tap_total_chunks>").append(totalChunks).append("</tap_total_chunks>\n");
            content = xmlBuilder.toString();
        } catch (Exception e) {
            log.warn("Error generating job info for job: " + status.getJobID(), e);
            return null;
        }

        JobInfo jobInfo = new JobInfo(content, contentType, valid);
        log.debug("Generated job info for job " + status.getJobID() + ": " + content);
        return jobInfo;
    }

    /**
     * Get job metadata with additional information
     */
    private List<Result> getJobMetadata(JobStatus status, Job job) {
        List<Result> metadata = new ArrayList<>();

        if (status == null || status.getJobID() == null) {
            return metadata;
        }

        try {
            Boolean skipExecutionId = false;

            if (status.getExecutionID() != null && !status.getExecutionID().trim().isEmpty()) {
                metadata.add(new Result("executionId", URI.create(status.getExecutionID())));
            } else {
                log.warn("ExecutionID is null or empty for job: " + status.getJobID());
            }

            if (status.getResultInfo() != null) {
                if (status.getResultInfo().getTotalRows() != null) {
                    metadata.add(new Result("rowcount", URI.create("final:" + status.getResultInfo().getTotalRows())));
                }

                if (status.getResultInfo().getResultLocation() != null) {
                    URL url = new URL(status.getResultInfo().getResultLocation());
                    String filePath = url.getPath();                
                    String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
                
                    URI resultURI = new URI(baseURL + pathPrefix + "/results/" + fileName);
                    Result res = new Result("result", resultURI);
                    metadata.add(res);
                }
            }
            return metadata;
        } catch (Exception e) {
            log.error("Error updating metadata for job: " + status.getJobID(), e);
        }

        return metadata;
    }

    /**
     * Check if a status is terminal
     */
    private boolean isTerminalStatus(JobStatus.ExecutionStatus status) {
        return status == JobStatus.ExecutionStatus.COMPLETED ||
                status == JobStatus.ExecutionStatus.ERROR ||
                status == JobStatus.ExecutionStatus.ABORTED ||
                status == JobStatus.ExecutionStatus.DELETED;
    }
}
