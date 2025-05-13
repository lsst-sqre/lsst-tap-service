package org.opencadc.tap.kafka.services;

import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.models.JobStatus;

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

    // We should probably move this elsewhere
    private static final String baseURL = System.getProperty("base_url");
    private static final String pathPrefix = System.getProperty("path_prefix");

    private final JobPersistence jobPersist;
    private final JobUpdater jobUpdater;

    public JobStatusListener() {
        IdentityManager im = AuthenticationUtil.getIdentityManager();
        this.jobPersist = new PostgresJobPersistence(new RandomStringGenerator(16), im, true);
        this.jobUpdater = (JobUpdater) jobPersist;
    }

    @Override
    public void onStatusUpdate(JobStatus status) {
        log.info("Job Status Update Received: " + status.toString());

        try {
            if (status == null || status.getJobID() == null) {
                log.warn("Received null status or status with null job ID");
                return;
            }

            Job job = jobPersist.get(status.getJobID());
            jobPersist.getDetails(job);

            ExecutionPhase previousPhase = jobUpdater.getPhase(status.getJobID());
            ExecutionPhase newPhase = JobStatus.ExecutionStatus.toExecutionPhase(status.getStatus());

            List<Result> diagnostics = getJobMetadata(status, job);
            ErrorSummary errorSummary = getErrorInfo(status);
            
            if (errorSummary != null) {
                jobUpdater.setPhase(status.getJobID(), previousPhase, newPhase, errorSummary, new Date());
            } else {
                jobUpdater.setPhase(status.getJobID(), previousPhase, newPhase, diagnostics, new Date());
            }

            log.info("Updated phase for job " + status.getJobID() + ": " + previousPhase + " -> " + newPhase);

            if (isTerminalStatus(status.getStatus())) {
                log.debug("Job " + status.getJobID() + " reached terminal status: " + status.getStatus());
            }
        } catch (Exception e) {
            log.error("Error processing status update for job ID: " +
                    (status != null ? status.getJobID() : "unknown"), e);
        }
    }

    /**
     * Get job error information if present
     */
    private ErrorSummary getErrorInfo(JobStatus status) {
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
                return new ErrorSummary(errorMessage, errorType);
            }
        } catch (Exception e) {
            log.error("Error updating error info for job: " + status.getJobID(), e);
        }
        return null;
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

            // Only add executionId once to Results
            if (job.getResultsList() != null) {
                for (Result result : job.getResultsList()) {
                    if (result.getName().equals("executionId")) {
                        skipExecutionId = true;
                        break;
                    }
                }
            }

            if (!skipExecutionId && status.getExecutionID() != null) {
                metadata.add(new Result("executionId", URI.create(status.getExecutionID())));
            }

            if (status.getResultInfo() != null) {
                if (status.getResultInfo().getTotalRows() != null) {
                    metadata.add(new Result("rowcount", URI.create("final:" + status.getResultInfo().getTotalRows())));
                }

                if (status.getResultInfo().getResultLocation() != null) {
                    URL url = new URL(status.getResultInfo().getResultLocation());
                    String filePath = url.getPath();

                    if (filePath.startsWith("/")) {
                        filePath = filePath.substring(1);
                    }

                    URI resultURI = new URI(baseURL + pathPrefix + "/results/" + filePath);
                    Result res = new Result("result", resultURI);
                    metadata.add(res);
                }
            }

            if (status.getQueryInfo() != null) {
                if (status.getQueryInfo().getCompletedChunks() != null
                        && status.getQueryInfo().getTotalChunks() != null) {
                    Result chunkProgress = new Result("chunkProgress",
                            URI.create(status.getQueryInfo().getCompletedChunks() + "/"
                                    + status.getQueryInfo().getTotalChunks()));
                    metadata.add(chunkProgress);
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