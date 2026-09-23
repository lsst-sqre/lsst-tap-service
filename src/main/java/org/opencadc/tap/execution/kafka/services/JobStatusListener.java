package org.opencadc.tap.execution.kafka.services;

import org.opencadc.tap.config.TapConfig;

import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import ca.nrc.cadc.tap.PluginFactoryImpl;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.tap.TableWriter;
import org.apache.log4j.Logger;
import org.opencadc.tap.logging.TAPLogger;
import org.opencadc.tap.execution.kafka.messages.JobStatus;

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
    private static final TAPLogger tapLog = new TAPLogger(JobStatusListener.class);

    // We should probably move this elsewhere
    private static final String baseURL = TapConfig.baseUrl();
    private static final String pathPrefix = TapConfig.pathPrefix();

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
                tapLog.logWarn(null, null, "Received null status or status with null job ID");
                return;
            }

            Job job = jobPersist.get(status.getJobID());
            jobPersist.getDetails(job);

            // Log user
            String username = job.getOwnerID() != null ? job.getOwnerID() : null;

            ExecutionPhase previousPhase = jobUpdater.getPhase(status.getJobID());
            ExecutionPhase newPhase = JobStatus.ExecutionStatus.toExecutionPhase(status.getStatus());

            // Check if previous phase was ABORTED
            if (previousPhase == ExecutionPhase.ABORTED) {
                log.debug("Job " + status.getJobID() + " is already ABORTED. Ignoring update to " + newPhase);
                return;
            }

            // Skip logging if phase hasn't changed
            boolean phaseChanged = !previousPhase.equals(newPhase);

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
                job.setJobInfo(jobInfo);
            }

            if (diagnostics != null && !diagnostics.isEmpty()) {
                job.setResultsList(diagnostics);
            }

            // Set the end time if the job is in a terminal state
            if (isTerminalStatus(status.getStatus())) {
                job.setEndTime(new Date());
            }

            jobPersist.put(job);

            if (phaseChanged && !isTerminalStatus(status.getStatus())) {
                tapLog.jobPhase(job.getID(), username, previousPhase.toString(), newPhase.toString());
            }

            // Skip duplicate terminal status updates
            if (isTerminalStatus(status.getStatus()) && !JobPhaseManager.isTerminalPhase(previousPhase)) {
                TAPLogger.Outcome outcome = buildOutcome(status, job);
                if (status.getStatus() == JobStatus.ExecutionStatus.ERROR) {
                    String errorMessage =
                            status.getErrorInfo() != null ? status.getErrorInfo().getErrorMessage() : null;
                    tapLog.jobFailed(job.getID(), username, errorMessage, outcome);
                } else if (status.getStatus() == JobStatus.ExecutionStatus.ABORTED) {
                    tapLog.jobAborted(job.getID(), username, TAPLogger.REASON_BACKEND, outcome);
                } else {
                    tapLog.jobFinished(job.getID(), username, newPhase.toString(), outcome);
                }
            }
        } catch (Exception e) {
            tapLog.logError(status != null ? status.getJobID() : null, null, "Error processing status update", e);
        }
    }

    /**
     * executionMs runs from submission (UWS start) to the terminal status.
     */
    private TAPLogger.Outcome buildOutcome(JobStatus status, Job job) {
        TAPLogger.Outcome outcome = new TAPLogger.Outcome().executionID(status.getExecutionID());
        if (job.getStartTime() != null && job.getEndTime() != null) {
            outcome.executionMs(job.getEndTime().getTime() - job.getStartTime().getTime());
        }
        JobStatus.QueryInfo queryInfo = status.getQueryInfo();
        if (queryInfo != null) {
            if (queryInfo.getStartTime() != null && queryInfo.getEndTime() != null) {
                outcome.queryMs(queryInfo.getEndTime() - queryInfo.getStartTime());
            }
            outcome.chunks(queryInfo.getCompletedChunks(), queryInfo.getTotalChunks());
            outcome.bytes(queryInfo.getBytesProcessed(), queryInfo.getBytesBilled(), queryInfo.getCached());
        }
        if (status.getResultInfo() != null && status.getResultInfo().getTotalRows() != null) {
            outcome.rowCount(status.getResultInfo().getTotalRows().longValue());
        }
        if (status.getErrorInfo() != null) {
            outcome.errorCode(status.getErrorInfo().getErrorCode());
        }
        return outcome;
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
            tapLog.logError(status.getJobID(), null, "Could not read error info from status update", e);
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
            tapLog.logError(job.getID(), null, "Could not create error document", e);
            return null;
        }
    }

    /**
     * Get job information, mainly used for progress information at this time.
     * Supports both chunk-based progress (Qserv) and byte-based progress (BigQuery).
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
        Long bytesProcessed = null;
        Long bytesBilled = null;
        Boolean cached = null;
        boolean hasProgress = false;

        try {
            if (status.getQueryInfo() != null) {
                // Chunk-based progress (Qserv)
                if (status.getQueryInfo().getCompletedChunks() != null
                        && status.getQueryInfo().getTotalChunks() != null) {
                    completedChunks = status.getQueryInfo().getCompletedChunks();
                    totalChunks = status.getQueryInfo().getTotalChunks();
                    if (totalChunks > 0) {
                        pctComplete = String.format("%.0f", (completedChunks / (double) totalChunks) * 100);
                        hasProgress = true;
                    }
                }

                // Byte-based progress (BigQuery)
                bytesProcessed = status.getQueryInfo().getBytesProcessed();
                bytesBilled = status.getQueryInfo().getBytesBilled();
                cached = status.getQueryInfo().getCached();
                if (bytesProcessed != null) {
                    hasProgress = true;
                }
            }
        } catch (Exception e) {
            tapLog.logWarn(status.getJobID(), null, "Could not calculate query progress", e);
            return null;
        }

        if (!hasProgress) {
            log.debug("Job " + status.getJobID() + " has no progress information available.");
            return null;
        }

        try {
            StringBuilder xmlBuilder = new StringBuilder();
            xmlBuilder.append("<progress>\n");

            if (pctComplete != null) {
                xmlBuilder.append("  <percentComplete>").append(pctComplete).append("</percentComplete>\n");
                xmlBuilder.append("  <itemsProcessed>").append(completedChunks).append("</itemsProcessed>\n");
                xmlBuilder.append("  <totalItems>").append(totalChunks).append("</totalItems>\n");
            }

            // Build statusMessage with BigQuery byte-based info if available
            StringBuilder statusMsg = new StringBuilder();
            if (bytesProcessed != null) {
                statusMsg.append("Bytes processed: ").append(bytesProcessed);
            }
            if (bytesBilled != null) {
                if (statusMsg.length() > 0) statusMsg.append(", ");
                statusMsg.append("Bytes billed: ").append(bytesBilled);
            }
            if (cached != null) {
                if (statusMsg.length() > 0) statusMsg.append(", ");
                statusMsg.append("Cached: ").append(cached);
            }

            xmlBuilder.append("  <message>").append(statusMsg).append("</message>\n");
            xmlBuilder.append("</progress>");

            content = xmlBuilder.toString();
        } catch (Exception e) {
            tapLog.logWarn(status.getJobID(), null, "Could not build job progress info", e);
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
                metadata.add(new Result("executionId", URI.create("execid:" + status.getExecutionID())));
            } else {
                tapLog.logWarn(status.getJobID(), null, "Status update has no executionID");
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
            tapLog.logError(status.getJobID(), null, "Could not read job metadata from status update", e);
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
