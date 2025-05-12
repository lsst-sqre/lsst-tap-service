package org.opencadc.tap.impl.util;

import org.apache.log4j.Logger;

import java.net.URI;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.UploadManager;

import org.opencadc.tap.impl.QServQueryRunner;
import org.opencadc.tap.impl.StorageUtils;
import org.opencadc.tap.kafka.models.JobRun.UploadTable;
import org.opencadc.tap.kafka.models.JobRun;
import org.opencadc.tap.kafka.services.CreateDeleteEvent;
import org.opencadc.tap.kafka.services.CreateJobEvent;
import org.opencadc.tap.impl.context.WebAppContext;

/**
 * Service class for Kafka job operations.
 * Handles generating and sending jobs to Kafka.
 * 
 * @author stvoutsin
 * 
 */
public class KafkaJobService {
    private static final Logger log = Logger.getLogger(KafkaJobService.class);

    /**
     * Default expiration time in minutes for job result URLs
     */
    private static final int DEFAULT_JOB_RESULT_EXPIRATION_MINUTES = 120;

    /**
     * Prepares and submits a job to Kafka for execution.
     * 
     * @param job                   The job to execute
     * @param jobRunner             The job runner instance
     * @param createJobEventService Kafka job creation service
     * @param databaseString        Database connection string
     * @param bucketURL             Storage bucket URL
     * @param bucket                Storage bucket name
     * @param jobUpdater            JobUpdater implementation
     * @return true if submission was successful, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean prepareAndSubmitJob(Job job,
            JobRunner jobRunner,
            CreateJobEvent createJobEventService,
            String databaseString,
            String bucketURL,
            String bucket,
            JobUpdater jobUpdater)
            throws JobNotFoundException, JobPersistenceException {

        String jobId = job.getID();
        log.debug("Preparing to send job to Kafka: " + jobId);

        try {
            if (createJobEventService == null) {
                Object service = WebAppContext.getContextAttribute("jobProducer");
                if (service != null && service instanceof CreateJobEvent) {
                    createJobEventService = (CreateJobEvent) service;
                } else {
                    throw new RuntimeException("CreateJobEvent service not available");
                }
            }

            JobSubmissionInfo jobInfo = extractJobInfo(job, jobRunner, bucketURL, bucket);

            // Submit job to Kafka
            String eventJobId = createJobEventService.submitQuery(
                    jobInfo.sql,
                    jobId,
                    jobInfo.resultDestination,
                    jobInfo.resultLocation,
                    jobInfo.resultFormat,
                    jobInfo.ownerID,
                    databaseString,
                    jobInfo.maxrec,
                    jobInfo.uploadTables);

            log.debug("Job sent to Kafka successfully with event ID: " + eventJobId);

            try {
                ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
                if (ExecutionPhase.HELD.equals(currentPhase)) {
                    boolean transitioned = JobPhaseManager.transitionJobPhase(
                            jobId, ExecutionPhase.HELD, ExecutionPhase.EXECUTING, jobUpdater);
                    if (!transitioned) {
                        log.warn("Failed to set job " + jobId + " to EXECUTING, phase may have changed");
                    }
                }
            } catch (Exception ex) {
                log.error("Failed to update job phase after Kafka submission: " + jobId, ex);
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to send job to Kafka: " + jobId, e);
            try {
                JobPhaseManager.setErrorPhase(
                        jobId,
                        "Failed to send job to Kafka: " + e.getMessage(),
                        ErrorType.FATAL,
                        jobUpdater);
            } catch (Exception ex) {
                log.error("Failed to set job " + jobId + " to ERROR state", ex);
            }
            return false;
        }
    }

    /**
     * Submits a job deletion request to Kafka.
     * 
     * @param job                      The job to delete
     * @param createDeleteEventService Kafka delete event service
     * @param jobUpdater               JobUpdater implementation
     * @param jobPersistence           JobPersistence implementation
     * @return true if deletion request was successful, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean submitJobDeletion(Job job,
            CreateDeleteEvent createDeleteEventService,
            JobUpdater jobUpdater,
            JobPersistence jobPersistence)
            throws JobNotFoundException, JobPersistenceException {

        if (job.getID() == null || job.getID().trim().isEmpty()) {
            throw new IllegalArgumentException("Job ID cannot be null or empty");
        }

        log.debug("Preparing to send job deletion request to Kafka for jobId: " + job.getID());

        jobPersistence.getDetails(job);
        String executionId = null;
        String jobId = job.getID();

        if (job.getResultsList() != null) {
            for (Result result : job.getResultsList()) {
                if (result.getName().equals("executionId")) {
                    executionId = result.getURI().toString();
                }
            }
        }

        try {
            if (executionId == null || executionId.trim().isEmpty()) {
                throw new IllegalArgumentException("ExecutionID cannot be null or empty");
            }

            if (createDeleteEventService == null) {
                Object service = WebAppContext.getContextAttribute("deleteProducer");
                if (service != null && service instanceof CreateDeleteEvent) {
                    createDeleteEventService = (CreateDeleteEvent) service;
                } else {
                    throw new RuntimeException("CreateDeleteEvent service not available");
                }
            }

            String ownerId = job.getOwnerID();
            String deletedExecutionId = createDeleteEventService.submitDeletion(executionId, ownerId, jobId);
            log.debug("Job deletion request sent to Kafka successfully for executionId: " + deletedExecutionId);

            // Update job phase if jobId and jobUpdater are provided
            if (jobId != null && !jobId.trim().isEmpty() && jobUpdater != null) {
                try {
                    ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
                    if (ExecutionPhase.EXECUTING.equals(currentPhase)) {
                        boolean transitioned = JobPhaseManager.transitionJobPhase(
                                jobId, ExecutionPhase.EXECUTING, ExecutionPhase.ABORTED, jobUpdater);
                        if (!transitioned) {
                            log.warn("Failed to set job " + jobId + " to ABORTED, phase may have changed");
                        }
                    }
                } catch (Exception ex) {
                    log.error("Failed to update job phase after deletion request: " + jobId, ex);
                }
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to send job deletion request to Kafka for jobId: " + jobId +
                    ", executionId: " + executionId, e);

            if (jobId != null && !jobId.trim().isEmpty() && jobUpdater != null) {
                try {
                    JobPhaseManager.setErrorPhase(
                            jobId,
                            "Failed to send job deletion request to Kafka: " + e.getMessage(),
                            ErrorType.FATAL,
                            jobUpdater);
                } catch (Exception ex) {
                    log.error("Failed to set job " + jobId + " to ERROR state", ex);
                }
            }

            return false;
        }
    }

    /**
     * Extract job information from the job runner.
     * 
     * @param job       The job
     * @param jobRunner The job runner instance
     * @param bucketURL Storage bucket URL
     * @param bucket    Storage bucket name
     * @return JobSubmissionInfo containing extracted job information
     */
    private static JobSubmissionInfo extractJobInfo(Job job, JobRunner jobRunner, String bucketURL, String bucket) {

        JobSubmissionInfo info = new JobSubmissionInfo();
        info.ownerID = job.getOwnerID();

        if (jobRunner instanceof QServQueryRunner) {
            QServQueryRunner qRunner = (QServQueryRunner) jobRunner;
            QueryRunner queryRunner = (QueryRunner) jobRunner;
            info.sql = qRunner.internalSQL;

            info.resultDestination = StorageUtils.generateJobResultSignedUrl(
                    job.getID(), "application/x-votable+xml", DEFAULT_JOB_RESULT_EXPIRATION_MINUTES);
            info.resultLocation = StorageUtils.generateResultLocation(job.getID());
            info.resultFormat = VOTableUtil.createResultFormat(job.getID(), queryRunner);
            info.maxrec = queryRunner.maxRows;

            try {

                if (!queryRunner.uploadTableLocations.isEmpty() && queryRunner.uploadTableSchemaLocations != null) {
                    log.debug("Found " + queryRunner.uploadTableLocations.size() + " upload table locations");
                    for (Map.Entry<String, URI> entry : queryRunner.uploadTableLocations.entrySet()) {
                        String tableName = entry.getKey();
                        String formattedTable = formatTableName(entry.getKey(),getUsername());
                        String sourceUrl = entry.getValue().toString();
                        String schemaUrl = queryRunner.uploadTableSchemaLocations.get(tableName).toString();
                        UploadTable uploadTable = new UploadTable(formattedTable, sourceUrl, schemaUrl);
                        info.uploadTables.add(uploadTable);
                    }
                }
            } catch (Exception e) {
                log.error("Error parsing upload parameter: " + e.getMessage(), e);
            }
        }

        return info;
    }

    /**
     * Class to hold job submission information.
     */
    private static class JobSubmissionInfo {
        String sql = "";
        String resultDestination = "";
        String resultLocation = "";
        JobRun.ResultFormat resultFormat = null;
        String ownerID = "";
        Integer maxrec = null;
        List<UploadTable> uploadTables = new ArrayList<>();
    }

    /**
     * Get the username of the caller.
     * 
     * @return the username of the caller
     */
    protected static String getUsername() {
        
        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(caller);
        String username;

        if ((authMethod != null) && (authMethod != AuthMethod.ANON)) {
            final Set<HttpPrincipal> curPrincipals = caller.getPrincipals(HttpPrincipal.class);
            final HttpPrincipal[] principalArray = new HttpPrincipal[curPrincipals.size()];
            username = ((HttpPrincipal[]) curPrincipals.toArray(principalArray))[0].getName();
        } else {
            username = null;
        }

        log.info("Username:" + username);
        return username;
    }

    /**
     * Format table name to include username, i.e.
     * user_<username>.TAP_UPLOAD_tableName
     * 
     * @param originalTableName The original table name
     * @param username          The username
     * @return Formatted table name
     */
    private static String formatTableName(String originalTableName, String username) {
        if (username == null || username.isEmpty()) {
            username = "anonymous";
        }

        String baseName = originalTableName;
        if (originalTableName.startsWith("TAP_UPLOAD.")) {
            baseName = originalTableName.substring("TAP_UPLOAD.".length());
        }

        return "user_" + username + ".TAP_UPLOAD_" + baseName;
    }
}