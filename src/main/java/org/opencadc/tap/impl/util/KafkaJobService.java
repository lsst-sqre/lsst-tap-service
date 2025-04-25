package org.opencadc.tap.impl.util;

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

import java.util.List;

import org.apache.log4j.Logger;
import org.opencadc.tap.impl.QServQueryRunner;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.UploadManager;

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
                    jobInfo.uploadName,
                    jobInfo.uploadSource);

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
     * @param job                    The job to delete
     * @param createDeleteEventService Kafka delete event service
     * @param jobUpdater               JobUpdater implementation
     * @param jobPersistence          JobPersistence implementation
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
            List<Parameter> paramList = queryRunner.paramList;
            info.sql = qRunner.internalSQL;
            info.resultDestination = GCSStorageUtil.generateSignedUrl(
                    bucket, job.getID(), "application/x-votable+xml", 120);
            info.resultLocation = GCSStorageUtil.generateResultLocation(bucketURL, job.getID());
            info.resultFormat = VOTableUtil.createResultFormat(job.getID(), queryRunner);
            info.maxrec = queryRunner.maxRows;
            try {
                for (Parameter param : paramList) {
                    if (param.getName().equals(UploadManager.UPLOAD)) {
                        String paramAsString = param.getValue();
                        // Example <uws:parameter id="UPLOAD">ut1,https://tap-files.lsst.codes/ut1-oxz0xczususdgg5z</uws:parameter>
                        String[] paramParts = paramAsString.split(",");
                        if (paramParts.length > 1) {
                            info.uploadName = paramParts[0];
                            info.uploadSource = paramParts[1];
                        }
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
        String uploadName = "";
        String uploadSource = "";
    }
}