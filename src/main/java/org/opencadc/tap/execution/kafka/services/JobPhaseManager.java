package org.opencadc.tap.execution.kafka.services;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import org.apache.log4j.Logger;
import org.opencadc.tap.logging.TAPLogger;

import java.util.Date;

/**
 * Utility class for managing job phase transitions.
 * 
 * @author stvoutsin 
 */
public class JobPhaseManager {
    private static final Logger log = Logger.getLogger(JobPhaseManager.class);
    private static final TAPLogger tapLog = new TAPLogger(JobPhaseManager.class);

    /**
     * Transition a job from one phase to another (with timestamp).
     * 
     * @param jobId      The job identifier
     * @param fromPhase  The expected current phase
     * @param toPhase    The target phase
     * @param jobUpdater The JobUpdater to use
     * @return true if transition succeeded, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean transitionJobPhase(String jobId,
            ExecutionPhase fromPhase,
            ExecutionPhase toPhase,
            JobUpdater jobUpdater)
            throws JobNotFoundException, JobPersistenceException {
        try {
            Date now = new Date();
            ExecutionPhase result = jobUpdater.setPhase(jobId, fromPhase, toPhase, now);

            if (result == null) {
                ExecutionPhase actual = jobUpdater.getPhase(jobId);

                tapLog.logWarn(jobId, null, "Failed to set job to " + toPhase +
                        ", phase changed from " + fromPhase +
                        " to " + actual + " instead");
                return false;
            }

            log.debug("Job " + jobId + " transitioned from " + fromPhase + " to " + toPhase);
            return true;
        } catch (Throwable e) {
            tapLog.logError(jobId, null, "Phase transition failed", e);
            return false;
        }
    }

    /**
     * Move a job to the ERROR phase with an error summary.
     * 
     * @param jobId        The job identifier
     * @param errorMessage The error message to include
     * @param errorType    The type of error (default: FATAL)
     * @param jobUpdater   The JobUpdater to use
     * @return true if transition succeeded, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean setErrorPhase(String jobId,
            String errorMessage,
            ErrorType errorType,
            JobUpdater jobUpdater)
            throws JobNotFoundException, JobPersistenceException {
        try {
            Date now = new Date();
            ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
            ErrorSummary es = new ErrorSummary(errorMessage, errorType != null ? errorType : ErrorType.FATAL);

            ExecutionPhase result = jobUpdater.setPhase(jobId, currentPhase, ExecutionPhase.ERROR, es, now);

            if (result == null) {
                tapLog.logWarn(jobId, null, "Failed to set job to ERROR, phase may have changed from " + currentPhase);
                return false;
            }

            log.debug("Job " + jobId + " set to ERROR state: " + errorMessage);
            if (!isTerminalPhase(currentPhase)) {
                tapLog.jobFailed(jobId, null, errorMessage, null);
            }
            return true;
        } catch (Throwable e) {
            tapLog.logError(jobId, null, "Error setting job to ERROR state", e);
            return false;
        }
    }

    /**
     * Check if the job is in one of the specified phases.
     * 
     * @param jobId      The job identifier
     * @param jobUpdater The JobUpdater to use
     * @param phases     Varargs of phases to check against
     * @return true if job is in one of the specified phases, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public static boolean isInPhase(String jobId, JobUpdater jobUpdater, ExecutionPhase... phases)
            throws JobNotFoundException, TransientException, JobPersistenceException {
        ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);

        for (ExecutionPhase phase : phases) {
            if (phase.equals(currentPhase)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if the job is in a terminal phase (COMPLETED, ERROR, ABORTED).
     * 
     * @param jobId      The job identifier
     * @param jobUpdater The JobUpdater to use
     * @return true if job is in a terminal phase, false otherwise
     * @throws JobNotFoundException If the job is not found
     */
    public static boolean isTerminal(String jobId, JobUpdater jobUpdater)
            throws JobNotFoundException, JobPersistenceException {
        return isInPhase(jobId, jobUpdater,
                ExecutionPhase.COMPLETED, ExecutionPhase.ERROR, ExecutionPhase.ABORTED);
    }

    /**
     * Check if the phase is COMPLETED, ERROR, ABORTED or ARCHIVED.
     */
    public static boolean isTerminalPhase(ExecutionPhase phase) {
        return ExecutionPhase.COMPLETED.equals(phase) || ExecutionPhase.ERROR.equals(phase)
                || ExecutionPhase.ABORTED.equals(phase) || ExecutionPhase.ARCHIVED.equals(phase);
    }

}