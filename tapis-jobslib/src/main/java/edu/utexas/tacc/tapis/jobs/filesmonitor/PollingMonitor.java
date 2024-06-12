package edu.utexas.tacc.tapis.jobs.filesmonitor;

import java.sql.Connection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferStatusEnum;
import edu.utexas.tacc.tapis.jobs.events.JobEventManager;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class PollingMonitor
 implements TransferMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(PollingMonitor.class);
    
    // Maximum number of minutes for failures to connect to Files before quitting.
    private static final long MAX_FAILURE_MINUTES = 5;
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // Reasons to quit polling.
    private enum ReasonCode {TOO_MANY_FAILURES, TOO_MANY_ATTEMPTS}

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Initialize the stepwise increasing sleep intervals.
    private final List<Pair<Integer,Long>> _steps = getDefaultSteps();
    private Pair<Integer,Long>             _curStep = _steps.get(0);
    private int                            _curStepIndex;
    private int                            _curStepTryCount;
    private Instant                        _firstFailureInSeries;

    // The reason why the last call to millisToWait returned null.
    private ReasonCode                     _reasonCode;

    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* monitorByCorrelationId:                                                */
    /* ---------------------------------------------------------------------- */
     /** The monitoring command that blocks the calling thread until the transfer 
     * completes successfully or fails with an exception.
     * 
     * @param job the job that initiated the transfer
     * @param transferId uuid assigned to this task by Files
     * @param corrId the correlation id (or tag) associated with the transfer
     * @param postEvent post an event when a terminal state is reached
     * @throws TapisException when the transfer does not complete successfully
     */
    @Override
    public void monitorTransfer(Job job, String transferId, String corrId, boolean postEvent)
     throws TapisException 
    {
        // Get the client from the context.
        var jobCtx = job.getJobCtx(); 

        // Poll the Files service until the transfer completes or fails.
        boolean lastAttemptFailed = false; // no failed monitoring attempts yet!
        while (true) {
            // *** Async command check ***
            try {jobCtx.checkCmdMsg();}
            catch (JobAsyncCmdException e) {
                // Cancel the transfer before passing the exception up.
                jobCtx.getJobFileManager().cancelTransfer(transferId);
                throw e;
            }

            // ----------------------- Poll Files -----------------------
            // Get the transfer information.  We query the client cache for
            // a client with a fresh JWT, which is necessary on long running
            // transfers that outlive the JWT.  Recoverable and unrecoverable
            // exceptions can be thrown here.
            FilesClient filesClient = jobCtx.getServiceClient(FilesClient.class);
            TransferTask task = getTransferTask(job, transferId, filesClient);
            
            // Check result integrity.
            if (task == null || task.getStatus() == null) 
            {
                String msg = MsgUtils.getMsg("JOBS_INVALID_TRANSFER_RESULT", job.getUuid(), transferId, corrId);
                throw new JobException(msg);
            }
            TransferStatusEnum status = task.getStatus();
            
            // Successful termination, we don't need to poll anymore.
            if (status == TransferStatusEnum.COMPLETED) {
                _log.debug(MsgUtils.getMsg("JOBS_TRANSFER_COMPLETE", job.getUuid(), transferId, corrId));
                postEvent(job, status, transferId);
                break;
            }
            
            // Unsuccessful termination.
            if (status == TransferStatusEnum.FAILED || status == TransferStatusEnum.CANCELLED) {
            	job.setCondition(JobConditionCode.JOB_TRANSFER_FAILED_OR_CANCELLED);
                postEvent(job, status, transferId);
                String msg = MsgUtils.getMsg("JOBS_TRANSFER_INCOMPLETE", job.getUuid(), transferId, corrId, 
                                             status, task.getErrorMessage());
                throw new JobException(msg);
            }
            
            // ----------------------- Sleep ----------------------------
            // Sleep for the prescribed amount of time.
            Long waitMillis = millisToWait(lastAttemptFailed);
            if (waitMillis == null) {
            	job.setCondition(JobConditionCode.JOB_TRANSFER_MONITORING_TIMEOUT);
                String msg = MsgUtils.getMsg("JOBS_TRANSFER_POLLING_ERROR", job.getUuid(), transferId, corrId, 
                                             _reasonCode.name());
                throw new JobException(msg);
            }

            // Wait the policy-determined number of milliseconds; exceptions are logged.
            if (_log.isDebugEnabled())
                _log.debug(MsgUtils.getMsg("JOBS_TRANSFER_WAIT", job.getUuid(), transferId, corrId, status, waitMillis));
            try {Thread.sleep(waitMillis);} 
                catch (InterruptedException e) {
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_INTERRUPTED", job.getUuid(), 
                                                     getClass().getSimpleName());
                    _log.debug(msg, e);
                    throw new JobException(msg, e);
                }
        }
    }

    /* ---------------------------------------------------------------------- */
    /* isAvailable:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Fall back to polling the Files service to detect when a transfer is complete.
     * @return true if this implementation can be used, false otherwise
     */
    @Override
    public boolean isAvailable() {return true;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getTransferTask:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Retrieve the current status of a transfer task.
     * 
     * @param job the job who issued the task
     * @param transferId the task id
     * @param filesClient the Files client
     * @return the transfer task or null
     * @throws TapisImplException unrecoverable error
     * @throws TapisServiceConnectionException recoverable error
     */
    private TransferTask getTransferTask(Job job, String transferId, 
                                         FilesClient filesClient) 
     throws TapisImplException, TapisServiceConnectionException
    {
        TransferTask task = null;
        try {task = filesClient.getTransferTask(transferId);}
            catch (Exception e) {
                
                // Look for a recoverable error in the exception chain. Recoverable
                // exceptions are those that might indicate a transient network
                // or server error, typically involving loss of connectivity.
                Throwable transferException = 
                    TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
                if (transferException != null) {
                    throw new TapisServiceConnectionException(transferException.getMessage(), 
                                e, RecoveryUtils.captureServiceConnectionState(
                                   filesClient.getBasePath(), TapisConstants.FILES_SERVICE));
                }
                
                // Unrecoverable error.
                if (e instanceof TapisClientException) {
                    TapisClientException e1 = (TapisClientException) e;
                    String msg = MsgUtils.getMsg("JOBS_GET_TRANSFER_ERROR", job.getUuid(),
                                                 transferId, e1.getCode(), e1.getMessage());
                    throw new TapisImplException(msg, e1, e1.getCode());
                } else {
                    String msg = MsgUtils.getMsg("JOBS_GET_TRANSFER_ERROR", job.getUuid(),
                                                 transferId, 0, e.getMessage());
                    throw new TapisImplException(msg, e, 0);
                 }
            }
        
        // No exceptions.
        return task;
    }
    
    /* ---------------------------------------------------------------------- */
    /* millisToWait:                                                          */
    /* ---------------------------------------------------------------------- */
    private Long millisToWait(boolean lastAttemptFailed) 
    {
        // Determine if we've had too many failed attempts.
        Instant now = Instant.now();
        if (tooManyFailures(lastAttemptFailed, now)) {
            _reasonCode = ReasonCode.TOO_MANY_FAILURES;
            return null;
        }
        
        // Get the maximum tries for this step and
        // handle the infinite try case.
        int maxTries = _curStep.getLeft();
        if (maxTries <= 0) return _curStep.getRight();
        
        // Have we consumed this step?  We won't run out of
        // steps if the last step has infinite retries.
        if (_curStepTryCount >= maxTries) {
            // We may have processed all our steps.
            if (++_curStepIndex >= _steps.size()) {
                _reasonCode = ReasonCode.TOO_MANY_ATTEMPTS;
                return null;
            }
            
            // Move to the next step.
            _curStep = _steps.get(_curStepIndex);
            _curStepTryCount = 0;
        }
        
        // Consume the next try on the current step.
        _curStepTryCount++;
        return _curStep.getRight();
    }

    /* ---------------------------------------------------------------------- */
    /* tooManyFailures:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Determine if we've had a run of failed monitoring attempts that 
     * exceed the configured allowed maximum.
     * 
     * @param lastAttemptFailed true if the last attempt failed
     * @param now the current time
     * @return true if we've experienced failures for too long, false otherwise
     */
    private boolean tooManyFailures(boolean lastAttemptFailed, Instant now)
    {
        // Maybe there's nothing to worry about.
        if (!lastAttemptFailed) {
            _firstFailureInSeries = null;
            return false;
        }
        
        // Is this failure the beginning of a new failure series?
        if (_firstFailureInSeries == null) _firstFailureInSeries = now;
           
        // Determine if the duration of a series of consecutive
        // failures has exceeded its time limit.
        if (_firstFailureInSeries.plus(MAX_FAILURE_MINUTES, ChronoUnit.MINUTES).isAfter(now))
            return true;
        
        // We're still ok.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDefaultSteps:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Default step configuration that should work for most uses.  The first
     * element is the number of tries, the second element is the sleeptime in
     * milliseconds.  The last step must have -1 assigned to its first element
     * to signify an infinite number of retries.
     * 
     * @return the step list
     */
    private List<Pair<Integer,Long>> getDefaultSteps()
    {
        // Each step specifies a number of tries with the given delay in milliseconds. 
        ArrayList<Pair<Integer,Long>> steps = new ArrayList<>();
        steps.add(Pair.of(3,   5000L));   // 5 seconds
        steps.add(Pair.of(10,  10000L));  // 10 seconds    
        steps.add(Pair.of(100, 30000L));  // 30 seconds
        steps.add(Pair.of(-1,  60000L));  // 1 minute (infinite tries) 
        
        return steps;
    }
    
    /* ---------------------------------------------------------------------- */
    /* postEvent:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Create a job event and write it to the database and the notifications
     * service.  Only terminal transfer statuses for staging input and archiving
     * outputs are posted.  
     * 
     * This method operates on a best effort basis (no exceptions thrown).  
     * 
     * @param job the waiting on the Files service transaction
     * @param transferStatus a terminal transaction status
     * @param transferId the transaction id
     */
    private void postEvent(Job job, TransferStatusEnum transferStatus, String transferId)
    {
        var eventMgr = JobEventManager.getInstance();
        try {
            if (job.getStatus() == JobStatusType.STAGING_INPUTS)
               eventMgr.recordStagingInputsEvent(job, transferStatus, transferId);
            else if (job.getStatus() == JobStatusType.ARCHIVING)
                eventMgr.recordArchivingEvent(job, transferStatus, transferId);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", job.getUuid(), 
                                         job.getOwner(), job.getTenant(), e.getMessage());
            _log.error(msg, e);
        }
    }
}
