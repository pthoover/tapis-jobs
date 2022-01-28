package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisQuotaException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public final class QuotaChecker 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(QuotaChecker.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public QuotaChecker(JobExecutionContext ctx)
    {
        _jobCtx = ctx;
        _job = ctx.getJob();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* checkQuotas:                                                           */
    /* ---------------------------------------------------------------------- */
    /** This method enforces four quotas.
     * 
     *    - number of tapis jobs submitted to an execution system
     *    - number of tapis jobs submitted by a particular user on an execution system
     *    - number of tapis jobs submitted to a batchqueue
     *    - number of tapis jobs submitted by a particular user to a batchqueue
     *  
     * @param jobCtx the current job context
     * @throws TapisException
     */
    public void checkQuotas() 
      throws TapisException
    {
        // number of tapis jobs submitted to an execution system
        checkMaxSystemJobs();

        // number of tapis jobs submitted by a particular user on an execution system
        checkMaxSystemUserJobs();

        // number of tapis jobs submitted to a batchqueue
        checkMaxSystemQueueJobs();

        // number of tapis jobs submitted by a particular user to a batchqueue
        checkMaxSystemUserQueueJobs();
    }
    
    /* ********************************************************************** */
    /*                           Private Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* checkMaxSystemJobs:                                                    */
    /* ---------------------------------------------------------------------- */
    private void checkMaxSystemJobs() throws TapisException
    {
        // Get the effective maximum number of jobs for this app on this system.
        int maxJobs = getMaxJobs();
        
        // Enforce the quota.
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        int curJobs = 
            _jobCtx.getJobsDao().countActiveSystemJobs(execSys.getTenant(), execSys.getId());
        
        // Test.
        if (curJobs >= maxJobs) 
        {
            // Recoverable error.
            String msg = MsgUtils.getMsg("JOBS_QUOTA_MAX_JOBS", _job.getUuid(),
                                         execSys.getTenant(), execSys.getId(),
                                         maxJobs);
            _log.warn(msg);
            throw new TapisQuotaException(msg, 
                RecoveryUtils.captureQuotaState(execSys, _job, _jobCtx.getLogicalQueue(),
                                                maxJobs, getMaxJobsPerUser()));        
        }
        
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_CURRENT_SYSTEM_JOBS", _job.getUuid(),
                       execSys.getTenant(), execSys.getId(), curJobs));
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkMaxSystemUserJobs:                                                */
    /* ---------------------------------------------------------------------- */
    private void checkMaxSystemUserJobs() throws TapisException
    {
        // Get the effective maximum number of jobs per user for this app on this system.
        int maxJobsPerUser = getMaxJobsPerUser();
        
        // Enforce the quota.
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        int curJobsForUser = 
            _jobCtx.getJobsDao().countActiveSystemUserJobs(execSys.getTenant(), 
                                                           execSys.getId(),
                                                           _job.getOwner());
        
        // Test.
        if (curJobsForUser >= maxJobsPerUser) 
        {
            // Recoverable error.
            String msg = MsgUtils.getMsg("JOBS_QUOTA_MAX_USER_JOBS", _job.getUuid(),
                                         execSys.getTenant(), execSys.getId(),
                                         maxJobsPerUser, _job.getOwner());
            _log.warn(msg);
            throw new TapisQuotaException(msg, 
                RecoveryUtils.captureQuotaState(execSys, _job, _jobCtx.getLogicalQueue(),
                                                getMaxJobs(), maxJobsPerUser));        
        }
        
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_CURRENT_SYSTEM_USER_JOBS", _job.getUuid(),
                       execSys.getTenant(), execSys.getId(), curJobsForUser, 
                       _job.getOwner()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkMaxSystemQueueJobs:                                               */
    /* ---------------------------------------------------------------------- */
    private void checkMaxSystemQueueJobs() throws TapisException
    {
        // Does the exec system queue have a hard limit on the number of jobs?
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        LogicalQueue logicalQueue = _jobCtx.getLogicalQueue();
        
        // Make sure we have a queue.  If not, skip this quota check.
        if (logicalQueue == null) return;
        
        // Is there a queue limit on the number of jobs?
        long maxQueueJobs = logicalQueue.getMaxJobs();
        if (maxQueueJobs <= 0) return;
        
        // Enforce the quota.
        int curQueueJobs = 
            _jobCtx.getJobsDao().countActiveSystemQueueJobs(execSys.getTenant(), execSys.getId(),
                                                            logicalQueue.getName());
        
        // Test.
        if (curQueueJobs >= maxQueueJobs) 
        {
            // Recoverable error.
            String msg = MsgUtils.getMsg("JOBS_QUOTA_MAX_QUEUE_JOBS", _job.getUuid(),
                                         execSys.getTenant(), execSys.getId(),
                                         maxQueueJobs, logicalQueue.getName());
            _log.warn(msg);
            throw new TapisQuotaException(msg, 
                RecoveryUtils.captureQuotaState(execSys, _job, logicalQueue,
                                                getMaxJobs(), getMaxJobsPerUser()));        
        }
        
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_CURRENT_SYSTEM_QUEUE_JOBS", _job.getUuid(),
                       execSys.getTenant(), execSys.getId(), curQueueJobs, 
                       logicalQueue.getName()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkMaxSystemUserQueueJobs:                                           */
    /* ---------------------------------------------------------------------- */
    private void checkMaxSystemUserQueueJobs() throws TapisException
    {
        // Does the exec system queue have a hard limit on the number of jobs?
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        LogicalQueue logicalQueue = _jobCtx.getLogicalQueue();
        
        // Make sure we have a queue.  If not, skip this quota check.
        if (logicalQueue == null) return;
        
        // Is there a queue limit on the number of jobs?
        long maxUserQueueJobs = logicalQueue.getMaxJobsPerUser();
        if (maxUserQueueJobs <= 0) return;
        
        // Enforce the quota.
        int curUserQueueJobs = 
            _jobCtx.getJobsDao().countActiveSystemUserQueueJobs(execSys.getTenant(), 
                                                                execSys.getId(),
                                                                _job.getOwner(), 
                                                                logicalQueue.getName());
        
        // Test.
        if (curUserQueueJobs >= maxUserQueueJobs) 
        {
            // Recoverable error.
            String msg = MsgUtils.getMsg("JOBS_QUOTA_MAX_USER_QUEUE_JOBS", _job.getUuid(),
                                         execSys.getTenant(), execSys.getId(),
                                         maxUserQueueJobs, _job.getOwner(), logicalQueue.getName());
            _log.warn(msg);
            throw new TapisQuotaException(msg, 
                RecoveryUtils.captureQuotaState(execSys, _job, logicalQueue,
                                                getMaxJobs(), getMaxJobsPerUser()));
        }
        
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_CURRENT_SYSTEM_USER_QUEUE_JOBS", _job.getUuid(),
                       execSys.getTenant(), execSys.getId(), curUserQueueJobs, 
                       _job.getOwner(), logicalQueue.getName()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* getMaxJobs:                                                            */
    /* ---------------------------------------------------------------------- */
    private int getMaxJobs() throws TapisException
    {
        // Does the app hard limit on the number of jobs?
        TapisApp app = _jobCtx.getApp();
        Integer maxAppJobs = app.getMaxJobs();
        if (maxAppJobs == null || maxAppJobs <= 0) maxAppJobs = Integer.MAX_VALUE;
        
        // Does the exec system have a hard limit on the number of jobs?
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        Integer maxSystemJobs = execSys.getJobMaxJobs();
        if (maxSystemJobs == null || maxSystemJobs <= 0) maxSystemJobs = Integer.MAX_VALUE;
        
        // Get the effective maximum number of jobs for this app on this system.
        return Math.min(maxAppJobs, maxSystemJobs);
    }

    /* ---------------------------------------------------------------------- */
    /* getMaxJobsPerUser:                                                     */
    /* ---------------------------------------------------------------------- */
    private int getMaxJobsPerUser() throws TapisException 
    {
        // Does the app hard limit on the number of jobs per user?
        TapisApp app = _jobCtx.getApp();
        Integer maxAppJobsPerUser = app.getMaxJobsPerUser();
        if (maxAppJobsPerUser == null || maxAppJobsPerUser <= 0) 
            maxAppJobsPerUser = Integer.MAX_VALUE;
        
        // Does the exec system have a hard limit on the number of jobs per user?
        TapisSystem execSys = _jobCtx.getExecutionSystem();
        Integer maxSystemJobsPerUser = execSys.getJobMaxJobsPerUser();
        if (maxSystemJobsPerUser == null || maxSystemJobsPerUser <= 0) 
            maxSystemJobsPerUser = Integer.MAX_VALUE;
        
        // Get the effective maximum number of jobs per user for this app on this system.
        return Math.min(maxAppJobsPerUser, maxSystemJobsPerUser);
    }
}
