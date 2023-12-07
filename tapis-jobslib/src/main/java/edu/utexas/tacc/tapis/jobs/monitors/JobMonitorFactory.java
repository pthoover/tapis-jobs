package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicyParameters;
import edu.utexas.tacc.tapis.jobs.monitors.policies.StepwiseBackoffPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

/** All supported monitors are instantiated using this class. */
public class JobMonitorFactory 
{
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a monitor based on the type of job and its execution environment
     * using the default monitor policy.  This method either returns the 
     * appropriate monitor or throws an exception.
     * 
     * @param jobCtx job context
     * @return the monitor designated for the current job type and environment
     * @throws TapisException when no monitor is found or a network error occurs
     */
    public static JobMonitor getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Use the default policy with the default parameters to create a monitor.
        var parms  = new MonitorPolicyParameters();
        parms.setDefaultMaxElapsedSecond(jobCtx.getJob());
        var policy = new StepwiseBackoffPolicy(jobCtx.getJob(), parms);
        return getInstance(jobCtx, policy);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a monitor based on the type of job and its execution environment.
     * This method either returns the appropriate monitor or throws an exception.
     * 
     * @param jobCtx job context
     * @return the monitor designated for the current job type and environment
     * @throws TapisException when no monitor is found or a network error occurs
     */
    public static JobMonitor getInstance(JobExecutionContext jobCtx, MonitorPolicy policy) 
     throws TapisException 
    {
        // Extract required information from app.
        var app = jobCtx.getApp();
        var runtime = app.getRuntime();
        var jobType = jobCtx.getJob().getJobType();
        
        // The result.
        JobMonitor monitor = null;
        
        // ------------------------- FORK -------------------------
        if (jobType == JobType.FORK) {
            monitor = switch (runtime) {
                case DOCKER      -> new DockerNativeMonitor(jobCtx, policy);
                case SINGULARITY -> getSingularityOption(jobCtx, policy, app);
                case ZIP         -> new ZipNativeMonitor(jobCtx, policy);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobMonitorFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (jobType == JobType.BATCH) {
            // Get the scheduler under which containers will be launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Doublecheck that a scheduler is assigned.
            if (scheduler == null) {
                String msg = MsgUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), 
                                              jobCtx.getJob().getUuid());
                throw new JobException(msg);
            }
            
            // Get the monitor for each supported runtime/scheduler combination.
            monitor = switch (runtime) {
                case DOCKER      -> getBatchDockerMonitor(jobCtx, policy, scheduler);
                case SINGULARITY -> getBatchSingularityMonitor(jobCtx, policy, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobMonitorFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", jobType, "JobMonitorFactory");
            throw new JobException(msg);
        }
        
        return monitor;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSingularityOption:                                                  */
    /* ---------------------------------------------------------------------- */
    private static JobMonitor getSingularityOption(JobExecutionContext jobCtx,
                                                   MonitorPolicy policy,
                                                   TapisApp app)
     throws JobException
    {
        // We are only interested in the singularity options.  These have
        // been validated in JobExecStageFactory, so no need to repeat here.
        var opts = app.getRuntimeOptions();
        boolean start = opts.contains(RuntimeOptionEnum.SINGULARITY_START);
        
        // Create the specified monitor.
        if (start) return new SingularityStartMonitor(jobCtx, policy);
          else return new SingularityRunMonitor(jobCtx, policy);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDockerMonitor:                                                 */
    /* ---------------------------------------------------------------------- */
    private static JobMonitor getBatchDockerMonitor(JobExecutionContext jobCtx,
                                                    MonitorPolicy policy,
                                                    SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker monitor. 
        JobMonitor monitor = switch (scheduler) {
            case SLURM -> null; // not implemented
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler.name(), "JobMonitorFactory");
                throw new JobException(msg);
            }
        };
        
        return monitor;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityMonitor:                                            */
    /* ---------------------------------------------------------------------- */
    private static JobMonitor getBatchSingularityMonitor(JobExecutionContext jobCtx,
                                                         MonitorPolicy policy,
                                                         SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker monitor. 
        JobMonitor monitor = switch (scheduler) {
            case SLURM -> new SlurmMonitor(jobCtx, policy);
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler.name(), "JobMonitorFactory");
                throw new JobException(msg);
            }
        };
        
        return monitor;
    }
}
