package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

/** All supported launchers are instantiated using this class. */
public class JobLauncherFactory 
{
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a launcher based on the type of job and its execution environment.
     * This method either returns the appropriate launcher or throws an exception.
     * 
     * @param jobCtx job context
     * @return the launcher designated for the current job type and environment
     * @throws TapisException when no launcher is found or a network error occurs
     */
    public static JobLauncher getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Extract required information from app and job.
        var app = jobCtx.getApp();
        var runtime = app.getRuntime();
        var jobType = jobCtx.getJob().getJobType();
        
        // The result.
        JobLauncher launcher = null;
        
        // ------------------------- FORK -------------------------
        if (jobType == JobType.FORK) {
            launcher = switch (runtime) {
                case DOCKER      -> new DockerNativeLauncher(jobCtx);
                case SINGULARITY -> getSingularityOption(jobCtx, app);
                case ZIP         -> new ZipNativeLauncher(jobCtx);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobLauncherFactory");
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
            
            // Get the laucher for each supported runtime/scheduler combination.
            launcher = switch (runtime) {
                case DOCKER      -> getBatchDockerLauncher(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityLauncher(jobCtx, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME",
                                                 scheduler + "(" + runtime +")", 
                                                 "JobLauncherFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", jobType, "JobLauncherFactory");
            throw new JobException(msg);
        }

        // Must be non-null.
        return launcher;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSingularityOption:                                                  */
    /* ---------------------------------------------------------------------- */
    private static JobLauncher getSingularityOption(JobExecutionContext jobCtx,
                                                    TapisApp app)
     throws TapisException
    {
        // We are only interested in the singularity options.  These have
        // been validated in JobExecStageFactory, so no need to repeat here.
        var opts = app.getRuntimeOptions();
        boolean start = opts.contains(RuntimeOptionEnum.SINGULARITY_START);
        
        // Create the specified monitor.
        if (start) return new SingularityStartLauncher(jobCtx);
          else return new SingularityRunLauncher(jobCtx);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDockerLauncher:                                                */
    /* ---------------------------------------------------------------------- */
    private static JobLauncher getBatchDockerLauncher(JobExecutionContext jobCtx,
                                                      SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker launcher. 
        JobLauncher launcher = switch (scheduler) {
            case SLURM -> null;  // at least 1 case is required.
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(DOCKER)", 
                                             "JobLauncherFactory");
                throw new JobException(msg);
            }
        };
        
        // Make sure we always return a non-null launcher.
        if (launcher == null) {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                          scheduler + "(DOCKER)", 
                                         "JobLauncherFactory");
            throw new JobException(msg);
        }
        
        return launcher;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityLauncher:                                           */
    /* ---------------------------------------------------------------------- */
    private static JobLauncher getBatchSingularityLauncher(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker launcher. 
        JobLauncher launcher = switch (scheduler) {
            case SLURM -> new SingularityRunSlurmLauncher(jobCtx);
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(SINGULARITY)", 
                                             "JobLauncherFactory");
                throw new JobException(msg);
            }
        };
        
        return launcher;
    }
}
