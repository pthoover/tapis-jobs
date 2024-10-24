package edu.utexas.tacc.tapis.jobs.stagers.kubernetes;

import edu.utexas.tacc.tapis.jobs.schedulers.KubernetesScheduler;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;


/**
 *
 * @author phoover
 */
public class KubernetesStager
  extends AbstractJobExecStager
{
    // constructors


    /**
     *
     * @param jobCtx
     * @param schedulerType
     * @throws TapisException
     */
    public KubernetesStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType) throws TapisException
    {
        super(jobCtx, schedulerType);
    }


    // publc methods


    @Override
    public void stageJob() throws TapisException
    {
        // Create the wrapper script.
        String wrapperScript = generateWrapperScriptContent();

        // Create the Kubernetes manifest file
        String manifest = ((KubernetesScheduler) _jobScheduler).getManifest();

        // Get the ssh connection used by this job
        // to communicate with the execution system.
        var fm = _jobCtx.getJobFileManager();

        // Install the wrapper script on the execution system.
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);

        // Install the manifest file.
        fm.installExecFile(manifest, JobExecutionUtils.JOB_KUBE_MANIFEST_FILE, JobFileManager.RWRW);
    }

    @Override
    public String generateWrapperScriptContent() throws TapisException
    {
        // Initialize the script content in superclass.
        initBashBatchScript();

        // Add zero or more module load commands.
        _cmdBuilder.append(_jobScheduler.getModuleLoadCalls());
        _cmdBuilder.append("kubectl \"$@\"");

        return _cmdBuilder.toString();
    }

    @Override
    public String generateEnvVarFileContent() throws TapisException
    {
        return null;
    }

    @Override
    public JobExecCmd createJobExecCmd() throws TapisException
    {
        return null;
    }
}
