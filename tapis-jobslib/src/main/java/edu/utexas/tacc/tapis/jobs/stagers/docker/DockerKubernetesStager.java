package edu.utexas.tacc.tapis.jobs.stagers.docker;

import edu.utexas.tacc.tapis.jobs.schedulers.KubernetesScheduler;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;


/**
 *
 * @author phoover
 */
public class DockerKubernetesStager
  extends AbstractJobExecStager
{
    // data fields


    private final DockerKubernetesCmd _kubeCmd;


    // constructors


    /**
     *
     * @param jobCtx
     * @param schedulerType
     * @throws TapisException
     */
    public DockerKubernetesStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType) throws TapisException
    {
        super(jobCtx, schedulerType);

        _kubeCmd = new DockerKubernetesCmd();
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

        // Add the actual Kubernetes command.
        _cmdBuilder.append(_kubeCmd.generateExecCmd(_job));
        _cmdBuilder.append(" > /dev/null\n");

        // Add logic to determine the job identity
        _cmdBuilder.append("active=$(kubectl get job ");
        _cmdBuilder.append(_job.getUuid());
        _cmdBuilder.append(" --output=jsonpath='{.status.active}')");
        _cmdBuilder.append("if [ -z \"$active\" ]; then\n");
        _cmdBuilder.append("    exit 1\n");
        _cmdBuilder.append("fi\n");
        _cmdBuilder.append("echo \"");
        _cmdBuilder.append(_job.getUuid());
        _cmdBuilder.append("\"\n");

        return _cmdBuilder.toString();
    }

    @Override
    public String generateEnvVarFileContent() throws TapisException
    {
        throw new TapisRuntimeException("Unimplemented method");
    }

    @Override
    public JobExecCmd createJobExecCmd() throws TapisException
    {
        return new DockerKubernetesCmd();
    }
}
