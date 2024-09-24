package edu.utexas.tacc.tapis.jobs.launchers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;


/**
 *
 * @author phoover
 */
public class KubernetesLauncher
  extends AbstractJobLauncher
{
    // data fields


    private static final Logger _log = LoggerFactory.getLogger(KubernetesLauncher.class);


    // constructors


    /**
     *
     * @param jobCtx
     * @throws TapisException
     */
    public KubernetesLauncher(JobExecutionContext jobCtx) throws TapisException
    {
        super(jobCtx);
    }


    // publc methods


    @Override
    public void launch() throws TapisException
    {
        // Throttling adds a randomized delay on heavily used hosts.
        throttleLaunch();

        String launchCmd = getLaunchCommand();

        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(),
                                       _job.getUuid(), launchCmd));

        // Get the command object.
        TapisRunCommand runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();

        // Start the container and retrieve the pid.
        int exitStatus = runCmd.execute(launchCmd);
        String launchResult  = runCmd.getOutAsString();

        // Let's see what happened.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                                         _job.getUuid(), launchCmd, launchResult, exitStatus);
            throw new JobException(msg);
        }

        String statusCmd = getStatusCommand();

        exitStatus = runCmd.execute(statusCmd);

        String statusResult = runCmd.getOutAsString();

        if (exitStatus != 0 || statusResult == null || statusResult.isEmpty()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                                         _job.getUuid(), launchCmd, launchResult, exitStatus);
            throw new JobException(msg);
        }

        // Note success.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(),
                                         _job.getUuid(), launchResult, exitStatus);
            _log.debug(msg);
        }

        // Save the id.
        _jobCtx.getJobsDao().setRemoteJobId(_job, _job.getUuid());
    }


    // protected methods


    @Override
    protected String getLaunchCommand() throws TapisException
    {
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(super.getLaunchCommand());
        cmdBuilder.append(" apply -f ");
        cmdBuilder.append(JobExecutionUtils.JOB_KUBE_MANIFEST_FILE);

        return cmdBuilder.toString();
    }

    /**
     *
     * @return
     * @throws TapisException
     */
    protected String getStatusCommand() throws TapisException
    {
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(super.getLaunchCommand());
        cmdBuilder.append(" get job ");
        cmdBuilder.append(_job.getUuid());
        cmdBuilder.append(" --output=jsonpath='{.status.active}'");

        return cmdBuilder.toString();
    }
}
