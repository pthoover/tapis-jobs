package edu.utexas.tacc.tapis.jobs.launchers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;


/**
 *
 * @author phoover
 */
public class KubernetesLauncher
  extends AbstractJobLauncher
{
    private static final Logger _log = LoggerFactory.getLogger(KubernetesLauncher.class);

    public KubernetesLauncher(JobExecutionContext jobCtx) throws TapisException
    {
        super(jobCtx);
    }

    @Override
    public void launch() throws TapisException
    {
        // Throttling adds a randomized delay on heavily used hosts.
        throttleLaunch();

        String cmd = getLaunchCommand();

        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(),
                                       _job.getUuid(), cmd));

        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();

        // Start the container and retrieve the pid.
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsString();

        // Let's see what happened.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                                         _job.getUuid(), cmd, result, exitStatus);
            throw new JobException(msg);
        }

        // Note success.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(),
                                         _job.getUuid(), result, exitStatus);
            _log.debug(msg);
        }

        // Save the id.
        _jobCtx.getJobsDao().setRemoteJobId(_job, result);
    }
}
