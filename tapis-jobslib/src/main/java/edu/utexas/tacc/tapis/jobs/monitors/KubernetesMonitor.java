package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;


/**
 *
 * @author phoover
 */
public final class KubernetesMonitor
  extends AbstractJobMonitor
{
    protected KubernetesMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {
        super(jobCtx, policy);
    }

    @Override
    public String getExitCode() {
        return null;
    }

    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException
    {
        return null;
    }
}
