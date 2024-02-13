package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;


/**
 *
 * @author phoover
 */
public class KubernetesCanceler
  extends AbstractJobCanceler
{
    public KubernetesCanceler(JobExecutionContext jobCtx)
    {
        super(jobCtx);
    }

    @Override
    public void cancel() throws JobException, TapisException
    {
    }
}
