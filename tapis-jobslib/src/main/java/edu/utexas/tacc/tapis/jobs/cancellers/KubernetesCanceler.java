package edu.utexas.tacc.tapis.jobs.cancellers;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;


/**
 *
 * @author phoover
 */
public class KubernetesCanceler
  extends AbstractJobCanceler
{
    // nested classes


    /**
     *
     */
    private static class CommandResponse
    {
      public final int exitCode;
      public final String output;


      /**
       *
       * @param code
       * @param out
       */
      public CommandResponse(int code, String out)
      {
        exitCode = code;
        output = out;
      }
    }


    // data fields


    private static final Logger _log = LoggerFactory.getLogger(KubernetesCanceler.class);


    // constructors


    /**
     *
     * @param jobCtx
     */
    public KubernetesCanceler(JobExecutionContext jobCtx)
    {
        super(jobCtx);
    }


    // public methods


    @Override
    public void cancel() throws TapisException
    {
      try {
        String[] podNames = getPodNames();

        for (String pod : podNames)
            deletePod(pod);

        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_KUBERNETES_CANCEL", _job.getUuid()));
      }
      catch (TapisException err) {
          String execSysId = _jobCtx.getExecutionSystem().getId();
          String message = MsgUtils.getMsg("JOBS_KUBERNETES_CANCEL_ERROR", _job.getUuid(), _job.getRemoteJobId(), execSysId);

          _log.error(message, err);
      }
    }


    // private methods


    /**
     *
     * @param command
     * @return
     * @throws TapisException
     */
    private CommandResponse runWrapperCommand(String command) throws TapisException
    {
        String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append("cd ");
        cmdBuilder.append(conditionalQuote(execDir));
        cmdBuilder.append(";./");
        cmdBuilder.append(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        cmdBuilder.append(" ");
        cmdBuilder.append(command);

        TapisRunCommand runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int status = runCmd.execute(cmdBuilder.toString());
        String result  = runCmd.getOutAsString();

        runCmd.logNonZeroExitCode();

        return new CommandResponse(status, result);
    }

    /**
     *
     * @return
     * @throws TapisException
     */
    private String[] getPodNames() throws TapisException
    {
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(" get pods --selector=job-name=");
        cmdBuilder.append(_job.getRemoteJobId());
        cmdBuilder.append(" --output=jsonpath='{.items[*].metadata.name}'");

        CommandResponse response = runWrapperCommand(cmdBuilder.toString());

        if (response.exitCode != 0 || StringUtils.isBlank(response.output))
            return new String[0];

        return response.output.split("\\s");
    }

    /**
     *
     * @param pod
     * @return
     * @throws TapisException
     */
    private CommandResponse deletePod(String pod) throws TapisException
    {
      StringBuilder cmdBuilder = new StringBuilder();

      cmdBuilder.append(" delete pod ");
      cmdBuilder.append(pod);

      return runWrapperCommand(cmdBuilder.toString());
    }
}
