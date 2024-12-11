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
 * Provides the ability to cancel Kubernetes jobs
 *
 * @author phoover
 */
public class KubernetesCanceler
  extends AbstractJobCanceler
{
    // nested classes


    /**
     * Contains the exit code and output returned from a remote command
     */
    private static class CommandResponse
    {
      public final int exitCode;
      public final String output;


      /**
       *
       * @param code the process exit code
       * @param out the process output
       */
      public CommandResponse(int code, String out)
      {
        exitCode = code;
        output = out;
      }
    }


    // data fields


    // logging
    private static final Logger _log = LoggerFactory.getLogger(KubernetesCanceler.class);


    // constructors


    /**
     *
     * @param jobCtx the job execution context
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

          // kubernetes doesn't explicitly provide the means for killing a job. The
          // closest we can get to that behavior is to delete the individual pods
          for (String pod : podNames) {
              writePodLog(pod);
              deletePod(pod);
          }

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
     * Executes the wrapper script for the job. The  script in turn calls
     * kubectl with the given arguments
     *
     * @param command arguments for kubectl
     * @return the response produced by running the script
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
     * Gets a list of pod names for the job
     *
     * @return a list of pod names
     * @throws TapisException
     */
    private String[] getPodNames() throws TapisException
    {
        String selector;

        if (_job.isMpi())
            selector = "training.kubeflow.org/job-name";
        else
            selector = "job-name";

        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(" get pods --selector=");
        cmdBuilder.append(selector);
        cmdBuilder.append("=");
        cmdBuilder.append(_job.getRemoteJobId());
        cmdBuilder.append(" --output=jsonpath='{.items[*].metadata.name}'");

        CommandResponse response = runWrapperCommand(cmdBuilder.toString());

        if (response.exitCode != 0 || StringUtils.isBlank(response.output))
            return new String[0];

        // kubectl returns a whitespace-separated list of names
        return response.output.split("\\s");
    }

    /**
     * Writes a log file for a pod
     *
     * @param pod name of the pod
     * @return
     * @throws TapisException
     */
    private CommandResponse writePodLog(String pod) throws TapisException
    {
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(" logs ");
        cmdBuilder.append(pod);
        cmdBuilder.append(" --all-containers=true > output/");
        cmdBuilder.append(pod);
        cmdBuilder.append(".log");

        return runWrapperCommand(cmdBuilder.toString());
    }

    /**
     * Deletes a pod
     *
     * @param pod name of the pod
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
