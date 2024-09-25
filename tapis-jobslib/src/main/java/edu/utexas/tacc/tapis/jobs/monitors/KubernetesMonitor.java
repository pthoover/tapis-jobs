package edu.utexas.tacc.tapis.jobs.monitors;

import static edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode.SCHEDULER_TERMINATED;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;


/**
 *
 * @author phoover
 */
public final class KubernetesMonitor
  extends AbstractJobMonitor
{
    // data fields


    private static final Logger _log = LoggerFactory.getLogger(KubernetesMonitor.class);


    // constructors


    /**
     *
     * @param jobCtx
     * @param policy
     */
    protected KubernetesMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {
        super(jobCtx, policy);
    }


    // publc methods


    @Override
    public String getExitCode() {
        return null;
    }

    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException
    {
        // Sanity check--we can't do much without the remote job id.
        if (StringUtils.isBlank(_job.getRemoteJobId())) {
            String msg = MsgUtils.getMsg("JOBS_MISSING_REMOTE_JOB_ID", _job.getUuid());

            throw new JobException(msg);
        }

        String status;

        try {
            status = getStatus();
        }
        catch (TapisException err) {
            // Exception already logged
            return JobRemoteStatus.NULL;
        }

        JobRemoteStatus jobStatus;

        if (status.equals("Pending"))
            jobStatus = JobRemoteStatus.QUEUED;
        else if (status.equals("Running") || status.equals("Suspended"))
            jobStatus = JobRemoteStatus.ACTIVE;
        else if (status.equals("Complete"))
            jobStatus = JobRemoteStatus.DONE;
        else if (status.equals("Failed")) {
            List<Integer> codes = getPodExitCodes();
            int exitCode = 0;

            for (Integer code : codes) {
                if (code != 0) {
                    exitCode = code;

                    break;
                }
            }

            String msg = MsgUtils.getMsg("JOBS_MONITOR_FAILURE_RESPONSE",
                                         getClass().getSimpleName(), _job.getRemoteJobId(),
                                         status, exitCode, _job.getUuid());

            _log.warn(msg);

            // Update the finalMessage field in the jobCtx to reflect this status.
            _job.setCondition(SCHEDULER_TERMINATED);

            String finalMessage = MsgUtils.getMsg("JOBS_USER_APP_FAILURE", _job.getRemoteJobId(),
                                                  status, exitCode);

            _job.getJobCtx().setFinalMessage(finalMessage);

            jobStatus = JobRemoteStatus.FAILED;
        }
        else {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_UNKNOWN_RESPONSE",
                                         getClass().getSimpleName(), _job.getRemoteJobId(),
                                         status, _job.getUuid());

            _log.warn(msg);

            jobStatus = JobRemoteStatus.DONE;
        }

        return jobStatus;
    }


    // protected methods


    @Override
    protected void cleanUpRemoteJob()
    {
        try {
            StringBuilder cmdBuilder = new StringBuilder();

            cmdBuilder.append(" delete job ");
            cmdBuilder.append(_job.getRemoteJobId());

            runWrapperCommand(cmdBuilder.toString());
        }
        catch (TapisException err) {
            // Exception already logged
        }
    }


    // private methods


    /**
     *
     * @param command
     * @return
     * @throws TapisException
     */
    private JobMonitorCmdResponse runWrapperCommand(String command) throws TapisException
    {
        String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append("cd ");
        cmdBuilder.append(conditionalQuote(execDir));
        cmdBuilder.append(";./");
        cmdBuilder.append(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        cmdBuilder.append(" ");
        cmdBuilder.append(command);

        String cmd = cmdBuilder.toString();

        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_MONITOR_COMMAND", _job.getUuid(),
                                       _jobCtx.getExecutionSystem().getHost(),
                                       _jobCtx.getExecutionSystem().getPort(),
                                       cmd));

        TapisRunCommand runCommand = _jobCtx.getExecSystemTapisSSH().getRunCommand();

        return runJobMonitorCmd(runCommand, cmd);
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

        JobMonitorCmdResponse response = runWrapperCommand(cmdBuilder.toString());

        if (response.rc != 0 || StringUtils.isBlank(response.result))
            return new String[0];

        return response.result.split("\\s");
    }

    /**
     *
     * @return
     * @throws TapisException
     */
    private String getStatus() throws TapisException
    {
        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(" get job ");
        cmdBuilder.append(_job.getRemoteJobId());
        cmdBuilder.append(" --output=jsonpath='{.status.conditions[?(@.status==\"True\")].type}'");

        JobMonitorCmdResponse response = runWrapperCommand(cmdBuilder.toString());
        String status = "";

        if (response.rc == 0 && !StringUtils.isBlank(response.result))
            status = response.result;
        else {
            String[] podNames = getPodNames();

            for (String pod : podNames) {
                cmdBuilder = new StringBuilder();

                cmdBuilder.append(" get pod ");
                cmdBuilder.append(pod);
                cmdBuilder.append(" --output=jsonpath='{.status.phase}'");

                response = runWrapperCommand(cmdBuilder.toString());

                if (response.rc == 0 && !StringUtils.isBlank(response.result)) {
                    status = response.result;

                    if (!status.equals("Pending")) {
                        status = "Running";

                        break;
                    }
                }
            }
        }

        return status;
    }

    /**
     *
     * @return
     * @throws TapisException
     */
    private List<Integer> getPodExitCodes() throws TapisException
    {
        List<Integer> result = new ArrayList<Integer>();
        String[] podNames = getPodNames();

        for (String pod : podNames) {
            StringBuilder cmdBuilder = new StringBuilder();

            cmdBuilder.append(" get pod ");
            cmdBuilder.append(pod);
            cmdBuilder.append(" -o jsonpath='{.status.containerStatuses[*].name}'");

            JobMonitorCmdResponse response = runWrapperCommand(cmdBuilder.toString());

            if (response.rc == 0 && !StringUtils.isBlank(response.result)) {
                String[] containerNames = response.result.split("\\s");

                for (String container : containerNames) {
                    cmdBuilder = new StringBuilder();

                    cmdBuilder.append(" get pod ");
                    cmdBuilder.append(pod);
                    cmdBuilder.append(" -o jsonpath='{.status.containerStatuses[?(@.name==\"");
                    cmdBuilder.append(container);
                    cmdBuilder.append("\")].state.terminated.exitCode}'");

                    response = runWrapperCommand(cmdBuilder.toString());

                    if (response.rc == 0 && !StringUtils.isBlank(response.result))
                        result.add(Integer.valueOf(response.result));
                }
            }
        }

        return result;
    }
}
