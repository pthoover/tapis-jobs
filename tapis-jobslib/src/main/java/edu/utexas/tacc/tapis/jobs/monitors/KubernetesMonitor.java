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
 * Monitors Kubernetes jobs
 *
 * @author phoover
 */
public final class KubernetesMonitor
  extends AbstractJobMonitor
{
    // data fields


    // logging
    private static final Logger _log = LoggerFactory.getLogger(KubernetesMonitor.class);

    private String _exitCode;


    // constructors


    /**
     *
     * @param jobCtx the job execution context
     * @param policy the job monitoring policy
     */
    protected KubernetesMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {
        super(jobCtx, policy);
    }


    // publc methods


    @Override
    public String getExitCode() {
        return _exitCode;
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
        else if (status.equals("Created") || status.equals("Running") || status.equals("Suspended"))
            jobStatus = JobRemoteStatus.ACTIVE;
        else if (status.equals("Complete") || status.equals("Succeeded")) {
            _exitCode = SUCCESS_RC;
            jobStatus = JobRemoteStatus.DONE;
        }
        else if (status.equals("Failed")) {
            List<Integer> codes = getPodExitCodes();
            _exitCode = SUCCESS_RC;

            for (Integer code : codes) {
                if (code != 0) {
                    _exitCode = code.toString();

                    break;
                }
            }

            String msg = MsgUtils.getMsg("JOBS_MONITOR_FAILURE_RESPONSE",
                                         getClass().getSimpleName(), _job.getRemoteJobId(),
                                         status, _exitCode, _job.getUuid());

            _log.warn(msg);

            // Update the finalMessage field in the jobCtx to reflect this status.
            _job.setCondition(SCHEDULER_TERMINATED);

            String finalMessage = MsgUtils.getMsg("JOBS_USER_APP_FAILURE", _job.getRemoteJobId(),
                                                  status, _exitCode);

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
            String resourceType;

            if (_job.isMpi())
                resourceType = "mpijob ";
            else
                resourceType = "job ";

            StringBuilder cmdBuilder = new StringBuilder();

            cmdBuilder.append(" delete ");
            cmdBuilder.append(resourceType);
            cmdBuilder.append(_job.getRemoteJobId());

            runWrapperCommand(cmdBuilder.toString());
        }
        catch (TapisException err) {
            // Exception already logged
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

        JobMonitorCmdResponse response = runWrapperCommand(cmdBuilder.toString());

        if (response.rc != 0 || StringUtils.isBlank(response.result))
            return new String[0];

        // kubectl returns a whitespace-separated list of names
        return response.result.split("\\s");
    }

    /**
     * Gets the status of the job
     *
     * @return the status of the job
     * @throws TapisException
     */
    private String getStatus() throws TapisException
    {
        String resourceType;

        if (_job.isMpi())
            resourceType = "mpijob ";
        else
            resourceType = "job ";

        StringBuilder cmdBuilder = new StringBuilder();

        cmdBuilder.append(" get ");
        cmdBuilder.append(resourceType);
        cmdBuilder.append(_job.getRemoteJobId());
        cmdBuilder.append(" --output=jsonpath='{.status.conditions[?(@.status==\"True\")].type}'");

        JobMonitorCmdResponse response = runWrapperCommand(cmdBuilder.toString());
        String status = "";

        // kubernetes might not assign a status to a job until it's finished, so,
        // in that case, the individual pods of a running job need to be checked
        // to determine current status
        if (response.rc == 0 && !StringUtils.isBlank(response.result)) {
            // the response is a space-delimited list of statuses, in chronological
            // order. The most recent one is returned
            int index = response.result.lastIndexOf(' ');

            if (index >= 0)
                status = response.result.substring(index + 1);
            else
                status = response.result;
        }
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

                    // any status other than Pending means that the pod has
                    // done something, so it must be running
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
     * Gets the exit codes of the pods created by a job
     *
     * @return a list of exit codes
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
