package edu.utexas.tacc.tapis.jobs.monitors;

import static edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode.SCHEDULER_TERMINATED;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private static final Logger _log = LoggerFactory.getLogger(KubernetesMonitor.class);


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
        // Sanity check--we can't do much without the remote job id.
        if (StringUtils.isBlank(_job.getRemoteJobId())) {
            String msg = MsgUtils.getMsg("JOBS_MISSING_REMOTE_JOB_ID", _job.getUuid());

            throw new JobException(msg);
        }

        TapisRunCommand runCommand = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        String command = getBaseCommand() + " status";

        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_MONITOR_COMMAND", _job.getUuid(),
                                       _jobCtx.getExecutionSystem().getHost(),
                                       _jobCtx.getExecutionSystem().getPort(),
                                       command));

        // Execute the query with retry capability.
        JobMonitorCmdResponse response;

        try {
        	response = runJobMonitorCmd(runCommand, command);
        }
        catch (TapisException err) {
            // Exception already logged
            return JobRemoteStatus.NULL;
        }

        // We should have gotten something.
        if (StringUtils.isBlank(response.result))
            return JobRemoteStatus.EMPTY;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;

        try {
            root = mapper.readTree(response.result);
        }
        catch (JsonProcessingException err) {
            throw new JobException(err.getMessage());
        }

        String status = root.at("/status").asText();
        JobRemoteStatus jobStatus;

        if (status.equals("Pending"))
            jobStatus = JobRemoteStatus.QUEUED;
        else if (status.equals("Running") || status.equals("Suspended"))
            jobStatus = JobRemoteStatus.ACTIVE;
        else if (status.equals("Complete"))
            jobStatus = JobRemoteStatus.DONE;
        else if (status.equals("Failed")) {
            List<JsonNode> codes = root.path("pods").findValues("exitCode");
            int exitCode = 0;

            for (JsonNode code : codes) {
                int value = code.asInt();

                if (value != 0) {
                    exitCode = value;

                    break;
                }
            }

            String msg = MsgUtils.getMsg("JOBS_MONITOR_FAILURE_RESPONSE",
                                         getClass().getSimpleName(), _job.getUuid(),
                                         status, exitCode, _job.getUuid());

            _log.warn(msg);

            // Update the finalMessage field in the jobCtx to reflect this status.
            _job.setCondition(SCHEDULER_TERMINATED);

            String finalMessage = MsgUtils.getMsg("JOBS_USER_APP_FAILURE", _job.getUuid(),
                                                  status, exitCode);

            _job.getJobCtx().setFinalMessage(finalMessage);

            jobStatus = JobRemoteStatus.FAILED;
        }
        else {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_UNKNOWN_RESPONSE",
                                         getClass().getSimpleName(), _job.getUuid(),
                                         status, _job.getUuid());

            _log.warn(msg);

            jobStatus = JobRemoteStatus.DONE;
        }

        return jobStatus;
    }

    @Override
    protected void cleanUpRemoteJob()
    {
        try {
            TapisRunCommand runCommand = _jobCtx.getExecSystemTapisSSH().getRunCommand();
            String command = getBaseCommand() + " cleanup";

            runJobMonitorCmd(runCommand, command);
        }
        catch (TapisException err) {
            // Exception already logged
        }
    }

    private String getBaseCommand() throws TapisException
    {
        // Create the command that changes the directory to the execution
        // directory and runs the wrapper script.  The directory is expressed
        // as an absolute path on the system.
        String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);
        return String.format("cd %s;./%s", conditionalQuote(execDir), JobExecutionUtils.JOB_WRAPPER_SCRIPT);
    }
}
