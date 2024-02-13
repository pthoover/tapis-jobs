package edu.utexas.tacc.tapis.jobs.stagers.docker;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;


/**
 *
 * @author phoover
 */
public class DockerKubernetesCmd
  implements JobExecCmd
{
    // public methods


    @Override
    public String generateExecCmd(Job job)
    {
        StringBuilder builder = new StringBuilder();

        builder.append("kubectl apply -f ");
        builder.append(JobExecutionUtils.JOB_KUBE_MANIFEST_FILE);

        return builder.toString();
    }

    @Override
    public String generateEnvVarFileContent()
    {
        throw new TapisRuntimeException("Unimplemented method");
    }
}
