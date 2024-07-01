package edu.utexas.tacc.tapis.jobs.schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.JobArgSpec;
import edu.utexas.tacc.tapis.jobs.model.submit.JobParameterSet;
import edu.utexas.tacc.tapis.jobs.stagers.docker.DockerKubernetesCmd;
import edu.utexas.tacc.tapis.jobs.utils.YamlDocument;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;


/**
 *
 * @author phoover
 */
public class KubernetesScheduler
  implements JobScheduler
{
    // data fields


    private static final Logger _log = LoggerFactory.getLogger(KubernetesScheduler.class);
    private static final Pattern _paramPattern = Pattern.compile("\\s*([^=\\s]+)\\s*=\\s*(\\S*)\\s*");
    private static final String _resourceFile = "edu/utexas/tacc/tapis/jobs/" + JobExecutionUtils.JOB_KUBE_MANIFEST_FILE;
    private static final List<Pattern> _skipList = new ArrayList<Pattern>();
    private final JobExecutionContext _jobCtx;
    private final KubernetesOptions _kubeOptions;


    static {
        try {
            _skipList.add(Pattern.compile("apiVersion"));
            _skipList.add(Pattern.compile("kind"));
            _skipList.add(Pattern.compile("spec\\.schedule"));
        }
        catch (Exception err) {
            _log.error(err.getMessage(), err);
        }
    }


    // constructors


    /**
     *
     * @param jobCtx
     * @throws TapisException
     */
    public KubernetesScheduler(JobExecutionContext jobCtx) throws TapisException
    {
        _jobCtx = jobCtx;
        _kubeOptions = new KubernetesOptions(jobCtx);
    }


    // public methods


    @Override
    public String getBatchDirectives()
    {
        throw new TapisRuntimeException("Unimplemented method");
    }

    @Override
    public String getModuleLoadCalls() throws JobException
    {
        // There's nothing to do unless a Tapis profile was specified.
        if (StringUtils.isBlank(_kubeOptions.getTapisProfile())) return "";

        final int capacity = 1024;
        var buf = new StringBuilder(capacity);

        // Make sure we retrieve the profile.
        var profile = _jobCtx.getSchedulerProfile(_kubeOptions.getTapisProfile());

        // Get the array of module load specs.
        var specs = profile.getModuleLoads();
        if (specs == null || specs.isEmpty()) return "";

        // Iterate through the list of specs.
        for (var spec : specs) {
            // There has to be a load command.
            var loadCmd = spec.getModuleLoadCommand();
            if (StringUtils.isBlank(loadCmd)) continue;

            // We allow commands that don't require module parameters.
            var modules = spec.getModulesToLoad();
            if (modules == null || modules.isEmpty()) {
                buf.append(loadCmd).append("\n");
                continue;
            }

            // Put in the required spacing.
            if (!loadCmd.endsWith(" ")) loadCmd += " ";

            // Create a module load command for each specified module.
            for (var module : modules)
                if (StringUtils.isNotBlank(module))
                    buf.append(loadCmd).append(module).append("\n");
        }

        // End with a blank line.
        buf.append("\n");
        return buf.toString();
    }

    @Override
    public String getBatchJobIdFromOutput(String output, String cmd) throws JobException
    {
        throw new TapisRuntimeException("Unimplemented method");
    }

    /**
     *
     * @return
     * @throws JobException
     */
    public String getManifest() throws JobException
    {
        YamlDocument manifest;

        try (InputStream inStream = DockerKubernetesCmd.class.getClassLoader().getResourceAsStream(_resourceFile)) {
            manifest = new YamlDocument(inStream);
        }
        catch (IOException err) {
            throw new JobException(err.getMessage());
        }

        manifest.setValue("metadata.name", _jobCtx.getJob().getUuid());
        manifest.setValue("spec.template.spec.containers.name", _kubeOptions.getContainerName());
        manifest.setValue("spec.template.spec.containers.image", _kubeOptions.getImage());
        manifest.setValue("spec.template.spec.containers.resources.limits.cpu", _kubeOptions.getCpu());
        manifest.setValue("spec.template.spec.containers.resources.limits.memory", _kubeOptions.getMemory());

        setContainerArgs(manifest);

        if (!_kubeOptions.getEnv().isEmpty())
            setEnvVariables(manifest);

        if (!_kubeOptions.getMounts().isEmpty())
            setVolumeMounts(manifest);

        return manifest.toString();
    }

    /**
     *
     * @return
     */
    public KubernetesOptions getOptions()
    {
        return _kubeOptions;
    }


    // private methods


    /*
     *
     */
    private boolean skipArgument(String name)
    {
        for (Pattern pattern : _skipList) {
            if (pattern.matcher(name).matches())
                return true;
        }

        return false;
    }

    /*
     *
     */
    private void setContainerArgs(YamlDocument manifest) throws JobException
    {
        Job job = _jobCtx.getJob();
        JobParameterSet paramSet = job.getParameterSetModel();
        List<JobArgSpec> args = paramSet.getContainerArgs();

        for (JobArgSpec arg : args) {
            Matcher match = _paramPattern.matcher(arg.getArg());

            if (match.matches()) {
                String key = match.group(1);
                String value = match.group(2);

                if (!skipArgument(key))
                    manifest.setValue(key, value);
            }
        }
    }

    /*
     *
     */
    private void setEnvVariables(YamlDocument manifest) throws JobException
    {
        List<Pair<String,String>> pairs = _kubeOptions.getEnv();

        for (Pair<String, String> pair : pairs) {
            Map<String, String> newVar = new LinkedHashMap<String, String>(2);

            newVar.put("name", pair.getLeft());
            newVar.put("value", TapisUtils.conditionalQuote(pair.getRight()));

            manifest.appendNode("spec.template.spec.containers.env", newVar);
        }
    }

    /*
     *
     */
    private void setVolumeMounts(YamlDocument manifest) throws JobException
    {
        List<KubernetesOptions.Mount> mounts = _kubeOptions.getMounts();

        for (KubernetesOptions.Mount mount : mounts) {
            Map<String, Object> hostPath = new LinkedHashMap<String, Object>(2);
            Map<String, Object> volume = new LinkedHashMap<String, Object>(2);
            Map<String, Object> volumeMount = new LinkedHashMap<String, Object>(3);

            hostPath.put("path", mount.getHostPath());
            hostPath.put("type", "Directory");
            volume.put("name", mount.getName());
            volume.put("hostPath", hostPath);
            volumeMount.put("name", mount.getName());
            volumeMount.put("mountPath", mount.getMountPath());

            if (mount.isReadOnly())
                volumeMount.put("readOnly", true);

            manifest.appendNode("spec.template.spec.volumes", volume);
            manifest.appendNode("spec.template.spec.containers.volumeMounts", volumeMount);
        }
    }
}
