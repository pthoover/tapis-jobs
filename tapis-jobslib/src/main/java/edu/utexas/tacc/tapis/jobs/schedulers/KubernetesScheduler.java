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
import edu.utexas.tacc.tapis.jobs.utils.YamlDocument;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;


/**
 * Creates a Kubernetes manifest file
 *
 * @author phoover
 */
public class KubernetesScheduler
  implements JobScheduler
{
    // data fields


    // logging
    private static final Logger _log = LoggerFactory.getLogger(KubernetesScheduler.class);

    // Regular expression for parsing path-value pairs for a YAML document. The
    // expression captures three groups, other than the original unparsed string:
    //   1 - a path query
    //   2 - an operator, either an equals or plus-equals sign
    //   3 - a value assigned or appended to the target of the path query
    // Leading and trailing whitespace is ignored. An equals sign indicates that a value should be assigned, while a
    // plus-equals sign indicates that a value should be appended
    private static final Pattern _paramPattern = Pattern.compile("\\s*([^\\+=\\s]+)\\s*(\\+?=)\\s*(\\S.*)");

    // the names of resources that contain templates for Kubernetes manifests
    private static final String _resourceFile = "edu/utexas/tacc/tapis/jobs/kubernetes/manifest.yaml";
    private static final String _mpiResourceFile = "edu/utexas/tacc/tapis/jobs/kubernetes/mpi_manifest.yaml";

    // a list of path queries that are to be ignored
    private static final List<Pattern> _skipList = new ArrayList<Pattern>();

    private final JobExecutionContext _jobCtx;
    private final KubernetesOptions _kubeOptions;


    static {
        try {
            // ensure that a manifest describes a job that is not recurring
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
     * @param jobCtx the job execution context
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

    /**
     * {@inheritDoc}
     *
     * Copied from {@link edu.utexas.tacc.tapis.jobs.schedulers.SlurmScheduler#getModuleLoadCalls()}
     */
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
     * Creates the contents of a Kubernetes manifest
     *
     * @return the contents of the manifest
     * @throws TapisException
     */
    public String getManifest() throws TapisException
    {
        YamlDocument manifest;
        String resourceName;

        if (_jobCtx.getJob().isMpi())
            resourceName = _mpiResourceFile;
        else
            resourceName = _resourceFile;

        // use a template as the basis for the manifest
        try (InputStream inStream = KubernetesScheduler.class.getClassLoader().getResourceAsStream(resourceName)) {
            manifest = new YamlDocument(inStream);
        }
        catch (IOException err) {
            throw new JobException(err.getMessage());
        }

        // set values using standard Tapis parameters
        manifest.setValue("metadata.name", "tapis-" + _jobCtx.getJob().getUuid());

        if (_jobCtx.getJob().isMpi()) {
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.metadata.labels.app", _jobCtx.getApp().getId());
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.metadata.labels.app", _jobCtx.getApp().getId());
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.spec.serviceAccountName", _jobCtx.getExecutionSystem().getEffectiveUserId());
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.spec.serviceAccountName", _jobCtx.getExecutionSystem().getEffectiveUserId());
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.spec.containers.name", _kubeOptions.getContainerName() + "-launcher");
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.spec.containers.name", _kubeOptions.getContainerName() + "-worker");
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.spec.containers.image", _kubeOptions.getImage());
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.spec.containers.image", _kubeOptions.getImage());
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.spec.containers.resources.limits.cpu", _kubeOptions.getCpu());
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.spec.containers.resources.limits.cpu", _kubeOptions.getCpu());
            manifest.setValue("spec.mpiReplicaSpecs.Launcher.template.spec.containers.resources.limits.memory", _kubeOptions.getMemory() + "M");
            manifest.setValue("spec.mpiReplicaSpecs.Worker.template.spec.containers.resources.limits.memory", _kubeOptions.getMemory() + "M");
        }
        else {
            manifest.setValue("spec.template.metadata.labels.app", _jobCtx.getApp().getId());
            manifest.setValue("spec.template.spec.serviceAccountName", _jobCtx.getExecutionSystem().getEffectiveUserId());
            manifest.setValue("spec.template.spec.containers.name", _kubeOptions.getContainerName());
            manifest.setValue("spec.template.spec.containers.image", _kubeOptions.getImage());
            manifest.setValue("spec.template.spec.containers.resources.limits.cpu", _kubeOptions.getCpu());
            manifest.setValue("spec.template.spec.containers.resources.limits.memory", _kubeOptions.getMemory() + "M");
        }

        // set values using path-value pairs supplied by the user as scheduler
        // options. Setting them here allows the user to override default values
        setManifestValues(manifest);

        if (!_kubeOptions.getEnv().isEmpty())
            setEnvVariables(manifest);

        if (!_kubeOptions.getMounts().isEmpty())
            setVolumeMounts(manifest);

        return manifest.toString();
    }


    // private methods


    /**
     * Determines whether or not a path query should be ignored
     *
     * @param query a path query
     * @return true if the query should be ignored, false otherwise
     */
    private boolean skipValue(String query)
    {
        for (Pattern pattern : _skipList) {
            if (pattern.matcher(query).matches())
                return true;
        }

        return false;
    }

    /**
     * Sets node values in a Kubernetes manifest according to a list of
     * path-value pairs
     *
     * @param manifest the manifest document
     * @throws JobException
     */
    private void setManifestValues(YamlDocument manifest) throws JobException
    {
        for (String pair : _kubeOptions.getManifestValues()) {
            Matcher match = _paramPattern.matcher(pair);

            if (match.matches()) {
                String key = match.group(1);
                String operator = match.group(2);
                String value = match.group(3);

                // an equals sign indicates that the value of a selected node
                // should be set to the given value. A plus-equals sign indicates
                // that the value should be appended to the current value
                if (!skipValue(key)) {
                    if (operator.equals("+="))
                        manifest.appendValue(key, value);
                    else
                        manifest.setValue(key, value);
                }
            }
        }
    }

    /**
     * Adds environment variables to containers in a Kubernetes manifest
     *
     * @param manifest the manifest document
     * @throws JobException
     */
    private void setEnvVariables(YamlDocument manifest) throws JobException
    {
        List<Pair<String,String>> pairs = _kubeOptions.getEnv();

        for (Pair<String, String> pair : pairs) {
            Map<String, String> newVar = new LinkedHashMap<String, String>(2);

            newVar.put("name", pair.getLeft());
            newVar.put("value", TapisUtils.conditionalQuote(pair.getRight()));

            if (_jobCtx.getJob().isMpi()) {
                manifest.appendNode("spec.mpiReplicaSpecs.Launcher.template.spec.containers.env", newVar);
                manifest.appendNode("spec.mpiReplicaSpecs.Worker.template.spec.containers.env", newVar);
            }
            else
                manifest.appendNode("spec.template.spec.containers.env", newVar);
        }
    }

    /**
     * Adds volume definitions to a Kubernetes manifest
     *
     * @param manifest the manifest document
     * @throws JobException
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

            if (_jobCtx.getJob().isMpi()) {
                manifest.appendNode("spec.mpiReplicaSpecs.Launcher.template.spec.volumes", volume);
                manifest.appendNode("spec.mpiReplicaSpecs.Worker.template.spec.volumes", volume);
                manifest.appendNode("spec.mpiReplicaSpecs.Launcher.template.spec.containers.volumeMounts", volumeMount);
                manifest.appendNode("spec.mpiReplicaSpecs.Worker.template.spec.containers.volumeMounts", volumeMount);
            }
            else {
                manifest.appendNode("spec.template.spec.volumes", volume);
                manifest.appendNode("spec.template.spec.containers.volumeMounts", volumeMount);
            }
        }
    }
}
