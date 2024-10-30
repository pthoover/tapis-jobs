package edu.utexas.tacc.tapis.jobs.schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.docker.DockerRunCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;


/**
 * Contains options used to create a Kubernetes manifest file
 *
 * @author phoover
 */
public class KubernetesOptions
{
    // nested classes


    /**
     * A class that contains information pertinent to a Kubernetes volume
     */
    public static class Mount
    {
        private final String _name;
        private final String _hostPath;
        private final String _mountPath;
        private final boolean _readOnly;


        /**
         * constructor
         *
         * @param name name of the volume
         * @param hostPath path of the mount point on the host
         * @param mountPath location to mount the volume in the container
         * @param readOnly should the volume be mounted read-only
         */
        public Mount(String name, String hostPath, String mountPath, boolean readOnly)
        {
            _name = name;
            _hostPath = hostPath;
            _mountPath = mountPath;
            _readOnly = readOnly;
        }

        /**
         *
         * @return the name of the volume
         */
        public String getName()
        {
            return _name;
        }

        /**
         *
         * @return the path of the mount point on the host
         */
        public String getHostPath()
        {
            return _hostPath;
        }

        /**
         *
         * @return the location to mount the volume in the container
         */
        public String getMountPath()
        {
            return _mountPath;
        }

        /**
         *
         * @return whether or not the volume should be mounted read-only
         */
        public boolean isReadOnly()
        {
            return _readOnly;
        }
    }


    // data fields


    // maximum length for a container label
    private static final int MAX_LABEL_LENGTH = 53;

    // Regular expression for parsing scheduler options. The expression captures
    // two groups, other than the original unparsed string:
    //   1 - either the name of an option or a path query for a YAML document
    //   2 - a value assigned to the option or path query
    // The expression also has a non-capturing group that matches an optional
    // operator, which may be an equals or plus-equals sign. Leading and
    // trailing whitespace is ignored
    private static final Pattern _optionPattern = Pattern.compile("\\s*([^\\+=\\s]+)\\s*(?:\\+?=)?\\s*(\\S.*)");

    private String _containerName;
    private String _cpu;
    private List<Pair<String,String>> _env;
    private String _image;
    private String _jobName;
    private List<String> _manifestValues;
    private String _memory;
    private List<Mount> _mounts;
    private String _tapisProfile;


    // constructors


    /**
     * Uses a job execution context to determine various Kubernets options
     *
     * @param jobCtx the job execution context
     * @throws TapisException
     */
    public KubernetesOptions(JobExecutionContext jobCtx) throws TapisException
    {
        setOptions(jobCtx);
    }


    // public methods


    /**
     *
     * @return the name of the container
     */
    public String getContainerName()
    {
        return _containerName;
    }

    /**
     *
     * @param name the name of the container
     */
    public void setContainerName(String name)
    {
        _containerName = name;
    }

    /**
     *
     * @return the number of CPUs to request
     */
    public String getCpu()
    {
        return _cpu;
    }

    /**
     *
     * @param cpu the number of CPUs to request
     */
    public void setCpu(String cpu)
    {
        _cpu = cpu;
    }

    /**
     *
     * @return a list of environment variables to set in the container
     */
    public List<Pair<String,String>> getEnv()
    {
        if (_env == null)
            _env = new ArrayList<Pair<String,String>>();

        return _env;
    }

    /**
     *
     * @param env a list of environment variables to set in the container
     */
    public void setEnv(List<Pair<String,String>> env)
    {
        _env = env;
    }

    /**
     *
     * @return the name of the container image
     */
    public String getImage() {
        return _image;
    }

    /**
     *
     * @param image the name of the container image
     */
    public void setImage(String image)
    {
        _image = image;
    }

    /**
     *
     * @return the name to assign to the job
     */
    public String getJobName()
    {
        return _jobName;
    }

    /**
     *
     * @param name the name to assign to the job
     */
    public void setJobName(String name)
    {
        _jobName = name;
    }

    /**
     *
     * @return a list of query paths and values
     */
    public List<String> getManifestValues()
    {
        if (_manifestValues == null)
            _manifestValues = new ArrayList<String>();

        return _manifestValues;
    }

    /**
     *
     * @param values a list of query paths and values
     */
    public void setManifestValues(List<String> values)
    {
        _manifestValues = values;
    }

    /**
     *
     * @return the amount of memory to request, in megabytes
     */
    public String getMemory() {
        return _memory;
    }

    /**
     *
     * @param memory the amount of memory to request, in megabytes
     */
    public void setMemory(String memory)
    {
        _memory = memory;
    }

    /**
     *
     * @return a list of volumes
     */
    public List<Mount> getMounts()
    {
        if (_mounts == null)
            _mounts = new ArrayList<Mount>();

        return _mounts;
    }

    /**
     *
     * @param mounts a list of volumes
     */
    public void setMounts(List<Mount> mounts)
    {
        _mounts = mounts;
    }

    /**
     *
     * @return the name of a Tapis profile
     */
    public String getTapisProfile()
    {
        return _tapisProfile;
    }

    /**
     *
     * @param profile the name of a Tapis profile
     */
    public void setTapisProfile(String profile)
    {
        _tapisProfile = profile;
    }


    // private methods


    /**
     * Uses the job execution context to determine values that will later be
     * used to create a Kubernetes manifest file
     *
     * @param jobCtx the job execution context
     * @throws TapisException
     */
    private void setOptions(JobExecutionContext jobCtx) throws TapisException
    {
        Job job = jobCtx.getJob();

        setSchedulerOptions(job);

        setEnvVariables(job);

        setStandardBindMounts(jobCtx);

        setTapisLocalBindMounts(jobCtx, job);

        // set the name of the container using the name of the Docker image
        String containerImage = jobCtx.getApp().getContainerImage();
        String[] parts = containerImage.split("/");
        String imageName = parts[parts.length - 1].split(":")[0];

        imageName = imageName.toLowerCase().replaceAll("[^a-z0-9\\-]", "");

        if (imageName.length() > MAX_LABEL_LENGTH)
            imageName = imageName.substring(0, MAX_LABEL_LENGTH);

        setContainerName(imageName + "-container");

        setImage(containerImage);

        setCpu(Integer.toString(job.getCoresPerNode()));

        setMemory(Integer.toString(job.getMemoryMB()));

        if (StringUtils.isBlank(getJobName()))
            setJobName(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
    }

    /**
     * Examines the scheduler options provided by the user to create a list of
     * path-value pairs for use in creating the Kubernetes manifest. Mostly
     * copied from {@link edu.utexas.tacc.tapis.jobs.schedulers.SlurmOptions#setUserSlurmOptions()}
     *
     * @param job the job
     * @throws JobException
     */
    private void setSchedulerOptions(Job job) throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = job.getParameterSetModel();
        var opts    = parmSet.getSchedulerOptions();
        if (opts == null || opts.isEmpty()) return;

        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "kubernetes", opt.getArg());
                throw new JobException(msg);
            }

            // Get the option and its value if one is provided.
            String option = null;
            String value  = ""; // default value when none provided
            int groupCount = m.groupCount();
            if (groupCount > 0) option = m.group(1);
            if (groupCount > 1) value  = m.group(2);

            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "kubernetes", opt.getArg());
                throw new JobException(msg);
            }

            // Save the parsed value.
            if (!assignCmd(option, value))
                getManifestValues().add(opt.getArg());
        }
    }

    /**
     * Examines a scheduler option to set Tapis-specific parameters
     *
     * @param option name of the option
     * @param value value of the option
     * @return whether or not the option was recognized as a Tapis option
     */
    private boolean assignCmd(String option, String value)
    {
        if (option.equals("--tapis-profile")) {
            setTapisProfile(value);

            return true;
        }

        return false;
    }

    /**
     * Determine the list of environment variables for the job. Mostly copied
     * from {@link edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager#getEnvVariables()}
     *
     * @param job the job
     */
    private void setEnvVariables(Job job)
    {
        // Get the list of environment variables.
        var parmSet = job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return;

        // Process each environment variable.
        var cmdEnv = getEnv();
        for (var kv : envList) cmdEnv.add(Pair.of(kv.getKey(), kv.getValue()));
    }

    /**
     * Adds the standard Tapis mount points to the list of volumes
     *
     * @param jobCtx the job execution context
     * @throws TapisException
     */
    private void setStandardBindMounts(JobExecutionContext jobCtx) throws TapisException
    {
        // Let the file manager make paths.
        var fm = jobCtx.getJobFileManager();
        List<Mount> mounts = getMounts();

        // Set standard bind mounts.
        mounts.add(new Mount("exec-system-input", fm.makeAbsExecSysInputPath(), Job.DEFAULT_EXEC_SYSTEM_INPUT_MOUNTPOINT, true));
        mounts.add(new Mount("exec-system-output", fm.makeAbsExecSysOutputPath(), Job.DEFAULT_EXEC_SYSTEM_OUTPUT_MOUNTPOINT, false));
        mounts.add(new Mount("exec-system-exec", fm.makeAbsExecSysExecPath(), Job.DEFAULT_EXEC_SYSTEM_EXEC_MOUNTPOINT, false));
    }

    /**
     * Add user-specified mount points. Mostly copied from {@link edu.utexas.tacc.tapis.jobs.stagers.docker.DockerStager#setTapisLocalBindMounts(DockerRunCmd)}
     *
     * @param jobCtx the job execution context
     * @param job the job
     * @throws TapisException
     */
    private void setTapisLocalBindMounts(JobExecutionContext jobCtx, Job job) throws TapisException
    {
        // Let the file manager make paths.
        var fm = jobCtx.getJobFileManager();
        List<Mount> mounts = getMounts();

        for (var reqInput : job.getFileInputsSpec()) {
            // We only process tapislocal input with automount set.
            if (!reqInput.isTapisLocal() || !reqInput.getAutoMountLocal())
                continue;

            String hostPath = fm.makeAbsExecSysTapisLocalPath(jobCtx.getExecutionSystem().getRootDir(), reqInput.getSourceUrl());
            String targetPath = reqInput.getTargetPath();

            if (!targetPath.startsWith("/"))
                targetPath = "/" + targetPath;

            mounts.add(new Mount(reqInput.getName(), hostPath, targetPath, true));
        }
    }
}
