package edu.utexas.tacc.tapis.jobs.schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

import static edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager._optionPattern;


/**
 *
 * @author phoover
 */
public class KubernetesOptions
{
    // nested classes


    /**
     *
     */
    public static class Mount
    {
        private final String _name;
        private final String _hostPath;
        private final String _mountPath;
        private final boolean _readOnly;


        /**
         *
         * @param name
         * @param hostPath
         * @param mountPath
         * @param readOnly
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
         * @return
         */
        public String getName()
        {
            return _name;
        }

        /**
         *
         * @return
         */
        public String getHostPath()
        {
            return _hostPath;
        }

        /**
         *
         * @return
         */
        public String getMountPath()
        {
            return _mountPath;
        }

        /**
         *
         * @return
         */
        public boolean isReadOnly()
        {
            return _readOnly;
        }
    }


    // data fields


    private static final int MAX_LABEL_LENGTH = 53;
    private static final Logger _log = LoggerFactory.getLogger(KubernetesOptions.class);
    private static final List<Pattern> _skipList = new ArrayList<Pattern>();
    private String _containerName;
    private String _cpu;
    private List<Pair<String,String>> _env;
    private String _image;
    private String _jobName;
    private List<String> _kubeArgs;
    private String _memory;
    private List<Mount> _mounts;
    private String _tapisProfile;


    static {
        try {
            _skipList.add(Pattern.compile("-f|--filename"));
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
    public KubernetesOptions(JobExecutionContext jobCtx) throws TapisException
    {
        setOptions(jobCtx);
    }


    // public methods


    /**
     *
     * @return
     */
    public String getContainerName()
    {
        return _containerName;
    }

    /**
     *
     * @param name
     */
    public void setContainerName(String name)
    {
        _containerName = name;
    }

    /**
     *
     * @return
     */
    public String getCpu()
    {
        return _cpu;
    }

    /**
     *
     * @param cpu
     */
    public void setCpu(String cpu)
    {
        _cpu = cpu;
    }

    /**
     *
     * @return
     */
    public List<Pair<String,String>> getEnv()
    {
        if (_env == null)
            _env = new ArrayList<Pair<String,String>>();

        return _env;
    }

    /**
     *
     * @param env
     */
    public void setEnv(List<Pair<String,String>> env)
    {
        _env = env;
    }

    /**
     *
     * @return
     */
    public String getImage() {
        return _image;
    }

    /**
     *
     * @param image
     */
    public void setImage(String image)
    {
        _image = image;
    }

    /**
     *
     * @return
     */
    public String getJobName()
    {
        return _jobName;
    }

    /**
     *
     * @param name
     */
    public void setJobName(String name)
    {
        _jobName = name;
    }

    /**
     *
     * @return
     */
    public List<String> getKubeArgs()
    {
        if (_kubeArgs == null)
        _kubeArgs = new ArrayList<String>();

        return _kubeArgs;
    }

    /**
     *
     * @param args
     */
    public void setKubeArgs(List<String> args)
    {
        _kubeArgs = args;
    }

    /**
     *
     * @return
     */
    public String getMemory() {
        return _memory;
    }

    /**
     *
     * @param memory
     */
    public void setMemory(String memory)
    {
        _memory = memory;
    }

    /**
     *
     * @return
     */
    public List<Mount> getMounts()
    {
        if (_mounts == null)
            _mounts = new ArrayList<Mount>();

        return _mounts;
    }

    /**
     *
     * @param mounts
     */
    public void setMounts(List<Mount> mounts)
    {
        _mounts = mounts;
    }

    /**
     *
     * @return
     */
    public String getTapisProfile()
    {
        return _tapisProfile;
    }

    /**
     *
     * @param profile
     */
    public void setTapisProfile(String profile)
    {
        _tapisProfile = profile;
    }


    // private methods


    /*
     *
     */
    private void setOptions(JobExecutionContext jobCtx) throws TapisException
    {
        Job job = jobCtx.getJob();

        setSchedulerOptions(job);

        setEnvVariables(job);

        setStandardBindMounts(jobCtx);

        setTapisLocalBindMounts(jobCtx, job);

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

    /*
     *
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
            if (!assignCmd(option, value) && !skipOption(option))
                getKubeArgs().add(opt.getArg());
        }
    }

    /*
     *
     */
    private boolean assignCmd(String option, String value)
    {
        switch (option) {
            case "--job-name":
            case "-J":
                setJobName(value);
                break;

            case "--tapis-profile":
                setTapisProfile(value);
                break;

            default:
                return false;
        }

        return true;
    }

    /*
     *
     */
    private boolean skipOption(String name)
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

    /*
     *
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

    /*
     *
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
