package edu.utexas.tacc.tapis.jobs.stagers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.schedulers.JobScheduler;
import edu.utexas.tacc.tapis.jobs.schedulers.SlurmScheduler;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

public abstract class AbstractJobExecStager
 implements JobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobExecStager.class);
    
    // Command buffer initial capacity.
    private static final int INIT_CMD_LEN = 2048;

    // Command line option parser.  This regex captures 3 groups:
    //
    //   0 - the complete value unparsed
    //   1 - the option starting with 1 or 2 hypens (-e, --env, etc.)
    //   2 - the value assigned to the option, which may be empty
    //
    // Leading and trailing whitespace is ignored, as is any whitespace between
    // the option and value.  The optional equals sign is also ignored, whether
    // there's whitespace on either side of it or not.
    // (\s=whitespace, \S=not whitespace)
    public static final Pattern _optionPattern = Pattern.compile("\\s*(--?[^=\\s]*)\\s*=?\\s*(\\S*)\\s*");
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Input parameters
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;
    
    // The buffer used to build command file content. 
    protected final StringBuilder _cmdBuilder;

    // The command that implements the JobExecCmd interface
    protected final JobExecCmd _jobExecCmd;

    // Fields related to batch job execution
    protected final JobScheduler _scheduler;
    protected final boolean _isBatch;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobExecStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType)
            throws TapisException
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
        // Initialize the command file text.
        _cmdBuilder = new StringBuilder(INIT_CMD_LEN);
        // Set the scheduler properties as needed.
        // NOTE: For now, we only support slurm. Once other schedulers are supported create the appropriate scheduler
        if (schedulerType == null) {
            _scheduler = null;
        } else if (SchedulerTypeEnum.SLURM.equals(schedulerType)) {
            _scheduler = new SlurmScheduler(jobCtx);
        } else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", schedulerType,
                                         AbstractJobExecStager.class.getSimpleName());
            throw new JobException(msg);
        }
        _isBatch = (schedulerType != null);
        _jobExecCmd = createJobExecCmd();
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    /**
     * Stage the application assets prior to running the job.
     *  1. Generate and install the wrapper script tapisjob.sh
     *  2. Generate and install the environment file tapisjob.env
     *
     * NOTE: Docker and Singularity use this implementation.
     *       Zip overrides this implementation.
     */
    @Override
    public void stageJob() throws TapisException
    {
        var fm = _jobCtx.getJobFileManager();
        // Create and install the wrapper script.
        String wrapperScript = generateWrapperScriptContent();
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);
        // Create and install the environment variable definition file.
        String envVarFile = generateEnvVarFileContent();
        fm.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     * 
     * @return the wrapper script content
     */
    public abstract String generateWrapperScriptContent() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for the environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    public abstract String generateEnvVarFileContent() throws TapisException;

    /* ---------------------------------------------------------------------- */
    /* createJobExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    /** This method creates the JobExecCmd.
     *
     * @return the wrapper script content
     */
    public abstract JobExecCmd createJobExecCmd() throws TapisException;

    /* ---------------------------------------------------------------------- */
    /* initBashScript:                                                        */
    /* ---------------------------------------------------------------------- */
    protected void initBashScript()
    {
        _cmdBuilder.append("#!/bin/bash\n\n");
        appendDescription();
    }
    
    /* ---------------------------------------------------------------------- */
    /* initBashBatchScript:                                                   */
    /* ---------------------------------------------------------------------- */
    protected void initBashBatchScript()
    {
        _cmdBuilder.append("#!/bin/bash\n\n");
        appendBatchDescription();
    }
    
    /* ---------------------------------------------------------------------- */
    /* concatAppArguments:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Assemble the application arguments into a single string and then assign
     * them to the caller.  If there are any arguments, the generated 
     * string always begins with a space character.
     * 
     * NOTE: The method depends on JobParmSetMarshaller.validateScratchList()
     *       to weed out any application parameter names that contain dangerous
     *       command line characters. This means that conditional quoting 
     *       need only be applied to values.
     * 
     * @return the app argument string or null if there aren't any.
     */
    protected String concatAppArguments()
    {
         // Get the list of user-specified container arguments.
         var parmSet = _job.getParameterSetModel();
         var opts    = parmSet.getAppArgs();
         if (opts == null || opts.isEmpty()) return null;
         
         // Assemble the application's argument string.
         StringBuilder args = new StringBuilder();
         for (var opt : opts) {
        	 // Split the argument into key/value components
        	 var arg = opt.getArg();
        	 var parts = TapisUtils.splitIntoKeyValue(arg);
        	 
        	 // Check if the value needs to be quoted and 
        	 // rebuild the arg string if it's been modified.
        	 if (parts.length == 2) {
        		 var value = TapisUtils.conditionalQuote(parts[1]);
        		 if (!value.equals(parts[1])) arg = parts[0] + " " + value;
        	 }
        	 
        	 // Add to the argument string.
        	 args.append(" ").append(arg);
         }
         return args.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* isAssigned:                                                            */
    /* ---------------------------------------------------------------------- */
    /**
     * Determine if a required option is assigned a value.
     * If not assigned throw an exceptions
     */
    protected void isAssigned(String runtimeName, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", runtimeName, option);
            throw new JobException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getExportPairListArgs:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Create strings of 'export key=value' separated by new line characters, 
     * which is appropriate for writing to a file that will be sourced by a 
     * script.
     * 
     * @param pairs NON-EMPTY list of pair values, one per occurrence
     * @return the string that contains all assignments
     */
    protected String getExportPairListArgs(List<Pair<String,String>> pairs) 
    {
    	// This is essentially the same code as in 
    	// AbstractSingularityExecCmd.getPairListArgs(),
    	// but here we insert the export keyword for sourcing
    	// by a bash script.
    	
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // Create a list of key=value assignment, each followed by a new line.
        for (var v : pairs) {
        	buf.append("export ");
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(conditionalQuote(v.getRight()));
            buf.append("\n");
        }
        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* resolveLogConfig:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Determine the stdout and stderr logging file(s).  Assign the fully qualified
     * paths to the combined or separate log files.
     *
     * @return resolved LogConfig
     * @throws TapisException on error
     */
    protected LogConfig resolveLogConfig() throws TapisException
    {
        // Get the user-supplied or defaulted log configuration and
        // create the new log configuration for this command.
        var origConfig     = _job.getParameterSetModel().getLogConfig();
        var resolvedConfig = new LogConfig();

        // We must always fully qualify at least one of the paths.
        var fm = _jobCtx.getJobFileManager();
        resolvedConfig.setStdoutFilename(fm.makeAbsExecSysOutputPath(origConfig.getStdoutFilename()));
        
        // Avoid recalculating the fully qualified path when there's only one log file.
        if (origConfig.canMerge())
            resolvedConfig.setStderrFilename(resolvedConfig.getStdoutFilename());
        else
            resolvedConfig.setStderrFilename(fm.makeAbsExecSysOutputPath(origConfig.getStderrFilename()));

        return resolvedConfig;
    }

    /* ---------------------------------------------------------------------- */
    /* getEnvVariables:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Determine the list of environment variables for the job.
     * Both the standard tapis and user-supplied environment variables are
     * assigned here.  The user is prevented at job submission time from
     * setting any environment variable that starts with the reserved "_tapis"
     * prefix, so collisions are not possible.
     *
     * @return list of env variables
     */
    protected List<Pair<String, String>> getEnvVariables()
    {
        var envVariables = new ArrayList<Pair<String, String>>();
        // Get the list of environment variables.
        var parmSet = _job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return envVariables;
        // Process each environment variable.
        for (var kv : envList) envVariables.add(Pair.of(kv.getKey(), kv.getValue()));
        return envVariables;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* appendDescription:                                                     */
    /* ---------------------------------------------------------------------- */
    private void appendDescription()
    {
        _cmdBuilder.append("# This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmdBuilder.append("# of running a Tapis application.  The order of execution is as follows:\n");
        _cmdBuilder.append("#\n");
        _cmdBuilder.append("#   1. Standard Tapis and user-supplied environment variables are exported.\n");
        _cmdBuilder.append("#   2. The application container is run with container options, environment\n");
        _cmdBuilder.append("#      variables and application parameters as supplied in the Tapis job,\n");
        _cmdBuilder.append("#      application and system definitions.\n");
        _cmdBuilder.append("\n");
    }

    /* ---------------------------------------------------------------------- */
    /* appendBatchDescription:                                                */
    /* ---------------------------------------------------------------------- */
    private void appendBatchDescription()
    {
        _cmdBuilder.append("# This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmdBuilder.append("# of running a Tapis application.  The order of execution is as follows:\n");
        _cmdBuilder.append("#\n");
        _cmdBuilder.append("#   1. The batch scheduler options are passed to the scheduler, including any\n");
        _cmdBuilder.append("#      user-specified, scheduler-managed environment variables.\n");
        _cmdBuilder.append("#   2. The application container is run with container options, environment\n");
        _cmdBuilder.append("#      variables and application parameters as supplied in the Tapis job,\n");
        _cmdBuilder.append("#      application and system definitions.\n");
        _cmdBuilder.append("\n");
    }

    /* ********************************************************************** */
    /*                          Accessors                                     */
    /* ********************************************************************** */

    public JobExecCmd getJobExecCmd() {
        return _jobExecCmd;
    }
}
