package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class SingularityRunStager
 extends AbstractSingularityStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunStager.class);

    // Container id file suffix.
    private static final String PID_SUFFIX = ".pid";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singularity run command object.
    private final SingularityRunCmd _singularityCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        super(jobCtx);
        _singularityCmd = configureExecCmd();
    }

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateWrapperScript() throws TapisException 
    {
        // The generated wrapper script will contain a singularity instance 
        // start command that conforms to this format:
        //
        //  singularity instance start [start options...] <container path> <instance name> [startscript args...]
        String cmdText = _singularityCmd.generateExecCmd(_job);
        
        // Build the command file content.
        initBashScript();
        
        // Add the docker command the the command file.
        _cmd.append(cmdText);
        
        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateEnvVarFile() throws TapisException 
    {
        return _singularityCmd.generateEnvVarFileContent();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getCmdTextWithEnvVars:                                                 */
    /* ---------------------------------------------------------------------- */
    protected String getCmdTextWithEnvVars() 
    {
        return _singularityCmd.getCmdTextWithEnvVars(_job);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSingularityRunCmd:                                                  */
    /* ---------------------------------------------------------------------- */
    protected SingularityRunCmd getSingularityRunCmd() {return _singularityCmd;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    private SingularityRunCmd configureExecCmd()
     throws TapisException
    {
        // Create and populate the singularity command.
        var singularityCmd = new SingularityRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Write all the environment variables to file.
        singularityCmd.setEnvFile(makeEnvFilePath());
        
        // Set the image.
        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());
        
        // Set the stdout/stderr redirection file.
        singularityCmd.setLogConfig(resolveLogConfig());

        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        singularityCmd.setEnv(getEnvVariables());

        // Set the singularity options.
        setSingularityOptions(singularityCmd);
        
        // Set the application arguments.
        singularityCmd.setAppArguments(concatAppArguments());
                
        return singularityCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setSingularityOptions:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Set the singularity options that we allow the user to modify.
     * 
     * @param singularityCmd the run command to be updated
     */
    private void setSingularityOptions(SingularityRunCmd singularityCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getContainerArgs();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
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
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignCmd(singularityCmd, option, value);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity run parameter.  If the parameter
     * pertains to run only--that is it's not a paramter also used by start--then
     * it will be set here.  If the parameter is not run only, then the command
     * parameter assignment method in the superclass will be called.
     * 
     * Note that this method overloads but does not override the superclass
     * method with the name.
     * 
     * @param singularityCmd the run command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected void assignCmd(SingularityRunCmd singularityCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            // Run common options.
            case "--app":
                singularityCmd.setApp(value);
                break;
            case "--ipc":
            case "-i":
                singularityCmd.setIpc(true);
                break;
            case "--nonet":
                singularityCmd.setNoNet(true);
                break;
            case "--pid":
            case "-p":
                singularityCmd.setPid(true);
                break;
            case "--pwd":
                singularityCmd.setPwd(value);
                break;
            case "--vm":
                singularityCmd.setVm(true);
                break;
            case "--vm-cpu":
                singularityCmd.setVmCPU(value);
                break;
            case "--vm-err":
                singularityCmd.setVmErr(true);
                break;
            case "--vm-ip":
                singularityCmd.setVmIP(value);
                break;
            case "--vm-ram":
                singularityCmd.setVmRAM(value);
                break;
        
            // It's either a common option or invalid.
            default: super.assignCmd(singularityCmd, option, value);
        }
    }
}
