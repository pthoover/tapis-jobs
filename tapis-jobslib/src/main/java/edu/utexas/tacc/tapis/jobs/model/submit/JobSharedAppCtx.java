package edu.utexas.tacc.tapis.jobs.model.submit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.TapisConstants;

public final class JobSharedAppCtx 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
	// Default shared app owner.
	private static final String NOT_SHARED_APP_OWNER = Job.DEFAULT_SHARED_APP_CTX;
	
	// Default shared app attributes.
	public static final List<JobSharedAppCtxEnum> EMPTY_APP_CTX_ATTRIBS = 
		Collections.unmodifiableList(new ArrayList<JobSharedAppCtxEnum>(0));
	
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // The shared context attributes in effect for the job being processed.
    public enum JobSharedAppCtxEnum {
        SAC_EXEC_SYSTEM_ID,
        SAC_EXEC_SYSTEM_EXEC_DIR,
        SAC_EXEC_SYSTEM_INPUT_DIR,
        SAC_EXEC_SYSTEM_OUTPUT_DIR,
        SAC_ARCHIVE_SYSTEM_ID,
        SAC_ARCHIVE_SYSTEM_DIR,
        SAC_DTN_SYSTEM_ID,
        SAC_DTN_SYSTEM_INPUT_DIR,
        SAC_DTN_SYSTEM_OUTPUT_DIR
    }
    
    // Number of possible values in preceding enum.
    public static final int MAX_SHARED_APP_CTX_ATTRIBS = 9;
     
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Shared application context flag assigned when app is accessed
    // and the field set is re-initialized only when sharing is in effect.
    // None of the fields should ever be null.
    private boolean                   _sharingEnabled;
    private List<JobSharedAppCtxEnum> _sharedAppCtxAttribs = EMPTY_APP_CTX_ATTRIBS;
    private String                    _sharedAppOwner = NOT_SHARED_APP_OWNER;
    
    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Called during job request processing to assign sharing attributes.
     * Not for use during job execution.  Attributes are assigned after construction.
     * 
     * @param app the application to be run by the new job
     */
    public JobSharedAppCtx(TapisApp app)
    {
    	// See if app indicates sharing.  If not, default field values are used.
    	_sharingEnabled = !StringUtils.isBlank(app.getSharedAppCtx());
    	if (!_sharingEnabled) return;  

        // Assign the flag and initialize the set if we are sharing.
        _sharedAppOwner = app.getSharedAppCtx();
        _sharedAppCtxAttribs = new ArrayList<JobSharedAppCtxEnum>(MAX_SHARED_APP_CTX_ATTRIBS);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Called by workers during job execution to conveniently access sharing attributes.
     * No fields are null after construction nor should they be modified.  
     * Not for use during job submission.
     * 
     * @param job the executing job
     */
    public JobSharedAppCtx(Job job)
    {
        // Expected to be read only access.
        _sharingEnabled = job.isSharedAppCtx();
        if (job.getSharedAppCtxAttribs() != null) _sharedAppCtxAttribs = job.getSharedAppCtxAttribs();
        if (job.getSharedAppCtx() != null) _sharedAppOwner = job.getSharedAppCtx();
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* calcExecSystemId:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an attribute is shared.  It must be called
     * AFTER any app value merging into the job submission request is performed. 
     * 
     * @param jobExecSystemId the non-null job request value after app merge
     * @param appExecSystemId the possibly null app definition value
     */
    public void calcExecSystemId(String jobExecSystemId, String appExecSystemId)
    {
        // Is the application shared with this user?
        if (!_sharingEnabled) return;
        
        // We only share values assigned in the app definition.
        if (StringUtils.isBlank(appExecSystemId)) return;
        
        // We share if the app and job request have the same value.
        if (appExecSystemId.equals(jobExecSystemId)) 
            _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_ID);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcArchiveSystemId:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an attribute is shared.  It must be called
     * AFTER any app value merging into the job submission request is performed. 
     * 
     * @param jobArchiveSystemId the non-null job request value after app merge
     * @param appArchiveSystemId the possibly null app definition value
     * @param jobExecSystemId the non-null execution system id
     */
    public void calcArchiveSystemId(String jobArchiveSystemId, String appArchiveSystemId,
                                    String jobExecSystemId)
    {
        // Is the application shared with this user?
        if (!_sharingEnabled) return;
        
        // If the application doesn't define an archive system, the assigned
        // system is by default the execution system.  If that is the case, 
        // we assume the application sharer intended for the default archive 
        // system to be shared.
        if (StringUtils.isBlank(appArchiveSystemId)) {
            if (jobArchiveSystemId.equals(jobExecSystemId))
                _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);
            return;
        }
        
        // We share if the app and job request have the same value.
        if (appArchiveSystemId.equals(jobArchiveSystemId)) 
            _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcDtnSystemId:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an attribute is shared.  It must be called
     * AFTER any app value merging into the job submission request is performed. 
     * Specifically, the exec system must already be shared, i.e. calcExecSystemId()
     * has already been called.
     * 
     * @param jobExecSystemId the non-null job request value after app merge
     * @param sysDtnSystemId the possibly null execution system definition DTN value
     */
    public void calcDtnSystemId(String jobExecSystemId, String sysDtnSystemId)
    {
        // Is the application shared with this user?
        if (!_sharingEnabled) return;
        
        // We can only share a dtn system if one is specified.
        if (StringUtils.isBlank(sysDtnSystemId)) return;
        
        // We only share the dtn if we have already shared the exec system.
        // This covers the constraint that exec system is as specified in the app.
        if (!isSharingExecSystemId()) return;
        
        // We share if the app and job request have the same value.
        _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_DTN_SYSTEM_ID);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcExecDirSharing:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an execution system directory attribute is shared.  
     * This method must be called AFTER the app directory value has been merged into 
     * the job submission request. 
     * 
     * @param attrib the attribute to be shared
     * @param jobDir the non-null job request directory
     * @param appDir the possibly null application directory
     * @param defaultDir the default directory if not explicitly assigned
     */
    public void calcExecDirSharing(JobSharedAppCtxEnum attrib, String jobDir, 
                                   String appDir, String defaultDir)
    {
        // Is the application and exec system shared with this user?
        if (!isSharingExecSystemId()) return;
        
        // Was the directory defined in the application?
        if (StringUtils.isBlank(appDir)) {
            if (jobDir.equals(defaultDir)) _sharedAppCtxAttribs.add(attrib);
            return;
        }
        
        // Sharing's in effect if the app and job directories are the same.
        if (appDir.equals(jobDir)) _sharedAppCtxAttribs.add(attrib);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcArchiveDirSharing:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an archive system directory attribute is shared.  
     * This method must be called AFTER the app directory value has been merged into 
     * the job submission request. 
     * 
     * @param jobDir the non-null job request directory
     * @param appDir the possibly null application directory
     * @param jobArchiveSystemId non-null archive system id 
     * @param appArchiveSystemId possibly null archive system id from app
     * @param jobExecSystemId non-null execution system id
     * @param defaultDir the default directory if not explicitly assigned
     */
    public void calcArchiveDirSharing(String jobDir, String appDir, 
                                      String jobArchiveSystemId, 
                                      String appArchiveSystemId,
                                      String jobExecSystemId,
                                      String defaultDir)
    {
        // Is the application and exec system shared with this user?
        if (!isSharingArchiveSystemId()) return;
        final var attrib = JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_DIR;
        
        // If application didn't specify an archive system and we've resolved
        // to archive on the shared execution system itself, then the archive 
        // directory is always shared.
        if (StringUtils.isBlank(appArchiveSystemId))
            if (jobArchiveSystemId.equals(jobExecSystemId) &&
                isSharingExecSystemId()) 
            {
                _sharedAppCtxAttribs.add(attrib);
                return;
            }
        
        // If the job directory was not defined in the application
        // and is assigned its default value, then it's shared.
        if (StringUtils.isBlank(appDir)) {
            if (jobDir.equals(defaultDir)) _sharedAppCtxAttribs.add(attrib);
            return;
        }
        
        // Sharing's in effect if the app and job directories are the same.
        if (appDir.equals(jobDir)) _sharedAppCtxAttribs.add(attrib);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcDtnDirSharing:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether a dtn system directory attribute is shared.  
     * This method must be called AFTER the app directory value has been merged into 
     * the job submission request and calcDtnSystemId() has been called. 
     * 
     * @param attrib the attribute to be shared
     * @param jobDtnDir the possibly null job request directory
     * @param appDtnDir the non-null application directory
     */
    public void calcDtnDirSharing(JobSharedAppCtxEnum attrib, String jobDtnDir, String appDtnDir)
    {
        // Is the application and exec system shared with this user?
        if (!isSharingDtnSystemId()) return;
        
        // Is the directory unset in the job submission request (explicitly or by inheritance)?
        if (TapisConstants.TAPIS_NOT_SET.equals(jobDtnDir)) return;
        
        // Sharing's in effect if the app and job directories are the same.
        if (appDtnDir.equals(jobDtnDir)) _sharedAppCtxAttribs.add(attrib);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemId:                                                       */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemId() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_ID);}
   
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemExecDir:                                                  */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemExecDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_EXEC_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemInputDir:                                                 */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemInputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_INPUT_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemOutputDir:                                                */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemOutputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_OUTPUT_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingArchiveSystemId:                                                    */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingArchiveSystemId() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingArchiveSystemDir:                                                   */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingArchiveSystemDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingDtnSystemId:                                                        */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingDtnSystemId() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_DTN_SYSTEM_ID);}
   
    /* ---------------------------------------------------------------------------- */
    /* isSharingDtnSystemInputDir:                                                  */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingDtnSystemInputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_DTN_SYSTEM_INPUT_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingDtnSystemOutputDir:                                                 */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingDtnSystemOutputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_DTN_SYSTEM_OUTPUT_DIR);}
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingExecSystemAppOwner:                                                */
    /* ---------------------------------------------------------------------------- */
    public String getSharingExecSystemAppOwner() {
    	if (isSharingExecSystemId()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingExecSystemExecDirAppOwner:                                         */
    /* ---------------------------------------------------------------------------- */
    public String getSharingExecSystemExecDirAppOwner() {
    	if (isSharingExecSystemExecDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingContainerImageUrlAppOwner:                                         */
    /* ---------------------------------------------------------------------------- */
    public String getSharingContainerImageUrlAppOwner() {
        // containerImage can only be defined in the app so it is always considered
        //   shared if we are in a shared app context.
        if (_sharingEnabled) return _sharedAppOwner;
        else return NOT_SHARED_APP_OWNER;
    }

    /* ---------------------------------------------------------------------------- */
    /* getSharingExecSystemInputDirAppOwner:                                        */
    /* ---------------------------------------------------------------------------- */
    public String getSharingExecSystemInputDirAppOwner() {
    	if (isSharingExecSystemInputDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingExecSystemOutputDirAppOwner:                                       */
    /* ---------------------------------------------------------------------------- */
    public String getSharingExecSystemOutputDirAppOwner() {
    	if (isSharingExecSystemOutputDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingArchiveSystemAppOwner:                                             */
    /* ---------------------------------------------------------------------------- */
    public String getSharingArchiveSystemAppOwner() {
    	if (isSharingArchiveSystemId()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingArchiveSystemDirAppOwner:                                          */
    /* ---------------------------------------------------------------------------- */
    public String getSharingArchiveSystemDirAppOwner() {
    	if (isSharingArchiveSystemDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingDtnSystemAppOwner:                                                */
    /* ---------------------------------------------------------------------------- */
    public String getSharingDtnSystemAppOwner() {
    	if (isSharingDtnSystemId()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingDtnSystemInputDirAppOwner:                                         */
    /* ---------------------------------------------------------------------------- */
    public String getSharingDtnSystemInputDirAppOwner() {
    	if (isSharingDtnSystemInputDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharingDtnSystemOutputDirAppOwner:                                        */
    /* ---------------------------------------------------------------------------- */
    public String getSharingDtnSystemOutputDirAppOwner() {
    	if (isSharingDtnSystemOutputDir()) return _sharedAppOwner;
    	else return NOT_SHARED_APP_OWNER;
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
    public boolean isSharingEnabled() {return _sharingEnabled;}
    public String getSharedAppOwner() {return _sharedAppOwner;} 
    public List<JobSharedAppCtxEnum> getSharedAppCtxResources() {return _sharedAppCtxAttribs;}
}
