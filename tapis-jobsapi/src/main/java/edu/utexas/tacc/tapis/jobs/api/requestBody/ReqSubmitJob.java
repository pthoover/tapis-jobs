package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInputArray;
import edu.utexas.tacc.tapis.jobs.model.submit.JobParameterSet;
import edu.utexas.tacc.tapis.shared.model.NotificationSubscription;
import io.swagger.v3.oas.annotations.media.Schema;

public class ReqSubmitJob 
 implements IReqBody
{
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
    private String   			    name;
    private String   			    owner;
    private String   			    tenant;
    private String   			    description;
    private String   			    appId;
    private String   			    appVersion;
    private String                  jobType;
    private Boolean  			    archiveOnAppError;  // not assigned by default
    private Boolean                 dynamicExecSystem;  // not assigned by default
    private String   			    execSystemId;
    private String   			    execSystemExecDir;
    private String   		     	execSystemInputDir;
    private String   			    execSystemOutputDir;
    private String                  execSystemLogicalQueue;  // can be null
    private String   			    archiveSystemId;
    private String   			    archiveSystemDir;
    private Integer   			    nodeCount;
    private Integer      		    coresPerNode;
    private Integer      		    memoryMB;
    private Integer      		    maxMinutes;
    private List<JobFileInput>      fileInputs;
    private List<JobFileInputArray> fileInputArrays;
    private JobParameterSet 	    parameterSet;             // assigned on first get
    private List<String>            execSystemConstraints;    // don't call--used internally only
    private List<String>            tags;                     // assigned on first get
    private List<NotificationSubscription> subscriptions;     // assigned on first get
    private Boolean                 isMpi;
    private String                  mpiCmd;
    private String                  cmdPrefix;
    
    // Constraints flattened and aggregated from app and job request.
    private transient String        consolidatedConstraints;          
    
    // Temporary storage for hpc queue name during request processing.
    private transient String        hpcQueueName;

	@Override
	public String validate() 
	{
		// Success.
		return null; // json schema validation is sufficient
	}
	
	/** --------------- Constraint Processing --------------- 
	 * 
	 * Combine the sql where clause fragments from the request and application
	 * constraints into one sql clause.  If neither are set, the combined clause
	 * is null.  The result is always placed in the synthetic consolidatedConstraints 
	 * field.
	 * 
	 * @param appConstraintList the application constraints, can be null
	 */
	public void consolidateConstraints(List<String> appConstraintList)
	{
	    // Flatten each list.
	    String reqConstraints;
	    if (execSystemConstraints == null || execSystemConstraints.isEmpty())
	        reqConstraints = "";
	      else reqConstraints = String.join(" ", execSystemConstraints);
	    String appConstraints;
	    if (appConstraintList == null || appConstraintList.isEmpty())
	        appConstraints = "";
	      else appConstraints = String.join(" ", appConstraintList);
	    
	    // Combine the sql content in a conjunction if necessary.
	    if (!reqConstraints.isEmpty() && !appConstraints.isEmpty())
	        consolidatedConstraints = "(" + reqConstraints + ") AND (" + appConstraints + ")";
	    else if (!reqConstraints.isEmpty()) consolidatedConstraints = reqConstraints;
	    else if (!appConstraints.isEmpty()) consolidatedConstraints = appConstraints;
	    else consolidatedConstraints = null;
	}
	
	@Schema(required = true)
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Schema(required = true)
	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	@Schema(required = true)
	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

	public Boolean getArchiveOnAppError() {
		return archiveOnAppError;
	}

	public void setArchiveOnAppError(Boolean archiveOnAppError) {
		this.archiveOnAppError = archiveOnAppError;
	}

	public Boolean getDynamicExecSystem() {
		return dynamicExecSystem;
	}

	public void setDynamicExecSystem(Boolean dynamicExecSystem) {
		this.dynamicExecSystem = dynamicExecSystem;
	}
	
	public String getExecSystemId() {
		return execSystemId;
	}

	public void setExecSystemId(String execSystemId) {
		this.execSystemId = execSystemId;
	}

	public String getExecSystemExecDir() {
		return execSystemExecDir;
	}

	public void setExecSystemExecDir(String execSystemExecDir) {
		this.execSystemExecDir = execSystemExecDir;
	}

	public String getExecSystemInputDir() {
		return execSystemInputDir;
	}

	public void setExecSystemInputDir(String execSystemInputDir) {
		this.execSystemInputDir = execSystemInputDir;
	}

	public String getExecSystemOutputDir() {
		return execSystemOutputDir;
	}

	public void setExecSystemOutputDir(String execSystemOutputDir) {
		this.execSystemOutputDir = execSystemOutputDir;
	}

    public String getExecSystemLogicalQueue() {
        return execSystemLogicalQueue;
    }

    public void setExecSystemLogicalQueue(String execSystemLogicalQueue) {
        this.execSystemLogicalQueue = execSystemLogicalQueue;
    }

    public String getArchiveSystemId() {
		return archiveSystemId;
	}

	public void setArchiveSystemId(String archiveSystemId) {
		this.archiveSystemId = archiveSystemId;
	}

	public String getArchiveSystemDir() {
		return archiveSystemDir;
	}

	public void setArchiveSystemDir(String archiveSystemDir) {
		this.archiveSystemDir = archiveSystemDir;
	}

	public Integer getNodeCount() {
		return nodeCount;
	}

	public void setNodeCount(Integer nodeCount) {
		this.nodeCount = nodeCount;
	}

	public Integer getCoresPerNode() {
		return coresPerNode;
	}

	public void setCoresPerNode(Integer coresPerNode) {
		this.coresPerNode = coresPerNode;
	}

	public Integer getMemoryMB() {
		return memoryMB;
	}

	public void setMemoryMB(Integer memoryMB) {
		this.memoryMB = memoryMB;
	}

	public Integer getMaxMinutes() {
		return maxMinutes;
	}

	public void setMaxMinutes(Integer maxMinutes) {
		this.maxMinutes = maxMinutes;
	}

	public List<JobFileInput> getFileInputs() {
	    if (fileInputs == null) fileInputs = new ArrayList<JobFileInput>();
		return fileInputs;
	}

	public void setFileInputs(List<JobFileInput> inputs) {
		this.fileInputs = inputs;
	}

    public List<JobFileInputArray> getFileInputArrays() {
        if (fileInputArrays == null) fileInputArrays = new ArrayList<JobFileInputArray>();
        return fileInputArrays;
    }

    public void setFileInputArrays(List<JobFileInputArray> inputs) {
        this.fileInputArrays = inputs;
    }

	public JobParameterSet getParameterSet() {
	    // Create parameter set on demand if needed.
	    if (parameterSet == null) parameterSet = new JobParameterSet();
		return parameterSet;
	}

	public void setParameterSet(JobParameterSet parameters) {
		this.parameterSet = parameters;
	}

	public List<String> getExecSystemConstraints() {
		return execSystemConstraints;
	}

	public void setExecSystemConstraints(List<String> execSystemConstraints) {
		this.execSystemConstraints = execSystemConstraints;
	}

	public List<NotificationSubscription> getSubscriptions() {
	    if (subscriptions == null) subscriptions = new ArrayList<NotificationSubscription>();
		return subscriptions;
	}

	public void setSubscriptions(List<NotificationSubscription> subscriptions) {
		this.subscriptions = subscriptions;
	}

    public List<String> getTags() {
        if (tags == null) tags = new ArrayList<String>();
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Boolean getIsMpi() {
        return isMpi;
    }

    public void setIsMpi(Boolean isMpi) {
        this.isMpi = isMpi;
    }

    public String getMpiCmd() {
        return mpiCmd;
    }

    public void setMpiCmd(String mpiCmd) {
        this.mpiCmd = mpiCmd;
    }

    public String getCmdPrefix() {
        return cmdPrefix;
    }

    public void setCmdPrefix(String cmdPrefix) {
        this.cmdPrefix = cmdPrefix;
    }

    @Schema(hidden = true)
    public String getConsolidatedConstraints() {
        return consolidatedConstraints;
    }

    @Schema(hidden = true)
    public String getHpcQueueName() {
        return hpcQueueName;
    }

    public void setHpcQueueName(String hpcQueueName) {
        this.hpcQueueName = hpcQueueName;
    }
}
