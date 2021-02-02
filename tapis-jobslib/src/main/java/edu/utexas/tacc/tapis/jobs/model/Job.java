package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;
import io.swagger.v3.oas.annotations.media.Schema;

public final class Job
{
	// Constants.
	public static final int DEFAULT_NODE_COUNT = 1;
	public static final int DEFAULT_CORES_PER_NODE = 1;
	public static final int DEFAULT_MEM_MB = 100;
	public static final int DEFAULT_MAX_MINUTES = 10;
	public static final int MAX_LAST_MESSAGE_LEN = 16384;
	public static final Boolean DEFAULT_ARCHIVE_ON_APP_ERROR = Boolean.TRUE;
	public static final Boolean DEFAULT_USE_DTN = Boolean.TRUE;
	public static final Boolean DEFAULT_DYNAMIC_EXEC_SYSTEM = Boolean.FALSE;
	public static final String EMPTY_JSON = "{}";
	
	// Default directory assignments.  All paths are relative to their system's 
	// rootDir unless otherwise noted.  Leading slashes are optional on relative
	// paths and required on absolute paths.  When the full path names of relative 
	// paths are constructed, double slashes at the point of concatenation are prevented.
	public static final String DEFAULT_EXEC_SYSTEM_INPUT_DIR   = "/${JobWorkingDir}/jobs/${JobUUID}";
	public static final String DEFAULT_EXEC_SYSTEM_OUTPUT_DIR  = DEFAULT_EXEC_SYSTEM_INPUT_DIR + "/output";
	public static final String DEFAULT_DTN_SYSTEM_INPUT_DIR    = "/${DtnMountPoint}/jobs/${JobUUID}";
	public static final String DEFAULT_DTN_SYSTEM_OUTPUT_DIR   = DEFAULT_DTN_SYSTEM_INPUT_DIR + "/output";
    public static final String DEFAULT_ARCHIVE_SYSTEM_DIR      = "/jobs/${JobUUID}/archive";
    public static final String DEFAULT_DTN_SYSTEM_ARCHIVE_DIR  = DEFAULT_DTN_SYSTEM_INPUT_DIR + "/archive";
	
    // Prefix for reserved template variables (macros).
    public static final String TAPIS_ENV_VAR_PREFIX = "_tapis";
	
	// Fields
    private int      			id;
    private String   			name;
    private String   			owner;
    private String   			tenant;
    private String   			description;
    
    private JobStatusType   	status = JobStatusType.PENDING;
    
    private String   			lastMessage;
    private Instant  			created;
    private Instant  			ended;
    private Instant  			lastUpdated;
    
    private String   			uuid;
    private String   			appId;
    private String   			appVersion;
    
    private boolean  			archiveOnAppError = DEFAULT_ARCHIVE_ON_APP_ERROR;
    private boolean             dynamicExecSystem = DEFAULT_DYNAMIC_EXEC_SYSTEM;
    
    private String   			execSystemId;
    private String   			execSystemExecDir;
    private String   			execSystemInputDir;
    private String   			execSystemOutputDir;
    private String              execSystemLogicalQueue;
    
    private String   			archiveSystemId;
    private String   			archiveSystemDir;
    
    private String              dtnSystemId;
    private String              dtnMountSourcePath;
    private String              dtnMountPoint;
    
    private int      			nodeCount = DEFAULT_NODE_COUNT;
    private int      			coresPerNode = DEFAULT_CORES_PER_NODE;
    private int      			memoryMB = DEFAULT_MEM_MB;
    private int      			maxMinutes = DEFAULT_MAX_MINUTES;
    
    private String   			fileInputs = EMPTY_JSON;
    private String   			parameterSet = EMPTY_JSON;
    private String              execSystemConstraints;
    private String              subscriptions = EMPTY_JSON;
    
    private int      			blockedCount;
    private String   			remoteJobId;
    private String   			remoteJobId2;
    private JobRemoteOutcome   	remoteOutcome;
    private String   			remoteResultInfo;
    private String   			remoteQueue;
    private Instant  			remoteSubmitted;
    private Instant  			remoteStarted;
    private Instant  			remoteEnded;
    private int      			remoteSubmitRetries;
    private int      			remoteChecksSuccess;
    private int      			remoteChecksFailed;
    private Instant  			remoteLastStatusCheck;
    
    private String              inputTransactionId;
    private String              inputCorrelationId;
    private String              archiveTransactionId;
    private String              archiveCorrelationId;
    
    private String   			tapisQueue;
    private boolean  			visible = true;
    private String   			createdby;
    private String   			createdbyTenant;
    private TreeSet<String>     tags;
    
    // ------ Runtime-only fields that do not get saved in the database ------
    // -----------------------------------------------------------------------
    
    // Store a reference to the execution context as soon as the worker 
    // creates the context in TenantQueueProcessor.
    @Schema(hidden = true)
    private transient JobExecutionContext _jobCtx;
    
    // Only one command at a time is stored, so there's the possibility
    // of an unread command being overwritten, but sending multiple
    // asynchronous commands to a job is indeterminate anyway. The field
    // contains last asynchronous message sent to this job that hasn't been read.
    @Schema(hidden = true)
    private final transient AtomicReference<CmdMsg> _cmdMsg = new AtomicReference<>(null);
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public Job()
    {
    	// This value only gets overwritten when populating from db.
    	setUuid(new TapisUUID(UUIDType.JOB).toString());
    	
    	// Set the initial time.
    	Instant now = Instant.now();
    	setCreated(now);
    	setLastUpdated(now);
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* toString:                                                                    */
    /* ---------------------------------------------------------------------------- */
    @Override
    public String toString() {return TapisUtils.toString(this);}

    /* ---------------------------------------------------------------------------- */
    /* constructDefaultExecSystemExecDir:                                           */
    /* ---------------------------------------------------------------------------- */
    /** Construct the default path name for the exec system exec directory given the
     * specified input directory.  If the input directory is null, then the exec
     * directory path name is constructed relative to the default input directory.
     * 
     * @param inputDir the path name relative to which the exec directory path name 
     *                 is constructed or null 
     * @return the constructed exec directory path name
     */
    public static String constructDefaultExecSystemExecDir(String inputDir, boolean useDTN)
    {
        if (StringUtils.isBlank(inputDir)) 
            if (useDTN) return DEFAULT_DTN_SYSTEM_INPUT_DIR;
              else return DEFAULT_EXEC_SYSTEM_INPUT_DIR;
        return inputDir;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* constructDefaultExecSystemOutputDir:                                         */
    /* ---------------------------------------------------------------------------- */
    /** Construct the default path name for the exec system output directory given the
     * specified input directory.  If the input directory is null, then the exec
     * directory path name is constructed relative to the default input directory.
     * 
     * @param inputDir the path name relative to which the output directory path name 
     *                 is constructed or null 
     * @return the constructed output directory path name
     */
    public static String constructDefaultExecSystemOutputDir(String inputDir, boolean useDTN)
    {
        if (StringUtils.isBlank(inputDir)) 
            if (useDTN) return DEFAULT_DTN_SYSTEM_OUTPUT_DIR;
              else return DEFAULT_EXEC_SYSTEM_OUTPUT_DIR;
        return StringUtils.removeEnd(inputDir, "/") + "/output";
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

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

	public JobStatusType getStatus() {
		return status;
	}

	public void setStatus(JobStatusType status) {
		this.status = status;
	}

	public String getLastMessage() {
		return lastMessage;
	}

	public void setLastMessage(String lastMessage) {
		this.lastMessage = lastMessage;
	}

	@Schema(type = "string")
	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	@Schema(type = "string")
	public Instant getEnded() {
		return ended;
	}

	public void setEnded(Instant ended) {
		this.ended = ended;
	}

	@Schema(type = "string")
	public Instant getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Instant lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

	public boolean isArchiveOnAppError() {
		return archiveOnAppError;
	}

	public void setArchiveOnAppError(boolean archiveOnAppError) {
		this.archiveOnAppError = archiveOnAppError;
	}

	public boolean isDynamicExecSystem() {
		return dynamicExecSystem;
	}

	public void setDynamicExecSystem(boolean dynamicExecSystem) {
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

    public String getDtnSystemId() {
        return dtnSystemId;
    }

    public void setDtnSystemId(String dtnSystemId) {
        this.dtnSystemId = dtnSystemId;
    }

    public String getDtnMountSourcePath() {
        return dtnMountSourcePath;
    }

    public void setDtnMountSourcePath(String dtnMountSourcePath) {
        this.dtnMountSourcePath = dtnMountSourcePath;
    }

    public String getDtnMountPoint() {
        return dtnMountPoint;
    }

    public void setDtnMountPoint(String dtnMountPoint) {
        this.dtnMountPoint = dtnMountPoint;
    }

	public int getNodeCount() {
		return nodeCount;
	}

	public void setNodeCount(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	public int getCoresPerNode() {
		return coresPerNode;
	}

	public void setCoresPerNode(int coresPerNode) {
		this.coresPerNode = coresPerNode;
	}

	public int getMemoryMB() {
		return memoryMB;
	}

	public void setMemoryMB(int memoryMB) {
		this.memoryMB = memoryMB;
	}

	public int getMaxMinutes() {
		return maxMinutes;
	}

	public void setMaxMinutes(int maxMinutes) {
		this.maxMinutes = maxMinutes;
	}

	public String getFileInputs() {
		return fileInputs;
	}

	public void setFileInputs(String inputs) {
		this.fileInputs = inputs;
	}

	public String getParameterSet() {
		return parameterSet;
	}

	public void setParameterSet(String parameters) {
		this.parameterSet = parameters;
	}

	public String getExecSystemConstraints() {
		return execSystemConstraints;
	}

	public void setExecSystemConstraints(String execSystemConstraints) {
		this.execSystemConstraints = execSystemConstraints;
	}

	public String getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(String subscriptions) {
		this.subscriptions = subscriptions;
	}

	public int getBlockedCount() {
		return blockedCount;
	}

	public void setBlockedCount(int blockedCount) {
		this.blockedCount = blockedCount;
	}

	public String getRemoteJobId() {
		return remoteJobId;
	}

	public void setRemoteJobId(String remoteJobId) {
		this.remoteJobId = remoteJobId;
	}

	public String getRemoteJobId2() {
		return remoteJobId2;
	}

	public void setRemoteJobId2(String remoteJobId2) {
		this.remoteJobId2 = remoteJobId2;
	}

	public JobRemoteOutcome getRemoteOutcome() {
		return remoteOutcome;
	}

	public void setRemoteOutcome(JobRemoteOutcome remoteOutcome) {
		this.remoteOutcome = remoteOutcome;
	}

	public String getRemoteResultInfo() {
		return remoteResultInfo;
	}

	public void setRemoteResultInfo(String remoteResultInfo) {
		this.remoteResultInfo = remoteResultInfo;
	}

	public String getRemoteQueue() {
		return remoteQueue;
	}

	public void setRemoteQueue(String remoteQueue) {
		this.remoteQueue = remoteQueue;
	}

	@Schema(type = "string")
	public Instant getRemoteSubmitted() {
		return remoteSubmitted;
	}

	public void setRemoteSubmitted(Instant remoteSubmitted) {
		this.remoteSubmitted = remoteSubmitted;
	}

	@Schema(type = "string")
	public Instant getRemoteStarted() {
		return remoteStarted;
	}

	public void setRemoteStarted(Instant remoteStarted) {
		this.remoteStarted = remoteStarted;
	}

	@Schema(type = "string")
	public Instant getRemoteEnded() {
		return remoteEnded;
	}

	public void setRemoteEnded(Instant remoteEnded) {
		this.remoteEnded = remoteEnded;
	}

	public int getRemoteSubmitRetries() {
		return remoteSubmitRetries;
	}

	public void setRemoteSubmitRetries(int remoteSubmitRetries) {
		this.remoteSubmitRetries = remoteSubmitRetries;
	}

	public int getRemoteChecksSuccess() {
		return remoteChecksSuccess;
	}

	public void setRemoteChecksSuccess(int remoteChecksSuccess) {
		this.remoteChecksSuccess = remoteChecksSuccess;
	}

	public int getRemoteChecksFailed() {
		return remoteChecksFailed;
	}

	public void setRemoteChecksFailed(int remoteChecksFailed) {
		this.remoteChecksFailed = remoteChecksFailed;
	}

	@Schema(type = "string")
	public Instant getRemoteLastStatusCheck() {
		return remoteLastStatusCheck;
	}

	public void setRemoteLastStatusCheck(Instant remoteLastStatusCheck) {
		this.remoteLastStatusCheck = remoteLastStatusCheck;
	}

	public String getTapisQueue() {
		return tapisQueue;
	}

	public void setTapisQueue(String tapisQueue) {
		this.tapisQueue = tapisQueue;
	}

    public TreeSet<String> getTags() {
        return tags;
    }

    public void setTags(TreeSet<String> tags) {
        this.tags = tags;
    }
    
	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean visible) {
		this.visible = visible;
	}

	public String getCreatedby() {
		return createdby;
	}

	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}

	public String getCreatedbyTenant() {
		return createdbyTenant;
	}

	public void setCreatedbyTenant(String createdbyTenant) {
		this.createdbyTenant = createdbyTenant;
	}

    public String getInputTransactionId() {
        return inputTransactionId;
    }

    public void setInputTransactionId(String inputTransactionId) {
        this.inputTransactionId = inputTransactionId;
    }

    public String getInputCorrelationId() {
        return inputCorrelationId;
    }

    public void setInputCorrelationId(String inputCorrelationId) {
        this.inputCorrelationId = inputCorrelationId;
    }

    public String getArchiveTransactionId() {
        return archiveTransactionId;
    }

    public void setArchiveTransactionId(String archiveTransactionId) {
        this.archiveTransactionId = archiveTransactionId;
    }

    public String getArchiveCorrelationId() {
        return archiveCorrelationId;
    }

    public void setArchiveCorrelationId(String archiveCorrelationId) {
        this.archiveCorrelationId = archiveCorrelationId;
    }

    // Get the current cmdMsg value and atomically set the field to null.
    @Schema(hidden = true)
    public CmdMsg getAndSetCmdMsg() {
        return _cmdMsg.getAndSet(null);
    }
    
    // Get the current cmdMsg value and atomically set the field to a new value.
    @Schema(hidden = true)
    public CmdMsg getAndSetCmdMsg(CmdMsg cmdMsg) {
        return _cmdMsg.getAndSet(cmdMsg);
    }

    @Schema(hidden = true)
    public void setCmdMsg(CmdMsg cmdMsg) {
        _cmdMsg.set(cmdMsg);
    }

    @Schema(hidden = true)
    public JobExecutionContext getJobCtx() {
        return _jobCtx;
    }

    @Schema(hidden = true)
    public void setJobCtx(JobExecutionContext jobCtx) {
        this._jobCtx = jobCtx;
    }

}
