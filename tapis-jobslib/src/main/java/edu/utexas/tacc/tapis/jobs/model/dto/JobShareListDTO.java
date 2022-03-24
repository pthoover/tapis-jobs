package edu.utexas.tacc.tapis.jobs.model.dto;

import java.time.Instant;

import edu.utexas.tacc.tapis.jobs.model.JobShared;

public class JobShareListDTO {
	 
	    private String   			tenant;
	    private String   			createdby;
	    private String   			jobUuid;
	    private String   			grantee;
	    private String          	jobResource ;
	    private String               jobPermission;
	    private Instant  			created;
	    private Instant  			lastUpdated;
	      
     JobShareListDTO(JobShared js){
    	 
     }

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getCreatedby() {
		return createdby;
	}

	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	public String getGrantee() {
		return grantee;
	}

	public void setGrantee(String grantee) {
		this.grantee = grantee;
	}

	public String getJobResource() {
		return jobResource;
	}

	public void setJobResource(String jobResource) {
		this.jobResource = jobResource;
	}

	public String getJobPermission() {
		return jobPermission;
	}

	public void setJobPermission(String jobPermission) {
		this.jobPermission = jobPermission;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	public Instant getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Instant lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
     
}
