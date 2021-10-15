package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;


public final class RespJobSearchAllAttributes extends RespAbstract{
	public List<Job> result;
	public RespJobSearchAllAttributes(List<Job> jobList, int limit, String orderBy, int skip, String startAfter, int totalCount) {
		result = new ArrayList<>();
	    if(jobList != null) {
			for (Job job : jobList)
		    {
		      result.add(job);
		    }
	    }

	    ResultListMetadata meta = new ResultListMetadata();
	    meta.recordCount = result.size();
	    meta.recordLimit = limit;
	    meta.recordsSkipped = skip;
	    meta.orderBy = orderBy;
	    meta.startAfter = startAfter;
	    meta.totalCount = totalCount;
	    metadata = meta;
	  }
	

}
