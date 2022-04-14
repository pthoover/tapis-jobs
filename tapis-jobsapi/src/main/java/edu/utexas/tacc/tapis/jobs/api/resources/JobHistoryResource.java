package edu.utexas.tacc.tapis.jobs.api.resources;

import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.responses.RespJobHistory;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.dto.JobHistoryDisplayDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/")
public class JobHistoryResource extends AbstractResource {
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobHistoryResource.class);
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    /* Jax-RS context dependency injection allows implementations of these abstract
     * types to be injected (ch 9, jax-rs 2.0):
     * 
     *      javax.ws.rs.container.ResourceContext
     *      javax.ws.rs.core.Application
     *      javax.ws.rs.core.HttpHeaders
     *      javax.ws.rs.core.Request
     *      javax.ws.rs.core.SecurityContext
     *      javax.ws.rs.core.UriInfo
     *      javax.ws.rs.core.Configuration
     *      javax.ws.rs.ext.Providers
     * 
     * In a servlet environment, Jersey context dependency injection can also 
     * initialize these concrete types (ch 3.6, jersey spec):
     * 
     *      javax.servlet.HttpServletRequest
     *      javax.servlet.HttpServletResponse
     *      javax.servlet.ServletConfig
     *      javax.servlet.ServletContext
     *
     * Inject takes place after constructor invocation, so fields initialized in this
     * way can not be accessed in constructors.
     */ 
     @Context
     private HttpHeaders        _httpHeaders;
  
     @Context
     private Application        _application;
  
     @Context
     private UriInfo            _uriInfo;
  
     @Context
     private SecurityContext    _securityContext;
  
     @Context
     private ServletContext     _servletContext;
  
     @Context
     private HttpServletRequest _request;

     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getJobStatus:                                                                      */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{jobUuid}/history")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Retrieve history of a previously submitted job by its UUID.\n\n"
                           + "The caller must be the job owner, creator or a tenant administrator."
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job's history retrieved.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespJobHistory.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Job not found.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
     public Response getJobHistory(@PathParam("jobUuid") String jobUuid, @QueryParam("limit") int limit, 
				@QueryParam("skip") int skip, @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobHistory", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                      entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // orderBy is of the format - fname1(desc), fname2, fname3(asc), ...
       // ThreadContext designed to never return null for SearchParameters
       SearchParameters srchParms = threadContext.getSearchParameters();
        
       if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
       int totalCount = -1; 
       
       // ------------------------- Retrieve Job Status-----------------------------
       JobStatusDTO jobstatus = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(),
                       threadContext.getOboTenantId(),JobResourceShare.JOB_HISTORY.name(),
                       JobTapisPermission.READ.name());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }
       catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }

       // ------------------------- Process Results --------------------------
       // Adjust status based on whether we found the job.
       if (jobstatus == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
       
       } else if(!jobstatus.getVisible()) {
    	   String msg = MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid, threadContext.getOboTenantId());
       	   _log.warn(msg);
       	   ResultName missingName = new ResultName();
       	   missingName.name = jobUuid;
       	   RespName r = new RespName(missingName);
       	   return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
            msg, prettyPrint, r)).build();
       }
       
       List<JobEvent> events = null;
       try {
		 events =jobsImpl.getJobEventsByJobUuid(jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId(),srchParms.getLimit(), srchParms.getSkip() );
        _log.debug("number of events: " + events.size());
       	} catch (TapisImplException e) {
    	   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       	} catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }
       
       List<JobHistoryDisplayDTO> jobHists = null;
       if(events != null) {
    	  try {
			jobHists = jobsImpl.getJobEventsSummary(events, threadContext.getOboUser(), threadContext.getOboTenantId());
		} catch (TapisImplException e) {
			_log.error(e.getMessage(), e);
	           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       	} catch (Exception e) {
	           _log.error(e.getMessage(), e);
	           return Response.status(Status.INTERNAL_SERVER_ERROR).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       }
       }
       // Success.
       // TODO OrderBy, limit
       RespJobHistory r = new RespJobHistory(jobHists, srchParms.getLimit(), srchParms.getOrderBy(), srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
	     
       return Response.status(Status.OK).entity(TapisRestUtils
    		   .createSuccessResponse(
               MsgUtils.getMsg("JOBS_HISTORY_RETRIEVED",  threadContext.getOboTenantId(),threadContext.getOboUser(),jobUuid), prettyPrint, r)).build();
     }
}
