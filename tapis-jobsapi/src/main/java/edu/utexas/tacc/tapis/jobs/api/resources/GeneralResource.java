package edu.utexas.tacc.tapis.jobs.api.resources;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.security.PermitAll;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.responses.RespProbe;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/")
public final class GeneralResource
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(GeneralResource.class);
    
    // Database check timeouts.
    private static final long DB_READY_TIMEOUT_MS  = 6000;   // 6 seconds.
    private static final long DB_HEALTH_TIMEOUT_MS = 60000;  // 1 minute.
    
    // The table we query during readiness checks.
    private static final String QUERY_TABLE = "jobs";
    
    // Keep track of the last db monitoring outcome.
    private static final CallSiteToggle _lastQueryDBSucceeded = new CallSiteToggle();
    private static final CallSiteToggle _lastQueryTenantsSucceeded = new CallSiteToggle();
    private static final CallSiteToggle _lastQueryQueueManagerSucceeded = new CallSiteToggle();
    
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
     
     // Count the number of healthchecks requests received.
     private static final AtomicLong _healthChecks = new AtomicLong();
    
     // Count the number of healthchecks requests received.
     private static final AtomicLong _readyChecks = new AtomicLong();
     
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* hello:                                                                       */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Path("/hello")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(
          description = "Logged connectivity test. No authorization required.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Message received.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "500", description = "Server error.")}
      )
  public Response sayHello(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
      // Trace this request.
      if (_log.isTraceEnabled()) {
          String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hello", 
                                     "  " + _request.getRequestURL());
          _log.trace(msg);
      }
      
      // Create the response payload.
      RespBasic r = new RespBasic("Hello from the Tapis Jobs Service.");
         
      // ---------------------------- Success ------------------------------- 
      // Success means we found the resource. 
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_FOUND", "hello", "0 items"), prettyPrint, r)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* healthcheck:                                                                 */
  /* ---------------------------------------------------------------------------- */
  /** This method does no logging and is expected to be as lightwieght as possible.
   * It's intended as the endpoint that monitoring applications can use to check
   * the liveness (i.e, no deadlocks) of the application.  In particular, 
   * kubernetes can use this endpoint as part of its pod health check.
   * 
   * Note that no JWT is required on this call.
   * 
   * A good synopsis of the difference between liveness and readiness checks:
   * 
   * ---------
   * The probes have different meaning with different results:
   * 
   *    - failing liveness probes  -> restart pod
   *    - failing readiness probes -> do not send traffic to that pod
   *    
   * See https://stackoverflow.com/questions/54744943/why-both-liveness-is-needed-with-readiness
   * ---------
   * 
   * @return a success response if all is ok
   */
  @GET
  @Path("/healthcheck")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(
          description = "Lightwieght health check for liveness. No authorization required.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Message received.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class))),
               @ApiResponse(responseCode = "503", description = "Service unavailable.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class)))}
      )
  public Response checkHealth()
  {
      // Assign the current check count to the probe result object.
      var jobsProbe = new JobsProbe();
      jobsProbe.checkNum = _healthChecks.incrementAndGet();
      
      // Check the database.
      if (queryDB(DB_HEALTH_TIMEOUT_MS)) jobsProbe.databaseAccess = true; 
      
      // Check the tenant manager.
      if (queryTenants()) jobsProbe.tenantsAccess = true;
      
      // Check rabbitmq.
      if (queryQueueMananger()) jobsProbe.queueAccess = true;
      
      // Create the response object.
      RespProbe r = new RespProbe(jobsProbe);
      
      // Failure case.
      if (jobsProbe.failed()) {
        String msg = MsgUtils.getMsg("TAPIS_NOT_HEALTHY", "Jobs Service");
        return Response.status(Status.SERVICE_UNAVAILABLE).
            entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // ---------------------------- Success ------------------------------- 
      // Create the response payload.
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_HEALTHY", "Jobs Service"), false, r)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* ready:                                                                       */
  /* ---------------------------------------------------------------------------- */
  /** This method does no logging and is expected to be as lightwieght as possible.
   * It's intended as the endpoint that monitoring applications can use to check
   * whether the application is ready to accept traffic.  In particular, kubernetes 
   * can use this endpoint as part of its pod readiness check.
   * 
   * Note that no JWT is required on this call.
   * 
   * A good synopsis of the difference between liveness and readiness checks:
   * 
   * ---------
   * The probes have different meaning with different results:
   * 
   *    - failing liveness probes  -> restart pod
   *    - failing readiness probes -> do not send traffic to that pod
   *    
   * See https://stackoverflow.com/questions/54744943/why-both-liveness-is-needed-with-readiness
   * ---------
   * 
   * @return a success response if all is ok
   */
  @GET
  @Path("/ready")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(
          description = "Lightwieght readiness check. No authorization required.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Service ready.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class))),
               @ApiResponse(responseCode = "503", description = "Service unavailable.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class)))}
      )
  public Response ready()
  {
      // Assign the current check count to the probe result object.
      var jobsProbe = new JobsProbe();
      jobsProbe.checkNum = _readyChecks.incrementAndGet();
      
      // Check the database.
      if (queryDB(DB_READY_TIMEOUT_MS)) jobsProbe.databaseAccess = true; 
      
      // Check the tenant manager.
      if (queryTenants()) jobsProbe.tenantsAccess = true;
      
      // Check rabbitmq.
      if (queryQueueMananger()) jobsProbe.queueAccess = true;
      
      // Create the response object.
      RespProbe r = new RespProbe(jobsProbe);
      
      // Failure case.
      if (jobsProbe.failed()) {
        String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Jobs Service");
        return Response.status(Status.SERVICE_UNAVAILABLE).
            entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // ---------------------------- Success -------------------------------
      // Create the response payload.
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_READY", "Jobs Service"), false, r)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* notificationsLiveness:                                                       */
  /* ---------------------------------------------------------------------------- */
  @POST
  @Path("/livenessNotification")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
          description = "Call back webhook used to test liveness of notification delivery.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Liveness acknowledged.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class))),
               @ApiResponse(responseCode = "400", description = "Input error.",
                   content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "401", description = "Not authorized.",
                   content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "500", description = "Jobs service error.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespProbe.class)))}
      )
  public Response livenessNotification()
  {
	  // We only accept calls from the Notifications service.
	  
	  // TODO: Maybe NotificationLiveness needs 2 threads, a sender and a receiver.
	  
	  
      // ---------------------------- Success -------------------------------
      // Create the response payload.
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_LIVENESS_ACK", "Jobs Service"), false)).build();
  }
  
  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* queryDB:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /** Probe the database with a simple database query and minimal logging.
   * 
   * @param timeoutMillis millisecond limit for success
   * @return true for success, false otherwise
   */
  private boolean queryDB(long timeoutMillis)
  {
      // Start optimistically.
      boolean success = true;
      
      // Any db error or a time expiration fails the connectivity check.
      try {
          // Try to run a simple query.
          long startTime = Instant.now().toEpochMilli();
          int result = getJobsImpl().queryDB(QUERY_TABLE);
          
          // Did the query take too long?
          long elapsed = Instant.now().toEpochMilli() - startTime;
          if (elapsed > timeoutMillis) {
              if (_lastQueryDBSucceeded.toggleOff()) {
                  String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", 
                                               "Excessive query time (" + elapsed + " milliseconds)");
                  _log.error(msg);
              }
              success = false;
          } else if (_lastQueryDBSucceeded.toggleOn())
              _log.info(MsgUtils.getMsg("TAPIS_PROBE_ERROR_CLEARED", "Jobs Service", "database"));
      }
      catch (Exception e) {
          // Any exception causes us to report failure on first recent occurrence.
          if (_lastQueryDBSucceeded.toggleOff()) {
              String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", e.getMessage());
              _log.error(msg, e);
          }
          success = false;
      }
      
      return success;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* queryTenants:                                                                */
  /* ---------------------------------------------------------------------------- */
  /** Retrieve the cached tenants map.
   * 
   * @return true if the map is not null, false otherwise
   */
  private boolean queryTenants()
  {
      // Start optimistically.
      boolean success = true;
      
      try {
          // Make sure the cached tenants map is not null.
          var tenantMap = TenantManager.getInstance().getTenants();
          if (tenantMap == null) {
              if (_lastQueryTenantsSucceeded.toggleOff()) {
                  String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", 
                                               "Null tenants map.");
                  _log.error(msg);
              }
              success = false;
          } else if (_lastQueryTenantsSucceeded.toggleOn())
              _log.info(MsgUtils.getMsg("TAPIS_PROBE_ERROR_CLEARED", "Jobs Service", "tenants"));
      } catch (Exception e) {
          if (_lastQueryTenantsSucceeded.toggleOff()) {
              String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", 
                                           e.getMessage());
              _log.error(msg, e);
          }
          success = false;
      }
      
      return success;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* queryQueueMananger:                                                          */
  /* ---------------------------------------------------------------------------- */
  /** Retrieve the singleton queue manager.
   * 
   * @return true if the queue manager initialized and is not null, false otherwise
   */
  private boolean queryQueueMananger()
  {
      // Start optimistically.
      boolean success = true;
      
      try {
          // Make sure the cached tenants map is not null.
          var qm = JobQueueManager.getInstance();
          if (qm == null) {
              if (_lastQueryQueueManagerSucceeded.toggleOff()) {
                  String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", 
                                               "Null QueueManager.");
                  _log.error(msg);
              }
              success = false;
          } else if (_lastQueryQueueManagerSucceeded.toggleOn())
              _log.info(MsgUtils.getMsg("TAPIS_PROBE_ERROR_CLEARED", "Jobs Service", "QueueManager"));
      } catch (Exception e) {
          if (_lastQueryQueueManagerSucceeded.toggleOff()) {
              String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Jobs Service", 
                                           e.getMessage());
              _log.error(msg, e);
          }
          success = false;
      }
      
      return success;
  }
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // Simple class to collect probe results.
  public final static class JobsProbe
  {
      public long    checkNum;
      public boolean databaseAccess;
      public boolean tenantsAccess;
      public boolean queueAccess;
      
      public boolean failed() {return !(databaseAccess && tenantsAccess && queueAccess);}
  }
}
