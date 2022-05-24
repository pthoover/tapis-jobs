package edu.utexas.tacc.tapis.jobs.reader;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager.ExchangeUse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This class reads the tenant's alternate queue, records the incident and
 * sends an email to the support email address.  RabbitMQ puts messages on 
 * alternate queues when a message is unroutable.
 * 
 * This class services all tenants and thus does not use the tenant id 
 * passed in as a parameter.  For convenience, the main method will supply
 * a pseudo-tenant id so that one does not have to be provided on the 
 * command line.
 * 
 * This class does not currently access the database so it can be started 
 * with the environment setting 'aloe.db.connection.pool.size=0'.  This will
 * cause a runtime exception if any attempt is made to connect to the
 * database.
 * 
 * @author rcardone
 */
public final class EventQueueReader
 extends AbstractQueueReader
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(EventQueueReader.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The tenant's alternate queue name.
    private final String _queueName;
    
    // Name of the exchange used by this queue.
    private final String _exchangeName;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public EventQueueReader(QueueReaderParameters parms) 
    {
        // Assign superclass field.
        super(parms);
        
        // Force use of the default binding key.
        _parms.bindingKey = JobQueueManagerNames.DEFAULT_BINDING_KEY;
        
        // Save the queue name in a field.
        _queueName = JobQueueManagerNames.getEventQueueName();
        
        // Save the exchange name;
        _exchangeName = JobQueueManagerNames.getEventExchangeName();
        
        // Print configuration.
        _log.info(getStartUpInfo(_queueName, _exchangeName));
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) 
     throws JobException 
    {
        // Parse the command line parameters.
        QueueReaderParameters parms = null;
        try {parms = new QueueReaderParameters(args);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_START_ERROR", e.getMessage());
            _log.error(msg, e);
            throw e;
          }
        
        // Start the worker.
        EventQueueReader reader = new EventQueueReader(parms);
        reader.start();
    }

    /* ---------------------------------------------------------------------- */
    /* start:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Initialize the process and its threads. */
    public void start()
      throws JobException
    {
      // Announce our arrival.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STARTED", _parms.name, 
                                    _queueName, getBindingKey()));
      
      // Start reading the queue.
      readQueue();
      
      // Announce our termination.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STOPPED", _parms.name, 
                                    _queueName, getBindingKey()));
    }

    /* ********************************************************************** */
    /*                            Protected Methods                           */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* process:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Process a delivered message.
     * 
     * @param delivery the incoming message and its metadata
     * @return true if the message was successfully processed, false if the 
     *          message should be rejected and discarded without redelivery
     */
    @Override
    protected boolean process(DeliveryResponse delivery)
    {
        // Tracing
        if (_log.isDebugEnabled()) { 
            String msg = JobQueueManager.getInstance().dumpMessageInfo(
              delivery.consumerTag, delivery.envelope, delivery.properties, delivery.body);
            _log.debug(msg);
        }
        
        // The body should always be a UTF-8 json string.
        String body;
        try {body = new String(delivery.body, "UTF-8");}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("ALOE_BYTE_ARRAY_DECODE", new String(Hex.encodeHex(delivery.body)));
                _log.error(msg);
                return false;
            }
        
        // Decode the input.
        JobEvent jobEvent = null;
        try {jobEvent = TapisGsonUtils.getGson(true).fromJson(body, JobEvent.class);}
            catch (Exception e) {
                if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
                String msg = MsgUtils.getMsg("ALOE_JSON_PARSE_ERROR", getName(), body, e.getMessage());
                _log.error(msg, e);
                return false;
            }
        
        // Make sure we got some message type.
        if (jobEvent.getEvent() == null) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", "null", getName());
            _log.error(msg);
            return false;
        }
        
        // Determine the precise command type, populate an object of that type
        // and then call the command-specific processor.
        boolean ack = true;
      
        return ack;
    }

    /* ---------------------------------------------------------------------- */
    /* getName:                                                               */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getName() {return _parms.name;}

    /* ---------------------------------------------------------------------- */
    /* getExchangeType:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected BuiltinExchangeType getExchangeType() {return BuiltinExchangeType.FANOUT;}
    
    /* ---------------------------------------------------------------------- */
    /* getExchangeName:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getExchangeName() {return _exchangeName;}
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeUse:                                                              */
    /* ---------------------------------------------------------------------------- */
    @Override
    protected ExchangeUse getExchangeUse() {return ExchangeUse.ALT;}
    
    /* ---------------------------------------------------------------------- */
    /* getQueueName:                                                          */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getQueueName() {return _queueName;}

    /* ---------------------------------------------------------------------- */
    /* getBindingKey:                                                         */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getBindingKey() {return _parms.bindingKey;}
}
