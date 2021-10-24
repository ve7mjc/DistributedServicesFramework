# AsynchronousAMQPClient, Robust Threaded Asynchronous AMQP Client
# To be extended by Producer and Consumer
# Matthew Currie - Nov 2020

import dsf.domain
from dsf.component import Component

from os import environ

import pika # AMQP Library

import functools
from queue import Queue
import logging
import time
from enum import Enum
import json # queue bindings cache

# SSL is required to manipulate SSLContext of 
# TLS connection
from ssl import SSLContext 

# Regarding blocking:
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

#from distributedservicesframework.statistics import Statistics
#from dsf.services import Component
#from dsf.messaging import MessageAdapter

class ClientType(Enum):
    Unspecified = 0,
    Producer = 1,
    Consumer = 2

#
# Client Ready Process:
# Configure and Establish TCP connection with on_open_error_callback,
#   on_open_callback, and on_close_callback
# Configure and Open Channel with on_open_callback
#   call channel.add_on_close_callback(..) for on_close callback
# Create Exchange(s) [optional] with callback
# Create Queue(s) [optional] with callback
# Configure Exchange-Queue Bindings [optional] with callback
# Unbind unused Exchange-Queue Bindings [optional] with callback
# Notify sub-classes (Producer or Consumer) client is ready via ::client_ready()
class AmqpClient(Component):

    # declare me in a Producer, Consumer, class etc
    _client_type = ClientType.Unspecified

    # Connection related
    _connection = None
    _channel = None
    _reconnect_attempts = 0
    _automatic_reconnect = True
    
    _exchange = None
    _exchange_type = None
    _queue = None
    _bindings = []
    
    # default client behaviors
    _setup_queue_enabled = False
    _setup_exchange_enabled = False
    _setup_bindings_enabled = False
    _bindings_cleanup_enabled = False
    
    _queues_bound = 0
    _queue_durable = False
    
    # thread-safe message queueu for:
    # outgoing, non-blocking publish with asynchronous ack
    # incoming messages
    #message_queue = Queue()
    
    # let the Component base class know we will require special with start() 
    # and stop() calls
    _threaded = True

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        #self.config_init(**kwargs)

        # disable pika logging for now
        pika_logger = logging.getLogger("pika").setLevel(logging.CRITICAL) # logging.CRITICAL

        # process keyword arguments
        loglevel = kwargs.get("loglevel", logging.DEBUG)
        loglevel = kwargs.get("level", loglevel) # alternate key name
        
        # moved this into child class
        # self._amqp_url = self.config.get("url", None)

        self._host = self.config.get("host", None)
        self._amqp_application_name = self.config.get("amqp_app_name", None)
        self._name = self.config.get("name", None)
        self._connection_parameters = self.config.get("connection_parameters",None)

        self._username = self.config.get("username", None)
        self._password = self.config.get("username", None)

#        statistics = kwargs.get("statistics", None)
#        self._amqp_url = kwargs.get("url", self._amqp_url)
#        self._host = kwargs.get("host", None)
#        self._amqp_application_name = kwargs.get("amqp_app_name", None)
#        self._name = kwargs.get("name", None)
#        self._connection_parameters = kwargs.get("connection_parameters", None)
        
        bindings = self.config.get("bindings",list())
        for binding in bindings:
            self.add_binding(*binding)

        # module name for the purpose of logging and disk writes
        if not self._name:
            if self._client_type == ClientType.Producer:
                self._name = "amqpclient.producer"
            elif self._client_type == ClientType.Consumer:
                self._name = "amqpclient.consumer"
            else:
                self._name = "amqpclient"

        # dynamic creation of queue bindings cache file
        self._bindings_cache_filename = "%s-bindings.cache" % self._name.lower()


    # Method called when this AMQP client has completed its connection
    # steps and a Producer or Consumer can now begin their specific steps.
    # Override me in a child class!
    def client_ready(self):
        self.log.warning("AmqpClient.client_ready() called - needs to be overridden!")

    # Add a routing_key pattern
    # Exchanges and Queues must be declared in order for this to be useful
    # must be done prior to calling connection.connect ( Thread.start() )
#    def add_binding(self, routing_key_pattern):
#        self._bindings.append(routing_key_pattern)

    # Connect to RabbitMQ using the asynchronous SelectConnection adapter
    # Returns a connection handle 'pika.SelectConnection'
    # Registers asynchronous callbacks: on_connection_open, 
    #   on_open_error_callback, and on_open_error_callback
    def connect(self):
        if self._connection_parameters:
            parameters = self._connection_parameters
        elif self._amqp_url and self._amqp_url.startswith("amqps://"):
            # Currently unable to connect to MSC AMQP broker due to certificate
            # mismatch unless we pass an empty ssl.SSLContext() to the underlying
            # SSL socket
            # Could the issue of certificate mismatch with MSC AMQP broker
            # be related to missing CA certs on our client side?
            parameters = pika.URLParameters(self._amqp_url)
            # bypass certificate errors
            parameters.ssl_options = pika.SSLOptions(SSLContext())
        elif self._amqp_url and self._amqp_url.startswith("amqp://"):
            parameters = pika.URLParameters(self._amqp_url)
        elif self._host: # simple host-only scenario
            parameters = pika.ConnectionParameters(self._host, 5672, '/')
        else: # default to localhost
            parameters = pika.ConnectionParameters("localhost", 5672, '/')
        
        # todo - add connection parameters to this log entry
        self.log.debug("AMQP Connecting to %s" % parameters)

        self.log.debug("AMQP client using URL %s" % self._amqp_url)

        return pika.SelectConnection(parameters=parameters,
                    on_open_callback=self.on_connection_open,
                    on_open_error_callback=self.on_connection_open_error,
                    on_close_callback=self.on_connection_closed)

    # Callback method called by pika once the RabbitMQ connection has
    # been established.
    # Passes pika.SelectConnection handle which we should already have
    # Open a RabbitMQ channel by sending a Channel.Open RPC Command with 
    # an on_open callback
    def on_connection_open(self, _unused_connection):
        self.log.info('AMQP connected to %s' % self._amqp_url)
        self._connection.channel(on_open_callback=self.on_channel_open)

    # Callback method called by pika if the connection to RabbitMQ 
    # cannot be established.
    def on_connection_open_error(self, connection, error):

        import socket # for exception class comparisons

        # general AMQPConnectionError exception which may have other classes wrapped
        if isinstance(error, pika.exceptions.AMQPConnectionError):
            if len(error.args) > 1:
                self.log.info("heads up! pika.exceptions.AMQPConnectionError.args == %s" % len(error.args))
            else:
                error = error.args[-1] # pass on AMQPConnectionWorkflowFailed exception
        
        if isinstance(error, pika.adapters.utils.connection_workflow.AMQPConnectionWorkflowFailed):
            
            # Extract exception value from the last connection attempt
            error = error.exceptions[-1]
            
            if isinstance(error,pika.adapters.utils.connection_workflow.AMQPConnectorSocketConnectError):               
                #self.log.debug("isinstance(error, pika.adapters.utils.connection_workflow.AMQPConnectorSocketConnectError)")
                
                # TCP Timeout
                if isinstance(error.exception,socket.timeout):
                    self.log.error("AMQP connection timed out!")

            # Exception raised for address-related errors by getaddrinfo() and getnameinfo()
            #   eg. gaierror(-3, 'Temporary failure in name resolution')
            if (isinstance(error, socket.gaierror)):
                self.log.error('AMQP connection failure: %s' % (error.strerror))
            
        else:
            self.log.error('Connection open failed: %s', type(error))
            self.log.debug("type(error) = %s" % type(error))
        
        # the ioloop stays in play despite losing the connection so we need to force
        # it to exit so we can move on with code execution in self::run()
        self._connection.ioloop.stop()

    # Close the RabbitMQ connection
    def close_connection(self):
        self.log.debug('closing connection')
        if self._connection and self._connection.is_open and not self._connection.is_closing:
            self._connection.close()

    # Callback Method called from pika connection when connection is closed
    # param pika.connection.Connection connection: The closed connection obj
    # param Exception reason: exception representing reason for loss of
    #    connection.
    def on_connection_closed(self, connection, reason):
        # this connection closing was unexpected - stop() was not called
        if self.keep_working:
            self.log.warning('AMQP connection closed: %s', reason)
        # the ioloop stays in play despite losing the connection so we need to force
        # it to exit so we can move on with code execution in self::run()
        self._connection.ioloop.stop()

    # AMQP Client is read.
    # Connection and Channel are established and open
    # Exchanges, Queues, and Bindings are declared
    # QoS (Prefetch) is completed
    def _client_ready(self):
        self._reconnect_attempts = 0 # reset
        self.client_ready()

    # Callback method RabbitMQ calls when a channel has been opened
    # Passes ika.channel.Channel handle
    # Request a on_channel_close callback and proceed with session startup
    def on_channel_open(self, channel):
        self.log.debug('channel opened')        
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self._exchange)
        
    # Callback method called by RabbitMQ when a channel has been closed
    # Channels are closed any time a request has violated a protocol
    # TODO: Make sure we do not get into a reconnect flood here as we are 
    #  likely to repeat the same actions which caused this close
    # Passes the closed pika.channel.Channel and Exception reason
    def on_channel_closed(self, channel, reason):
        if self.keep_working:
            self.log.warning('AMQP channel was closed unexpectedly: %s' % reason)
        
        # a consumer 
        if self._connection.is_open and not self._connection.is_closing: 
            self._connection.close()

    # Cleanly close RabbitMQ channel
    # Send Channel.Close RPC command. Callback is already registered.
    def close_channel(self):
        self.log.debug('AMQP closing the channel')
        if self._channel.is_open and not self._channel.is_closing: 
            self._channel.close()

    # Method to allow us to specify a binding request prior to connection
    # which will be performed on connect and every reconnect
    def add_binding(self, queue, exchange, routing_key):
        binding = {}
        binding['queue'] = queue
        binding['exchange'] = exchange
        binding['routing_key'] = routing_key

        self._bindings.append(binding)
        self.log.debug("added binding: %s" % binding)
        
    # add by dict
    def add_bindings(self, bindings):
        if type(bindings) is not list:
            self.log.warning("bindings type is %s!" % type(bindings))
            return True
        for binding in bindings:
            self._bindings.append(binding)

    # Setup exchange on RabbitMQ
    # Send the Exchange.Declare RPC command with a callback method for pika
    def setup_exchange(self, exchange_name):
        if self._exchange and self._setup_exchange_enabled:
            self.log.debug('declaring exchange: %s', exchange_name)
            cb = functools.partial(
                self.on_exchange_declare, userdata=exchange_name)
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=self.EXCHANGE_TYPE,
                callback=cb)
            return

        # proceed directly to queue setup instead
        self.setup_queue(self._queue)
                
    # Callback method for pika on the outcome of Exchange.Declare RPC command
    # :param pika.Frame.Method method_frame Exchange.DeclareOk response
    # :param str|unicode userdata: Extra user data (exchange name)
    def on_exchange_declare(self, method_frame, userdata):
        self.log.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue)

    # Setup queue on RabbitMQ
    # Send the Queue.Declare RPC command with a callback request
    # :param str|unicode queue_name: The name of the queue to declare.
    def setup_queue(self, queue_name):
        if self._setup_queue_enabled and self._queue:
            cb = functools.partial(self.on_queue_declare, userdata=queue_name)
            self._channel.queue_declare(queue=queue_name, callback=cb, durable=self._queue_durable)
            return
        # or - skip directly to binding    
        self.do_queue_binds()

    # Callback Method for pika when Queue.Declare RPC call has finished
    # We may now begin Exchange<>Queue routing_key bindings
    # TODO: Confirm OK in frame?
    def on_queue_declare(self, method_frame, userdata):
        self.log.debug('declared queue: %s', userdata)
        self.do_queue_binds()

    # write/load a json file from disk containing a list of queues and bindings
    # for the purpose of cleaning up should our application request different
    def write_queue_bindings_cache(self):
        if self._bindings_cache_filename and len(self._bindings):
            bindings_cache = []
            for binding in self._bindings:
                bindings_cache.append(binding)
            with open(self._bindings_cache_filename, 'w') as outfile:
                json.dump(bindings_cache, outfile, indent=4)

    def load_queue_bindings_cache(self):
        if self._bindings_cache_filename:
            try:
                with open(self._bindings_cache_filename) as json_cache:
                    bindings_cache = json.load(json_cache)
                    self.log.debug("loaded %s queue bindings from %s: " % (len(bindings_cache), self._bindings_cache_filename))
                    if len(bindings_cache):
                        return bindings_cache
            except FileNotFoundError:
                pass # this is OK
            except Exception as e:
                self.log.error("error loading queue bindings cache %s" % e)

        return []                    
        #raise Exception("self._bindings_cache_filename not populated")
    
    # Recursive callback Method for Queue Binding cleanup
    # Clean up queue bindings if we determine that we have requested queue
    # bindings which are different than those we have requested in the past
    # This helps with scenarios where we do not have management access to a
    # RabbitMQ broker and we wish to clean up after ourselves
    # Client.client_ready() is called when this stage has completed
    def do_queue_bindings_cleanup(self, in_callback=False):
        # callback will be populated by the sender and so do our 
        # setup and initial action with callback, or proceed onward
        # if there is nothing to do
        if not in_callback:
            self._bindings_to_cleanup = []
            self._bindings_unbound = 0
            try:
                self._bindings_cache = self.load_queue_bindings_cache()
                
                # create a list of each binding which needs to be cleaned up 
                for cached_binding in self._bindings_cache:
                    cleanup = True
                    for binding in self._bindings:
                        if cached_binding == binding:
                            cleanup = False
                    if cleanup:
                        self._bindings_to_cleanup.append(cached_binding)

                num_bindings_to_cleanup = len(self._bindings_to_cleanup)
                self.log.debug("there are %s queue bindings to cleanup" % num_bindings_to_cleanup)
                
                # check to see if there are bindings to clean 
                # ONLY if we have been directed to do so
                if self._bindings_cleanup_enabled and len(self._bindings_to_cleanup):
                    binding = self._bindings_to_cleanup.pop()
                    self.log.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                    self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
                else:
                    # there are either no bindings to cleanup or we are 
                    # not directed to do so
                    self.write_queue_bindings_cache()
                    self._client_ready()

            except Exception as e:
                self.log.error(e)

        else: 
            # We are in a callback return.
            self._bindings_unbound += 1
            if len(self._bindings_to_cleanup):
                binding = self._bindings_to_cleanup.pop()
                self.log.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
            else:
                self.log.info("cleaned up %s bindings" % self._bindings_unbound)
                del self._bindings_to_cleanup
                del self._bindings_unbound
                self.write_queue_bindings_cache()
                self.client_ready()

    # matt added to handle multiple routing keys
    # Queue.BindOk response frame will be passed when in a callback
    # When loop is completed, we head to do_queue_bindings_cleanup()
    def do_queue_binds(self, callback=None):
        if self._setup_bindings_enabled:
            # called each time we return from a callback
            # check if we are in a callback
            if type(callback) is pika.frame.Method:
                if type(callback.method) is pika.spec.Queue.BindOk:
                    self.log.debug('routing key binded: %s', self._bindings[self._queues_bound-1])
                else:
                    self.log.error("routing key NOT binded: %s", self._bindings[self._queues_bound-1])

            # check if we are either done, or have no bindings to do
            if self._queues_bound < len(self._bindings):
                binding = self._bindings[self._queues_bound]
                self.log.debug("channel.queue_bind(%s,%s,routing_key=%s)" % (binding['queue'], binding['exchange'], binding['routing_key']))
                self._channel.queue_bind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_binds)
                self._queues_bound += 1
            else:
                self.log.debug("done adding %s routing_key bindings" % (int(self._queues_bound)))
                self.do_queue_bindings_cleanup()
        else:
            self.do_queue_bindings_cleanup()

    # This method is called by the Child.run() method after being called by
    # the Thread.start() method.
    def run(self):
        
        if hasattr(self,"pre_run_checks"):
            self.pre_run_checks()
        
        # default to reconnecting on unexpected channel and connection closures
        while self.keep_working:
    
            try:
                # Make sure that calling methods may check the status of connection
                # or throw an exception should it be illegally accessed
                self._connection = None
                
                self.prepare_connection() # child class specific methods
                
                # non-blocking, asynchronous call immediately returns a
                # connection object
                self._connection = self.connect() 
                
                # block here until released
                self.log.debug("calling self._connection.ioloop.start()")
                try:
                    self._connection.ioloop.start()
                except Exception as e:
                    print(type(e))
                    self.log.exception()
                    pass
                    #self.log.exception(e)
                self.log.debug("passed self._connection.ioloop.start()")

                # We have disconnected
                # Was this requested, and if not, is automatic reconnect enabled?
                # Otherwise, we must repeat this loop immediately the first time
                # and then increase our loop delay on subsequent reconect attempts
                # FANCY DELAY BLOCK!
                if self.keep_working and self._automatic_reconnect:
                    
                    self.set_failed("connection failed")
                    
                    if not self._reconnect_attempts:
                        # first reconnect attempt
                        self.log.info("reconnecting..")
                        self._reconnect_attempts = 1
                    else:
                        # 1,2,4,8,16,32,64,128, etc
                        delay_time = self._reconnect_attempts * self._reconnect_attempts 
                        max_delay_time = 120
                        if delay_time > max_delay_time: delay_time = max_delay_time
                        self.log.info("delaying reconnect attempt # %s for %s seconds" % (self._reconnect_attempts,delay_time))
                        
                        self._reconnect_attempts += 1
                        
                        # we are no longer blocking on the ioloop so we cant block here too long
                        # or we will be non-responsive to a shutdown request while waiting
                        # check stop_request frequently
                        delay_10_ms = delay_time * 100
                        for i in range(delay_10_ms):
                            if not self._stop_requested:
                                time.sleep(0.01)
                            else: break
                        
                # Connection has disconnected but automatic reconnect is disabled
                # Not sure who would be crazy enough to roll this way
                if not self._automatic_reconnect and self.keep_working:
                    self.set_failed("connection failed; auto reconnect is disabled")
                    self.stop("connection failed")
                    break

            except Exception as e:
                self.log.exception(stacklevel=4)
                self.set_failed("exception in run loop")
                return

        # we have completed work in this method
        self.log.debug('thread exiting')

    # call this method if you would like to gracefully shut down the consumer
    # this will result in the thread exiting once the ioloop has completed
    def stop(self,reason=None):
        
        self.log.debug("AmqpClient.stop() called")
        super().stop(reason)
        
        # No methods in this class, nor other threads are safe to interact 
        # with the ioloop and must instead register a callback request
        # self._connection is None before and after connection and when connected
        # <class 'pika.adapters.select_connection.SelectConnection'>
        # If a child class has defined a stop_activity, we will pass execution
        #  there for a graceful shutdown, otherwise try closing the channel and
        #  then connection
        if hasattr(self,"stop_activity"):
            if self._connection and self._connection.is_open and not self._connection.is_closing:
                self.log.debug("calling stop_activity with threadsafe callback")
                self._connection.ioloop.add_callback_threadsafe(self.stop_activity)
            else:
                if not self._connection:
                    self.log.warning("client.stop() called but self._connection = (%s)" % (type(self._connection)))
                else:
                    self.log.warning("client.stop() called but self._connection.is_open = %s, self._connection.is_closing=%s" 
                        % (self._connection.is_open,self._connection.is_closing))