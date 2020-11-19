# AsynchronousAMQPClient, Robust Threaded Asynchronous AMQP Client
# To be extended by Producer and Consumer
# Matthew Currie - Nov 2020

import pika # AMQP Client

from threading import Thread, Lock
import functools
from queue import Queue
import logging
import time
from enum import Enum
import json # queue bindings cache

# SSL is required to manipulate SSLContext of 
# TLS connection
from ssl import SSLContext 

from pprint import pprint

# Regarding blocking:
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

class ClientType(Enum):
    Unspecified = 0,
    Producer = 1,
    Consumer = 2

# Connection Progress
# Establish connection
# Open channel
# 

class AsynchronousAmqpClient(Thread):

    # Connection related
    _connection = None
    _channel = None
    _reconnect_attempts = 0
    should_reconnect = False
    _amqp_url = None
    
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
    message_queue = Queue()

    def __init__(self, client_type, **kwargs):

        # disable pika logging for now
        pika_logger = logging.getLogger("pika").setLevel(logging.CRITICAL)

        self._client_type = client_type

        # process keyword arguments
        loglevel = kwargs.get("loglevel", logging.INFO)
        self._amqp_url = kwargs.get("url", self._amqp_url)
        self._host = kwargs.get("host", None)
        self._amqp_application_name = kwargs.get("amqp_app_name", None)
        self.logger = kwargs.get("logger", None)
        self._module_name = kwargs.get("module_name", None)
        self._connection_parameters = kwargs.get("connection_parameters", None)
        
        # module name for the purpose of logging and disk writes
        if not self._module_name:
            if client_type == ClientType.Producer:
                self._module_name = "AMQP-Producer"
            elif client_type == ClientType.Consumer:
                self._module_name = "AMQP-Consumer"
            else:
                self._module_name = "AMQP-Client"
        
        # configure logging
        if not self.logger:
            self.logger = logging.getLogger(self._module_name)
        self.logger.setLevel(loglevel)

        # consumer specific?
#        self.was_consuming = False
#        self._consumer_tag = None
#        self._consuming = False
        
        # dynamic creation of queue bindings cache file
        self._bindings_cache_filename = "%s-bindings.cache" % self._module_name.lower()

        # threading.Thread constructor
        super().__init__()
    
    # convenience method to add a routing_key pattern
    # to the class instance list of bindings
    # must be done prior to calling connection.connect (Thread::start())
    def add_binding(self, routing_key_pattern):
        self._bindings.append(routing_key_pattern)

    def connect(self):
        # connect to RabbitMQ
        # returning the connection handle 'pika.SelectConnection'
        # asynchronous callbacks: on_connection_open, on_open_error_callback,
        # and on_open_error_callback

        if self._connection_parameters:
            parameters = self._connection_parameters
        elif self._amqp_url and self._amqp_url.startswith("amqps://"):
            # Currently unable to connect to MSC AMQP broker due to certificate
            # mismatch unless we pass an empty ssl.SSLContext() to the underlying
            # SSL socket
            # Could the issue of certificate mismatch with MSC AMQP broker
            # be related to missing CA certs on our client side?
            parameters = pika.URLParameters(self._amqp_url)
            # add this to bypass certificate errors
            parameters.ssl_options = pika.SSLOptions(SSLContext())
        elif self._amqp_url and self._amqp_url.startswith("amqp://"):
            parameters = pika.URLParameters(self._amqp_url)
        elif self._host:
            # simple host-only scenario
            parameters = pika.ConnectionParameters(self._host, 5672, '/')
        else:
            # default to localhost
            parameters = pika.ConnectionParameters("localhost", 5672, '/')
        
        # todo - add connection parameters to this log entry
        self.logger.info("Connecting to AMQP Broker")
            
        connection = pika.SelectConnection(parameters=parameters,
                on_open_callback=self.on_connection_open,
                on_open_error_callback=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed)
            
        return connection

    # reimplement me!
    def ready(self):
        pass

    # AMQP Client is read.
    # Connection and Channel are established and open
    # Exchanges, Queues, and Bindings are declared
    # QoS (Prefetch) is completed
    def client_ready(self):
        self._reconnect_attempts = 0 # reset
        self.ready()

    def close_connection(self):
        self.logger.debug('closing connection')
        self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        self.logger.debug('connection open')
        self.open_channel()

    def on_connection_open_error(self, connection, error):
        # called by pika if the connection to RabbitMQ can't be established.

        import socket # for exception class comparisons

        # general AMQPConnectionError exception which may have other classes wrapped
        if isinstance(error, pika.exceptions.AMQPConnectionError):
            if len(error.args) > 1:
                self.logger.debug("heads up! pika.exceptions.AMQPConnectionError.args == %s" % len(error.args))
            else:
                error = error.args[-1] # pass on AMQPConnectionWorkflowFailed exception
        
        if isinstance(error, pika.adapters.utils.connection_workflow.AMQPConnectionWorkflowFailed):
            
            # Extract exception value from the last connection attempt
            error = error.exceptions[-1]
            
            if isinstance(error,pika.adapters.utils.connection_workflow.AMQPConnectorSocketConnectError):               
                #self.logger.debug("isinstance(error, pika.adapters.utils.connection_workflow.AMQPConnectorSocketConnectError)")
                
                # TCP Timeout
                if isinstance(error.exception,socket.timeout):
                    self.logger.error("TCP connection attempt timed out!")

            # Exception raised for address-related errors by getaddrinfo() and getnameinfo()
            #   eg. gaierror(-3, 'Temporary failure in name resolution')
            if (isinstance(error, socket.gaierror)):
                self.logger.error('Connection failure: %s' % (error.strerror))
            
        else:
            self.logger.error('Connection open failed: %s', type(error))
            self.logger.debug("type(error) = %s" % type(error))
        
        # the ioloop stays in play despite losing the connection so we need to force
        # it to exit so we can move on with code execution in self::run()
        self._connection.ioloop.stop()

    def on_connection_closed(self, _unused_connection, reason):
        # method called when connection is closed
        # param pika.connection.Connection connection: The closed connection obj
        # param Exception reason: exception representing reason for loss of
        #    connection.

        if self.should_reconnect:
            # this connection closing was not expected, eg. it was not called
            # through self::stop()
            self.logger.warning('Connection closed: %s', reason)
        
        # the ioloop stays in play despite losing the connection so we need to force
        # it to exit so we can move on with code execution in self::run()
        self._connection.ioloop.stop()
        
    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        self._connection.channel(on_open_callback=self.on_channel_open)
        
    def close_channel(self):
        # Close channel cleanly
        # Send RabbitMQ Channel.Close RPC command
        self.logger.debug('Closing the channel')
        self._channel.close()

    def add_binding(self, queue, exchange, routing_key):
        binding = {}
        binding['queue'] = queue
        binding['exchange'] = exchange
        binding['routing_key'] = routing_key
        self._bindings.append(binding)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        self.logger.debug('channel opened')
        
        self._channel = channel
        self.add_on_channel_close_callback()
        
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        if self.should_reconnect:
            self.logger.warning('Channel was closed unexpectedly: %s' % reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        if self._exchange and self._setup_exchange_enabled:
            self.logger.debug('Declaring exchange: %s', exchange_name)
            # Note: using functools.partial is not required, it is demonstrating
            # how arbitrary data can be passed to the callback when it is called
            cb = functools.partial(
                self.on_exchange_declareok, userdata=exchange_name)
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=self.EXCHANGE_TYPE,
                callback=cb)
        else:
            # proceed directly to queue setup
            self.setup_queue(self._queue)
                
    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        self.logger.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        if self._setup_queue_enabled and self._queue:
            cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
            self._channel.queue_declare(queue=queue_name, callback=cb, durable=self._queue_durable)
        else:
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
                    self.logger.debug("loaded %s queue bindings from %s: " % (len(bindings_cache), self._bindings_cache_filename))
                    if len(bindings_cache):
                        return bindings_cache
            except FileNotFoundError:
                pass # this is OK
            except Exception as e:
                self.logger.error("error loading queue bindings cache %s" % e)

        return []                    
        #raise Exception("self._bindings_cache_filename not populated")
    
    # call Client::client_ready() when this stage is completed
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
                self.logger.debug("there are %s queue bindings to cleanup" % num_bindings_to_cleanup)
                
                # check to see if there are bindings to clean 
                # ONLY if we have been directed to do so
                if self._bindings_cleanup_enabled and len(self._bindings_to_cleanup):
                    binding = self._bindings_to_cleanup.pop()
                    self.logger.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                    self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
                else:
                    # there are either no bindings to cleanup or we are 
                    # not directed to do so
                    self.write_queue_bindings_cache()
                    self.client_ready()

            except Exception as e:
                self.logger.error(e)

        else: 
            # We are in a callback return.
            # There are 

            self._bindings_unbound += 1
            
            if len(self._bindings_to_cleanup):
                binding = self._bindings_to_cleanup.pop()
                self.logger.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
            else:
                self.logger.info("cleaned up %s bindings" % self._bindings_unbound)
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
                    self.logger.debug('routing key binded: %s', self._bindings[self._queues_bound-1])
                else:
                    self.logger.error("routing key NOT binded: %s", self._bindings[self._queues_bound-1])

            # check if we are either done, or have no bindings to do
            if self._queues_bound < len(self._bindings):
                binding = self._bindings[self._queues_bound]
                self.logger.debug("channel.queue_bind(%s,%s,routing_key=%s)" % (binding['queue'], binding['exchange'], binding['routing_key']))
                self._channel.queue_bind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_binds)
                self._queues_bound += 1
            else:
                self.logger.debug("done adding %s routing_key bindings" % (int(self._queues_bound)))
                self.do_queue_bindings_cleanup()
        else:
            self.do_queue_bindings_cleanup()

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        self.logger.debug('declared queue: %s', userdata)
        self.do_queue_binds()

#        hdrs = {u'?????': u' ????', u'?': u'?', u'??': u'?'}
#        properties = pika.BasicProperties(
#            app_id='example-publisher',
#            content_type='application/json',
#            headers=hdrs)
#
#        message = u'????? ???? ? ? ?? ?'
#        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
#                                    json.dumps(message, ensure_ascii=False),
#                                    properties)
#        self._message_number += 1
#        self._deliveries.append(self._message_number)
#        self.logger.info('Published message # %i', self._message_number)
#        self.schedule_next_message()

#    def stop_producer(self):
#        """Stop the example by closing the channel and connection. We
#        set a flag here so that we stop scheduling new messages to be
#        published. The IOLoop is started because this method is
#        invoked by the Try/Catch below when KeyboardInterrupt is caught.
#        Starting the IOLoop again will allow the publisher to cleanly
#        disconnect from RabbitMQ.
#        """
#        self.logger.debug('Stopping')
#        self._stopping = True
#        self.close_channel()
#        self.close_connection()

    # this method is called by the thread worker after calling the start()
    # of this instance class
    def run(self):        
        
        self._connection = None
        
        # default to reconnecting on unexpected channel and connection closures
        self.should_reconnect = True
        while self.should_reconnect:
            
            # call Producer or Consumer specific methods
            self.prepare_connection()
            
            self._connection = self.connect()
            self._connection.ioloop.start()
            
            # establish reconnect parameters with adaptive delays
            if self.should_reconnect:
                
                if not self._reconnect_attempts:
                    self.logger.info("reconnecting..")
                    self._reconnect_attempts = 1
                else:
                    # 1,2,4,8,16,32,64,128, etc
                    delay_time = self._reconnect_attempts * self._reconnect_attempts 
                    max_delay_time = 120
                    if delay_time > max_delay_time: delay_time = max_delay_time
                    self.logger.info("delaying reconnect attempt # %s for %s seconds" % (self._reconnect_attempts,delay_time))
                    
                    self._reconnect_attempts += 1
                    
                    # we are no longer blocking on the ioloop so we cant block here too long
                    # or we will be non-responsive to a shutdown request while waiting
                    delay_10_ms = delay_time * 100
                    for i in range(delay_10_ms):
                        if self.should_reconnect:
                            time.sleep(0.01)
                        else:
                            break

        # we have completed work in this method
        self.logger.debug('AsynchronousAmqpClient::run() work done; thread exiting..')

    # reimplement me!
    def stop_activity(self):
        self._closing = True
        if self._channel:
            self.close_channel()

    # call this method if you would like to gracefully shut down the consumer
    # this will result in the thread exiting once the ioloop has completed
    def stop(self):
        
        self.should_reconnect = False
        self.logger.debug('AsynchronousAmqpClient::stop() called; requested a threadsafe callback for ioloop to call stop_consuming()')
        
        # add a thread-safe callback to the ioloop which will allow
        # us to stop the ioloop from another thread. No methods are
        # are thread-safe to interact with the ioloop!
        self._connection.ioloop.add_callback_threadsafe(self.stop_activity)
        
