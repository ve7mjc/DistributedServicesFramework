# Asynchronous, Threaded AMQP Consumer
# Matthew Currie - Nov 2020

import dsf.domain

from pika.exceptions import ChannelWrongStateError

import functools # callbacks
from datetime import datetime

from dsf import utilities, exceptionhandling
from dsf.amqp.asynchronousclient import AsynchronousClient, ClientType
from dsf.amqp import amqputilities
from dsf.amqp.amqpmessage import AmqpMessage

#from dsf.messageadapters import MessageInputAdapter

# todo, if channel is closed on connect - it will not quit nicely (hangs)
#2020-11-16 00:12:53,062 pika.channel WARNING Received remote Channel.Close (404): "NOT_FOUND - no queue 'weather_test' in vhost '/'" on <Channel number=1 OPEN conn=<SelectConnection OPEN transport=<pika.adapters.utils.io_services_utils._AsyncPlaintextTransport object at 0x7f0c659fcc10> params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>
#2020-11-16 00:12:53,062 msc-processor.Consumer INFO Closing connection
#2020-11-16 00:12:53,064 msc-processor.Consumer WARNING Connection closed, reconnect necessary: (200, 'Normal shutdown')
#2020-11-16 00:12:53,064 msc-processor.Consumer DEBUG requested a threadsafe callback for ioloop to call stop_consuming()
#2020-11-16 00:12:59,588 msc-processor.Consumer DEBUG requested a threadsafe callback for ioloop to call stop_consuming()

# This Asynchronous Consumer will operate in one of two modes:
# - Shared Queue mode: If a reference to a Queue object is passed in to the 
#    constructor as a "message_queue" keyword-argument, the consumer will
#    begin placing received messages in this Queue as they arrive.
# - Derived Method: If a derived subclass declares an method named on_message(),
#    the messages will be passed to it. See this class _on_message(..) method
#    for prototype.

# Test Modes
# amqp_nack_requeue_all_messages
# todo - should we respond to amqp.consumer.* ?
class AsynchronousConsumer(AsynchronousClient):

    _client_type = ClientType.Consumer
    
    # Message Input Adapter
    _adapter_name = "AmqpConsumer"

    _exchange = None
    _exchange_type = None
    _queue = None
    
    # Default behavior
    _prefetch_count = 1
    _durable_queue = False
    should_reconnect = False
    _do_bindings_cleanup = False
    _application_name = None
    _declare_queue = True # should we attempt to create the queue on the server?
    
    # scratch vars
    _reconnect_attempts = 0
    _queues_bound = 0
    last_message_received_time = None
    _bindings = []
    _bindings_cache = None
    _bindings_to_cleanup = []
    _strip_routing_key_prefix = None
    
    # leave in classdef to make child class init more likely
    _ack_disabled_max_preflight = False
    _prefetch_count_pre_disable = None
    
    # Consumer will place messages if this is a Queue type
    _receive_messages_queue = None
    _receive_message_callback = None
    
    def __init__(self, **kwargs):

        self._queue = kwargs.get("queue", None)
        
        self._received_messages_queue = kwargs.get("message_queue", None)
        self._strip_routing_key_prefix = kwargs.get("strip_key_prefix", None)
        
        self._consumer_tag = None
        self._consuming = False

        super().__init__(**kwargs) # agent is configured here

        #if not self._queue: self.set_failed("a queue has not been specified")
        
    def pre_run_checks(self):
        if not self._queue: self.set_failed("queue not defined")

    # Called when the underlying AMQP Client is ready
    # The TCP (or TLS) connection is established, channel is open, 
    # exchanges, queues, and bindings are declared.
    # Consumer specific actions must be performed:
    # Set Basic.QoS - qos(prefetch)
    # Add Basic.Cancel callback and Send Basic.Consume RPC
    def client_ready(self):
        if self._prefetch_count is not None:
            self.set_qos(self._prefetch_count)
        else: self.start_consuming()
    
    # Consumer is ready. Messages may begin flowing.
    def consumer_ready(self):
        self._ready = True
        self.logger.debug("AMQP Consumer is ready.")

    # called prior to connection (and reconnection) attempt
    # declare here to quiet the base class warnings
    def prepare_connection(self): pass
        
    # testing mode
    # disable message ack and set pre_fetch to 0 which will
    # result in the flight of potentially the entire queue
    def enable_no_ack_max_prefetch_test_mode(self, value=True):
        self._ack_disabled_max_preflight = value
        if self._ack_disabled_max_preflight:
            self._prefetch_count_pre_disable = self._prefetch_count
            self._prefetch_count = 0
            self.logger.info("enable_no_ack_test_mode(True) called; disabling Basic.Ack RPC calls and setting prefetch to max (0) for testing")
        else:
            self._prefetch_count = self._prefetch_count_pre_disable
            self._prefetch_count_pre_disable = None
            self.logger.info("enable_no_ack_test_mode(False) called; enabling Basic.Ack RPC calls and setting prefetch back to %0" % self._prefetch_count_pre_disable)

    # Configure Consumer Message QoS/Prefetch by sending Basic.QoS to RabbitMQ
    def set_qos(self, prefetch_count):
        if prefetch_count: # do not send if not specified
            self.logger.debug("sending Basic.QoS (message prefetch) request for %d", self._prefetch_count)
            self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_basic_qos)
        else:
            self.start_consuming()

    # Callback method for pika when the Basic.QoS request has completed
    # We are ready to start consuming
    # :param pika.frame.Method method: The Basic.QosOk response frame
    # todo - confirm method_frame response
    def on_basic_qos(self, method):
        self.logger.debug('qos/prefetch successfully set to: %d', self._prefetch_count)
        self.start_consuming()

    # Configure Consumer and Begin
    # Send Basic.Cancel callback request
    # Send Basic.Consume RPC to begin consuming
    #   Provide callback for received messages
    #   Returns the unique RabbitMQ consumer tag
    def start_consuming(self):
        if not self._queue:
            self.log_error("queue not supplied for AMQP Consumer. Cannot consume.")
            return True
        try:
            self._channel.add_on_cancel_callback(self.on_consumer_cancelled_remote)
            self._consumer_tag = self._channel.basic_consume(
                self._queue, self._on_message, auto_ack=False)
            self._consuming = True
            self.consumer_ready()
        except:
            self.log_exception()
        
    @property
    def is_consuming(self): return self._consuming

    @property
    def consumer_tag(self): return self._consumer_tag

    # RabbitMQ has acknowledged cancellation of the consumer with a
    # Basic.CancelOk frame
    # We will now close the channel, which will result in an
    # on_channel_closed callback and then we will close the connection
    def on_consumer_cancelled_ok(self, _unused_frame, consumer_tag):
        self._consumer_tag = None
        self._consuming = False
        
        self.logger.debug('RabbitMQ acknowledged the cancellation of consumer with tag %s', consumer_tag)
        
        if self._channel.is_open: self._channel.close()
        elif self._connection.is_open: self._connection.close()

    # Consumer has been cancelled by remote server
    # RabbitMQ sent a Basic.Cancel for a consumer receiving messages
    # <class 'pika.frame.Method'> method_frame: The Basic.Cancel frame
    def on_consumer_cancelled_remote(self, method_frame):
        self._consumer_tag = None
        self._consuming = False
        
        self.logger.info('consumer was cancelled remotely, shutting down: %r', method_frame)
        
        if self._channel.is_open: self._channel.close()
        elif self._connection.is_open: self._connection.close()

        self.set_failed("consumer has been cancelled by remote server")

    # Callback method for pika when a message has arrived
    # 'pika.spec.Basic.Deliver' basic_deliver
    # 'pika.spec.BasicProperties' properties:
    # 'bytes' body
    def _on_message(self, _unused_channel, basic_deliver, properties, body):

        self.last_message_received_time = datetime.now()
        
        basic_deliver.routing_key = amqputilities.remove_routing_key_prefix(
            basic_deliver.routing_key,self._strip_routing_key_prefix)
        
        message = AmqpMessage(pika_tuple=(basic_deliver, properties, body))
            
        try:
            wrote_message = False
            # Determine where this message needs to go based on configuration
            # Check for a derived child class method named on_message(..)
            if hasattr(self,"on_message"):           
                self.logger.debug("Received Message # %s -> self.on_message(..); routing_key=%s; len(body)=%s" % (basic_deliver.delivery_tag, basic_deliver.routing_key, len(body)))
                self.on_message(message)
                wrote_message = True
            
            # Reference to a shared message Queue has been passed to the 
            # constructor. This occurs when this consumer is attached to 
            # a MessageProcessingPipeline among other use-cases
            elif self._received_messages_queue:
                self._received_messages_queue.put(message)
                self.logger.debug("Wrote Message # %s to Message Queue; routing_key=%s; len(body)=%s" % (basic_deliver.delivery_tag, basic_deliver.routing_key, len(body)))
            
            # Callback method has been registered with us. This is untested
            # and likely dangerous as the call will be coming from the ioloop
            elif self._receive_message_callback:
                self._receive_message_callback(message)
            
            else:
                self.logger.info("message received but neither an on_message(..) method or a message queue has been provided")
            

        except Exception as e:
            self.logger.error(exceptionhandling.traceback_string(e))

    # Acknowledge message delivery
    # Send Basic.Ack RPC with the delivery to the channel 
    # consumer_tag: optional check to ensure future ack requests match the
    #  proper consumer session (eg. following a consumer reconnect)
    #  delivery_tags are related to a particular consumer session only
    def ack_message(self, delivery_tag, multiple=False, consumer_tag=None):
        if self.test_mode("amqp_nack_requeue_all_messages"):
            self.logger.info("ack was requested for delivery_tag=%s but test_mode(amqp_nack_requeue_all_messages) is enabled" % delivery_tag)
            self.nack_message(delivery_tag, consumer_tag=consumer_tag, multiple=False, requeue=True)
            return
        if consumer_tag and consumer_tag != self._consumer_tag:
            self.logger.warning("ack for delivery_tag=%s,consumer_tag=%s but current consumer tag is %s!" % (delivery_tag,consumer_tag,self._consumer_tag))
        else:
            try:
                cb = functools.partial(self.__do_ack_message, delivery_tag, multiple)
                self._connection.ioloop.add_callback_threadsafe(cb)
            except ChannelWrongStateError:
                # this was moved from a method which was calling the self._channel directly
                # we not hot hit this exception any more
                self.logger.error("unable to ack msg # %s as channel is not open" % delivery_tag)
    
    # called from ioloop in a thread-safe callback
    def __do_ack_message(self, delivery_tag=0, multiple=False):
        try:
            self._channel.basic_ack(delivery_tag, multiple)
            self.logger.debug("requesting ioloop write an ack for %s to the channel" % delivery_tag)           
        except ChannelWrongStateError:
            self.logger.error("unable to ack msg # %s as channel is not open" % delivery_tag)

    # Send NACK for Message - Thread-Safe IOLoop Callback
    # channel.basic_nack(delivery_tag=None, multiple=False, requeue=True)
    # delivery-tag (integer) � int/long The server-assigned delivery tag
    # multiple (bool) � If set to True, the delivery tag is treated as �up 
    #   to and including�, so that multiple messages can be acknowledged with
    #   a single method. If set to False, the delivery tag refers to a sinegle 
    #   message. If the multiple field is 1, and the delivery tag is zero, 
    #   this indicates acknowledgement of all outstanding messages.
    # requeue (bool) � If requeue is true, the server will attempt to requeue 
    #   the message. If requeue is false or the requeue attempt fails the 
    #   messages are discarded or dead-lettered.
    def nack_message(self, delivery_tag, consumer_tag=None, multiple=False, requeue=False):
        cb = functools.partial(self.__do_nack_message, delivery_tag, consumer_tag, multiple, requeue)
        self._connection.ioloop.add_callback_threadsafe(cb)

    # called from ioloop in a thread-safe callback
    def __do_nack_message(self, delivery_tag, consumer_tag=None, multiple=False, requeue=False):
        if consumer_tag and consumer_tag != self._consumer_tag:
            self.logger.warning("nack for delivery_tag=%s,consumer_tag=%s but current consumer tag is %s!" % (delivery_tag,consumer_tag,self._consumer_tag))
        else:
            self.logger.debug("sending Basic.Nack message; delivery_tag=%s, multiple=%s, requeue=%s" % 
                (delivery_tag, multiple, requeue))
            cb = functools.partial(self._channel.basic_nack, delivery_tag, multiple, requeue)
            self._connection.ioloop.add_callback_threadsafe(cb)

    # Gracefully stop the Consumer
    # Send Basic.Cancel RPC command to RabbitMQ and register callback method
    # This method is only being called by the parent.stop() method
    # Make sure all calls are thread-safe since this was called from another 
    #  Thread and must manipulate the ioloop
    def stop_activity(self):
        # Cover a few different scenarios so we keep the noise down and
        # ensure we get a clean exit
        self.log_debug("consumer.stop_activity() called")
        if self._consuming:
            self.logger.debug('stopping consuming; sending Basic.Cancel RPC command')
            cb = functools.partial(self.on_consumer_cancelled_ok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)
        elif self._channel.is_open:
            # on_channel_close will be called after this where we will then
            #  shut down the connection
            self._connection.ioloop.add_callback_threadsafe(self.close_channel())
        else:
            # This scenario sees only the connection open. No consumer or channel.
            if self._connection.is_open:
                self._connection.ioloop.add_callback_threadsafe(self._connection.close())