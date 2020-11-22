# Asynchronous, Threaded AMQP Consumer
# Matthew Currie - Nov 2020

from pika.exceptions import ChannelWrongStateError

import functools # callbacks
from datetime import datetime

from distributedservicesframework import utilities, exceptionhandling
from distributedservicesframework.amqp.asynchronousclient import AsynchronousClient, ClientType
from distributedservicesframework.amqp.amqpmessage import AmqpMessage

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callbacks
# with the ioloop itself

# todo, if channel is closed on connect - it will not quit nicely (hangs)
#2020-11-16 00:12:53,062 pika.channel WARNING Received remote Channel.Close (404): "NOT_FOUND - no queue 'weather_test' in vhost '/'" on <Channel number=1 OPEN conn=<SelectConnection OPEN transport=<pika.adapters.utils.io_services_utils._AsyncPlaintextTransport object at 0x7f0c659fcc10> params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>
#2020-11-16 00:12:53,062 msc-processor.Consumer INFO Closing connection
#2020-11-16 00:12:53,064 msc-processor.Consumer WARNING Connection closed, reconnect necessary: (200, 'Normal shutdown')
#2020-11-16 00:12:53,064 msc-processor.Consumer DEBUG requested a threadsafe callback for ioloop to call stop_consuming()
#2020-11-16 00:12:59,588 msc-processor.Consumer DEBUG requested a threadsafe callback for ioloop to call stop_consuming()

# receiving messages
# Either re-implement on_message()
# or pass a Queue in

# Test Modes
# amqp_nack_requeue_all_messages
# 

class AsynchronousConsumer(AsynchronousClient):

    _exchange = None
    _exchange_type = None
    _queue = None
    
    _declare_queue = True # should we attempt to create the queue on the server?
    _bindings = []
    _bindings_cache = None
    _bindings_to_cleanup = []
    _do_bindings_cleanup = False
    _queues_bound = 0
    _durable_queue = False
    should_reconnect = False
    last_message_received_time = None
    _application_name = None
    
    _reconnect_attempts = 0
    
    # In production, experiment with higher prefetch values
    # for higher consumer throughput
    _prefetch_count = 1
    
    # leave in classdef to make child class init more likely
    _ack_disabled_max_preflight = False
    _prefetch_count_pre_disable = None
    
    # this is used when we want to connect a Producer and its
    # message delivery confirmations with messages that originated
    # from this consumer - such as in a data adapter / processor scenario
    # STOP - planning on using threadsafe callback connections instead
    #_message_ack_requests_queue = Queue()

    def __init__(self, **kwargs):
        
        self._queue = kwargs.get("queue", None)
        self._received_messages_queue = kwargs.get("message_queue", None)
        
        self.was_consuming = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False

        if not self._queue:
            raise Exception("a queue has not been specified")
        
        super().__init__(ClientType.Consumer, **kwargs)
   
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
        
    def consumer_ready(self):
        self.logger.info("AMQP Consumer is ready.")

    # called prior to connection
    def prepare_connection(self):
        pass
        
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
        # do not send if not specified
        if prefetch_count is not None:
            self.logger.debug("sending Basic.QoS (message prefetch) request for %d", self._prefetch_count)
            self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)
        else:
            self.start_consuming()

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        self.logger.debug('qos/prefetch successfully set to: %d', self._prefetch_count)
        self.start_consuming()

    # Configure Consumer and Begin
    # Send Basic.Cancel callback request
    # Send Basic.Consume RPC to begin consuming
    #   Provide callback for received messages
    #   Returns the unique RabbitMQ consumer tag
    def start_consuming(self):
        self.logger.debug("sending Basic.Cancel callback request")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(
            self._queue, self.__on_message, auto_ack=False)
        self.was_consuming = True
        self._consuming = True
        self.consumer_ready()
        
#    # add callback to be told if RabbitMQ cancels the consumer
#    # Basic.Cancel
#    def add_on_cancel_callback(self):
#        self.logger.info("sending Basic.Cancel callback request")
#        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        # RabbitMQ sent a Basic.Cancel for a consumer receiving messages
        # <class 'pika.frame.Method'> method_frame: The Basic.Cancel frame
        self.logger.info('consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()
        
        # Is the consumer_tag now invalid? Let's assume so for now since we
        # make use of this property to correlate Message Acknowledgements
        self._consumer_tag = None

    # this method is called immediately upon message arrival
    def __on_message(self, _unused_channel, basic_deliver, properties, body):
        # 'pika.spec.Basic.Deliver'
        # 'pika.spec.BasicProperties'
        # 'bytes'

        # do stats and health checks
        self.last_message_received_time = datetime.now()

        try:
            # prepare message for flight
            message = AmqpMessage(pika_tuple=(basic_deliver, properties, body))
            
            # pass to abstract method
            # check if a child class has this method present
            if hasattr(self,"on_message"):
                # child class has provided an on_message method
                self.logger.debug("Received Message # %s -> self.on_message(..); routing_key=%s; len(body)=%s" % (basic_deliver.delivery_tag, basic_deliver.routing_key, len(body)))
                self.on_message(message)
            elif self._received_messages_queue:
                # class instance has been provided a message Queue
                # This occurs when this AMQP client is attached to a Pipeline
                self._received_messages_queue.put(message)
                self.logger.debug("Wrote Message # %s to Message Queue; routing_key=%s; len(body)=%s" % (basic_deliver.delivery_tag, basic_deliver.routing_key, len(body)))
            else:
                self.logger.info("received message but not configured to do anything with it. Msg # %s; routing_key=%s; len(body)=%s" % (basic_deliver.delivery_tag, basic_deliver.routing_key, len(body)))
                return

        except Exception as e:
            self.logger.error(exceptionhandling.traceback_string(e))

    # Calls channel
    # Send Basic.Ack RPC to the channel to acknowledge message delivery
    # Acknowledge message delivery by sending a Basic.Ack RPC method for 
    # the delivery tag
    # - Option for consumer_tag so we can prevent asynchronous processes 
    # from confirming delivery of messages that may have been from a 
    # different consumer session
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
    # delivery-tag (integer) – int/long The server-assigned delivery tag
    # multiple (bool) – If set to True, the delivery tag is treated as “up 
    #   to and including”, so that multiple messages can be acknowledged with
    #   a single method. If set to False, the delivery tag refers to a sinegle 
    #   message. If the multiple field is 1, and the delivery tag is zero, 
    #   this indicates acknowledgement of all outstanding messages.
    # requeue (bool) – If requeue is true, the server will attempt to requeue 
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

    # Send Basic.Cancel RPC command to RabbitMQ
    # this is only being called by the stop() method
    # formerly stop_consuming
    def stop_activity(self):
        self._closing = True
        if self._channel and hasattr(self,"_consumer_tag"):
            self.logger.debug('sending Basic.Cancel RPC command')
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)
            self._consumer_tag = None
        else:
            self.close_channel()

    # RabbitMQ has acknowledged cancellation of the consumer with a
    # Basic.CancelOk frame.
    # We will now close the channel, which will result in an
    # on_channel_closed callback and then we will close the connection
    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        self.logger.debug('RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        if self._channel:
            self.close_channel()
        else: self.logger.debug("on_cancelok() going to close_channel() but already closed")