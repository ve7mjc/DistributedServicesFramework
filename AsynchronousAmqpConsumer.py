# Asynchronous, Threaded AMQP Consumer
# Matthew Currie - Nov 2020

import pika

#import threading
import functools
from datetime import datetime
#import json # for disk-based queue binding cache
#import logging
#import time

from AsynchronousAmqpClient import AsynchronousAmqpClient, ClientType

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callbacks
# with the ioloop itself

# todo, if channel is closed on connect - it will not quit nicely (hangs)
#2020-11-16 00:12:53,062|pika.channel|WARNING|Received remote Channel.Close (404): "NOT_FOUND - no queue 'weather_test' in vhost '/'" on <Channel number=1 OPEN conn=<SelectConnection OPEN transport=<pika.adapters.utils.io_services_utils._AsyncPlaintextTransport object at 0x7f0c659fcc10> params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>
#2020-11-16 00:12:53,062|msc-processor.Consumer|INFO|Closing connection
#2020-11-16 00:12:53,064|msc-processor.Consumer|WARNING|Connection closed, reconnect necessary: (200, 'Normal shutdown')
#2020-11-16 00:12:53,064|msc-processor.Consumer|DEBUG|requested a threadsafe callback for ioloop to call stop_consuming()
#2020-11-16 00:12:59,588|msc-processor.Consumer|DEBUG|requested a threadsafe callback for ioloop to call stop_consuming()

class AsynchronousAmqpConsumer(AsynchronousAmqpClient):

    _exchange = 'message'
    EXCHANGE_TYPE = 'topic'
    _queue = 'unnamed_queue'
    
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

    def __init__(self, **kwargs):
        
#        self.was_consuming = False
#        self._closing = False
#        self._consumer_tag = None
#        self._consuming = False
        
        super().__init__(ClientType.Consumer, **kwargs)
   
    # called once we have started the consumer.
    # The TCP or TLS connection is established, channels are opened, exchanges,
    # queues, and bindings are declared, qos(prefetch) is set
    def ready(self):
        self.logger.info("consumer %s ready" % self._application_name)
        self.set_qos(self._prefetch_count)
        self.start_consuming()
        
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

    def set_qos(self, prefetch_count):
        # Configure consumer prefetch value
        self.logger.debug("sending Basic.QoS (message prefetch) request for %d", self._prefetch_count)
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        self.logger.debug('qos/prefetch set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue, self._on_message, auto_ack=False)
        self.was_consuming = True
        self._consuming = True
        
    # add callback to be told if RabbitMQ cancels the consumer
    def add_on_cancel_callback(self):
        #self.logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        self.logger.info('consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self.logger.info('received message #%s, routing_key=%s from %s: %s', basic_deliver.delivery_tag, basic_deliver.routing_key,properties.app_id, body)
        self.logger.info("make sure to reimplement ::on_message(self, _unused_channel, basic_deliver, properties, body) in your AsynchronousAmqpConsumer class")

    def _on_message(self, _unused_channel, basic_deliver, properties, body):
        # do stats and health checks
        self.last_message_received_time = datetime.now()

        # pass to abstract method (check if exists?)
        # reimplementing this without overriding this method will throw an exception
        # which is desirable
        self.on_message(_unused_channel, basic_deliver, properties, body)

    # Acknowledge message delivery by sending a Basic.Ack RPC method for the delivery tag
    # do not call this from another thread (calling from on_message is OK)
    def acknowledge_message(self, delivery_tag):
        
        # this functionality is added for testing and development purposes
        if hasattr(self,"_ack_disabled_max_preflight") and self._ack_disabled_max_preflight: 
            self.logger.info("ack was requested for delivery_tag=%s but we are set to _ack_disabled=True" % delivery_tag)
            return # log and return
        else:
            try:
                self._channel.basic_ack(delivery_tag)
                self.logger.debug("writing an ack for %s to the channel" % delivery_tag)
            except pika.exceptions.ChannelWrongStateError:
                self.logger.error("unable to ack msg # %s as channel is not open" % delivery_tag)

    # requesting a channel write a Basic.Ack RPC method is not thread safe
    # and must be requested as a threadsafe callback from the ioloop
    def threadsafe_ack_message(self, delivery_tag):
        cb = functools.partial(self.acknowledge_message, delivery_tag)
        self._connection.ioloop.add_callback_threadsafe(cb)

    # Send Basic.Cancel RPC command to RabbitMQ
    # this is only being called by the stop() method
    # formerly stop_consuming
    def stop_activity(self):
        self._closing = True
        if self._channel:
            self.logger.debug('sending Basic.Cancel RPC command')
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

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