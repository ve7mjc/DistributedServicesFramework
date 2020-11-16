# Asynchronous, Threaded AMQP Consumer
# Matthew Currie - Nov 2020

import pika
from ssl import SSLContext # invalid cert fix
import threading
import functools
from datetime import datetime
import json # for disk-based queue binding cache

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

class AsynchronousAmqpConsumer(threading.Thread):

    _exchange = 'message'
    EXCHANGE_TYPE = 'topic'
    _queue = 'unnamed_queue'
    _bindings = []
    _bindings_cache = None
    _bindings_to_cleanup = []
    _do_bindings_cleanup = False
    _queues_bound = 0
    _durable_queue = False
    should_reconnect = False
    last_message_received_time = None
    _application_name = None

    def __init__(self, application_name, amqp_url, logger):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param str amqp_url: The AMQP url to connect with
        """
        self._application_name = application_name
        self.was_consuming = False
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1
        
        if logger:
            self.logger = logger
        else:
            print("WARNING: Making our own logger!")
            self.logger = logging.getLogger()
            
        # dynamic creation of queue bindings cache file
        if self._application_name:
            self._bindings_cache_filename = "%s-bindings.cache" % self._application_name
        else:
            self._bindings_cache_filename = "bindings.cache" % self._application_name
        
        super().__init__()

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        self.logger.info('Connecting to AMQP Broker: %s', self._url)
        parameters = pika.URLParameters(self._url)
        
        # add this to bypass certificate errors
        parameters.ssl_options = pika.SSLOptions(SSLContext())

        return pika.SelectConnection(
            parameters=parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self.logger.info('Connection is closing or already closed')
        else:
            self.logger.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        self.logger.debug('connection open')
        self.should_reconnect = False # clear the flag so we are able to shut down
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        self.logger.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        #logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

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
        #self.logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        
        #self.setup_exchange(self._exchange)
        self.setup_queue(self._queue) # skip exchange setup

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
        #if reason.reply_code != 0:
        #    self.logger.warning('Channel %i was closed unexpectedly: code=%s, reason=%s', channel, reason.reply_code, reason.reply_text)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        self.logger.debug('Declaring exchange: %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

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
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb, durable=self._durable_queue)

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
                    else:
                        return []
            except Exception as e:
                raise Exception(e)
        raise Exception("self._bindings_cache_filename not populated")            
    
    def do_queue_bindings_cleanup(self, in_callback=False):
        # callback will be populated by the sender and so do our 
        # setup and initial action with callback, or proceed onward
        # if there is nothing to do
        if not in_callback:
            self._bindings_to_cleanup = []
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
                
                # check to see if there are bindings to clean AND we have
                # been directed to do so
                if self._bindings_to_cleanup and self._do_bindings_cleanup:
                    binding = self._bindings_to_cleanup.pop()
                    self.logger.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                    self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
                
                # write cache since we either did not load one, or we have no 
                # bindings left to cleanup
                if not self._bindings_to_cleanup:
                    self.write_queue_bindings_cache()

            except Exception as e:
                self.logger.error(e)

        else: # this is a callback return!

            if len(self._bindings_to_cleanup):
                binding = self._bindings_to_cleanup.pop()
                self.logger.debug("calling queue_unbind(%s,%s,routing_key=%s" % (binding['queue'],binding['exchange'],binding['routing_key']))
                self._channel.queue_unbind(binding['queue'], binding['exchange'], routing_key=binding['routing_key'], callback=self.do_queue_bindings_cleanup)
            else:
                self.logger.debug("done unbinding")
                # only write a cache when we have proven to remove bindings
                self.write_queue_bindings_cache() 

    # matt added to handle multiple routing keys
    # Queue.BindOk response frame will be passed when in a callback
    def do_queue_binds(self, callback=None):

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
            self.set_qos() # set prefetch

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        self.logger.debug('declared queue : %s', userdata)
        self.do_queue_binds()

#    def on_bindok(self, _unused_frame, userdata):
#        """Invoked by pika when the Queue.Bind method has completed. At this
#        point we will set the prefetch count for the channel.
#        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
#        :param str|unicode userdata: Extra user data (queue name)
#        """
#        pass

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self.logger.debug("sending Basic.QoS prefetch request for %d", self._prefetch_count)
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
        self.logger.info("consumer ready")
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
        try:
            self._channel.basic_ack(delivery_tag)
            self.logger.debug("writing an ack for %s to the channel" % delivery_tag)
        except pika.exceptions.ChannelWrongStateError:
            self.logger.error("unable to ack msg # %s as channel is not open" % delivery_tag)

    # requesting a channel write a Basic.Ack RPC method is not thread safe
    # and must be requested as a threadsafe callback from the ioloop
    def threadsafe_ack_message(self, delivery_tag):
        cb = functools.partial(
            self.acknowledge_message, delivery_tag)
        self._connection.ioloop.add_callback_threadsafe(cb)

    # Send Basic.Cancel RPC command to RabbitMQ
    # this is only being called by the stop() method
    def stop_consuming(self):
        self._closing = True
        if self._channel:
            self.logger.debug('sending Basic.Cancel RPC command')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    # RabbitMQ has acknowledged cancellation of the consumer with a
    # Basic.CancelOk frame.
    # We will now close the channel, which will result in an
    # on_channel_closed callback and then we will close the connection
    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        self.logger.debug('RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        self.close_channel()

    # Send Channel.Close RPC command to RabbitMQ for a clean close
    def close_channel(self):
        self.logger.debug('sending Channel.Close RPC command')
        self._channel.close()

    # this method is called by the thread worker
    def run(self):        
        
        # reconnect logic
        while (not self._closing) or (self.should_reconnect):
            if self.should_reconnect:
                self.logger.debug("waiting (5s) to reconnect.")
                time.sleep(5)
            self._connection = self.connect()
            self._connection.ioloop.start()
        
        # we have completed work in this method
        self.logger.debug('thread exiting..')

    # add a thread-safe callback to the ioloop which will allow
    # us to stop the ioloop from another thread. No methods are
    # are thread-safe to interact with the ioloop!
    def stop(self):
        self.logger.debug('requested a threadsafe callback for ioloop to call stop_consuming()')
        self._connection.ioloop.add_callback_threadsafe(self.stop_consuming)
