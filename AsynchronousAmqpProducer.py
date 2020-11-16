# Asynchronous, Threaded AMQP Producer
# Matthew Currie - Nov 2020

import pika
from ssl import SSLContext # invalid cert fix
import threading
import functools
import queue
import logging
from threading import Thread, Lock

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

class AsynchronousAmqpProducer(threading.Thread):

    _exchange = None
    _exchange_type = None
    _queue = None
    _bindings = []
    _queues_bound = 0

    _durable_queue = False
    should_reconnect = False
    
    publish_message_queue = queue.Queue()
    blocking_publish_message = None
    blocking_publish_response = queue.Queue()
    blocking_publish_mutex = Lock()
    blocking_publish_message_number = None

    def __init__(self, url=None, logger=None):
        self._connection = None
        self._channel = None
        
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self._url = url
        if "://" not in url:
            self._host = url
        
        if logger:
            self.logger = logger
        else:
            print("WARNING: Making our own logger!")
            self.logger = logging.getLogger()
        
        super().__init__()
        
    def add_binding(self, routing_key_pattern):
        self._bindings.append(routing_key_pattern)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        self.logger.info('AMQP producer connecting to Broker: %s', self._url)
        
        if not self._host:
            parameters = pika.URLParameters(self._url)
            # bypass certificate errors
            parameters.ssl_options = pika.SSLOptions(SSLContext())
            return pika.SelectConnection(
                parameters=parameters,
                on_open_callback=self.on_connection_open,
                on_open_error_callback=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed)
        else:
            # keyword based
            return pika.SelectConnection(
                parameters=pika.ConnectionParameters(self._host),
                on_open_callback=self.on_connection_open,
                on_open_error_callback=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed)

    def ready(self):
        self.enable_delivery_confirmations()
        
        self.logger.info("producer is ready")
        
        # there may be messages waiting in the queue already
        self.do_publish()

    def close_connection(self):
#        self._consuming = False
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
        self.logger.debug('AMQP Connection successful.')
        self.should_reconnect = False # clear the flag so we are able to shut down
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        
        #example used this for a publisher!
        #LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        #self._connection.ioloop.call_later(5, self._connection.ioloop.stop)
        
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
        if self._stopping:
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

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
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
        #if reason.reply_code != 0:
        #    self.logger.warning('Channel %i was closed unexpectedly: code=%s, reason=%s', channel, reason.reply_code, reason.reply_text)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        if self._exchange:
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
            # go straight to
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
        if self._queue:
            self.logger.debug('setting up queue: %s', self_queue)
            cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
            self._channel.queue_declare(queue=queue_name, callback=cb, durable=self._durable_queue)
        else:
            # go straight to bindings
            self.do_queue_binds()

    # matt added to handle multiple routing keys
    def do_queue_binds(self, _unused_frame = None):

        if self._bindings:
            
            # called each time we return from a callback
            if (self._queues_bound) and (self._queues_bound <= len(self._routing_keys)):
                self.logger.debug('routing key added: %s', self._routing_keys[self._queues_bound-1])

            if self._queues_bound < len(self._routing_keys):
                self.logger.debug("channel.queue_bind(%s,%s,routing_key=%s)" % (self._queue, self._exchange, self._routing_keys[self._queues_bound]))
                self._channel.queue_bind(
                    self._queue,
                    self._exchange,
                    routing_key=self._routing_keys[self._queues_bound],
                    callback=self.do_queue_binds)
                self._queues_bound += 1
            else:
                self.logger.debug("done adding routing_key bindings (%s)" % (int(self._queues_bound)))
                self.ready()
        else:
            self.ready()

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

#    def on_bindok(self, _unused_frame, userdata):
#        """Invoked by pika when the Queue.Bind method has completed. At this
#        point we will set the prefetch count for the channel.
#        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
#        :param str|unicode uself.loggerserdata: Extra user data (queue name)
#        """
#        pass

#    def start_publishing(self):
#        """This method will enable delivery confirmations and schedule the
#        first message to be sent to RabbitMQ
#        """
#        self.logger.info('Issuing consumer related RPC commands')
#        self.enable_delivery_confirmations()
        
        # this was part of the example
        #self.schedule_next_message() 

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.
        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.
        """
        self.logger.debug('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.
        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame
        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
#        self.logger.info('Received %s for delivery tag: %i', confirmation_type,
#                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            # handle the scenario where this confirmation matches that of our
            # blocking publish_message
            if self.blocking_publish_message_number == method_frame.method.delivery_tag:
                blocking_publish_message_number = 0
                self.blocking_publish_response.put("ack")
            self._acked += 1
        elif confirmation_type == 'nack':
            # handle the scenario where this confirmation matches that of our
            # blocking publish_message
            if self.blocking_publish_message_number == method_frame.method.delivery_tag:
                blocking_publish_message_number = 0
                self.blocking_publish_response.put("nack")
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
#        self.logger.info(
#            'Published %i messages, %i have yet to be confirmed, '
#            '%i were acked and %i were nacked', self._message_number,
#            len(self._deliveries), self._acked, self._nacked)

#    def schedule_next_message(self):
#        """If we are not closing our connection to RabbitMQ, schedule another
#        message to be delivered in PUBLISH_INTERVAL seconds.
#        """
#        self.logger.info('Scheduling next message for %0.1f seconds',
#                    self.PUBLISH_INTERVAL)
#        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL,
#                                           self.publish_message)

    def _publish_message(self, message):
        (exchange, routing_key, body, properties) = message
        hdrs = []
        if not properties:
            properties = pika.BasicProperties()

        self._channel.basic_publish(exchange, routing_key,body,properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        
        # do not log this if it was likely the result of a blocking message
#        if not self.blocking_publish_message_number:
#            self.logger.debug('Published message producer#%s, routing_key=%s' % (self._message_number,routing_key))
        
    def do_publish(self, blocking_request=False):
        
        # is this legit?
        if self._channel is None or not self._channel.is_open:
            self.logger.error('do_publish() returned as channel is not ready')
            self.blocking_publish_response.put("error")
            return

        while not self.publish_message_queue.empty():
            # this is blocking
            self._publish_message(self.publish_message_queue.get())

        if blocking_request:
            # do not believe there is a possible race condition here as 
            # the blocking publish should never be run concurrently?
            self._publish_message(self.blocking_publish_message)
            self.blocking_publish_message_number = self._message_number

    def publish(self, exchange, routing_key, body, properties=None, blocking=False):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.
        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.
        """
        message = (exchange, routing_key, body, properties)

        if blocking:
            # blocking mode so we can get a message ack back immediately
            # and correlated to the message itself
            #self.blocking_publish_mutex.acquire()
            self.blocking_publish_message = message
            #self.blocking_publish_mutex.release()

            cb = functools.partial(self.do_publish, blocking_request=True)
            self._connection.ioloop.add_callback_threadsafe(cb)

            # block and wait for response
            return self.blocking_publish_response.get()
            
        else:
            self.publish_message_queue.put(message)
            if self._connection:
                self._connection.ioloop.add_callback_threadsafe(self.do_publish)
            return

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

#    def start_consuming(self):
#        """This method sets up the consumer by first calling
#        add_on_cancel_callback so that the object is notified if RabbitMQ
#        cancels the consumer. It then issues the Basic.Consume RPC command
#        which returns the consumer tag that is used to uniquely identify the
#        consumer with RabbitMQ. We keep the value to use it when we want to
#        cancel consuming. The on_message method is passed in as a callback pika
#        will invoke when a message is fully received.
#        """
#        #self.logger.info('Issuing consumer related RPC commands')
#        self.add_on_cancel_callback()
#        self._consumer_tag = self._channel.basic_consume(
#            self._queue, self.on_message, auto_ack=False)
#        self.was_consuming = True
#        self._consuming = True

#    def add_on_cancel_callback(self):
#        """Add a callback that will be invoked if RabbitMQ cancels the consumer
#        for some reason. If RabbitMQ does cancel the consumer,
#        on_consumer_cancelled will be invoked by pika.
#        """
#        #self.logger.info('Adding consumer cancellation callback')
#        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

#    def on_consumer_cancelled(self, method_frame):
#        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
#        receiving messages.
#        :param pika.frame.Method method_frame: The Basic.Cancel frame
#        """
#        self.logger.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
#        if self._channel:
#            self._channel.close()
#
#    def on_message(self, _unused_channel, basic_deliver, properties, body):
#        """Invoked by pika when a message is delivered from RabbitMQ. The
#        channel is passed for your convenience. The basic_deliver object that
#        is passed in carries the exchange, routing key, delivery tag and
#        a redelivered flag for the message. The properties passed in is an
#        instance of BasicProperties with the message properties and the body
#        is the message that was sent.
#        :param pika.channel.Channel _unused_channel: The channel object
#        :param pika.Spec.Basic.Deliver: basic_deliver method
#        :param pika.Spec.BasicProperties: properties
#        :param bytes body: The message body
#        """
#        self.logger.info('Received message # %s from %s: %s',
#                    basic_deliver.delivery_tag, properties.app_id, body)
#        self.acknowledge_message(basic_deliver.delivery_tag)
#
#    def acknowledge_message(self, delivery_tag):
#        """Acknowledge the message delivery from RabbitMQ by sending a
#        Basic.Ack RPC method for the delivery tag.
#        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
#        """
#        try:
#            self._channel.basic_ack(delivery_tag)    
#        except pika.exceptions.ChannelWrongStateError:
#            logger.error("unable to ack msg # %s as channel is not open" % delivery_tag)
#
#    # requesting a channel write a Basic.Ack RPC method is not thread safe
#    # and must be requested as a threadsafe callback from the ioloop
#    def threadsafe_ack_message(self, delivery_tag):
#        cb = functools.partial(
#            self.acknowledge_message, delivery_tag)
#        self._connection.ioloop.add_callback_threadsafe(cb)

    def stop_producer(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.
        """
        self.logger.debug('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

#    def stop_consuming(self):
#        """Tell RabbitMQ that you would like to stop consuming by sending the
#        Basic.Cancel RPC command.
#        """
#        self._stopping = True # matt added
#        if self._channel:
#            self.logger.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
#            cb = functools.partial(
#                self.on_cancelok, userdata=self._consumer_tag)
#            self._channel.basic_cancel(self._consumer_tag, cb)

#    def on_cancelok(self, _unused_frame, userdata):
#        """This method is invoked by pika when RabbitMQ acknowledges the
#        cancellation of a consumer. At this point we will close the channel.
#        This will invoke the on_channel_closed method once the channel has been
#        closed, which will in-turn close the connection.
#        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
#        :param str|unicode userdata: Extra user data (consumer tag)
#        """
#        self._consuming = False
#        self.logger.info(
#            'RabbitMQ acknowledged the cancellation of the consumer: %s',
#            userdata)
#        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        #self.logger.info('Closing the channel')
        self._channel.close()

    # CHECK RECONNECT FUNCTIONALITY!
    def run(self):
        
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

#            try:
            self._connection = self.connect()
            self._connection.ioloop.start()
#            except Exception as e:
#                self.logger.error("producer run() : %s" % e)
                 
#            except KeyboardInterrupt:
#                self.stop()
#                if (self._connection is not None and
#                        not self._connection.is_closed):
#                    # Finish closing
#                    self._connection.ioloop.start()

        self.logger.info('producer stopped')

#        # reconnect logic is hidden below
#        while (not self._closing) or (self.should_reconnect):
#            if self.should_reconnect:
#                self.logger.debug("waiting (5s) to reconnect.")
#                time.sleep(5)
#            self._connection = self.connect()
#            self._connection.ioloop.start()
#        
#        self.logger.debug('AsynchronousAmqpConsumer thread exiting..')
                

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        #self.logger.info('requesting consumer stop')
        
        # this is critically important as no methods are thread-safe to interact
        # with the ioloop!
        self._connection.ioloop.add_callback_threadsafe(self.stop_producer)
