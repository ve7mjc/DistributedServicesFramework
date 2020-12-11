# AsynchronousAMQPProducer
# Threaded Asynchronous AMQP Producer
# Matthew Currie - Nov 2020

import dsf.domain

from threading import Lock
import functools # callbacks
from queue import Queue
from pika import BasicProperties

from dsf.amqp import AsynchronousClient, ClientType
from dsf.amqp.amqpmessage import AmqpProducerMessage

# Important Note on Thread Safety
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and as all interaction with this ioloop must be
# done thread-safe by registering a thread-safe callback with the ioloop itself
class AsynchronousProducer(AsynchronousClient):
  
    _client_type = ClientType.Producer
  
    # Producer specific Class variables
    blocking_publish_response = Queue()
    blocking_message_id = None
    blocking_publish_mutex = Lock()
    blocking_publish_message_number = None
    
    # we will enable delivery confirmations on the channel and register a 
    # callback for message ack
    _enable_delivery_confirmations = True
    message_queue = Queue()

    # if we are using this AMQP Producer via a logging handler
    # we will enter a publish storm if there is any logging output
    _logger_publisher = False
    
    def __init__(self, **kwargs):
        
        self.config_init(**kwargs)
        self._exchange = self.config.get("exchange")
        self._amqp_url = self.config.get("url")

        # NOT IMPLEMENTED!
        # if we are using this AMQP Producer via a logging handler
        # we will enter a publish storm if there is any logging output
        self._logger_publisher = kwargs.get("logger_publisher", False)
        
        # Producer Specific Initialization
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._application_name = kwargs.get("application_name", None)
        
        super().__init__(**kwargs)

    # AMQPClient is now ready
    # Connection and Channel are established
    # Exchanges, Queues, and Bindings have been declared
    # Producer specific actions may now be performed
    def client_ready(self):
        
        if self._enable_delivery_confirmations:
            self.enable_delivery_confirmations()
        else: 
            self.producer_ready()
    
    # Called when the Producer is now considered ready to publish messages
    def producer_ready(self):
        
        self.logger.debug("AMQP Producer is ready for message publishing")
        self._ready = True

        # trigger the publish of any messages places in the queue while
        #  the client was initializing, connecting, or disconnected
        if self.can_publish():
            self._connection.ioloop.add_callback_threadsafe(self.__publish_callback)

    # Called from base class AMQPClient prior to a connection is attempted
    # It is important to note that this will be called on reconnects
    # session specific variables must be reset to reflect new a session
    def prepare_connection(self):
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

    def enable_delivery_confirmations(self):
        # Send Confirm.Select RPC method to RabbitMQ 
        # enable delivery confirmations on the channel. 
        # The only way to turn this off is to close the channel and create a 
        # new one. When the message is confirmed from RabbitMQ, the
        # on_delivery_confirmation method will be invoked
        self.logger.debug('Enabling Delivery Confirmations with Confirm.Select RPC command')
        self._channel.confirm_delivery(self._on_delivery_confirmation,
            self._on_confirm_delivery_requested)
    
    # called as a callback from channel.confirm_delivery() request has completed
    def _on_confirm_delivery_requested(self, method_frame):
        if method_frame.method.NAME.split('.')[1].lower() == "selectok":
            self.producer_ready()
            return
        self.set_failed("channel.confirm_delivery() request has failed")

    def _on_delivery_confirmation(self, method_frame):
        # RabbitMQ response to a Basic.Publish RPC command
        # passing in either a Basic.Ack or Basic.Nack frame with
        # the delivery tag of the message that was published. The delivery tag
        # is an integer counter indicating the message number that was sent
        # on the channel via Basic.Publish. Here we're just doing house keeping
        # to keep track of stats and remove message numbers that we expect
        # a delivery confirmation of from the list used to keep track of messages
        # that are pending confirmation.
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        delivery_tag = method_frame.method.delivery_tag

        # blocking message response?
        if self.blocking_message_id == delivery_tag:
            self.blocking_message_id = 0
            self.log_debug("delivery confirmation of id # %s = %s" % (delivery_tag,confirmation_type))
            self.blocking_publish_response.put(confirmation_type)
        else:
            # we could still fire a message here but are not
            self._deliveries.remove(method_frame.method.delivery_tag)
            
        # Statistics
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        # if we are connected to a Consumer Queue with outstanding messages?
        # the consumer tag should follow along though so we do not refer to
        # delivery_tags from different consumer_tags

        # KEEP FOR REF
        # self._connection.ioloop.call_later(self.PUBLISH_INTERVAL,
#                                           self.publish_message)

#    def _publish_message(self, message):
#        properties = pika.BasicProperties(
#            app_id='example-publisher',
#            content_type='application/json',
#            headers=hdrs)
        
        # do not log this if it was likely the result of a blocking message
#        if not self.blocking_publish_message_number:
#            self.logger.debug('Published message producer#%s, routing_key=%s' % (self._message_number,routing_key))

    # write (publish) to the channel 
    # __do_publish which is being called from the ioloop in a callback
    # amqp_message = <class 'Message'>
    def __do_basic_publish(self, amsg):

        if self.can_publish():
            self._message_number += 1
            self._deliveries.append(self._message_number)
            
            self._channel.basic_publish(
                amsg.exchange, 
                amsg.routing_key, 
                amsg.body, 
                amsg.properties)

    #        self.logger.debug("published message to channel: exchange=%s, routing_key=%s, len(body)=%s" % (
    #                amqp_message.exchange,
    #                amqp_message.routing_key,
    #                len(amqp_message.body)
    #            ))

            # return the message number so we can pass back
            # as the delivery_tag           
            return self._message_number
        else:
            self.log_warning("__do_basic_publish() called but cannot publish!")
            return None

    # to be called from the ioloop in a callback
    # a blocking publish is actually an asynchronous AMQP publish
    # but the requesting thread is blocked waiting for a delivery
    # confirmation
    def __publish_callback(self, message=None):
        
        # is this legit?
        if not self.can_publish():
            self.log_error('do_publish() returned as channel is not ready')
            if message: self.blocking_publish_response.put("error")
            return

        # If we are in a blocking response publish, we have the message passed
        # to us for immediate use
        if message:
            # note the message number so we can associate the asynchronously 
            # received delivery confirmation with this publish
            self.blocking_message_id = self.__do_basic_publish(message)
            # the sender thread is already blocked and waiting for a response
            # we will drop which will also send any non-blocking messages

        # we have been called from a thread-safe ioloop callback so we can
        # iterate the outgoing message queue if we so choose
        while not self.message_queue.empty():
            # this is blocking but should never be blocked long and
            # then we directly write the message to the channel
            message = self.message_queue.get()
            self.__do_basic_publish(message)

    # convenience method to publish an AmqpMessage
    def publish_message(self, amqp_message, **kwargs):
        kwargs["message"] = amqp_message
        return self.publish(**kwargs)

    # request to publish a message
    # Method 1 - kwargs: exchange, routing_key, body
    # Method 2 - pass AmqpProducerMessage
    def publish(self, **kwargs):
        try:
            # publish a message to RabbitMQ
            # track message numbers to check against delivery confirmations in
            # the on_delivery_confirmations method
            blocking = kwargs.get("blocking", False)

            # if we have been passed a <class 'AmqpProducerMessage'>
            if "message" in kwargs:
                if isinstance(kwargs["message"],AmqpProducerMessage):
                    message = kwargs.get("message")
                else:
                    self.log_warning("refusing to continue with a message of type %s" % message.type)
                    return
            else:
                message = AmqpProducerMessage.from_kwargs(**kwargs)

            # inject exchange if it is not available
            if not message.exchange:
                if not self._exchange: self.log_warning("exchange not desginated in AMQPMessage")
                else: message.exchange = self._exchange

            # place message in outgoing Queue
            # if we are trying to do a blocking mode, there could in fact be messages
            # in the Queue already -- TODO
            if not self.can_publish() and (self.is_failed() or self.is_stopped()):
                self.log_warning("producer.publish() called but channel and connection are not avaiable")

            # todo, do a check to see if the channel is stopping or closing
            # as we could reject this right here and right now
            if blocking:
                if self.can_publish():
                    # blocking mode so we can get a message ack back immediately and
                    # pass message asynchronously to ioloop in the callback request
                    self.log_debug("doing blocking publish!")
                    cb = functools.partial(self.__publish_callback, message)
                    self._connection.ioloop.add_callback_threadsafe(cb)
                    # now block and wait for response from message Queue
                    response = self.blocking_publish_response.get()
                    return response
                else: 
                    return "error"
            else:
                # the message queue is only used for messages we wish to send
                # asynchronously and is an outgoing queue
                self.message_queue.put(message)
                if self.can_publish():
                    self._connection.ioloop.add_callback_threadsafe(self.__publish_callback)
                    # we are now going to exit as we are non-blocking and have 
                    # enqueued a message and alrtered the ioloop there are messages
                else:
                    return True
        
        except Exception as e:
            self.log_exception()
            self.logger.error("producer::publish() %s" % e.__str__())
            

    # Gracefully this producer
    # This method is only called from the BaseClass.stop() method
    # self._stop_requested is now true and so forcing the connection ioloop
    # to close the channel will cause the BaseClass.run() method to exit
    # This method IS being called from a threadsafe callback to ioloop!
    def stop_activity(self):
        if self._channel.is_open and not self._channel.is_closing:
            self.log_debug("self._channel.close()")
            self._channel.close()
        elif self._connection.is_open and not self._connection.is_closing: 
            self.log_debug("self._connection.close()")
            self._connection.close()
        
    def can_publish(self):
        if self._channel and self._channel.is_open and not self._channel.is_closing:
            if self._connection and self._connection.is_open and not self._connection.is_closing:
                return True
        else: return False