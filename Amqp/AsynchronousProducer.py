# AsynchronousAMQPProducer
# Threaded Asynchronous AMQP Producer
# Matthew Currie - Nov 2020

from threading import Lock
import functools # callbacks
from queue import Queue
from pika import BasicProperties

from Amqp.AsynchronousClient import AsynchronousClient, ClientType
from Amqp import AmqpMessage

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

class AsynchronousProducer(AsynchronousClient):
  
    # Producer Specific Class Variables
    blocking_publish_response = Queue()
    blocking_message_id = None
    blocking_publish_mutex = Lock()
    blocking_publish_message_number = None

    def __init__(self, **kwargs):
        
        # Producer Specific Initialization
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._application_name = kwargs.get("application_name", None)
        
        super().__init__(ClientType.Producer, **kwargs)

    # Underlying AMQP Client is read.
    # Connection and Channel is established
    # Exchanges, Queues, and Bindings are declared
    def client_ready(self):     
        self.enable_delivery_confirmations()
        self.logger.info("producer is ready")

        self.__publish_callback() # there may be messages waiting in the queue already

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
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        # RabbitMQ response to a Basic.Publish RPC command
        # passing in either a Basic.Ack or Basic.Nack frame with
        # the delivery tag of the message that was published. The delivery tag
        # is an integer counter indicating the message number that was sent
        # on the channel via Basic.Publish. Here we're just doing house keeping
        # to keep track of stats and remove message numbers that we expect
        # a delivery confirmation of from the list used to keep track of messages
        # that are pending confirmation.
                
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()

        # blocking message response?
        if self.blocking_message_id:
            self.blocking_message_id = 0
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

    # to be called from the ioloop in a callback
    # a blocking publish is actually an asynchronous AMQP publish
    # but the requesting thread is blocked waiting for a delivery
    # confirmation
    def __publish_callback(self, message=None):
        
        # is this legit?
        if not self._channel:
            self.logger.error('do_publish() returned as channel is not ready')
            if blocking_message: self.blocking_publish_response.put("error")
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

    # request to publish a message - almost certainly from another thread
    # exchange, routing_key, body, **kwargs
    def publish(self, **kwargs):
        try:
            # publish a message to RabbitMQ
            # track message numbers to check against delivery confirmations in
            # the on_delivery_confirmations method

            # todo, do a check to see if the channel is stopping or closing
            # as we could reject this right here and right now
            if self._connection:

                exchange = kwargs.get("exchange")
                routing_key = kwargs.get("routing_key", None)
                body = kwargs.get("body", None)
                properties = kwargs.get("properties", None)
                
                blocking = kwargs.get("blocking", False)

                # if we have been passed a <class 'Amqp.Message'>
                if "message" in kwargs:
                    message = kwargs.get("message")
                else:
                    message = AmqpMessage(exchange=exchange,routing_key=routing_key,
                        body=body,properties=properties)

                # place message in outgoing Queue
                # if we are trying to do a blocking mode, there could in fact be messages
                # in the Queue already -- TODO

                if blocking:
                    # blocking mode so we can get a message ack back immediately and
                    # pass message asynchronously to ioloop in the callback request
                    cb = functools.partial(self.__publish_callback, message)
                    self._connection.ioloop.add_callback_threadsafe(cb)

                    # now block and wait for response from message Queue
                    return self.blocking_publish_response.get()
                else:
                    # the message queue is only used for messages we wish to send
                    # asynchronously and is an outgoing queue
                    self.message_queue.put(message)
                    #self.logger.info('enqueued message # %i', self._message_number + 1)
                    self._connection.ioloop.add_callback_threadsafe(self.__publish_callback)
                    # we are now going to exit as we are non-blocking and have 
                    # enqueued a message and alrtered the ioloop there are messages
                    
        except Exception as e:
            self.logger.error("producer::publish() %s" % e)
            raise e