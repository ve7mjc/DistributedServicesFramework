# AsynchronousAMQPProducer
# Threaded Asynchronous AMQP Producer
# Matthew Currie - Nov 2020

from threading import Lock
import functools # callbacks
from queue import Queue
from pika import BasicProperties

from AsynchronousAmqpClient import AsynchronousAmqpClient, ClientType

# TAKE NOTE
# self._connection.ioloop.start() runs in Thread::run()
# this ioloop is blocking and there are no methods of this class
# which are thread-safe unless they register a thread-safe callback
# with the ioloop itself

class AmqpMessage():
    
    __exchange = None
    __routing_key = None
    __body = None
    __properties = None
    
    @property
    def exchange(self):
        return self.__exchange
        
    @exchange.setter
    def exchange(self,value):
        self.__exchange = value
        
    @property
    def routing_key(self):
        return self.__routing_key
        
    @routing_key.setter
    def routing_key(self,value):
        self.__routing_key = value
        
    @property
    def body(self):
        return self.__body
        
    @body.setter
    def body(self,value):
        self.__body = value
        
    @property
    def properties(self):
        if not self.__properties:
            return BasicProperties()
        else:
            return self.__properties

    @properties.setter
    def properties(self,value):
        self.__properties = value
        

class AsynchronousAmqpProducer(AsynchronousAmqpClient):
  
    # Producer Specific Class Variables
    blocking_publish_response = Queue()
    blocking_message_id = None
    blocking_publish_mutex = Lock()
    blocking_publish_message_number = None

    def __init__(self, application_name, **kwargs):
        
        # Producer Specific Initialization
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        
        super().__init__(ClientType.Producer, **kwargs)

    # Underlying AMQP Client is read.
    # Connection and Channel is established
    # Exchanges, Queues, and Bindings are declared
    def ready(self):
        
        self.enable_delivery_confirmations()
        self.logger.info("producer is ready")
        
        # there may be messages waiting in the queue already
        self.do_publish()

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
        self.logger.debug('Issuing Confirm.Select (Delivery Confirmation) RPC command')
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
    # called by ioloop in a callback and includes
    # a AmqpMessage() object
    def basic_publish(self, amqp_message):

        self._message_number += 1
        self._deliveries.append(self._message_number)

        self._channel.basic_publish(
            amqp_message.exchange, 
            amqp_message.routing_key, 
            amqp_message.body, 
            amqp_message.properties)

        self.logger.debug("published message to channel: exchange=%s, routing_key=%s, len(body)=%s" % (
                amqp_message.exchange,
                amqp_message.routing_key,
                len(amqp_message.body)
            ))

        # return the message number so we can pass back
        # as the delivery_tag
        return self._message_number

    def do_publish(self, blocking_message=None):
        
        # is this legit?
        if not self._channel:
            self.logger.error('do_publish() returned as channel is not ready')
            # if we are blocking, a thread is waiting for a response via
            # the message_queue
            if blocking_message: self.blocking_publish_response.put("error")
            return

        if blocking_message:
            self.blocking_message_id = self.basic_publish(blocking_message)
            # we will now fall out and requesting client blocks until response
        else:    
            while not self.message_queue.empty():
                # this is blocking but should never be blocked long and
                # then we directly write the message to the channel
                out_message = self.message_queue.get()
                self.basic_publish(out_message)

    # request to publish a message - almost certainly from another thread
    def publish(self, exchange, routing_key, body, **kwargs):
        
        try:
            # publish a message to RabbitMQ
            # track message numbers to check against delivery confirmations in
            # the on_delivery_confirmations method

            # todo, do a check to see if the channel is stopping or closing
            # as we could reject this right here and right now
            if self._connection:

                properties = kwargs.get("properties",False)
                blocking = kwargs.get("blocking",False)

                msg = AmqpMessage()
                msg.exchange = exchange
                msg.routing_key = routing_key
                msg.body = body
                msg.properties = properties

                if blocking:
                    # blocking mode so we can get a message ack back immediately
                    # for 
                    # pass message asynchronously to ioloop in the callback request
                    cb = functools.partial(self.do_publish, msg)
                    self._connection.ioloop.add_callback_threadsafe(cb)

                    # block and wait for response from message Queue
                    return self.blocking_publish_response.get()
                else:
                    # the message queue is only used for messages we wish to send
                    # asynchronously and is an outgoing queue
                    self.message_queue.put(msg)
                    self.logger.info('enqueued message # %i', self._message_number)
                    self._connection.ioloop.add_callback_threadsafe(self.do_publish)
                    # we are now going to exit as we are non-blocking and have 
                    # enqueued a message and alrtered the ioloop there are messages 
        except Exception as e:
            self.logger.error("producer::publish() %s" % e)
            c5lib.print_tb()
            raise e