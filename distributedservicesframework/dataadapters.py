from distributedservicesframework.amqp.asynchronousconsumer import AsynchronousConsumer
from distributedservicesframework.amqp.asynchronousproducer import AsynchronousProducer
from distributedservicesframework.amqp import amqputilities
from distributedservicesframework.amqp.amqpmessage import AmqpMessage

from distributedservicesframework.component import Component

# Abstract Message Input and Output Adapters
# We are choosing to leave the Component base class out of Adapter for now as
# it is present in the data adapters so far

# abstract class
class MessageAdapter(): # Component
    
    # ability for a data adapter to arbitrarily throttle the rate of
    # message flow.
    _throttle_messages_sec = None
    _type = None # declare in child class; eg. FileWriter
    
    def __init__(self,**kwargs):
        self._throttle_messages_sec = kwargs.get("throttle",None)
        super().__init__(**kwargs)
    
    @property
    def message_direction(self):
        return self._message_direction
    
    # Return type of MessageAdapter, eg. FileWriter
    @property
    def type(self):
        return self._type

# abstract class
class MessageInputAdapter(MessageAdapter):
    
    _message_direction = "in"
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

# Double check MRO concept here!
class MessageInputAdapterAmqpConsumer(AsynchronousConsumer,MessageInputAdapter):
    
    _type = "AMQPConsumer"
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

# Output Adapters
# call a class method with a message as parameter (best way to do blocking!)
# - pass a reference to a message queue on adapter initialization so the 
#   producer may get messages as able
# - call a class method with a message as parameter
#   helpful for simple blocking publishes with

class MessageOutputAdapter(MessageAdapter):
    
    _message_direction = "out"
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    # Write Message to Adapter
    def write(self, message):
        raise Exception("write not implemented in MessageOutputAdapter child class!")

# Message --> Console Writer
# blocking? we can consider the console stream rate to be insignificant
# fairly sure it is non-blocking anyways -- eg write to a buffer and flush
class MessageOutputAdapterConsoleWriter(MessageOutputAdapter,Component):
    
    _type = "ConsoleWriter"
    
    def __init__(self,**kwargs):      
        # Component, then MessageOutputAdapter
        super().__init__(**kwargs)
        
    def write(self,message):
        print("ConsoleWriter: %s" % message)
    
