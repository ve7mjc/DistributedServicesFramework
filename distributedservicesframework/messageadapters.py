#from dsf.amqp import AsynchronousConsumer, AsynchronousProducer, amqputilities
from dsf.component import Component
from dsf import exceptionhandling

import threading
import logging

# Abstract Message Input and Output Adapters
# We are choosing to leave the Component base class out of Adapter for now as
# it is present in the data adapters so far

# start()
# stop()
# keep_working - shut down was issued
# is_stopped() - adapter is stopped
# @properties
# adapter_type - return descriptive short name, eg: AmqpConsumer
# direction - "in" or "out"

# abstract class
class MessageAdapter(): # Component
    
    # ability for a data adapter to arbitrarily throttle the rate of
    # message flow.
    _throttle_messages_sec = None
    _adapter_type = None # declare in child class; eg. FileWriter
    
    def __init__(self,**kwargs):
        self._throttle_messages_sec = kwargs.get("throttle",None)
        super().__init__(**kwargs)
    
    # "in" or "out"
    @property
    def direction(self):
        return self._message_direction
    
    # Return type of MessageAdapter, eg. FileWriter, AmqpConsumer
    @property
    def adapter_type(self):
        return self._adapter_type
    
# abstract class
class MessageInputAdapter(MessageAdapter):
    
    _adapter_type = "GenericInput"
    _message_direction = "in"
    
    _message_queue = None
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def setmessagequeue(self,handle):
        self._message_queue = handle
        
    def write(self,message):
        if self._message_queue:
            self._message_queue.put("test!")
        else:
            print("no message queue for input adapter!")

# Double check MRO concept here!
#class MessageInputAdapterAmqpConsumer(AsynchronousConsumer,MessageInputAdapter):
#    
#    _adapter_type = "AMQPConsumer"
#    
#    def __init__(self,**kwargs):
#        super().__init__(**kwargs)


class SimulatedMessageInputAdapter(MessageInputAdapter,Component):
    
    _adapter_type = "SimulatedMessageInput"
    _threaded = True

    def __init__(self,**kwargs):
        super().__init__()
        
    def run(self):
        self.setready()
        while self.keep_working:
            self.powernap(500)
            self.write("testing one two")

# Output Adapters
# call a class method with a message as parameter (best way to do blocking!)
# - pass a reference to a message queue on adapter initialization so the 
#   producer may get messages as able
# - call a class method with a message as parameter
#   helpful for simple blocking publishes with

class MessageOutputAdapter(MessageAdapter):
    
    _adapter_type = "GenericOutput"
    _message_direction = "out"
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    # Write Message to Adapter
    def write(self, message):
        raise Exception("write not implemented in MessageOutputAdapter child class!")

#class MessageOutputAdapterAmqpProducer(AsynchronousProducer,MessageOutputAdapter):
#    
#    _adapter_type = "AmqpProducer"
#    
#    _publish_exchange = "test"
#    _publish_routing_key = None
#    _blocking = True
#    
#    def __init__(self,**kwargs):
#        super().__init__(**kwargs)
#        
#    def write(self, message):
#        kwargs = {}
#        kwargs["blocking"] = self._blocking
#        kwargs["exchange"] = self._publish_exchange
#        kwargs["routing_key"] = "test"
#        kwargs["body"] = message
#        # properties
#        self.publish(**kwargs)

# Message --> Console Writer
# blocking? we can consider the console stream rate to be insignificant
# fairly sure it is non-blocking anyways -- eg write to a buffer and flush
class MessageOutputAdapterConsoleWriter(MessageOutputAdapter,Component):
    
    _adapter_type = "ConsoleWriter"

    def __init__(self,**kwargs):      
        # Component, then MessageOutputAdapter
        super().__init__(**kwargs)
        self.setready()
        
    def write(self,message):
        print("ConsoleWriter: %s" % message)