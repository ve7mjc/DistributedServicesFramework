from dsf.amqp import AsynchronousConsumer, AsynchronousProducer, amqputilities
from dsf.amqp import AmqpConsumerMessage, AmqpProducerMessage

import dsf.domain
from dsf.component import Component
from dsf import exceptionhandling
from dsf.event import *

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
class MessageAdapter(Component): # Component
    
    # ability for a data adapter to arbitrarily throttle the rate of
    # message flow.
    _throttle_messages_sec = None
    _adapter_type = None # declare in child class; eg. FileWriter
    
    _message_types = None
    
    def __init__(self,**kwargs):
        
        self._pipeline_hdl = kwargs.get("pipeline_hdl", None)
        if not self._pipeline_hdl: self.log_info("pipeline handle not supplied")
            
        self._throttle_messages_sec = kwargs.get("throttle",None)
        
        super().__init__(**kwargs)
    
    @property
    def pipeline(self):
        return self._pipeline_hdl

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
class MessageInputAdapterAmqpConsumer(AsynchronousConsumer,MessageInputAdapter):
    
    _adapter_type = "AMQPConsumer"
    
    _message_types = [AmqpConsumerMessage]
    
    def __init__(self,**kwargs):
        self.config_init() # inject config!
        #kwargs["queue"] = self.config.get("queue",None)
        super().__init__(**kwargs)
        
    # Received an AmqpMessage and will enqueue it for the pipeline
    def on_message(self,amqp_message):  
        self._message_queue.put(amqp_message)


class SimulatedMessageInputAdapter(MessageInputAdapter):
    
    _adapter_type = "SimulatedMessageInput"
    _threaded = True
    
    _message_types = [str]

    def __init__(self,**kwargs):
        kwargs["logger_name"] = "simulatedinput"
        super().__init__(**kwargs)
        
    def run(self):
        self.set_ready()
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

class MessageOutputAdapterAmqpProducer(AsynchronousProducer,MessageOutputAdapter):
    
    _adapter_type = "AmqpProducer"
    
    _publish_exchange = "test"
    _publish_routing_key = None
    _blocking = True
    
    _message_types = [AmqpProducerMessage]
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def write(self, amqp_message):
        self.log_debug("Wroting message %s; blocking=%s" 
            % (amqp_message.routing_key,self._blocking))
        return self.publish_message(amqp_message,blocking=self._blocking)

# Message --> Console Writer
# blocking? we can consider the console stream rate to be insignificant
# fairly sure it is non-blocking anyways -- eg write to a buffer and flush
class MessageOutputAdapterConsoleWriter(MessageOutputAdapter):
    
    _adapter_type = "ConsoleWriter"
    
    _message_types = [str]

    def __init__(self,**kwargs):
        # Component, then MessageOutputAdapter
        kwargs["logger_name"] = "consolewriter"
        super().__init__(**kwargs)
        self.set_ready()
        
    def write(self,message):
        print("ConsoleWriter: %s" % message)
        self.report_event(OutputAdapterEvent.Delivered)