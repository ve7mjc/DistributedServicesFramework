from dsf import utilities
from enum import Enum
import json

class EventType(Enum):
    pass

class ComponentEvent(EventType):
    
    Created = "component_created",
    Started = "component_started",
    Ready = "component_ready",
    Failed = "component_failed",
    Stopping = "component_stopping",
    Stopped = "component_stopped",
    Heartbeat = "component_heartbeat",

class InputAdapterEvent(EventType):
    
    Listening = "input_listening",
    Receiving = "input_receiving",
    ReceivedMessage = "input_received_msg",
    ReceiveTimeout = "input_receive_timeout"
    
class OutputAdapterEvent(EventType):
    
    WroteMessage = "output_wrote_message",
    Delivered = "output_delivered",

class Event():

    data = {}
    
    def __init__(self,component_name=None,event_type=None,message=None):
        self.data["timestamp"] = utilities.utc_timestamp()
        self.data["component"] = component_name
        self.data["type"] = event_type.value[0]
        if message: self.data["message"] = message
        
    def to_json(self):
        return json.dumps(self.data)
        
    @property
    def timestamp(self): return self.data["timestamp"]