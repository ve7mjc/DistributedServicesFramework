from dsf import utilities
from enum import Enum
import json

class ComponentEvent(Enum):
    
    Created = "created",
    Started = "started",
    Ready = "ready",
    Failed = "failed",
    Stopping = "stopping",
    Stopped = "stopped",
    Heartbeat = "heartbeat",

class Event():
    
    data = {}
    
    def __init__(self,component_name=None,event_type=None,message=None):
        self.data["timestamp"] = utilities.utc_timestamp()
        self.data["component"] = component_name
        self.data["type"] = event_type.value[0]
        if message: self.data["message"] = message
        
    def to_json(self):
        return json.dumps(self.data)