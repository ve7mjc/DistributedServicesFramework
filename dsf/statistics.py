# Statistics Module
# 

import dsf.domain

# get_metric(field_name) returns value associated with metric key
# (deprecated) - will instead have the watchdog go and periodically do their
#   checks for statistics metric_keys.. 
# register_watchdog(watchdog_hdl, metric_key) registers a watchdog instance
#   against a specified metric_key so we can automatically feed them

from datetime import datetime, date, timezone

# forcing logging in for the moment as while we would prefer to extend a 
# Component Class, we have too many circular imports to chase for now
import logging 

class Statistics():
    
    _parent = None
    _instance_start_timestamp = None
    _metrics = {}
    _name = None
    logger = None
    
    _watchdog_metric_key_registrations = {}
    
    def __init__(self, **kwargs):
        
        _parent = kwargs.get("parent", None)
        _instance_start_timestamp = self.now()
        
        self.log = logging.getLogger("statisticsengine")
        self.log.setLevel("INFO")
        
        return # end _init_
    
    # Increment indicated metric by value
    # Increment by 1 if value not supplied
    def metric(self, field_name, value = 1):
        if field_name not in self._metrics:
            self._metrics[field_name] = 0
        self._metrics[field_name] += value
        self.log.debug("metric reported '%s' of '%s'" % (field_name,value))
        
        # feed watchdogs which might be waiting for this metric
        # it might be better if we simply record the last time of this metric and go
        # backwards?
        if field_name in self._watchdog_metric_key_registrations:
            for watchdog_hdl in self._watchdog_metric_key_registrations[field_name]:
                watchdog_hdl.feed()

    # let it crash for now without keychecking
    def get_metric(self, field_name):
        if field_name in self._metrics:
            return self._metrics[field_name]
            
    def stats_string_test(self):
        out = "STATISTICS DUMP:"
        for metric_name in self._metrics:
            out = out + "\r\n  - %s = %s" % (metric_name,self._metrics[metric_name])
        return out

    def register_watchdog(self, watchdog_handle, metric_key):
        if metric_key not in self._watchdog_metric_key_registrations:
            self._watchdog_metric_key_registrations[metric_key] = []
        self._watchdog_metric_key_registrations[metric_key].append(watchdog_handle)
        return False

    @property 
    def metrics_types(self):
        return self._metrics.keys()
            
    def statistics(self):
        return self._statistics
    
    # Return current system POSIX timestamp
    # return value: float
    def now(self):
         return datetime.now().timestamp()
    
    # Elapsed running time of this instance in seconds
    def runtime_elapsed_seconds(self):
        return self.now() - self._instance_start_timestamp

