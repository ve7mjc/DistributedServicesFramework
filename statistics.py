# Statistics Module
# 

from datetime import datetime, date, timezone

class Statistics():
    
    __parent = None
    __instance_start_timestamp = None
    __metrics = {}
    __name = None
    
    __watchdog_metric_key_registrations = {}
    
    def __init__(self, **kwargs):
        
        __parent = kwargs.get("parent", None)
        __instance_start_timestamp = self.now()
        return # end __init__
    
    # Increment indicated metric by value
    # Increment by 1 if value not supplied
    def metric(self, field_name, value = 1):
        if field_name not in self.__metrics:
            self.__metrics[field_name] = 0
        self.__metrics[field_name] += value
        # feed watchdogs if any
        if field_name in self.__watchdog_metric_key_registrations:
            for watchdog_hdl in self.__watchdog_metric_key_registrations[field_name]:
                watchdog_hdl.feed()

    # let it crash for now without keychecking
    def get_metric(self, field_name):
        if field_name in self.__metrics:
            return self.__metrics[field_name]
            
    def stats_string_test(self):
        out = "STATISTICS DUMP:"
        for metric_name in self.__metrics:
            out = out + "\r\n%s=%s" % (metric_name,self.__metrics[metric_name])
        return out

    def register_watchdog(self, watchdog_handle, metric_key):
        if metric_key not in self.__watchdog_metric_key_registrations:
            self.__watchdog_metric_key_registrations[metric_key] = []
        self.__watchdog_metric_key_registrations[metric_key].append(watchdog_handle)
        return False

    @property 
    def metrics_types(self):
        return self.__metrics.keys()
            
    def statistics(self):
        return self.__statistics
    
    # Return current system POSIX timestamp
    # return value: float
    def now(self):
         return datetime.now().timestamp()
    
    # Elapsed running time of this instance in seconds
    def runtime_elapsed_seconds(self):
        return self.now() - self.__instance_start_timestamp

