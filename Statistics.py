# Statistics Module
# 

from datetime import datetime, date, timezone



class Statistics():
    
    __parent = None
    __instance_start_timestamp = None
    __metrics = {}
    __name = None
    
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

    # let it crash for now without keychecking
    def get_metric(self, field_name):
        if field_name in self.__metrics:
            return self.__metrics[field_name]
            
    def stats_string_test(self):
        out = ""
        for metric_name in self.__metrics:
            out = out + "%s=%s\r\n" % (metric_name,self.__metrics[metric_name])
        return out

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

