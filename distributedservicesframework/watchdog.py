from dsf import utilities

class Watchdog():
    
    __last_value = None
    __fed_time = None
    __expired = False
    __last_checked = utilities.utc_timestamp()
    __timeout_secs = None
    __self_reset = True
    
    # if this Watchdog is tied to a Statistics Metric Key
    __statistics_metric_key = None
    __statistics_hdl = None
    
    def __init__(self, **kwargs):
        
        #self.__statistics_connection = kwargs["statistics", None]
        self.__timeout_secs = kwargs.get("timeout_secs", None)
        self.__statistics_metric_key = kwargs.get("statistics_metric", None)
        self._name = kwargs.get("name", self.__statistics_metric_key)
        self.__service_monitor = kwargs.get("service_monitor", None)
        
        # register this watchdog with a Statistics instance if specified
        if self.__statistics_metric_key and self.__statistics_hdl:
            self.__service_monitor.statistics.register_watchdog(self, self.__statistics_metric_key)
        
        self.feed()
    
    def feed(self):
        if self.__self_reset: self.reset() # self reset
        self.__fed_time = utilities.utc_timestamp()
        return False
    
    # check watchdog last fed against timeout
    def check(self):
        self.__last_checked = utilities.utc_timestamp()
        if not self.__expired and self.__timeout_secs:
            if (self.__last_checked - self.__fed_time) >= self.__timeout_secs:
                self._expire()
                return True
        return False
        
    @property
    def timeout_secs(self):
        return self.__timeout_secs

    @property
    def name(self):
        return self._name
        
    @property
    def expired(self):
        return self.__expired
        
    ## PRIVATE MEMBERS
        
    def _expire(self):
        self.__expired = True
        return
        
    # reset the expiry and feed it
    def reset(self):
        self.__expired = False
