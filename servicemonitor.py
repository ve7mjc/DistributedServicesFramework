# Service Monitor
# integrated watchdog functionality?

from threading import Thread, Lock
import time
from datetime import datetime
import logging

from distributedservicesframework.statistics import Statistics
from distributedservicesframework.component import Component
from distributedservicesframework import utilities

class Watchdog():
    
    __statistics_connection = None
    __last_value = None
    __fed_time = None
    __expired = False
    __last_checked = utilities.utc_timestamp()
    __timeout_secs = None
    __self_reset = True
    __statistics_metric_key = None
    
    def __init__(self, **kwargs):
        
        #self.__statistics_connection = kwargs["statistics", None]
        self.__timeout_secs = kwargs.get("timeout_secs", None)
        self.__statistics_metric_key = kwargs.get("statistics_metric", None)
        self._name = kwargs.get("name", self.__statistics_metric_key)
        self.__service_monitor = kwargs.get("service_monitor", None)
        
        # register this watchdog with a Statistics instance
        if self.__statistics_metric_key and self.__service_monitor:
            self.__service_monitor.statistics.register_watchdog(self, self.__statistics_metric_key)
        
        self.feed()
    
    # check watchdog last fed against timeout
    def check(self):
        self.__last_checked = utilities.utc_timestamp()
        if not self.__expired and self.__timeout_secs:
            if (self.__last_checked - self.__fed_time) >= self.__timeout_secs:
                self._expire()
                return True
        return False
        
    def _expire(self):
        self.__expired = True
        return
        
    # reset the expiry and feed it
    def reset(self):
        self.__expired = False

    def feed(self):
        if self.__self_reset: self.reset() # self reset
        self.__fed_time = utilities.utc_timestamp()
        return False

    @property
    def name(self):
        return self._name

# task scheduler

class Task():
    pass

class ServiceMonitor(Component,Thread):
    
    # could also use if __statistics != None
    _statistics_enabled = False
    
    _watchdogs = []

    def __init__(self, **kwargs):
        
        # Configure reference to Statistics instance if provided
        self._statistics = kwargs.get("statistics", None)
        if self._statistics:
            self._statistics_enabled = True

        Thread.__init__(self)
        
        Component.__init__(self)

    # add a watchdog that looks for a statistics call on a particular
    # field and times out when time is reached without activity
    def add_statistics_metric_watchdog(self, metric_key, timeout_secs):
        wd = Watchdog(statistics_metric=metric_key, timeout_secs=timeout_secs, service_monitor=self)
        self._watchdogs.append(wd)
        return

    # call once per second
    def check_watchdogs(self):
        for watchdog in self._watchdogs:
            if watchdog.check(): self.logger.info("watchdog %s has timed out!" % watchdog.name)

    # called every time period
    def do_task(self): 
        #if self.__statistics:
        for metric_type in self.statistics.metrics_types:
            print("%s: %s" % (metric_type, self.statistics.get_metric(metric_type)))

    def run(self):
        
        # do setup
        last_report_run = datetime.now().timestamp()
        
        while self.keep_working:

            self.check_watchdogs()
            time.sleep(0.2)
            
            # 60 second interval
            if (datetime.now().timestamp() - last_report_run) >= 300:
                # time period has elapsed
                #self.do_task()
                last_report_run = utilities.utc_timestamp()
                self.logger.info(self.statistics.stats_string_test())

        return # end while self.keep_working

    # redeclare method here to prevent Thread stop method from being called
    def stop(self, reason=None):
        Component.stop(self,reason)