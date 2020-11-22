# Service Monitor
# integrated watchdog functionality?

from threading import Thread, Lock
import time
from datetime import datetime
import logging

from DistributedServicesFramework.Statistics import Statistics
from DistributedServicesFramework.Component import Component

class Watchdog():
    
    __statistics_connection = None
    __last_value = None
    
    def __init__(self, **kwargs):
        self.__statistics_connection = kwargs["statistics", None]
        self.__watch_stats_var = kwargs["stats_event", None]
        self.__timeout_secs = kwargs["timeout_secs", None]
    
    def check(self):
        self.__statistics_connection.metric['']
        return False
        # action; none for now
        
    def feed(self):
        return False

    @property
    def name(self):
        return self.__watch_stats_var
        

class ServiceMonitor(Component,Thread):
    
    # the run method (work loop) checks this flag repeatedly
    # and will stop work and exit the loop on true
    __shutdown_requested = False
    
    # could also use if __statistics != None
    __statistics_enabled = False
    
    __watchdogs = []

    def __init__(self, **kwargs):
        
        # Configure reference to Statistics instance if provided
        self.__statistics = kwargs.get("statistics", None)
        if self.__statistics:
            self.__statistics_enabled = True
            
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        
        Thread.__init__(self)
        Component.__init__(self)

    # add a watchdog that looks for a statistics call on a particular
    # field and times out when time is reached without activity
    def add_watchdog(self, stats_event, timeout_secs, name=None):
        wd = Watchdog(stats_event=stats_event, timeout_secs=timeout_secs)
        self.__watchdogs.append(wd)

    # call once per second
    def check_watchdogs(self):
        for watchdog in self.__watchdogs:
            if watchdog.check(): self.logger.info("watchdog %s has timed out!" % watchdog.name)

    # called every second
    def do_second(self): 
        #if self.__statistics:
        for metric_type in self.__statistics.metrics_types:
            print("%s: %s" % (metric_type, self.__statistics.get_metric(metric_type)))

    def run(self):
        
        # do setup
        last_second = datetime.now().timestamp()
        
        self.logger.info(self.statistics.stats_string_test())
        
        while self.keep_working():
            time.sleep(0.01)
            if (datetime.now().timestamp() - last_second) >= 30:
                # time period has elapsed
                self.do_second()
                last_second = datetime.now().timestamp()
                self.logger.info(self.statistics.stats_string_test())

        return # end run loop


    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    def keep_working(self):
        if self.__shutdown_requested: return False
        else: return True

    # Thread() has no official stop or shutdown method - the thread finishes
    # when the Thread::run() method returns and thus we must implement our own
    # control logic
    def stop(self, reason=None):
        self.__shutdown_requested = True