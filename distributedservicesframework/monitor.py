# Service Monitor
# integrated watchdog functionality?

import time
from datetime import datetime
import logging

from dsf.statistics import Statistics
from dsf import utilities, exceptionhandling
from dsf.watchdog import Watchdog
from dsf.component import Component

class Task():
    pass

# Intention to have a single Monitor instance per Service
# Classes may obtain a handle to a Monitor, or interface through a MixIn Class
# Monitor is Threaded and may remain asynchronous and able to monitor components
#  from an isolated
class Monitor(Component):

    _watchdogs = []
    
    # list of tasks of type Dict; see register_periodict_task()
    _scheduled_tasks = []
    
    # let the Component base class know we will require special with start() 
    # and stop() calls
    _threaded = True

    def __init__(self, **kwargs):

        # pass the keyword arguments up
        # bring the logging, name down
        super().__init__(**kwargs)
        
        self.register_periodic_task(self.check_watchdogs,0.5,name="check_watchdogs")
        # self.register_periodic_task(self.task_one,2,name="task_one")
        self.register_periodic_task(self.task_statistics_to_log,10,name="statistics_console")
        
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
    
    def register_periodic_task(self, function, time_seconds=1, **kwargs):
        task = {}
        if "name" in kwargs: task["name"] = kwargs.get("name")
        else: task["name"] = "PeriodicTask #%s" % (len(self._scheduled_tasks)+1)
        task["function"]=function
        task["time_secs"]=time_seconds
        task["paused"]=False
        task["autostart"]=True
        task["last_run"]=None
        self._scheduled_tasks.append(task)
        return False
        
    def stop_periodic_task(self,task_name):
        for task in self._scheduled_tasks:
            if task_name == task["name"]:
                task["paused"]=True
                return False
        self.logger.error("Periodic Task %s does not exist! Cannot stop." % task_name)
        return True
    
    def do_task_scheduling(self):
        current_utc_time = utilities.utc_timestamp()
        for task in self._scheduled_tasks:
            if not task["paused"] and ((current_utc_time - task["last_run"]) >= task["time_secs"]):
                task_function = task["function"]
                task["last_run"] = current_utc_time # log the start, not the end
                if callable(task_function): 
                    try: # we cannot jeopordize other tasks
                        task_function()
                    except Exception as e:
                        self.logger.error("periodic task %s has thrown an exception! %s" % (task["name"],exceptionhandling.traceback_string(e)))
                else: self.logger.error("periodic task %s function is not callable!" % task["name"])

    def task_statistics_to_log(self):
        pass
        #self.log_info(self.statistics.stats_string_test())

    def task_one(self):
        self.stop_periodic_task("task_one")
        print("task 1!")

    # we are a Component and thus our do_run method is called
    # from the super run() method
    def run(self):
        
        # Start the tasks - ok, really just priming the last_run field
        # so the scheduler knows we need to run it time_secs from now
        for task in self._scheduled_tasks:
            current_utc_time = utilities.utc_timestamp()
            if task["autostart"]:
                task["last_run"] = current_utc_time
        
        # do setup
        last_report_run = datetime.now().timestamp()

        while self.keep_working:
            
            self.do_task_scheduling()
            
            self.powernap(200)

        return # end while self.keep_working

    # redeclare method here to prevent Thread stop method from being called
#    def stop(self, reason=None):
#        super().stop()
        #Component.stop(self,reason)