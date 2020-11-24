# Service Monitor
# integrated watchdog functionality?

from threading import Thread, Lock
import time
from datetime import datetime
import logging

from distributedservicesframework.statistics import Statistics
from distributedservicesframework.component import Component
from distributedservicesframework import utilities, exceptionhandling
from distributedservicesframework.watchdog import Watchdog

# task scheduler

class Task():
    pass

class ServiceMonitor(Component,Thread):

    _watchdogs = []
    
    # list of tasks of type Dict; see register_periodict_task()
    _scheduled_tasks = []

    def __init__(self, **kwargs):

        # pass the keyword arguments up
        # bring the logging, name down
        Component.__init__(self, **kwargs)

        # Thread competes for self_name!
        Thread.__init__(self)
        
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
        self.logger.info(self.statistics.stats_string_test())

    def task_one(self):
        self.stop_periodic_task("task_one")
        print("task 1!")

    # we are a Component and thus our do_run method is called
    # from the super run() method
    def do_run(self):
        
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
            
            # sleep for 200ms before doing it again
            time.sleep(0.2)

        return # end while self.keep_working

    # redeclare method here to prevent Thread stop method from being called
    def stop(self, reason=None):
        Component.stop(self,reason)