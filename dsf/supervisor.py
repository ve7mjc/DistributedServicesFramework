# Service Monitor
# integrated watchdog functionality?

import dsf.domain

import time
from datetime import datetime
import logging

from dsf.statistics import Statistics
from dsf import utilities, exceptionhandling
from dsf.event import *
from dsf.watchdog import Watchdog
from dsf.component import Component
from dsf.amqp import AsynchronousProducer, AsynchronousConsumer

from queue import Empty,Queue


# (1) Supervisor instance per Service
# - Monitor all components of Service
#   - Watchdogs
#   - Events
# - Link to Remote Monitoring Servicse via AMQP
# - Maintain comprehensive service state remotely
class Supervisor(Component):
    
    _watchdogs = []
    
    # list of tasks of type Dict; see register_periodict_task()
    _scheduled_tasks = []
    
    # let the Component base class know we will require special with start() 
    # and stop() calls
    _threaded = True
    
    # TODO: this results in boundless memory consumption
    _eventlog = []
    
    _registered_components = []
    
    _logger_name = "supervisor"
    
    _heartbeats = {}
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        try:
            self.kwconfig(kwargs,"monitor") # kw args namespace for modules
            self._amqp_producer = AsynchronousProducer(**kwargs,logger_name="supervisor.amqp_producer",exchange=self.kwconfig.get("publish_exchange"))
            self._amqp_consumer = AsynchronousConsumer(**kwargs,logger_name="supervisor.amqp_consumer",queue=self.kwconfig.get("control_queue"))

            self.register_periodic_task(self.check_watchdogs,0.5,name="check_watchdogs")
            # self.register_periodic_task(self.task_one,2,name="task_one")
            self.register_periodic_task(self.task_statistics_to_log,10,name="statistics_console")
        except Exception as e:
            self.log_exception()
        
    @property
    def producer(self): return self._amqp_producer
        
    @property
    def consumer(self): return self._amqp_consumer
        
    # add a watchdog that looks for a statistics call on a particular
    # field and times out when time is reached without activity
    def add_statistics_metric_watchdog(self, metric_key, timeout_secs):
        wd = Watchdog(statistics_metric=metric_key,timeout_secs=timeout_secs,service_monitor=self)
        self._watchdogs.append(wd)

    # call once per second
    def check_watchdogs(self):
        for watchdog in self._watchdogs:
            if watchdog.check(): self.logger.info("watchdog %s has timed out!" % watchdog.name)

    # called every time period
    def do_task(self): 
        #if self.__statistics:
        for metric_type in self.statistics.metrics_types:
            self.log_debug("%s: %s" % (metric_type, self.statistics.get_metric(metric_type)))
    
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

#    def send_status(self):
#        self.producer.publish(routing_key="monitor.status",body="testing!",blocking=True)

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

#    def register_event(self,agent_obj,event):
#        self.log_info("Event: %s %s %s" % (event.timestamp,event.component.name,event.type))
#        if self.keep_running:
#            self.producer.publish(routing_key="monitor.status",body="testing!",blocking=True)

    def process_component_events(self):
        # Called from thread
        while True:
            try:
                event = dsf.domain.component_events.get_nowait()
            except Empty: break

            if event.data["type"] == ComponentEvent.Heartbeat:
                print("detected heartbeat!")
                print(event.to_json())

    # Called from Component.thread.start(target=_run())
    """ ### THREAD SAFETY ### """
    def run(self):
        self.log_debug("monitor.run() starting")
        try:
            self._amqp_producer.start()
            self._amqp_consumer.start()
        except Exception as e:
            self.log_exception()

        # Start the tasks - ok, really just priming the last_run field
        # so the scheduler knows we need to run it time_secs from now
        for task in self._scheduled_tasks:
            current_utc_time = utilities.utc_timestamp()
            if task["autostart"]:
                task["last_run"] = current_utc_time

        last_report_run = datetime.now().timestamp()

        """ LOOP """
        try:
            while self.keep_working:
                self.heartbeat()
                self.process_component_events()
                #self.do_task_scheduling()
        except Exception as e:
            print("exception in service loop")
            self.log_exception()        
            
        """ SHUTDOWN """
        
        # cleanup
        self.producer.stop()
        self.consumer.stop()

        self.log_debug("passed amqp client stops")
        
        self.producer.join(1)
        self.consumer.join(1)
        
    """ ### THREAD SAFETY ### """
    
    
#        self._amqp_producer.join(1)
#        self._amqp_consumer.join(1)