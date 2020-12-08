# Service Monitor
# integrated watchdog functionality?

import dsf.domain

import time
from datetime import datetime
import logging

from dsf.statistics import Statistics
from dsf import utilities, exceptionhandling
from dsf.event import *
from dsf.exceptions import *
from dsf.watchdog import Watchdog
from dsf.component import Component
from dsf.amqp import AsynchronousProducer, AsynchronousConsumer

from queue import Empty,Queue

from copy import copy

import json

# Status Report
# Availability
class StatusReport():
    
    data = {}
    
    def __init__(self):
        self.data["timestamp"] = utilities.utc_timestamp()
        if hasattr(dsf.domain,"service") and hasattr(dsf.domain.service,"name"):
            self.data["service"] = dsf.domain.service.name
    def to_json(self):
        return json.dumps(self.data)


# (1) Supervisor instance per Service
# - Monitor all components of Service
#   - Watchdogs
#   - Events
# - Link to Remote Monitoring Services via AMQP
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
    service = None
    
    _logger_name = "supervisor"
    
    _heartbeats = {}
    _last_report_publish_time = 0
    _minimum_check_in_time_secs = 30
    
    publish_queue = Queue()
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        try:
            # Proceed with Caution - ensure that events do not result in events
            #  aka no recursion bombs
            
            p_kwargs = copy(kwargs)
            p_kwargs["logger_name"] = "supervisor.amqp_producer"
            p_kwargs["exchange"] = kwargs.get("supervisor.exchange")
            self._amqp_producer = AsynchronousProducer(**p_kwargs)
            
            c_kwargs = copy(kwargs)
            c_kwargs["logger_name"] = "supervisor.amqp_consumer"
            c_kwargs["queue"] = kwargs.get("supervisor.queue")
            self._amqp_consumer = AsynchronousConsumer(**c_kwargs)
            
            self.register_periodic_task(self.check_watchdogs,0.5)
            # self.register_periodic_task(self.task_one,2,name="task_one")
            self.register_periodic_task(self.task_statistics_to_log,10)
            self.register_periodic_task(self.periodic_service_status_report,5)
            
            # transitioning to gelf/udp
            #self.log_queue = dsf.domain.logging.queue
            
        except Exception as e:
            self.log_exception()
        
    @property
    def amqp_producer(self): return self._amqp_producer
        
    @property
    def amqp_consumer(self): return self._amqp_consumer
    
    # register a service with this supervisor?
    def register_service(self,service_obj):
        self.service = service_obj
        self.log_debug("Registered Service %s" % service_obj.name)

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
        for metric_type in self.statistics.metrics_types:
            self.log_debug("%s: %s" % (metric_type, self.statistics.get_metric(metric_type)))
    
    def register_periodic_task(self, function, time_seconds=1, **kwargs):
        task = {}
        task["name"] = function.__func__.__name__
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

#    def do_log_queue(self):
#        while True:
#            try:
#                gelfmsg = self.log_queue.get_nowait()
#                gelfmsg.host = self.service.name
#                if gelfmsg._logger_name != self._amqp_producer.logger_name:
#                    self.amqp_producer.publish(exchange="services",routing_key="services.logs",body=gelfmsg.to_json())   
#                # exchange, routing_key, body, properties, blocking
#                #
#                #print("log_msg = %s" % gelfmsg.to_json())
#            except Empty: break

    def process_component_events(self):
        # Called from thread
        try:
            while True:
                report_event = False
                try:
                    event = dsf.domain.component_events.get_nowait()
                    if event.type_code == ComponentEvent.Heartbeat.value:
                        self._heartbeats[event["component"]] = event
                    else: report_event = True
                    
                    self.log_debug("Received Event <%s> from %s; reporting=%s" % 
                        (event.data["type"], event.data["component"], report_event))
                    
                    if report_event:
                        #event.data["service"] = self.service.name
                        self.amqp_producer.publish(exchange="services",routing_key="services.events",body=event.to_json())
                    
                except Empty: break
                    
        except Exception as e:
            self.log_exception()
            
    def wait_components_ready(self,components,timeout_secs=3):
        started_time = utilities.utc_timestamp()
        total_components_ready = 0
        components_ready = 0
        while (utilities.utc_timestamp() - started_time) < timeout_secs:
            components_ready = 0 # clear every round
            for component in components:
                if component.is_ready(): components_ready += 1
                if component.is_failed():
                    raise ComponentStartFailed(type(component).__name__)
            if components_ready == len(components):
                self.log_debug("leaving wait_components_ready()")
                return components_ready
            time.sleep(0.01) # test for impact without
    
    # if we have not reported to the remote server in time_secs, we will
    # force ourself to report in
    
    def periodic_service_status_report(self):
        try:
            sr = StatusReport()
            sr.data["status"] = "ok"
            self.amqp_producer.publish(exchange="services",routing_key="services.reports.status",body=sr.to_json())
            self.log_debug("sending periodic service status report")
            
        except Exception as e:
            exceptionhandling.print_full_traceback_console()
            print(e.__repr__())

    # def periodic_check_in(self):
#        if (utilities.utc_timestamp() - self._last_report_publish_time) >= self._minimum_check_in_time_secs:
#            event = Event(component_name=self.name,event_type=ComponentEvent.Heartbeat)
#            #self.amqp_producer.publish(exchange="services",routing_key="services.events",body=event.to_json())
#            self.log_debug("have not published in %s secs; sending a heartbeat" % self._minimum_check_in_time_secs)
#            self._last_report_publish_time = utilities.utc_timestamp()
        
    # Called from Component.thread.start(target=_run())
    """ ### THREAD SAFETY ### """
    def run(self):
        
        self.log_debug("monitor.run() starting")
        
        try:
            self._amqp_producer.start()
            self._amqp_consumer.start()
        except Exception as e:
            self.log_exception()
            return
            
        # block waiting for AMQP clients to be ready
        try:
            self.wait_components_ready([self._amqp_producer,self._amqp_consumer])
        except (ComponentStartFailed, ComponentStartTimeout) as e:
            self.log_warning("wait_components_ready")
            self.set_failed("amqp clients")
            return

        # Start the tasks - ok, really just priming the last_run field
        # so the scheduler knows we need to run it time_secs from now
        for task in self._scheduled_tasks:
            current_utc_time = utilities.utc_timestamp()
            if task["autostart"]:
                task["last_run"] = current_utc_time

        last_report_run = datetime.now().timestamp()

        if self.keep_working:
            self.log_info("Supervisor running")
            self.set_ready() # this may unblock other servicse

        """ LOOP """
        while self.keep_working:
            try:
                #self.process_heartbeats()
                self.process_component_events()
                #self.do_log_queue()
                self.do_task_scheduling()
                pass
            except Exception as e:
                print("exception in service loop")
                self.log_exception()

        """ SHUTDOWN """
        
        # cleanup
        self.amqp_producer.stop()
        self.amqp_consumer.stop()

        self.log_debug("passed amqp client stops")
        
        self.amqp_producer.join(1)
        self.amqp_consumer.join(1)
        
        self.log_debug("supervisor leaving thread run method")
        
    """ ### THREAD SAFETY ### """

#        self._amqp_producer.join(1)
#        self._amqp_consumer.join(1)