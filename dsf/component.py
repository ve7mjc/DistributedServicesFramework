#
# Common Component Properties and Methods
#
# Provide functionality to identifiable software components within a distributed
# environment.
#
# This class is not Threaded but is Thread-ready via multiple inheritance and 
# provides a consistent keep_working getter property and stop() setter method
# 
# Extend this class when an object within the system may benefit from the
# following:
#
# - Distinguishable Name and Common Interface
# - Common logging interface plugged into root logger
# - Test Mode Configuration - Fine grained and extensible run-time test-modes
# - Integrated statistics and metrics reporting
#   partipate in parent statistics with component based reporting
# - Interface to application System Monitor and Watchdog
#
"""
# Important Notes
#
# Extending Component
# - __init__(): Make sure this class init is called. It is written to 
#   cooperate with multiple inheritance.
# - self.setready(): ensure that when the child class is ready that it
#   make this call. This should either be a) prior to returning from its own 
#   init method, or b) in a threaded run method prior to entering a loop
# - Threaded work loop: If a threaded worker loop is desired, a) define a
#   child.run() method and make sure to call child.start() and child.stop()
#   in application
#
# The largest inheritance challenges will be with:
#  - self._name ; the name of the component is used to obtain a logger
#  - self._logger ; 
# Multiple Inheritance
# class FooBar(Foo, Bar):
#  super will call base classes from right to left
#  super().__init__() - this calls all constructors up to Foo (aka all)
#  super(Foo, self).__init__(bar) - call all constructors after Foo up to Bar
"""

import dsf.domain

from dsf.event import *
#from dsf.event import *
#from dsf.agent import Agent

from enum import Enum
import logging
from threading import Thread
from time import sleep, perf_counter
import queue

# start() instruct component to become ready; if threaded, thread will start
# stop() instruct component to stop activity/work; thread will stop; expect
#   not to be able to start again
# is_ready() if component ready to do its primary job. May be immediately after
#   initialization, or in a run loop
# is_failed() 
# is_stopped() if completely stopped
# 
# PROPERTIES
# failed_reason - string why component is failed

# Features:
# Test Mode functionality to support convenient and reliable fine-grained 
#  testing requirements
#
class Component():

    # the presence of a key indicates it is enabled and a value
    # indicates a setting of other True or mode specific value
    # this will not be accessible outside this class, instead use the property
    _enabled_test_modes = {}
    
    # monitoring and statistics
    _created_timestamp = None
   
    # component level statistics. Needs to be configured in the constructor
    # and should be passed the component name and reference handle to global
    # Statistics instance
    _statistics = None
    _central_events_queue = None
    
    _logger = None
    _loglevel = None
    _prelogger_log_messages = None
    
    # Component relationships
    _parent = None
    _children = []

    # State related
    _initialized = False
    _started = False
    _stopped = False
    _ready = False
    _failed = False
    _failed_reason = None
    
    # This variable works with the inverted getter property (self.keep_working) 
    # and the setter method stop() to control whether a work loop such as 
    # threading.Thread::run method (thread worker) may continue working.
    # TODO: Mutex Lock
    _stop_requested = False
    
    # Start and Stop Operation
    #  Designate how the class.stop(), class.is_stopped(), 
    #  class.keep_running and other aspects behave with this 
    #  implementation. 
    # See StartMode and StopMode enum classdefs for more information
    _threaded = False
    _thread = None
    _start_required = False
    _stop_required = False
    
    #_agent = None
    
    def __init__(self, **kwargs):
        try:
            # default name to that of child class type
            if not hasattr(self,"_name"):
                self._name = self.get("_name",self.__class__.__name__)
            
            # reference to a thread-safe queue to write all of our events
            self._central_events_queue = dsf.domain.component_events
            
            # Add a Thread using composition
            if self._threaded:
                self._start_required = True
                self._stop_required = True
                self._thread = Thread(target=self._run,name="%s-thread" % self.name)

            logger_name = kwargs.get("logger_name", self.get("_logger_name", self.name))
            self._logger = dsf.domain.logging.get_logger(logger_name)
            self.set_loglevel(kwargs.get("loglevel","debug"))
            
            # ** Avoid calls which may result in emitting events or produce 
            #    logging output above this! **
            
            if self._name != self.__class__.__name__: 
                self.log_debug("initializing %s <class '%s'>" % (self._name,self.__class__.__name__))
            else: self.log_debug("initializing <class '%s'>" % self.__class__.__name__)

            dsf.domain.register_component(self)
            
            # Set our status to ready if not Threaded and do not require start() 
            # to be called
            if not self._start_required:
                self.set_ready()
                
            self._pre_logger_log_messages() # deprecate?
            
            self._initialized = True

            super().__init__()
            
            self.report_event(ComponentEvent.Created)
            
        except Exception as e:
            self.log_exception()
            
    # Get class attribute; return default if attribute not exists OR is None
    def get(self,attribute,default):
        if hasattr(self,attribute):
            value = getattr(self,attribute)
            if value is None: return default
            else: return value
        else: return default
              
    # whether this class init method has been called
    # shall only be set by the init mothod of this class
    @property
    def initialized(self): return self._initialized

    def set_name(self, name): self._name = name

    @property
    def name(self): return self._name

    @property
    def thread(self): return self._thread
   
    # join() raises a RuntimeError if an attempt is made to join the current 
    #  thread as that would cause a deadlock. It is also an error to join() a 
    #  thread before it has been started and attempts to do so raise the same 
    #  exception.
    def join(self,timeout=None):
        if self.thread: self.thread.join(timeout)

    #@property
    #def agent(self): return self._agent
        
    def kwconfig(self,kwargs,prefix):
        nkwargs = {}
        for key in kwargs:
            if key.startswith("%s." % prefix):
                nkwargs[key[len(prefix)+1:]] = kwargs[key]
                #print("%s = %s" % (key[len(prefix)+1:],kwargs[key]))
        self.kwconfig = nkwargs
        return nkwargs

    def set_ready(self):
        self.log_debug("%s <class '%s'> ready!" % (self.name,type(self).__name__))
        self._ready = True
        self.report_event(ComponentEvent.Ready)
        if self._failed:
            self.log_warning("setready() called; note that we are failed with %s" % self.failed_reason)

    _last_heartbeat_time = 0
    _heartbeat_holdoff_time = 0.5
    def heartbeat(self):
        if (perf_counter() - self._last_heartbeat_time) >= self._heartbeat_holdoff_time:
            self._last_heartbeat_time = perf_counter()
            self.report_event(ComponentEvent.Heartbeat)

    def report_event(self,event_type,*args,**kwargs):
        if not isinstance(self._central_events_queue,queue.Queue):
            self.log_warning("report_event() unable to central events queue (%s type)" % 
                type(self._central_events_queue).__name__)
        try:
            if type(event_type) is ComponentEvent:
                message = None
                if len(args): message = args[0]
                event = Event(component_name=self.name,event_type=event_type,message=message)
                self._central_events_queue.put(event)
        except Exception as e:
            self.log_exception()

    # When this component is ready to perform its intended 
    # purpose - eg. it has connected to a remote service, authenticated, 
    # configured itself and may be awaiting further comands 
    def is_ready(self): return self._ready

    # Called when a critical and unrecoverable condition has occured
    # Optional reason. Use this method to ensure necessary steps are taken
    # Call self.stop() to log an exception and request a stop as this method
    #  will be called afterwards
    def set_failed(self, reason):
        self._ready = False
        self._failed = True
        self._failed_reason = reason
        self.log_error("component failed: %s" % reason)
        self.report_event(ComponentEvent.Failed,reason)
        self.stop()
        
    # An unrecoverable condition is present which prevents 
    # this component from being able to fulfil its intended function
    # Components start out not failed and require a set_failed() call
    def is_failed(self): return self._failed
    
    @property
    def failed_reason(self): return self._failed_reason
        
    @property
    def logger(self): return self._logger
        
    def set_logger_name(self,name):
        if self._logger: self._logger = dsf.domain.logging.get_logger(name)
        self._logger_name = name

    # set the logging level of a logger by name or if only one argument
    # supplied, apply to this instance logger!
    def set_loglevel(self, param1, param2=None):
        if not param2:
            self.logger.setLevel(param1.upper())
        else:
            dsf.domain.logging.get_logger(param1).setLevel(param2.upper())
        
    # Convenience method to point us to class logger instance while also
    #  permitting features such as enqueing a log message prior to logger 
    #  creation
    def log(self,level,msg,*args,**kwargs):
        if not isinstance(msg, str): msg = str(msg)
        if kwargs.get("squashlines",None):
            msg = msg.replace("\r", "").replace("\n", "")
            del kwargs["squashlines"]
        if self._logger:
            self._logger.log(level,msg,*args,**kwargs)
        else:
            log_message = (level,msg,args,kwargs)
            if not self._prelogger_log_messages: 
                self._prelogger_log_messages = []
            self._prelogger_log_messages.append(log_message)
            
    def log_error(self,msg,*args,**kwargs):
        self.log(logging.ERROR,msg,*args,**kwargs)
    def log_warning(self,msg,*args,**kwargs):
        self.log(logging.WARNING,msg,*args,**kwargs)
    def log_info(self,msg,*args,**kwargs):
        self.log(logging.INFO,msg,*args,**kwargs)
    def log_debug(self,msg,*args,**kwargs):
        self.log(logging.DEBUG,msg,*args,**kwargs)
    
    # Log the Exception on the stack
    # - filename, line number, and method where exception occurred
    # - exception type and message
    def log_exception(self,stacklevel=1):
        try:
            context = dsf.domain.exception_context()
            source_info = "{filen}->{method}():{line}: {etype}: {msg}".format(
                filen = context["filename"], method = context["method_name"],
                line = context["lineno"], etype = context["typestr"],
                msg = context["message"])
            self.log_error(source_info,squashlines=True)
        except Exception as e:
            self.log_error("exception in log_exception()! %s - %s" % (type(e).__name__,e.__str__()))

    # Dump enqueued log messages to logger
    # Create logger instance if necessary
    def _pre_logger_log_messages(self):
        # Create a root logger if we do not have a logger
        logger = getattr(self,"_logger",None)
        if not logger: logger = logging.getLogger()
        if self._prelogger_log_messages:
            for log_msg in self._prelogger_log_messages:
                level,msg,args,kwargs = log_msg
                logger.log(level,msg,*args,**kwargs)

    # present a start method so we may be called similar to a class which 
    # extends a Thread
    # OVERRIDING - Call this method via super().start(**kwargs) last!
    def start(self,**kwargs):
        
        if not self._start_required:
            self.log_debug("start() called but self._start_required=False")
            return
            
        if self.get("_started",False): 
            self.log_debug("start() called but already started")
            return

        # Start thread, or if no thread, consider us ready
        if self._threaded: self.thread.start()
        else: 
            self._ready = True
        
        self._started = True
        self.report_event(ComponentEvent.Started)
    
    @property
    
    def keep_working(self):
        return not self._stop_requested
    
    # Private method to call from the Thread so we may do tasks
    #  before and after the child class run loop
    # Thread safety!
    def _run(self):
        try:            
            self.log_debug("entering _run() via thread")
            if hasattr(self,"run"):
                try:
                    self.run()
                except Exception as e:
                    self.logger.critical("error in self.run() method! see next")
                    self.set_failed("critical exception in run()")
                    self.log_exception(stacklevel=1)
            else:
                self.logger.warning("%s.start() has been called but no run() method has been defined!" % type(self).__name__)

            # optional child on_stop() method 
            if hasattr(self,"on_stop"): self.on_stop()
                
            self.log_debug("thread worker _run() exiting")
            
        except Exception as e:
            self.log_exception()
            
        self.report_event(ComponentEvent.Stopped)
        self._stopped = True

    
    # Block for designated time; Releasable by watching 
    # duration_ms - how long in ms to block for. Values >= 100 result in 
    #   the the use of system clock elapsed time to increase the accuracy
    #   uses time.perf_counter() in Python >= 3.3 performance counter, i.e. a 
    #   clock with the highest available resolution
    # check_interval - the longest time we may go without checking for a releas
    # Precision: Generally accepted that Linux ~ 1 msec and Windows ~ 16 msec
    def powernap(self,duration_ms=0,**kwargs):
        # opportunity to optimize this method and reduce calls to system time
        # based on how far away from the target we may be
        checkintervalms = kwargs.get("check_intervalms",5)
        high_accuracy = kwargs.get("high_accuracy",False)
        watchvar = kwargs.get("watch_var",self.keep_working)
        if duration_ms < 100 and not high_accuracy:
            for i in range(floor(checkintervalms/duration_ms)):
                if not watchvar: return
                sleep(checkintervalms / 1000)
            sleep((checkintervalms % duration_ms) / 1000)
            return
            
        # high accuracy version with remainder
        start_time_secs = perf_counter()
        while watchvar:
            elapsed_time = perf_counter() - start_time_secs
            if elapsed_time >= (duration_ms - checkintervalms) / 1000:
                sleep((duration_ms % checkintervalms) / 1000)
                break
            sleep(checkintervalms / 1000)
        return



    ### TEST MODE METHODS
    # examples
    # - amqp_nack_requeue_all_messages
    def enable_test_mode(self, test_mode_name, value=True):
        self._enabled_test_modes[test_mode_name] = value
        if hasattr(self, "logger"):
            self.logger.info("enabled test mode %s (val=%s)" % (test_mode_name,value))
    
    # support passing of keyword arguments dict
    def enable_test_modes(self, **kwargs):
        for test_mode in kwargs:
            self.enable_test_mode(test_mode, kwargs.get(test_mode, True))
    
    # return class private list of test modes enabled
    @property
    def enabled_test_modes(self): return self._enabled_test_modes
    
    # return the status of a single test_mode
    # returns value (True or other test mode related value) or False
    # if the test_mode is not enabled
    def test_mode(self, test_mode_name):
        if test_mode_name in self._enabled_test_modes:
            return self._enabled_test_modes[test_mode_name]
        return False

    # Register a child component
    # Pass our handle as its parent
    def registerchild(self,child):
        if self is child:
            self.log_error("registerchild called with itself. ignoring to prevent a recursion bomb")
            return
            
        if not isinstance(child,Component):
            self.logger.error("addchild(child) called with object of type `%s` - not a Component!" % type(child).__name__)
            return True
            
        if child in self._children:
            name = getattr(child,"name","unknown")
            self.logger.error("addchild(child) called for `` which is already in our children!" % name)
            return True
            
        self._children.append(child)
        child.setparent(self)
    
    @property
    def children(self): return self._children
        
    def setparent(self,parent): self._parent = parent
    
    @property
    def parent(self): return self._parent

    # Thread does not have a shutdown method. Set class variable
    #  self._stop_requested which the threaded run() method will check and stop
    #  when able
    # OVERRIDING this method: Subclasses should call this method first!
    def stop(self,reason=None):
        
        if not self._stop_required:
            self.log_debug("stop() called but self._stop_required==False")
            self._stopped = True
            return False
            
        if self._stop_requested: return False
        if not self._started:
            self.log_debug("stop() called but self.started==False")
            return False

        if self._threaded:
            self.report_event(ComponentEvent.Stopping)
        else:
            self._ready = False
            self._stopped = True
            self.report_event(ComponentEvent.Stopped)

        if not reason:
            self.log_debug("shutdown requested")
        elif isinstance(reason,str):
            self.log_debug("shutdown requested: %s" % reason)

        self._stop_requested = True

    @property
    def stop_requested(self): 
        return self._stop_requested

    # is_stopped() method - Return True if this class instance reports stopped
    # In the case of Threaded working, it means the run loop has exited
    def is_stopped(self): return self._stopped
