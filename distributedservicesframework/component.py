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

import dsf.servicedomain as servicedomain

from enum import Enum
from datetime import datetime
import logging
import sys, os
from pathlib import Path
#from sys import stdout, argv
from threading import Thread
from time import sleep, perf_counter

from sys import exc_info
import traceback

# Features:
# Test Mode functionality to support convenient and reliable fine-grained 
#  testing requirements
#
class Component():

    # the presence of a key indicates it is enabled and a value
    # indicates a setting of other True or mode specific value
    # this will not be accessible outside this class, instead use the property
    _enabled_test_modes = {}
    
    # do we want to maintain a link to "the" service monitor this way?
    _monitor = None
   
    # component level statistics. Needs to be configured in the constructor
    # and should be passed the component name and reference handle to global
    # Statistics instance
    _statistics = None
    
    _logger = None
    _loglevel = None
    _prelogger_log_messages = None
    
    # Component relationships
    _parent = None
    _children = []

    # State related
    _initialized = False
    _started = False
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
    _start_required = False
    _stop_required = False
    
    def __init__(self, **kwargs):

        # Threading. We do not inherit a Thread but instead
        # use composition to enable a Component to have a thread worker
        self._thread = None
        if self._threaded:
            self._thread = Thread(target=self._run)

        # The module name will default to the name of the type of the child
        #  class.
        if not hasattr(self, "_name"):
            self._name = self.__class__.__name__

        if "loglevel" in kwargs: self._loglevel = kwargs.get("loglevel")
        
        self._logger = self.get_logger(self.name)
        self.set_loglevel("debug")

        # Set our status to ready if not Threaded and do not require start() 
        # to be called
        if not self._threaded and not self._start_required:
            self._started = True
            self._ready = True
            
        self._pre_logger_log_messages()

        self._initialized = True

        super().__init__(**kwargs)
            
    # whether this class init method has been called
    # shall only be set by the init mothod of this class
    @property
    def initialized(self): return self._initialized
            
    @property
    def name(self): return self._name

    @property
    def thread(self): return self._thread

    # When this component is ready to perform its intended 
    # purpose - eg. it has connected to a remote service, authenticated, 
    # configured itself and may be awaiting further comands 
    def is_ready(self): return self._ready

    # An unrecoverable condition is present which prevents 
    # this component from being able to fulfil its intended function
    # Components start out not failed and require a set_failed() call
    def is_failed(self): return self._failed
    
    @property
    def failed_reason(self):
        return self._failed_reason
    
    def setready(self):
        self.log_debug("%s <class '%s'> ready!" % (self.name,type(self).__name__))
        self._ready = True
        if self._failed:
            self.log_warning("setready() called; note that we are failed with %s" % self.failed_reason)
    
    # Called when a critical and unrecoverable condition has occured
    # Optional reason. Use this method to ensure necessary steps are taken
    # Call self.stop() to log an exception and request a stop as this method
    #  will be called afterwards
    def set_failed(self, reason):
        self._ready = False
        self._failed = True
        self._failed_reason = reason
        
    @property
    def failed_reason(self):
        return self._failed_reason
        
    # Obtain a handle to the logging module root logger
    # Remove any unexpected handlers which may be present
    # Attach a StreamHandler (console) and FileHanlder (log files)
    # We do not need to maintain a handle to the root logger as it can be
    #  obtained at any time through a global module-level method
    def configure_root_logger(self):
        
        # Many logging method calls will attempt to automatically configure 
        #  loggers which have not been configured (no handlers, etc)
        # .getLogger does not cause the logger to configure
        root_logger = logging.getLogger()
        
        # Some libraries add handler(s) to the root logger during their import
        #  for various reasons including squelching warnings but most 
        #  importantly before we even establish logging (looking at you pika!)
        # Remove any attached handlers so we may impose our ways upon it
        while root_logger.hasHandlers():
            root_logger.removeHandler(logger.handlers[0])

        # Add a StreamHandler attached to stdout
        # StreamHandler appears to default to writing output to STDERR 
        # so we pass sys.stdout 
        root_stream_handler = logging.StreamHandler(sys.stdout)
        root_logger.addHandler(root_stream_handler)
        
        # make sure that all children of the root logger will have a formatted {app_name}.{module_name}
        root_logger_formatting = '%(asctime)s %(name)s %(levelname)s %(message)s'
        #root_logger_formatting = '%(asctime)s ' + self.name + '.%(name)s %(levelname)s %(message)s'
        formatter = logging.Formatter(root_logger_formatting)
        root_stream_handler.setFormatter(formatter)
        
        # File Handler
        file_handler = logging.FileHandler('%s.log' % self.name.replace(" ", "_"))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        # By default the root logger is set to WARNING and all loggers you define
        #  appear to inherit that value if they are loglevel.NOTSET
        # Here we are setting the loglevel of the root logger
        #root_logger.setLevel(logging.NOTSET)
        
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
#        exc_type, exc_obj, exc_tb = exc_info()
#        filename = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#        method_name = exc_tb.tb_frame.f_code.co_name
#        return "in {filename}->{method_name}() line {line_no}: {exc_type}: {msg}".format(
#        exc_type=exc_type.__name__, msg=exception_string, filename=filename, line_no=exc_tb.tb_lineno, method_name=method_name)
        # lets get file and line number in!
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
            if stacklevel < 1: stacklevel = 1
            exc_type, exc_obj, exc_tb = exc_info()
            exc_tb = traceback.extract_tb(exc_tb,limit=stacklevel)[-1]
            filename = exc_tb.filename.replace(servicedomain.app_dirnamestr,".")
            method_name = exc_tb.name
            lineno = exc_tb.lineno
            source_info = "{filename}->{method_name}():{line_no}: {exc_type}: {msg}".format(
                exc_type=exc_type.__name__, msg=exc_obj.__str__(), filename=filename, line_no=lineno, method_name=method_name)
            self.log_error(source_info,squashlines=True)
        except Exception as e:
            self.log_error("exception in log_exception()! %s - %s" % (type(e),e.__str__()))

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
        
    def get_logger(self,name):
        logger = logging.getLogger(name)
        return logger
        
#    # attempt to determine a suitable logger name for a passed
#    #  object
#    def get_logger_name_module(self,instance):
#        if "logger_name" in kwargs:
#            logger_name = kwargs.get("logger_name")
#        elif hasattr(self,"_logger_name") and self._logger_name is not None:
#            logger_name = self._logger_name
#        elif hasattr(self,"_name") and self._name is not None:
#            self._logger_name = self._name
#        else:
#            self._logger_name = type(self).__name__.lower()
    
    @property
    def logger(self):
        return self._logger

    def set_name(self, name):
        self._name = name
    
    def utc_timestamp(self):
        return datetime.now().timestamp()

    @property
    def monitor(self):
        return self._monitor

    @property
    def statistics(self):
        return self._statistics
    
    # Private method to call from the Thread so we may do tasks
    #  before and after the child class run loop
    def _run(self):
        if hasattr(self,"run"):
            try: 
                self.run()
            except Exception as e:
                self.logger.error("error in self.run() method! see next message.")
                self.log_exception()
        else:
            self.logger.warning("%s.start() has been called but no run() method has been defined" % type(self).__name__)
        self._ready = False # @property.is_stopped() will be True now

    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    @property
    def keep_working(self): 
        return not self._stop_requested
        
    @property
    def stop_requested(self):
        return self._stop_requested
    
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

    # set the logging level of a logger by name or if only one argument
    # supplied, apply to this instance logger!
    def set_loglevel(self, param1, param2=None):
        if not param2: 
            self.logger.setLevel(param1.upper())
        else:
            logging.getLogger(param1).setLevel(param2.upper())

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
    def enabled_test_modes(self):
        return self._enabled_test_modes
    
    # return the status of a single test_mode
    # returns value (True or other test mode related value) or False
    # if the test_mode is not enabled
    def test_mode(self, test_mode_name):
        if test_mode_name in self._enabled_test_modes:
            return self._enabled_test_modes[test_mode_name]
        return False

    
    # present a start method so we may be called similar to a class which 
    # extends a Thread
    # OVERRIDING - Call this method via super().start(**kwargs) last!
    def start(self,**kwargs):
        if hasattr(self,"_started") and self._started is True:
            return

        # Start thread, or if no thread, consider us ready        
        if self._threaded: self.thread.start()
        else: self._ready = True
        
        self._started = True

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
    def children(self):
        return self._children
        
    def setparent(self,parent):
        self._parent = parent
    
    @property
    def parent(self):
        return self._parent
    
        
    # Thread has no stop or shutdown method - instead we set a class variable
    #  self._stop_requested which the threaded run() method will check and stop
    #  at some point after
    # OVERRIDING this method: Subclasses should call this method first!
    def stop(self,reason=None):

        if not hasattr(self,"_started") and not self._started:
            print("not started!")
            return

        if self.stop_requested:
            self.log_error("additional shutdown requested received!")

        if not reason:
            self.log_info("normal shutdown requested")
        elif isinstance(reason,str):
            self.log_info("shutdown requested: %s" % reason)
        elif isinstance(reason,Exception):
            self.log_exception(reason)
            reason_msg = "failed. shutting down due to %s" % reason.__str__()
            self.set_failed(reason.__str__())
        
        # consider this scenario an immediate "not ready"
        if not self._threaded and not self._stop_required:
            self._ready = False
        
        self._stop_requested = True

    # is_stopped() method - Return True if this class instance reports stopped
    # In the case of Threaded working, it means the run loop has exited
    def is_stopped(self):
        if self._threaded and hasattr(self.thread,"is_alive"):
            return self.thread.is_alive()
        return not self._ready
