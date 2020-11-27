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

# Component.start(**kwargs)
# Component.stop(reason=None)
# Component.is_stopped()
# Component.is_ready()
# ._name - Set name of this component in child class
# 

from enum import Enum
from datetime import datetime

import logging
from sys import stdout

# Multiple Inheritance Notes
# class FooBar(Foo, Bar):
#  super will call base classes from right to left
#  super().__init__() - this calls all constructors up to Foo (aka all)
#  super(Foo, self).__init__(bar) - call all constructors after Foo up to Bar

from threading import Thread
from time import sleep, perf_counter

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
    _service_monitor = None
    
    # component level statistics. Needs to be configured in the constructor
    # and should be passed the component name and reference handle to global
    # Statistics instance
    _statistics = None
    
    # This variable works with the inverted getter property (self.keep_working) 
    # and the setter method stop() to control whether a work loop such as 
    # threading.Thread::run method (thread worker) may continue working.
    # TODO: Mutex Lock
    _stop_requested = False
    
    _logger = None
    _loglevel = None
    _prelogger_log_messages = None
    
    # Start and Stop Operation
    #  Designate how the class.stop(), class.is_stopped(), 
    #  class.keep_running and other aspects behave with this 
    #  implementation. 
    # See StartMode and StopMode enum classdefs for more information
    _threaded = False
    _start_required = False
    _stop_required = False
    
    def __init__(self, **kwargs):

        # Default module name to name of inhereting class
        # os.path.splitext(os.path.basename(sys.argv[0]))[0])
        # Some scenarios such as that of Service multi-Inheritance
        # will set the name first
        if not hasattr(self, "_name"):
            self._name = self.__class__.__name__.lower()

        if "loglevel" in kwargs: self._loglevel = kwargs.get("loglevel")
        
        self._logger = self.get_logger()
        
        super().__init__()

        # Set our status to ready if not Threaded and do not require start() 
        # to be called
        if not self._threaded and not self._start_required:
            self._ready = True

        self._pre_logger_log_messages()
            
    @property
    def name(self): return self._name

    # When this component is ready to perform its intended 
    # purpose - eg. it has connected to a remote service, authenticated, 
    # configured itself and may be awaiting further comands 
    def is_ready(self): return self._ready
    _ready = False

    # An unrecoverable condition is present which prevents 
    # this component from being able to fulfil its intended function
    # Components start out not failed and require a set_failed() call
    def is_failed(self): return self._failed
    _failed = False
    
    @property
    def failed_reason(self):
        return self._failed_reason
    _failed_reason = None
    
    # Called when we have detected a critical and unrecoverable fault
    # Optional reason which can be retrieved. Use this method so we make
    # sure the necessary steps are taken
    def set_failed(self, failure_reason=None):
        self._ready = False
        self._failed = True
        self._failed_reason = failure_reason
        self.stop("failure: %s" % failure_reason)

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
        root_stream_handler = logging.StreamHandler(stdout)
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
        root_logger.setLevel(logging.WARNING)
        
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
        
    def log_exception(self,exception):
        etype = type(exception).__name__
        if hasattr(exception,"__str__"):
            message = exception.__str__()
        msg = "%s: %s" % (etype,message)
        self.log_error(msg,squashlines=True)

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
        
    def get_logger(self,name=None):
        # Configure a logger for this class instance
        # default to useing the name of the lowercase class instance name (name of child class)
        # if it is not specified
        # The arduous process of selecting a name for our little logger. 
        # In order of preference. Subject to change.
#        if __self__ == self:
#            print("called from self !!!")
##            if "logger_name" in kwargs:
##                name = kwargs.get("logger_name")
#            if hasattr(self,"_logger_name") and self._logger_name:
#                name = self._logger_name
#            elif hasattr(self,"_name") and self._nameNone:
#                name = self._name
#            else: name = type(self).__name__.lower()
        
        return logging.getLogger(name)
        
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
    def service_monitor(self):
        return self._service_monitor

    @property
    def statistics(self):
        return self._statistics
    
    # Here we provide a run method so that we may name our child class
    # Thread::start() will call this overridden method which will in turn
    # Why EXACTLY was this done? Are we going to do cleanup also?
    def run(self):
        
        if hasattr(self,"do_run"): 
            try: 
                self.do_run()
            except Exception as e:
                self.logger.error("error in do_run method! %s" % e)
                raise Exception(e)
                # todo - beef this up
        else:
            self.logger.warning("%s.run() has been called. Make sure this class has a do_run() method defined" % type(self).__name__)

        # @property.is_stopped() will be True now
        self._ready = False

    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    @property
    def keep_working(self): 
        return not self._stop_requested
    
    # duration_ms - how long in ms to block for. Values >= 100 result in 
    #   the the use of system clock elapsed time to increase the accuracy
    #   uses time.perf_counter() in Python >= 3.3 performance counter, i.e. a 
    #   clock with the highest available resolution
    # check_interval - the longest time we may go without checking for a releas
    # Precision: Generally accepted that Linux ~ 1 msec and Windows ~ 16 msec
    def releasable_sleep_ms(self,duration_ms=0,checkintervalms=5,high_accuracy=False):
        # opportunity to optimize this method and reduce calls to system time
        # based on how far away from the target we may be
        watchvar = self.keep_working
        if duration_ms >= 100 or high_accuracy:
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
        if self._threaded and hasattr(super(),"start"):
            super().start(**kwargs)
        else:
            self._ready = True

    # Thread has no stop or shutdown method - instead we set a class variable
    #  self._stop_requested which the threaded run() method will check and stop
    #  at some point after
    # OVERRIDING this method: Subclasses should call this method first!
    def stop(self, reason=None):
        self._stop_requested = True
        stop_reason_message = "stop requested"
        if reason: stop_reason_message += ": %s" % reason
        self.log_info(stop_reason_message)

        if not self._threaded and not self._stop_required:
            self._ready = False

    # is_stopped() method - Return True if this class instance has reported
    #  to be stopped.
    # In the case of Threaded working, it means the run loop has exited
    # Returns True immediately after stop() returns from being called when the 
    #  StopMode of this class is set to STOP_NOT_REQUIRED or STOP_CALL_REQUIRED
    def is_stopped(self):
        if self._threaded and hasattr(self,"is_alive"):
            return self.is_alive()
        return not self._ready() # invert
