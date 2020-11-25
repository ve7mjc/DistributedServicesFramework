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

from distributedservicesframework.statistics import Statistics

from datetime import datetime
import logging

# Multiple Inheritance Notes
# class FooBar(Foo, Bar):
#  super will call base classes from right to left
#  super().__init__() - this calls all constructors up to Foo (aka all)
#  super(Foo, self).__init__(bar) - call all constructors after Foo up to Bar

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
    
    _loglevel = None
    
    def __init__(self, **kwargs):

        # Default module name to name of inhereting class
        # os.path.splitext(os.path.basename(sys.argv[0]))[0])
        # Some scenarios such as that of Service multi-Inheritance
        # will set the name first
        if not hasattr(self, "_name"):
            self._name = self.__class__.__name__.lower()

        # deprecated. phase out _module_name in favor of __name
        if hasattr(self,"_module_name"):
            self._name = self._module_name

        if "loglevel" in kwargs: self._loglevel = kwargs.get("loglevel")

        # Logger - Do not override logging it if has been configured
        # prior to this constructor.
        if not hasattr(self,"_logger"):
            
            # Just the logger name - it is important
            if "logger_name" in kwargs:
                self._logger_name = kwargs.get("logger_name")
            elif hasattr(self,"_logger_name") and self._logger_name is not None:
                pass # happy, we are.
            elif hasattr(self,"_name") and self._name is not None:
                self._logger_name = self._name
            else:
                self._logger_name = type(self).__name__.lower()
            
            self._logger = logging.getLogger(self._logger_name)
            if not self._loglevel: self._loglevel = logging.WARNING
            self.logger.setLevel(self._loglevel)
        
        # create a class instance if one does not exist
        if not self._statistics:
            self._statistics = Statistics(component_name=self.name)
        
        # bestow super powers to this class so it may participate in 
        # multiple-inheritance greatness
        super().__init__()
            
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
            
        # set this True so calls to Child::stopped() will return True
        # This method is likely called from a Thread::start() call and the 
        # do_run() child method has exited and thus work is stopped
        self._stop_requested = True

    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    @property
    def keep_working(self): 
        return not self._stop_requested

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
    def start(self,**kwargs):        
        if hasattr(super(),"start"):     
            # pass this call to a parent if it exists (eg. a Thread)
            super().start(**kwargs)
        else:
            # if we have not provided a start() method in our child class
            # then we do not have a startup process
            self._ready = True            

    # Thread() has no official stop or shutdown method - the thread finishes
    # when the Thread::run() method returns and thus we must implement our own
    # control logic
    # Overriding this method: subclasses should call this method first
    def stop(self, reason=None):
        self._stop_requested = True
        stop_reason_message = "stop requested"
        if reason: stop_reason_message += ": %s" % reason
        self.logger.info(stop_reason_message)

    # Provide a stopped property so we may query the status of this component
    # after have requested it stop through the stop() method.
    # Most importantly we want to be able to query a Thread status with 
    # Thread::is_alive() but unless we in a subclass which extends a Thread
    # then we will not have this method. Instead return the _stop_requested()
    # status unless we provide an override in a subclass
    @property
    def stopped(self):
        if hasattr(self,"is_alive"):
            return self.is_alive()
        else: return self._stop_requested