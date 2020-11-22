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

# Multiple Inheritance Tips
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
    
    # This variable and the companion inverted getter property (self.keep_working) 
    # and the setter method stop() may be used to control whether a work loop such 
    # as threading.Thread::run method (thread worker) may continue working.
    # TODO: Mutex Lock
    _stop_requested = False
    
    def __init__(self,**kwargs):
        
        # Default module name to name of inhereting class
        # os.path.splitext(os.path.basename(sys.argv[0]))[0])
        # Some scenarios such as that of Service multi-Inheritance
        # will set the name first
        if not hasattr(self, "_name"):
            self._name = self.__class__.__name__.lower()

        # deprecated. phase out _module_name in favor of __name
        if hasattr(self,"_module_name"):
            self._name = self._module_name
        
        # Logger - Do not override logging it if has been configured
        # prior to this constructor
        if not hasattr(self, "_logger"):
            logger_name = kwargs.get("logger_name", self.name).lower()
            self._logger = logging.getLogger(logger_name)
            self.logger.setLevel(logging.NOTSET)
            
        self._statistics = Statistics(component_name=self._name)
            
    @property
    def name(self):
        return self._name
        
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
    
    # here we provide a run method so that we may name our child class
    # Thread::run() methods do_run() instead to remain consistent
    # Namely for AMQP AsynchronousClienst
    def run(self):
        if hasattr(self,"do_run"): self.do_run()
    
    # set logging of any logger based on dot notated taxonomy
    # eg. "root.AMQP-Consumer" -> DEBUG
    def set_logger_loglevel(self, logger_name, level_string):
        logging.getLogger(logger_name).setLevel(level_string.upper())
    
    # set the logging level of this components logger
    def set_loglevel(self, level_string):
        self.logger.setLevel(level_string.upper())

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

    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    @property
    def keep_working(self):
        if self._stop_requested: return False
        else: return True

    # Thread() has no official stop or shutdown method - the thread finishes
    # when the Thread::run() method returns and thus we must implement our own
    # control logic
    def stop(self, reason=None):
        self._stop_requested = True
        stop_reason_message = "stop requested"
        if reason: stop_reason_message += ": %s" % reason
        self.logger.info(stop_reason_message)
        # subclasses should call this method first
        