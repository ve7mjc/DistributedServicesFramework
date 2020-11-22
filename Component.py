#
# Common Component Properties and Methods

from Statistics import Statistics
    
# Multiple Inheritance Tips
# class FooBar(Foo, Bar):
#  super will call base classes from right to left
#  super().__init__() - this calls all constructors up to Foo (aka all)
#  super(Foo, self).__init__(bar) - call all constructors after Foo up to Bar

# Features:
# Test Mode functionality to support convenient and reliable fine-grained 
#  testing requirements

class Component():
    
    # the presence of a key indicates it is enabled and a value
    # indicates a setting of other True or mode specific value
    # this will not be accessible outside this class, instead use the property
    __enabled_test_modes = {}
    
    # do we want to maintain a link to "the" service monitor this way?
    __service_monitor = None 
    
    __statistics = Statistics()
    
    def __init__(self,**kwargs):
        pass

    @property
    def service_monitor(self):
        return self.__service_monitor

    @property
    def statistics(self):
        return self.__statistics

    ### TEST MODE METHODS
    # examples
    # - amqp_nack_requeue_all_messages
    def enable_test_mode(self, test_mode_name, value=True):
        self.__enabled_test_modes[test_mode_name] = value
        if hasattr(self, "logger"):
            self.logger.info("enabled test mode %s (val=%s)" % (test_mode_name,value))
    
    # support passing of keyword arguments dict
    def enable_test_modes(self, **kwargs):
        for test_mode in kwargs:
            self.enable_test_mode(test_mode, kwargs.get(test_mode, True))
    
    # return class private list of test modes enabled
    @property
    def enabled_test_modes(self):
        return self.__enabled_test_modes
    
    # return the status of a single test_mode
    # returns value (True or other test mode related value) or False
    # if the test_mode is not enabled
    def test_mode(self, test_mode_name):
        if test_mode_name in self.__enabled_test_modes:
            return self.__enabled_test_modes[test_mode_name]
        return False

