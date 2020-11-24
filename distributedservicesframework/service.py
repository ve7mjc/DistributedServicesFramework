# C5Lib - Matthew Currie Nov 2020
#
# Common Libraries for Awesome Highly Available Microservices
#
# Logging
# - Common formatting with timestamps and submodule options
# - Flexible output per module to console and/or disk
# - (TODO) centralized network logging (gelf?)
#
# AMQP Clients
# - Threaded and asynchronous Consumer and Producer
# - Auto-reconnect with in-depth logging for troubleshooting
#
# ServiceMonitor
# - Centralized reporting of this service status and health
# - Threaded watchdog
#

import logging
import argparse
import configparser
import signal

import sys # argv[..], exit
import os.path # file path checking

from distributedservicesframework.component import Component
from distributedservicesframework import exceptionhandling
from distributedservicesframework.servicemonitor import ServiceMonitor
from distributedservicesframework.statistics import Statistics

#from distributedservicesframework.component import Component
#from distributedservicesframework import exceptionhandling
#from distributedservicesframework.servicemonitor import ServiceMonitor
#from distributedservicesframework.statistics import Statistics

from threading import Thread
import time

class Service(Component,Thread):

    _config_filename = None
    _config = None
    _config_required = False

    def __init__(self, **kwargs):

        # Set the handler for signal signalnum to the function handler
        # will be called with 2 arguments - signal.SIG_IGN or signal.SIG_DFL. 
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Process keyword arguments from class initializer
        _config_required = kwargs.get("config_required", False)

        if not hasattr(self,"_name") or self._name is None:
            self._name = kwargs.get("name", self.__class__.__name__)
        
        # Configure logging level for the ROOT LOGGER 
        # Priority is given to the presence of a a keyword argument, then
        # a or pre-declared a member variable in child class
        # priority given to the keyword argument
        if hasattr(kwargs,"root_logger_loglevel"):
            self._root_logger_loglevel = kwargs.get("loglevel")
        elif getattr(self,"_root_logger_loglevel",None) == None:
            self._root_logger_loglevel = logging.INFO

        # Commandline Arguments
        # application may access arguments via self.cli_args
        # application should also be able to add their own arguments
        parser = argparse.ArgumentParser(description=self.name)
        #parser.add_argument('-u','--receivePort', dest='udp_port', type=int, required=True,
        #                   help='port to listen for UDP AIVDM messages')
        #parser.add_argument('-l','--serverPort', dest='tcp_port', type=int, required=True,
        #                   help='port to listen for TCP clients to serve AIVDM messages')
        #parser.add_argument('-b','--backfill', dest='backfill', type=int,
        #                   help='provide new client connections with a smart backfill of traffic over past specified seconds')
        self.cli_args = parser.parse_args() # required in order to at least process -h or --help

        # search for configuration file and load
        # todo - add config file args from cli
        self.load_config()

        # Importance of Handling the Logging "root logger"; at what point in the
        # application execution as well as how it is configured.
        #
        # By default the root logger is set to WARNING and all loggers you define
        # inherit that value.
        #
        # logging.basicConfig - Does basic configuration for the logging system 
        # by creating a StreamHandler with a default Formatter and adding it 
        # to the root logger. 
        # The functions debug(), info(), warning(), error() and critical() 
        # will call basicConfig() automatically if no handlers are defined 
        # for the root logger.
        #
        # Many of the functions do nothing if the root logger already has 
        # handlers configured for it, for example:
       
        # Get handle to root logger and configure
        # We will take advantage of the events of root logger children 
        # propogating upward to the root logger
        # logging.getLogger() returns the root logger if no name specified
        self.root_logger = logging.getLogger()
        
        # Some libraries may a handler to the root logger before we do
        # StreamHandler appears to default to writing output to STDERR 
        # so we pass sys.stdout
        if len(self.root_logger.handlers) == 0:
            self.root_logger.addHandler(logging.StreamHandler(sys.stdout))
        else:
            # TODO - remove/delete the handlers instead
            print("notice: root logger already had %s handler(s) of type %s!" 
                % (len(self.root_logger.handlers), type(self.root_logger.handlers[0])))

        root_handler = self.root_logger.handlers[0]
        
        # make sure that all children of the root logger will have a formatted {app_name}.{module_name}
        root_logger_formatting = '%(asctime)s %(name)s %(levelname)s %(message)s'
        #root_logger_formatting = '%(asctime)s ' + self.name + '.%(name)s %(levelname)s %(message)s'
        formatter = logging.Formatter(root_logger_formatting)
        root_handler.setFormatter(formatter)
        
        file_handler = logging.FileHandler('%s.log' % self.name.replace(" ", "_"))
        file_handler.setFormatter(formatter)
        self.root_logger.addHandler(file_handler)
        
        # Here we are setting the loglevel of the root logger
        self.root_logger.setLevel(self._root_logger_loglevel)
        
        # Configure a logger for this class instance
        # default to useing the name of the lowercase class instance name (name of child class)
        # if it is not specified
        # The arduous process of selecting a name for our little logger. 
        # In order of preference. Subject to change.
        if "logger_name" in kwargs:
            self._logger_name = kwargs.get("logger_name")
        elif hasattr(self,"_logger_name") and self._logger_name is not None:
            pass # happy, we are.
        elif hasattr(self,"_name") and self._name is not None:
            self._logger_name = self._name
        else:
            self._logger_name = type(self).__name__.lower()
        
        self._logger = self.root_logger.getChild(self._logger_name)
        
        # now that logger is up we can report whether we are using a config file or not
        if not self.config: self.logger.debug("config file not being used; not required")

        Component.__init__(self)
        
        # Thread::name - A thread has a name used for identification purposes 
        # only. It has no semantics. Multiple threads may be given the same name.
        # The initial name is set by the constructor.
        Thread.__init__(self, name="%s.Service" % self.name)
        
        # pass reference to our Statistics Class instance which the
        # Component constructor would have established
        self._service_monitor = ServiceMonitor(statistics=self._statistics)
        self._service_monitor.start()
        
        # __init__ fini
        pass # like marking a corner!

    def load_config(self):

        # configuration file ({app_name}.cfg or config.ini)
        # search for a number of possible config filenames
        # TODO: add support for cli argument config file
        possible_config_filenames = [
            "config.ini",
            "config.json",
            "%s.cfg" % self.name,
            "%s.config" % self.name,
            "%s.config.json" % self.name,
        ]

        # search to see if any of these configurations exist
        for filename_to_check in possible_config_filenames:
            if os.path.exists(filename_to_check):
                self._config_filename = filename_to_check
                break
        
        if self._config_filename:
            config = configparser.ConfigParser(allow_no_value=True)
            
            # This optionxform method transforms option names on every read, 
            # get, or set operation. The default converts the name to lowercase. 
            # This also means that when a configuration file gets written, 
            # all keys will be lowercase. 
            # We override this method to prevent this transformation
            config.optionxform = lambda option: option
            try:
                config.read(self._config_filename)
                self._config = config
            except Exception as e:
                print("unable to process config file %s: %s" % (self._config_filename, e))

        if self._config_required and not self.config:
            raise Exception("config required but not available")
    
    @property
    def config(self):
        return self._config

    def run(self):
        
        # we are running
        
        # pass to child run loop if one is present
        # wrap in try-except block to ensure the highest likelihood
        # we can do a graceful shutdown on fatal exception in the run_loop
        try:
            if hasattr(self, "do_run"): self.do_run()
        except Exception as e:
            self.logger.error("fatal exception in run_loop; must shut down : %s" % exceptionhandling.traceback_string(e))
            exceptionhandling.print_full_traceback_console()
        
        # subclass is done work and we are shutting down
        
        # dump session statistics to logging
        self.logger.info(self.statistics.stats_string_test())
        
        # call for the clean-up portion
        Thread.run(self)
        
        # logging module registers logging.shutdown() to atexit automatically        
        return

    
    # Signal handler to catch and handle signals
    def signal_handler(self, signum, frame):
        if hasattr(self,"__sigint_caught"):
            self.logger.warning("caught an additional SIGINT")
        else:
            self.__sigint_caught = True
            self.stop("Caught SIGINT")

    # optional for certain, likely uncommon scenarios
    def wait_forever(self):
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("caught KeyboardInterrupt in wait_forever")
            self.stop("keyboard interrupt!")

    # Thread() has no official stop or shutdown method - the thread finishes
    # when the Thread::run() method returns and thus we must implement our own
    # control logic
    def stop(self, reason=None):
        Component.stop(self,reason)
        # Service specific stop actions
        self.service_monitor.stop()

    # Thread::is_alive() - Return whether the thread is alive.
    # This method returns True just before the run() method starts until just 
    # after the run() method terminates. The module function enumerate() 
    # returns a list of all alive threads.
    
    # Thread::join([(float)timeout]) - Wait until the thread terminates. 
    # This blocks the calling thread until the thread whose join() method is 
    # called terminates – either normally or through an unhandled exception – 
    # or until the optional timeout occurs.
    # - When the timeout argument is present and not None, it should be a 
    # floating point number specifying a timeout for the operation in seconds 
    # (or fractions thereof). As join() always returns None, you must call 
    # isAlive() after join() to decide whether a timeout happened – if the 
    # thread is still alive, the join() call timed out.
    # - When the timeout argument is not present or None, the operation will 
    # block until the thread terminates.
