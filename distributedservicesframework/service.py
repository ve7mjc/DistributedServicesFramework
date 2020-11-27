# C5Lib - Matthew Currie Nov 2020
#
# Common Libraries for Awesome Highly Available Microservices
#
# Logging
# - Common formatting with timestamps and submodule options
# - Flexible output per module to console and/or disk
# - (TODO) centralized network logging (gelf?)
#
# MessageAdapters
# - Asynchronous Consumer and Producer
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

from threading import Thread
import time

from datetime import datetime
import logging

from dsf.services import Component
from dsf import exceptionhandling, utilities
from dsf.monitor import Monitor

# Multiple Inheritance is a tricky thing - like riding a unicycle
# over a dental floss tight rope over a forest of razor blades
class Service(Component,Thread):

    _config_required = False
    
    _config_filename = None
    _config = None
    
    # let the Component base class know we will require special with start() 
    # and stop() calls
    _threaded = True

    def __init__(self, **kwargs):
        try:
            # Set the handler for signal signalnum to the function handler
            # will be called with 2 arguments - signal.SIG_IGN or signal.SIG_DFL
            signal.signal(signal.SIGINT, self.signal_handler)
            
            # Process keyword arguments from class initializer
            self._config_required = kwargs.get("config_required", False)

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
            
            self.configure_root_logger()
            
            # now that logger is up we can report whether we are using a config file or not
            if self.config: self.log_info("loaded configuration from %s" % self._config_filename)

            # logger initializes here
            Component.__init__(self)
            
            # Thread::name - A thread has a name used for identification purposes 
            # only. It has no semantics. Multiple threads may be given the same name.
            # The initial name is set by the constructor.
            Thread.__init__(self, name="%s.Service" % self.name)
            
            # pass reference to our Statistics Class instance which the
            # Component constructor would have established
            self._service_monitor = Monitor(statistics=self._statistics)
            self._service_monitor.start()
            
        except Exception as e:
            # we are abandoning ship
            self.configure_root_logger() # configure a logger with our formatting
            self._logger = self.get_logger()
            self._pre_logger_log_messages() # print any buffered log messages
            self.log_exception(e)
            exit() # call for exit, must stop constructor(s)
            
        # __init__ fini
        pass

    def load_config(self,config_filename=None):

        # We do not have a logger at this point so we will need to create one
        #  if we are going to raise Exception and terminate
        
        # Auto Config File Search
        # configuration file ({app_name}.cfg or config.ini)
        # search for a number of possible config filenames
        # TODO: add support for cli argument config file
        possible_config_filenames = [
            "config.ini",
            "config.cfg",
            "%s.cfg" % self.name.lower(),
            "%s.config" % self.name.lower(),
        ]

        # search to see if any of these configurations exist
        for filename_to_check in possible_config_filenames:
            if os.path.exists(filename_to_check):
                self._config_filename = filename_to_check
                break
        
        try:
            if self._config_filename:
                config = configparser.ConfigParser(allow_no_value=True)
                # This optionxform method transforms option names on every read, 
                #  get, or set operation. The default converts the name to lowercase. 
                #  This also means that when a configuration file gets written, 
                #  all keys will be lowercase.
                # We override this method to prevent this transformation
                # INI format requires that config files must have section headers
                config.optionxform = lambda option: option
                config.read(self._config_filename)
                self._config = config
                
            # Raise an Exception if we required a config but was not able to
            #  locate or load it
            if self._config_required and not self.config:
                raise Exception("config required but not available")
        
        except Exception as e:
            msg = "unable to load config file %s: %s" % (self._config_filename, e.__str__())
            raise Exception(msg)

        # Process Configuration
    
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
        if self.statistics:
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
            while True: time.sleep(1)
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

