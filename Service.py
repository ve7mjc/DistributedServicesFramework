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

from DistributedServicesFramework.ServiceMonitor import ServiceMonitor
from DistributedServicesFramework.Statistics import Statistics
from DistributedServicesFramework.Component import Component

from threading import Thread
import time

class Service(Component,Thread):

    _config_filename = None
    _config = None
    _config_required = False
    
    # the run method (work loop) checks this flag repeatedly
    # and will stop work and exit the loop on true
    _shutdown_requested = False
    
    # do this before subclass initialization? as we have
    # no control over when super().init is calleed
    __statistics = Statistics()

    def __init__(self, **kwargs):
        
        # Set the handler for signal signalnum to the function handler
        # will be called with 2 arguments - signal.SIG_IGN or signal.SIG_DFL. 
        signal.signal(signal.SIGINT, self.signal_handler)
                
        # process keyword arguments from class initializer
        _config_required = kwargs.get("config_required", False)
        self.app_name = kwargs.get("app_name", os.path.splitext(os.path.basename(sys.argv[0]))[0])
        self._root_loglevel = kwargs.get("loglevel", logging.WARNING)

        # Thread::name - A thread has a name. The name can be passed to the 
        # constructor, and read or changed through the name attribute.
        # A string used for identification purposes only. It has no semantics. 
        # Multiple threads may be given the same name. 
        # The initial name is set by the constructor.

        # Commandline Arguments
        # application may access arguments via self.cli_args
        # application should also be able to add their own arguments
        parser = argparse.ArgumentParser(description=self.app_name)
        #parser.add_argument('-u','--receivePort', dest='udp_port', type=int, required=True,
        #                   help='port to listen for UDP AIVDM messages')
        #parser.add_argument('-l','--serverPort', dest='tcp_port', type=int, required=True,
        #                   help='port to listen for TCP clients to serve AIVDM messages')
        #parser.add_argument('-b','--backfill', dest='backfill', type=int,
        #                   help='provide new client connections with a smart backfill of traffic over past specified seconds')
        self.cli_args = parser.parse_args() # required in order to at least process -h or --help

        # configuration file ({app_name}.cfg or config.ini)
        # search for a number of possible config filenames
        # TODO: add support for cli argument config file

        self.config = None

        possible_config_filenames = [
            "config.ini",
            "config.json",
            "%s.cfg" % self.app_name,
            "%s.config" % self.app_name,
            "%s.config.json" % self.app_name,
        ]

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
                self.config = config
            except Exception as e:
                print("unable to process config file %s: %s" % (self._config_filename,e))

        if self._config_required and not self.config:
            raise Exception("config required but not available")

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
        
        # some libraries have been observed adding a handler to the root logger before we do
        # here we are only creating one if there are not currently any
        # the StreamHandler appears to default to writing output to STDERR 
        # so we pass sys.stdout
        if len(self.root_logger.handlers) == 0:
            coneole_handler = logging.StreamHandler(sys.stdout)
            self.root_logger.addHandler(coneole_handler)
        else:
            # for thought - we could delete or clear the handlers?
            print("notice: something may have beat us to the root logger as there is %s handler(s) of type %s!" 
                % (len(self.root_logger.handlers), type(self.root_logger.handlers[0])))

        root_handler = self.root_logger.handlers[0]
        
        # make sure that all children of the root logger will have a formatted {app_name}.{module_name}
        root_logger_formatting = '%(asctime)s ' + self.app_name + '.%(name)s %(levelname)s %(message)s'
        formatter = logging.Formatter(root_logger_formatting)
        root_handler.setFormatter(formatter)
        
        file_handler = logging.FileHandler('%s.log' % self.app_name.replace(" ", "_"))
        file_handler.setFormatter(formatter)
        self.root_logger.addHandler(file_handler)
        
        self.root_logger.setLevel(self._root_loglevel)
        
        # Configure logger for this module
        self.logger = logging.getLogger(self.app_name + "." + type(self).__name__)
        self.logger.setLevel(self._root_loglevel)
        
        # now that logger is up we can report whether we are using a config file or not
        if self.config is None:
            self.logger.debug("config file not being used; not required")
        
        #print(super())
        Component.__init__(self)
        Thread.__init__(self, target=self.__run_handler, name="%s.Service" % self.app_name)
        # super().__init__() # Component, Thread
        
        # pass reference to Statistics() instance
        self.__service_monitor = ServiceMonitor(statistics=self.__statistics)
        self.__service_monitor.start()
        
        # __init__ fini
        pass

    def __run_handler(self):
        # prework
        
        self.logger.info("I am called before self.run()!")
        
        if hasattr(self, "run"):
            self.run()
        
        self.logger.info("service shut down properly.")
        # post work
        
    #def start(self):
        
        # target is the callable object to be invoked by the run() method. Defaults to None, meaning nothing is called.
    
    # Signal handler to catch and handle signals
    def signal_handler(self, signum, frame):
        if hasattr(self,"__sigint_caught"):
            self.logger.warning("caught an additional SIGINT")
        else:
            self.__sigint_caught = True
            self.stop("Caught SIGINT")

    def set_loglevel(self, loglevel):
        logging.getLogger().setLevel(loglevel)
    
    def loglevel_debug(self):
        return logging.DEBUG
    
    def loglevel_warning(self):
        return logging.WARNING
        
    def loglevel_info(self):
        return logging.INFO
    
    # create, configure, and return a Logger for an application or module
    def get_logger(self, module_name=None, **kwargs):
        if not module_name: module_name = self.app_name
        logger_name = module_name
#            if name:
#                logger_name = "%s.%s" % (logger_name,name)
        logger = logging.getLogger(logger_name)
        logger.setLevel(kwargs.get("level", logging.INFO))
        return logger

    # wait on this process to finish work but respond to system exits
    # provided as option make control logic more convenient in a __main__ block
    # idiot! you forgot to put this in a while forever loop haha
    def wait_forever(self):
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("caught KeyboardInterrupt in wait_forever")
            self.stop("keyboard interrupt!")

    # Deleting (Calling destructor)
    def __del__(self, reason = None):
        pass
        # Warning regarding logging here - you may receive a NameError when calling 
        # logging.FileHandler.emit() late during the Python finalization

    # convenience method to reverse logic and make flow logic more clear
    # Returns true if a shutdown has not been requested
    def keep_working(self):
        if self._shutdown_requested: return False
        else: return True

    # Thread() has no official stop or shutdown method - the thread finishes
    # when the Thread::run() method returns and thus we must implement our own
    # control logic
    def stop(self, reason=None):
        shutdown_message = "Shutdown requested"
        if reason: shutdown_message = "%s; Reason = %s" % (shutdown_message,reason)
        self.logger.debug(shutdown_message)
        self._shutdown_requested = True
        self.__service_monitor.stop()
    
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
