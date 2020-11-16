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

import sys # argv[..], exit
import os.path # file path checking
import traceback

# Service Monitor
# integrated watchdog functionality?
class ServiceMonitor():
    
    def __init__(self):
        pass
    
    def start(self):
        pass
        
    def stop(self):
        pass

# redef of Logging LogLevels for child applications
class LogLevel():
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0

# convenience for tracebacks for debugging
def print_tb():
    print(tb())
def tb():
    return traceback.print_exc(file=sys.stdout)

class Supervisor():

    _config_filename = None
    _config = None

    def __init__(self, app_name = None):
        
        # application name for the purpose of logging, reporting, and more
        if app_name: self.app_name = app_name
        else: 
            # pull application name from script name without extension
            self.app_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

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
            try:
                config.read(self._config_filename, )
                self.config = config
            except Exception as e:
                print("unable to process config file %s: %s" % (self._config_filename,e))

        # By default the root logger is set to WARNING and all loggers you define
        # inherit that value.
        #logging.root.setLevel(logging.NOTSET)
        
        # logging.basicConfig - Does basic configuration for the logging system by creating a StreamHandler 
        # with a default Formatter and adding it to the root logger. 
        # The functions debug(), info(), warning(), error() and critical() will call basicConfig() 
        # automatically if no handlers are defined for the root logger.
        # This function does nothing if the root logger already has handlers configured for it.
        #logging.basicConfig(level=logging.NOTSET)
       
        # Configure Root Logger
        # Take advantage of child logger events propogating upward
        self.root_logger = logging.getLogger() # returns root logger if no name specified
        
        # some libraries have been observed adding a handler to the root logger before we do
        if len(self.root_logger.handlers) == 0:
            ch = logging.StreamHandler()
            self.root_logger.addHandler(ch)
            
        root_handler = self.root_logger.handlers[0]
        formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s|%(message)s')
        root_handler.setFormatter(formatter)
        
        file_handler = logging.FileHandler('%s.log' % self.app_name.replace(" ", "_"))
        file_handler.setFormatter(formatter)
        self.root_logger.addHandler(file_handler)
        
        # Configure logger for this module
        self.logger = logging.getLogger(self.app_name + "." + type(self).__name__)        
        self.logger.setLevel(logging.DEBUG)
        
        # now that logger is up we can report whether we are using a config file or not
        if self.config is None:
            self.logger.info("config file not found")
        
        self.service_monitor = ServiceMonitor()
        self.service_monitor.start()

    # create, configure, and return a Logger for the application
    def getLogger(self,name=None,level=logging.INFO):
        logger_name = self.app_name
        if name:
            logger_name = "%s.%s" % (logger_name,name)          
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        return logger
        
    def no_config_exception(self):
        if not self.config:
            raise Exception("config not available")
        else: return

    def shutdown(self):
        self.service_monitor.stop()
        self.logger.debug("starting clean shutdown")

    # Deleting (Calling destructor)
    def __del__(self, reason = None):
        pass
        # Warning regarding logging here - you may receive a NameError when calling 
        # logging.FileHandler.emit() late during the Python finalization