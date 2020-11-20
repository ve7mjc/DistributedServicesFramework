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

import threading
# import traceback

# Service Monitor
# integrated watchdog functionality?
class ServiceMonitor():
    
    def __init__(self):
        pass
    
    def start(self):
        pass
        
    def stop(self):
        pass
        
class Service(threading.Thread):

    _config_filename = None
    _config = None
    _config_required = False

    def __init__(self, **kwargs):
        
        # process keyword arguments from class initializer
        _config_required = kwargs.get("config_required", False)
        self.app_name = kwargs.get("app_name", os.path.splitext(os.path.basename(sys.argv[0]))[0])
        self._root_loglevel = kwargs.get("loglevel", logging.WARNING)

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
        self.logger.setLevel(logging.WARNING)
        
        # now that logger is up we can report whether we are using a config file or not
        if self.config is None:
            self.logger.info("config file not found")
        
        super().__init__()
        
        self.service_monitor = ServiceMonitor()
        self.service_monitor.start()
        
        # __init__ fini
        pass
    
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

    def shutdown(self):
        self.service_monitor.stop()
        self.logger.debug("starting clean shutdown")

    # Deleting (Calling destructor)
    def __del__(self, reason = None):
        pass
        # Warning regarding logging here - you may receive a NameError when calling 
        # logging.FileHandler.emit() late during the Python finalization

