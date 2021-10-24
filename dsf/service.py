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

import dsf.domain
from dsf.supervisor import Supervisor
from dsf.services import Component

import threading, time

import signal

class Service(Component):

    _threaded = True

    def __init__(self,**kwargs):
        try:
            # Initialize and Configure Supervisor
            kwargs["blocking_secs"] = 5
            dsf.domain.init_supervisor(**kwargs)
            
            super().__init__(**kwargs)
            
            # Set handler for SIGINT (CTRL-C)
            signal.signal(signal.SIGINT, self.signal_handler)

            # Set handler for SIGTERM (Docker graceful stop)
            signal.signal(signal.SIGTERM, self.signal_handler)

            # Docker note: do not expect any further console output from a 
            # docker container being stopped via 'docker-compose down'
            
            # Process keyword arguments from class initializer
            dsf.domain.config.set_required(self.get("_config_required",False))

            loaded_config = dsf.domain.config.load(
                logger=dsf.domain.logging.get_logger("Configuration"))
            
            dsf.domain.register_service(self)

        except Exception as e:
            self.log.exception()
            self.stop("Fault in Service.__init__()") # call for exit, must stop constructor(s)
            exit()
        
    # Signal handler
    def signal_handler(self, signum, frame):
        print("### CTR-C ###")
        if hasattr(signal,"strsignal"): # python >= 3.8
            strsig = signal.strsignal(signum)
        else:
            sig_names = {
                    23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 
                    4:"SIGILL", 2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 
                    0:"SIG_DFL", 1:"SIG_IGN"
                }
            strsig = sig_names[signum]
        
        # throw some hints when catching second SIGINT
        if hasattr(self,"_signal_caught"):
            dsf.domain.troubleshoot_hanging_shutdown()
        
        self._signal_caught = True
        self.stop("Caught %s" % strsig)
        self.log.info("Caught %s. Starting orderly shutdown" % strsig)

    # gracefully shut down and close resources prior to releasing our threaded
    #  worker on stop
    def on_stop(self, reason=None):
        dsf.domain.supervisor.stop()