# Distributed Services Framework - Python Edition!

# add this module path to the search path so we can access modules and members
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__)))
    
import DistributedServicesFramework.Amqp

# bring the Class into this scope
from DistributedServicesFramework.Service import Service

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
from sys import stdout
from traceback import print_exc

def get_traceback():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return { "type" : exc_type, "filename" : fname, "lineno" : exc_tb.tb_lineno }

def print_full_traceback():
    print(print_exc())

def get_traceback_string():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return "type: %s; file: %s(%s)" % (exc_type, fname, exc_tb.tb_lineno)

def print_traceback():
    print(get_traceback_string())

# enable applications and their components to create a logger under the root logger
from logging import getLogger, INFO
def get_module_logger(module_name, **kwargs):
    logger_name = module_name
    logger = getLogger(logger_name)
    logger.setLevel(kwargs.get("level", INFO))
    return logger

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_start_marker():
    print(bcolors.OKGREEN + "##### START MARKER" + bcolors.ENDC)