
# enable applications and their components to create a logger under the root logger
#from logging import getLogger, INFO
#def get_module_logger(module_name, **kwargs):
#    logger_name = module_name
#    logger = getLogger(logger_name)
#    logger.setLevel(kwargs.get("level", INFO))
#    return logger

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

from datetime import datetime

def utc_timestamp():
    return datetime.now().timestamp()