
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
    
# Measure memory size of object including its attributes
# Performs self-recursive calls until is has "seen" all attributes of
#  a class
# Credit: Wissam Jarjoui
# https://gist.github.com/bosswissam/a369b7a31d9dcab46b4a034be7d263b2#file-pysize-py
import sys
def get_size(obj, seen=None):
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size