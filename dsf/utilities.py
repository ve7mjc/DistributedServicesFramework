#import dsf.domain

# enable applications and their components to create a logger under the root logger
#from logging import getLogger, INFO
#def get_module_logger(module_name, **kwargs):
#    logger_name = module_name
#    logger = getLogger(logger_name)
#    logger.setLevel(kwargs.get("level", INFO))
#    return logger

"""
    Black: \u001b[30m
    Red: \u001b[31m
    Green: \u001b[32m
    Yellow: \u001b[33m
    Blue: \u001b[34m
    Magenta: \u001b[35m
    Cyan: \u001b[36m
    White: \u001b[37m
    Bright Black: \u001b[30;1m
    Bright Red: \u001b[31;1m
    Bright Green: \u001b[32;1m
    Bright Yellow: \u001b[33;1m
    Bright Blue: \u001b[34;1m
    Bright Magenta: \u001b[35;1m
    Bright Cyan: \u001b[36;1m
    Bright White: \u001b[37;1m
    Reset: \u001b[0m
"""

from sys import stdout
class TerminalColor():
    
#    HEADER = '\033[95m'

#    OKBLUE = '\033[94m'
#    OKCYAN = '\033[96m'
#    OKGREEN = '\033[92m'
#    WARNING = '\033[93m'

    BG_INFO = 17
    BG_WARNING = 166
    BG_ERROR = 1
    
    BG_YELLOW = 226
    BG_GRAY = 236
    BG_DKGRAY = 234
    
    BG_OK = 34
    BG_FAIL = 1
    
#    ENDC = '\033[0m'
#    UNDERLINE = '\033[4m'
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(30,38)
    
    FG_COLOR_SEQ = u"\033[1;%dm"
    BG_COLOR_SEQ = u"\u001b[48;5;%dm"
    BOLD_SEQ = u"\033[1m"
    RESET_SEQ = u"\033[0m"
    
    def print_colorbar(self):

        print("\r\nutilities.test_console_text_colors() - Console Text Color Test:\r\n")
        for i in range(0, 16):
            for j in range(0, 16):
                code = str(i * 16 + j)
                stdout.write(u"\u001b[48;5;" + code + "m " + code.ljust(4))
            print(u"\u001b[0m")
        stdout.write(u"\u001b[38;5;" + code + "m " + code.ljust(4) + "\r\n\r\n")
    

from datetime import datetime, timezone

def utc_timestamp():
    return datetime.now().timestamp()

def utc_timestamp_to_datetime(utc_timestamp):
#    now_timestamp = time.time()
#    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
#    utc_datetime + offset
    return datetime.fromtimestamp(utc_timestamp,timezone.utc)
    
    

# Measure memory size of object including its attributes
# Performs self-recursive calls until is has "seen" all attributes of
#  a class
# Credit: Wissam Jarjoui
# https://gist.github.com/bosswissam/a369b7a31d9dcab46b4a034be7d263b2#file-pysize-py
from sys import getsizeof
def get_size(obj, seen=None):
    size = getsizeof(obj)
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

# Match dotted notation string against a pattern
# eg. service.data.messages matches service.*.messages
def match_topic_pattern(topic,pattern,case_sensitive=False):

    if "*" in topic or "#" in topic:
        raise Exception("match_topic_pattern(\"%s\",..) topic must not contain '*' or '#' characters!" % topic)

    if not case_sensitive:
        topic = topic.lower()
        pattern = pattern.lower()
    
    if topic == pattern: return True # direct match
        
    topic_elements = topic.split(".")
    pattern_elements = pattern.split(".")
    
    for i in range(len(topic_elements)):
        if i == len(pattern_elements)-1: # at end of pattern
            if pattern_elements[i] == "#": return True # easy, final match
            if pattern_elements[i] == "*": return True # special case
            if topic_elements[i] == pattern_elements[i]: True
            else: break # no match and no more pattern space to search
        elif i == len(topic_elements)-1: # at end of topic
            if topic_elements[i] != pattern_elements[i]:
                if pattern_elements[i] != "*" and pattern_elements[i] != "#":
                    return False
            if len(pattern_elements) == len(topic_elements)+1:
                if pattern_elements[i+1] == "#" or pattern_elements[i+1] == "*": return True
                else: return False
            else: return False
        elif topic_elements[i] != pattern_elements[i] and pattern_elements[i] != "*":
                return False

    #return False # topic does not match pattern