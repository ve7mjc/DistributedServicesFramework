print("\033[92m" + "##### START MARKER" + "\033[0m")

from pathlib import Path
from os import path
from sys import argv, stdout

from dsf import utilities

# vars
monitor = None

try: # this may never be needed - but a canary of sorts
    if _called: print("`import dsf.domain` called again!")
except Exception: pass
_called = True

# Application Level / Environment
app_path = Path(path.abspath(argv[0]))
app_fullpathstr = str(app_path)
app_basenamestr = str(app_path.name)
app_dirnamestr = str(app_path.parent)
app_extensionstr = str(app_path.suffix)
app_basenamenosuff = app_basenamestr.split(".")[0]

"""
Logging 
"""

import logging as pylogging
from copy import copy # log record

"""
"""

# Reject or Accept log record name (logger name) against pattern and 
# Python logging Filters are no-longer special classes. The class must simply
#  have a filter(record) method. This class can be attached to a handler
#  or a logger directly.
class PatternAcceptanceFilter:
    
    filters = []
    
    # must match pattern AND level minimum
    def add_accept_filter(self,pattern,level):
        self.add_filter(pattern,level,"accept")
        
    def add_reject_filter(self,pattern,level):
        self.add_filter(pattern,level,"reject")

    def add_filter(self,pattern,level,action):
        if type(level) is str:
            level = getattr(pylogging, level.upper(),pylogging.DEBUG)
        af = {}
        af["pattern"] = pattern.lower()
        af["level"] = level
        af["action"] = "reject"
        self.filters.append(af)
    
    def filter(self,record):
        #record.message
        # default to WARNING if we do not have a filter in place
        if not len(self.filters):
            if record.levelno >= pylogging.WARNING:
                return True

        for af in self.filters:
            if utilities.match_topic_pattern(record.name,af["pattern"]) and record.levelno >= af["level"]:
                return True

        return False # default if no match


# Highlighter -- Highlight log lines which contain pattern
class ColoredFormatter(pylogging.Formatter):
      
    hl_rules = []
    _name_length_limit = 30

    _broken_style_hack = False
    
    default_colormap = {
#        'WARNING': termcolor.YELLOW,
#        'INFO': termcolor.WHITE,
#        'DEBUG': termcolor.BLUE,
#        'CRITICAL': termcolor.YELLOW,
#        'ERROR': termcolor.RED,
        'HIGHLIGHT': 17
    }
    
    def __init__(self,fmt,**kwargs):
        self.colormap = kwargs.get("colormap",self.default_colormap)
        self._level_color_enabled = kwargs.get("level_color_enabled",True)
        self._format_pattern = fmt
        pylogging.Formatter.__init__(self,self.formatstr())

    # default to priority of 50 + # of rule added
    # Lower priorities win over higher. Pass priority kwarg in if needed.
    def add_highlighter(self,type,pattern,action,color,**kwargs):
        rule = {}
        rule["priority"] = kwargs.get("priority",10+len(self.hl_rules))
        rule["type"] = type
        rule["pattern"] = pattern
        rule["action"] = action
        rule["color"] = color
        self.hl_rules.insert(0,rule)

    def clear_highlighting_rules(self):
        self.hl_rules = []

    def enable_level_coloring(self,value=True):
        self.level_colors_enabled = value
    
    # Function to allow markup in a log message formatting string
    #  with a property to bypass manipulation
    def formatstr(self,**kwargs):
        
        fmtstr = copy(self._format_pattern)
        
        if kwargs.get("highlight_line",False):
            HLBG = termcolor.BG_COLOR_SEQ % int(kwargs.get("color", self.default_colormap["HIGHLIGHT"]))
            fmtstr = HLBG + self._format_pattern + termcolor.RESET_SEQ

        #if self._level_color_enabled: # this is not actually level color but bold of name + filename
        #fmtstr = fmtstr.replace("$RESET", self.termcolor.RESET_SEQ).replace("$BOLD", self.termcolor.BOLD_SEQ)

        # Clear out remaining placeholders from format string
        fmtstr = fmtstr.replace("$RESET", "").replace("$BOLD", "").replace("$LVLS", "").replace("$LVLR", "")

        return fmtstr

    def format(self, record):
        
        fmt_record = copy(record) # copy to prevent referencing a pointer
        
        # truncate length of name 
        if self._name_length_limit:
            if len(fmt_record.name) > self._name_length_limit:
                fmt_record.name = "..%s" % fmt_record.name[-28:]
        
        # Make a copy of record <class 'logging.LogRecord'> to prevent other 
        #  logging handlers from receiving a colorized copy
        # 'name','levelname','msg','pathname','funcName','levelno','filename'
        if not self._level_color_enabled and not len(self._highlight_rules):
            return pylogging.Formatter.format(self, fmt_record)
            
        # hack to modify member private fmt of _style in logging formatter
        # do checks so we can bail if this implementation is broken
        # Could pass it a reference to a style that we modify?
        if not hasattr(self,"_style") and not isinstance(self._style,pylogging.PercentStyle):
            if not self._broken_style_hack:
                logger.warning("Warning: log message bgcolor highlighting relies on the 'logging.Formatter' class having a _style member")
                self._broken_style_hack = True
            return pylogging.Formatter.format(self, fmt_record)
        
        # "[%(name)-30s][$LVLS%(levelname)-8s$LVLR]  %(message)s ($BOLD%(filename)s$RESET:%(lineno)d)"
        
        # Line Highlighting
        highlight_line = False
        hlcolor = self.default_colormap.get("HIGHLIGHT", 17)
        matched_rule = -1 # for priority -- rules added first win
        for i in range(len(self.hl_rules)):
            rule = self.hl_rules[i]
            if matched_rule < 0 or rule["priority"] < matched_rule:

                if rule["type"] == "name" and rule.get("pattern",None) and utilities.match_topic_pattern(getattr(record,rule["type"]),rule["pattern"]):
                    if rule["action"] == "highlight_line": highlight_line = True
                    if "color" in rule:
                        hlcolor = rule["color"]
                    matched_rule = rule["priority"]
                
                if rule["type"] == "level" and rule["pattern"].lower() == fmt_record.levelname.lower():
                    #logger.debug("checking %s against %s for color %s" % (rule["pattern"].lower(),fmt_record.levelname.lower(),rule["color"]))
                    if rule["action"] == "highlight_line": highlight_line = True
                    if "color" in rule:
                        hlcolor = rule["color"]
                    matched_rule = rule["priority"]
            
        if highlight_line: self._style._fmt = self.formatstr(highlight_line=True,color=hlcolor)
        else: self._style._fmt = self.formatstr()

#        elif self._level_color_enabled:
#            self._style._fmt = self._format_std.replace("$LVLS",self.termcolor.COLOR_SEQ % (30 + self.colormap[fmt_record.levelname])).replace("$LVLR",self.termcolor.RESET_SEQ)
#        else:            
#            self._style._fmt = self.formatstr().replace("$LVLS","").replace("$LVLR","")
        
        return pylogging.Formatter.format(self, fmt_record)

class LoggingSystem():
    
    _loggers = []
    
    def __init__(self):
        self.pattern_filter = PatternAcceptanceFilter()

        # Configure Console Formatter - Colored LogLevels and Highlighting
        console_fmt = "[%(name)-30s][$LVLS%(levelname)-8s$LVLR]  %(message)s ($BOLD%(filename)s$RESET:%(lineno)d)"
        #console_format = self.formatter_message(console_fmt,True)
        self.console_formatter = ColoredFormatter(console_fmt)

    # Remove any handlers which may have been added by other libraries
    # Attach a StreamHandler (console) and FileHanlder (log files)
    def configure_root_logger(self):
        
        root_logger = pylogging.getLogger()
        
        while root_logger.hasHandlers():
            root_logger.removeHandler(root_logger.handlers[0])

        # Add console output Handler
        # StreamHandler appears to default to writing output to STDERR
        console_handler = pylogging.StreamHandler(stdout)       
        console_handler.addFilter(self.pattern_filter)
        console_handler.setFormatter(self.console_formatter)
        root_logger.addHandler(console_handler)
        
        # Add File Handler
        file_format = '%(asctime)s %(name)s %(levelname)s %(message)s'
        #root_logger_formatting = '%(asctime)s ' + self.name + '.%(name)s %(levelname)s %(message)s'
        file_formatter = pylogging.Formatter(file_format)
        file_handler = pylogging.FileHandler('%s.log' % app_basenamenosuff)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        # defaults
        self.add_highlighter("level","error","highlight_line",termcolor.BG_ERROR)
        self.add_highlighter("level","critical","highlight_line",termcolor.BG_ERROR)
        self.add_highlighter("level","warning","highlight_line",termcolor.BG_WARNING)
        #self.console_formatter.add_highlighter(50,"level","info","highlight_line",termcolor.BG_INFO)

        # By default the root logger is set to WARNING and all loggers you define
        # Setting the root logger to DEBUG (lowest possible value) will result in 
        #  all log messages being processed - debug and above
        # We do not inted on using the root logger directly so this allows us to
        #  have full control over logging levels at the component level
        root_logger.setLevel(pylogging.DEBUG)

        self.root_logger = root_logger
        
        self.logger = self.get_logger("domain")
        
        return root_logger

    def clear_highlighting_rules(self):
        self.console_formatter.clear_highlighting_rules()

    def add_highlighter(self,type,pattern,action,color,**kwargs):
        self.console_formatter.add_highlighter(type,pattern,action,color,**kwargs)
        
    def add_accept_filter(self,pattern,level):
        self.pattern_filter.add_accept_filter(pattern,level)
        
    def add_reject_filter(self,pattern,level):
        self.pattern_filter.add_reject_filter(pattern,level)

    # We may want to find loggers matching patterns, etc
    def register_logger(self,logger):
        self._loggers.append(logger)
        
    def get_logger(self,name):
        logger = pylogging.getLogger(name)
        self.register_logger(logger)
        return logger


    
"""
Exception Handling
"""
from sys import exc_info
import sys # from sys import exc_info
import traceback

def exception_context(stacklevel=1):
    if stacklevel < 1: 
        raise Exception("last_exception_details(stacklevel=%s) called. stacklevel must be >= 1!" % stacklevel)
    exc_type, exc_obj, exc_tb = exc_info()
    exc_tb = traceback.extract_tb(exc_tb,limit=stacklevel)[-1]
    # exc_tb is now <class 'traceback.FrameSummary'>   
    details = {}
    details["filename"] = exc_tb.filename.replace(app_dirnamestr,".")
    details["method_name"] = exc_tb.name
    details["lineno"] = exc_tb.lineno
    details["typestr"] = exc_type.__name__
    details["message"] = exc_obj.__str__()
    return details

def troubleshoot_hanging_shutdown():
    print("## Troubleshoot Hanging Shutdown ##")
    import threading
    for thread in threading.enumerate():
        if thread.is_alive():
            print(" - %s ALIVE!" % (thread.name))

"""
Components
"""

_components = []

def register_component(obj):
    _components.append(obj)
    #if monitor: obj.agent.set_monitor(monitor)
    #print("registered Component <class `%s`> as \"%s\"" % (type(componentobj).__name__,componentobj.name))

"""
Service
"""

_services = []

def register_service(serviceobj):
    _services.append(serviceobj)

"""
CONFIG
"""
import configparser

class Config():

    required = False
    data = None
    loaded = False

    def load(self,filename=None,required=False):

        from os import path

        # We do not have a logger at this point so we will need to create one
        #  if we are going to raise Exception and terminate
        
        # Auto Config File Search
        # configuration file ({app_name}.cfg or config.ini)
        # search for a number of possible config filenames
        # TODO: add support for cli argument config file
        auto_filenames = [
            "config.ini",
            "config.cfg",
            "%s.cfg" % app_basenamenosuff.lower(),
            "%s.config" % app_basenamenosuff.lower(),
        ]

        if not filename:
            for item in auto_filenames:
                if path.exists(item):
                    filename = item
                    break
        
        try:
            if filename:
                config = configparser.ConfigParser(allow_no_value=True)
                # This optionxform method transforms option names on every read, 
                #  get, or set operation. The default converts the name to lowercase. 
                #  This also means that when a configuration file gets written, 
                #  all keys will be lowercase.
                # Redeclare this method to prevent this transformation
                # INI format requires that config files must have section headers
                config.optionxform = lambda option: option
                config.read(filename)
                self.data = config
                self.loaded = True
                
            # Raise an Exception if we required a config but was not able to
            #  locate or load it
            if required and not self.loaded:
                raise Exception("required but not found")
        
        except Exception as e:
            raise Exception("unable to load config file %s: %s" % (filename,e.__str__()))

    def set_required(self,value=True):
        self.required = value

"""
UTILITIES
"""
from datetime import datetime

def utc_timestamp():
    return datetime.now().timestamp()

termcolor = utilities.TerminalColor()

"""
Supervisor
"""
from queue import Queue

# agents push their events here
# available any time
component_events = Queue()

### Events=

#_monitor_creating = False
#def get_agent(comp_obj):
#    from monitor import MonitorAgent
#    agent = MonitorAgent(comp_obj)
#
#    if not monitor and not _monitor_creating:
#        create_domain_monitor()    
#    
#    return agent
#    
#    if not monitor:
#        if not _monitor_creating:
#            _monitor_creating = True
#        create_domain_monitor()

#    if comp_obj is not monitor: # avoid recursion!
#        agent = monitor.get_agent(comp_obj)
#        return agent

def init_supervisor(**kwargs):
    global supervisor
    from dsf.supervisor import Supervisor
    supervisor = Supervisor(**kwargs)
    supervisor.start()
    
    # we can now go find all the components that may have started before us
#    for component in _components:
#        if component.agent is not None:
#            component.agent.set_monitor(monitor)

# GO TIME

logging = LoggingSystem()
logging.configure_root_logger()

config = Config()

initialized = True