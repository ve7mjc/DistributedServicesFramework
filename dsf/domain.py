print("\033[92m" + "##### START MARKER" + "\033[0m")

from pathlib import Path
from os import path
from sys import argv, stdout
import sys # for sys.path

from dsf import utilities

# Temporary Global Configuration
# - Move to service instance
logging_gelf_udp_host = "graylog.bcwarn.net"
logging_gelf_udp_port = 12201
supervisor_amqp_host = "amqp.data.bcwarn.net"
supervisor_amqp_exchange = "services"
supervisor_amqp_queue = "monitor_rx_dev"

# vars
monitor = None

try: # this may never be needed - but a canary
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

lib_path = Path(__file__)
lib_dirnamestr = str(lib_path.parent)

# add the folder this file resides in to the search path
sys.path.append(lib_dirnamestr)

"""
Logging
"""

import logging as pylogging
from logging.handlers import QueueHandler, QueueListener
from logging import FileHandler, StreamHandler, Handler
from copy import copy # log record
import json
from pygelf import GelfUdpHandler # GelfTcpHandler, GelfTlsHandler, GelfHttpHandler
from sys import stdout

# Reject or Accept log record name (logger name) against pattern and 
# Python logging Filters are no-longer special classes. The class must simply
#  have a filter(record) method. This class can be attached to a handler
#  or a logger directly.
class PatternAcceptanceFilter():
    
    filters = []
    
    def add_filter(self,pattern,level,action):
        if type(level) is str:
            level = getattr(pylogging, level.upper(),pylogging.DEBUG)
        af = {}
        af["pattern"] = pattern.lower()
        af["level"] = level
        af["action"] = "reject"
        self.filters.append(af)
    
    def filter(self,record):
        if record.levelname == "ERROR":
            pass
            # kwargs["print_log_call_line"]
#            print(record.msg)
#            print(record.args.__str__())
        #record.message
        # default to WARNING if we do not have a filter in place
        if not len(self.filters):
            if record.levelno >= pylogging.WARNING:
                return True

        for af in self.filters:
            if utilities.match_topic_pattern(record.name,af["pattern"]) and record.levelno >= af["level"]:
                return True

        return False # default if no match

from collections import UserDict

# 'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename', 'funcName', 
# 'getMessage', 'levelname', 'levelno', 'lineno', 'message', 'module', 'msecs', 
# 'msg', 'name', 'pathname', 'process', 'processName', 'relativeCreated', 
# 'stack_info', 'thread', 'threadName']
class GelfLogMessage(dict):
    
    def add_item(self,attr_name,var,test=None):
        if var is not test:
            setattr(self,attr_name,var)
    
    def __init__(self,**kwargs):
        self.version = "1.1" # must include 
        if "logrecord" in kwargs:
            record = kwargs.get("logrecord")    
            if record:
                self.host = record.name # must include
                self.short_message = record.msg # must include
                # optional
                self.timestamp = datetime.strptime(record.asctime,"%Y-%m-%d %H:%M:%S,%f").timestamp()
                self.level = record.levelno
                self.line = record.lineno
                self.file = record.filename
                self._logger_name = record.name
                
                self.add_item("_exc_info",record.exc_info)
                self.add_item("_exc_text",record.exc_text)
                self.add_item("_process",record.process)
                self.add_item("_threadName",record.threadName)

    def clear_nulls(self):
        attrs = copy(vars(self))
        for attr in attrs:
            val = getattr(self,attr)
            if val is None or val == "" or val == 0:
                delattr(self,attr)
    
    def to_json(self):
        return json.dumps(self.__dict__)

#    @property
#    def __dict__(self):
#        return super().__dict__
#        dict.__dict__

      
#    def __dict__(self):
#        return self.__dict__
#        for attrib in self:
#            return attrib

class GelfQueueHandler(QueueHandler):
    
    def __init__(self,queue):
        super().__init__(queue)
    
    # Prepares a record for queuing. The object returned by this method is enqueued.
    # The base implementation formats the record to merge the message, 
    #  arguments, and exception information, if present. It also removes 
    #  unpickleable items from the record in-place.
    def prepare(self,record):
        gelf = GelfLogMessage(logrecord=record)
        return gelf


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

class Logger():
    
    _name = None
    _logger = None
    
    def __init__(self,name):
        self._logger = pylogging.getLogger(name)
        
    def set_name(self,name):
        # not certain we can rename a logger so get a new one instead
        if self._logger: self._logger = dsf.domain.logging.get_logger(name)
        self._name = name
    
    # set the logging level of a logger by name or if only one argument
    # supplied, apply to this instance logger!
    def set_level(self, param1, param2=None):
        if not param2:
            self._logger.setLevel(param1.upper())
        else:
            dsf.domain.logging.get_logger(param1).setLevel(param2.upper())
    
    # Convenience method to point us to class logger instance while also
    #  permitting features such as enqueing a log message prior to logger 
    #  creation
    # stacklevel 1 is default
    # stackback 0 == stacklevel 1
    def log(self,level,msg,*args,**kwargs):
        # stacklevel 
        kwargs["stacklevel"] = kwargs.get("stackback", 0) + 2 # def level 1
        if not isinstance(msg, str): 
            msg = str(msg)
        if kwargs.get("squashlines",None):
            msg = msg.replace("\r", "").replace("\n", "")
            del kwargs["squashlines"]
        if self._logger:
            del kwargs["stackback"]
            self._logger.log(level,msg,*args,**kwargs)
#        else:
#            log_message = (level,msg,args,kwargs)
#            if not self._prelogger_log_messages: 
#                self._prelogger_log_messages = []
#            self._prelogger_log_messages.append(log_message)
    
    def error(self,msg,*args,**kwargs):
        kwargs["stackback"] = kwargs.get("stackback", 0) + 1
        self.log(pylogging.ERROR,msg,*args,**kwargs)
        
    def warning(self,msg,*args,**kwargs):
        kwargs["stackback"] = kwargs.get("stackback", 0) + 1
        self.log(pylogging.WARNING,msg,*args,**kwargs)
        
    def info(self,msg,*args,**kwargs):
        kwargs["stackback"] = kwargs.get("stackback", 0) + 1
        self.log(pylogging.INFO,msg,*args,**kwargs)
        
    def debug(self,msg,*args,**kwargs):
        kwargs["stackback"] = kwargs.get("stackback", 0) + 1
        self.log(pylogging.DEBUG,msg,*args,**kwargs)
    
    # todo, add color highlighting!
    def trace(self,msg,*args,**kwargs):
        kwargs["stackback"] = kwargs.get("stackback", 0) + 1
        msg = "##### %s" % msg
        self.log(pylogging.WARNING,msg,*args,**kwargs)
    
    # Log the Exception on the stack
    # - filename, line number, and method where exception occurred
    # - exception type and message
    # keyword arguments:
    #  exc_stackback - stack backtrack of exception, typ default (0) or 1 
    #  stackback - stacklevel backtrack of logging call, typ default (0)
    #   see log_{debug|info|error|etc}
    def exception(self,**kwargs):
        try:
            # we need to split the kwargs at this point since both the
            frames = exception_context(frame_limit=None)
            
            if "exc_stackback" in kwargs: del kwargs["exc_stackback"]

            kwargs["stackback"] = kwargs.get("stackback", 0) + 1
            kwargs["squashlines"] = True
            for i in range(len(frames)):
                frame = frames[i]
                header = "Exception"
                if i == 0: 
                    source_info = ("{hdr} {filen}->{method}():{line}: {etype}: {msg}"
                        .format(hdr = header, filen = frame["filename"], 
                        method = frame["method_name"], line = frame["lineno"], 
                        etype = frame["typestr"], msg = frame["message"]))
                else:
                    header = "stack level %d:" % (len(frames)-i-1)
                    source_info = (" {hdr} {filen}->{method}():{line}:"
                        .format(hdr = header, filen = frame["filename"], 
                        method = frame["method_name"], line = frame["lineno"], 
                        ))

                if not "extra" in kwargs:
                    kwargs["extra"] = {}
                kwargs["extra"]["print_log_call_line"] = False
                self.log(pylogging.ERROR,source_info,**kwargs)
        except Exception as e:
            self.error("exception in log_exception()! %s - %s"
                % (type(e).__name__,e.__str__()))


class LoggingSystem():
    
    _loggers = []
    _configured = False
    
    class LogHandler(Handler):
        
        def __init__(self):
            self.pattern_filter = PatternAcceptanceFilter()
            self.addFilter(self.pattern_filter)
            
        def add_accept_pattern(self,pattern,level):
            self.pattern_filter.add_filter(pattern,level,"accept")
            
        def add_reject_pattern(self,pattern,level):
            self.pattern_filter.add_filter(pattern,level,"reject")

    # handlers
    class Console(StreamHandler,LogHandler):
        
        def __init__(self):
            StreamHandler.__init__(self,stdout)
            LoggingSystem.LogHandler.__init__(self)
            self.pattern = ("%(asctime)s [%(name)-30s][$LVLS%(levelname)-8s$LVLR]  "
                "%(message)s ($BOLD%(filename)s$RESET:%(lineno)d)")
            self.formatter = ColoredFormatter(self.pattern)
            self.setFormatter(self.formatter)
            
        def clear_highlighting_rules(self):
            self.formatter.clear_highlighting_rules()

        def add_highlighter(self,type,pattern,action,color,**kwargs):
            self.formatter.add_highlighter(type,pattern,action,color,**kwargs)
            
        
    class Remote(GelfUdpHandler,LogHandler):
        
        def __init__(self):
            GelfUdpHandler.__init__(self,host=logging_gelf_udp_host, 
                port=logging_gelf_udp_port, debug=True, compress=False)
            LoggingSystem.LogHandler.__init__(self)
        
    class File(FileHandler,LogHandler):
        
        def __init__(self):
            FileHandler.__init__(self,'%s.log' % app_basenamenosuff)
            LoggingSystem.LogHandler.__init__(self)
            self.pattern = '%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
            self.formatter = pylogging.Formatter(self.pattern)
            self.setFormatter(self.formatter)
            
    def __init__(self):
        
        self.console = self.Console()
        self.remote = self.Remote()
        self.file = self.File()

        # autoconfigure - take note!
        self.configure_root_logger()

    # Remove any handlers which may have been added by other libraries
    # Attach a StreamHandler (console) and FileHanlder (log files)
    def configure_root_logger(self):
        
        if self._configured: return True

        root_logger = pylogging.getLogger()
        
        while root_logger.hasHandlers():
            root_logger.removeHandler(root_logger.handlers[0])
            
        root_logger.addHandler(self.console)
        root_logger.addHandler(self.file)
        root_logger.addHandler(self.remote)

#        # Add console output Handler
#        # StreamHandler appears to default to writing output to STDERR
#        self.console.hander = StreamHandler(stdout)
#        self.console.hander.addFilter(self.console_pattern_filter)
#        self.console.hander.setFormatter(self.console_formatter)
#        root_logger.addHandler(self.console.hander)
#        
#        # Add File Handler
#        self.file.strformat = '%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
#        #root_logger_formatting = '%(asctime)s ' + self.name + '.%(name)s %(levelname)s %(message)s'
#        self.file.formatter = pylogging.Formatter(self.file.strformat)
#        self.file.handler = pylogging.FileHandler('%s.log' % app_basenamenosuff)
#        self.file.handler.setFormatter(self.file.formatter)
#        root_logger.addHandler(self.file.handler)
#        
#        # GELF Logger
#        gelf_handler = GelfUdpHandler(host=logging_gelf_udp_host, 
#            port=logging_gelf_udp_port, debug=True, compress=False)
#        gelf_handler.addFilter(self.gelf_pattern_filter)
#        root_logger.addHandler(gelf_handler)

        # default color highlighting in console
        self.console.add_highlighter("level","error","highlight_line",termcolor.BG_ERROR)
        self.console.add_highlighter("level","critical","highlight_line",termcolor.BG_ERROR)
        self.console.add_highlighter("level","warning","highlight_line",termcolor.BG_WARNING)

        # By default the root logger is set to WARNING and all loggers you define
        # Setting the root logger to DEBUG (lowest possible value) will result in 
        #  all log messages being processed - debug and above
        # We do not inted on using the root logger directly so this allows us to
        #  have full control over logging levels at the component level
        root_logger.setLevel(pylogging.DEBUG)

        self.root_logger = root_logger
        
        self.log = self.get_logger("domain")

        # handler defaults
        self.remote.add_accept_pattern("*","info")
        self.file.add_accept_pattern("*","info")
        
        self._configured = True
        return root_logger


    # We may want to find loggers matching patterns, etc
    def register_logger(self,logger):
        self._loggers.append(logger)
        
    def get_logger(self,name):
        logger = Logger(name)
        self.register_logger(logger)
        return logger



"""
Exception Handling
"""
from sys import exc_info
import sys # from sys import exc_info
import traceback

def exception_context(frame_limit=None):

    exc_type, exc_obj, exc_tb = exc_info()
    
    # Traceback StackSummary is referenced to most recent exception
    # 0 is the furthest from the exception itself
    stack_summary = traceback.extract_tb(exc_tb) # StackSummary
    
    # Reverse it so we may work from exception backwards
    stack_summary.reverse()
    
    traceback_frames = []
    
    for i in range(len(stack_summary)):
        frame = stack_summary[i] # 'traceback.FrameSummary'
        
        tb_f = {}
        tb_f["filename"] = getattr(frame,"filename","").replace(app_dirnamestr,".")
        tb_f["method_name"] = getattr(frame,"name",None)
        tb_f["lineno"] = getattr(frame,"lineno",None)
        tb_f["typestr"] = exc_type.__name__
        tb_f["message"] = exc_obj.__str__()
        traceback_frames.append(tb_f)

        if frame_limit and i >= frame_limit:
            break

    return traceback_frames

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
service = None

#  only designed to support a single Service at this time
def register_service(serviceobj):
    global service, _services
    service = serviceobj
    _services.append(serviceobj)
    if supervisor:
        supervisor.register_service(serviceobj)
    
def services():
    return _services

"""
CONFIG
"""
import configparser

class Config():

    required = False
    data = {}
    configparser = None
    loaded = False
    log = None

    def set_logger(self,logger):
        self.log = logger
    
    def get(self,section,key=None):
        self.log.debug("Searching Config for section '%s' with key '%s'" % (section,key))
        if self.has_section(section):
            if key:
                if key in self.data[section]:
                    self.log.debug("Config returning %s" % self.data[section][key])
                    return self.data[section][key]
                else: 
                    return None
            else:
                self.log.debug("Config returning %s" % self.data[section])
                return self.data[section]

        return None

    def set(self,section,key,value):
        if not section: 
            return True
        if not self.has_section(section):
            self.data[section] = {}
        if key: 
            self.data[section][key] = value

    def has_section(self,name):
        if name in self.data:
            return True
        return False

    # return:
    def load(self,filename=None,required=None,logger=None):

        if logger:
            self.set_logger(logger)

        from os import path

        # if filename is supplied, required is True unless specified otherwise
        if filename and required is not False:
            required = True

        # We do not have a logger at this point so we will need to create one
        #  if we are going to raise Exception and terminate
        
        # Config File Search
        # configuration file ({app_name}.cfg or config.ini)
        # search for a number of possible config filenames
        # TODO: add support for cli argument config file
        search_folders = [
                Path.cwd(),
                "./etc",
            ]
        search_names = [
                "config.ini",
                "config.cfg",
                "%s.cfg" % app_basenamenosuff.lower(),
                "%s.config" % app_basenamenosuff.lower(),
            ]

        if not filename:
            for folder in search_folders:
                for name in search_names:
                    test_path = "%s/%s" % (folder,name)
                    if path.exists(test_path):
                        filename = test_path
                        break
        
        try:
            if not filename:
                return None

            else:
                self.configparser = configparser.ConfigParser(allow_no_value=True)
                # The optionxform method transforms option names on every read, 
                # get, or set operation. The default converts the name to 
                # lowercase.
                # This also means that when a configuration file gets written, 
                # all keys will be lowercase.
                # Redeclare this method to prevent this transformation
                # INI format requires that config files must have section headers
                self.configparser.optionxform = lambda option: option
                self.configparser.read(filename)

                # dict object of sections
                self.data = self.configparser._sections

                self.loaded = True
                
            # Raise an Exception if we required a config but was not able to
            #  locate or load it
            if required and not self.loaded:
                raise Exception("required but not found")
            
            self.log.info("Loaded configuration from %s" % filename)

            # return the config filename upon success
            return filename
        
        except Exception as e:
            raise Exception("Unable to0 load config file %s: %s" % (filename,e.__str__()))


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
    import time
    try:
        global supervisor
        from dsf.supervisor import Supervisor

        kwargs["supervisor.exchange"] = supervisor_amqp_exchange
        kwargs["supervisor.queue"] = supervisor_amqp_queue
        supervisor = Supervisor(**kwargs)
        
        if supervisor._enabled:
            supervisor.start()

        blocking_secs = kwargs.get("blocking_secs", None)
        if blocking_secs and supervisor.isEnabled:
            wait_start_time = utilities.utc_timestamp()
            while not supervisor.is_ready():
                if supervisor.is_failed():
                    print("supervisor is failed; bailing!")
                    return
                time.sleep(0.05)
                if (utilities.utc_timestamp()-wait_start_time) >= blocking_secs:
                    print("supervisor timeout!")
                    break
    except Exception as e:
        print("domain.init_supervisor(e) = %s" % e.__repr__())
    
    # we can now go find all the components that may have started before us
#    for component in _components:
#        if component.agent is not None:
#            component.agent.set_monitor(monitor)

# GO TIME

log = LoggingSystem()
logging = log # backwards compatibility until we stamp out domain.logging
config = Config()

initialized = True
