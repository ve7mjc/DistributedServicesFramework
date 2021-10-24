import logging as pylogging
from logging.handlers import QueueHandler, QueueListener
from copy import copy # log record
import json
from pygelf import GelfUdpHandler # GelfTcpHandler, GelfTlsHandler, GelfHttpHandler
from sys import stdout

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

class LoggingSystem():
    
    _loggers = []
    
    def __init__(self):
        
        self.console_pattern_filter = PatternAcceptanceFilter()
        self.gelf_pattern_filter = PatternAcceptanceFilter()

        # Configure Console Formatter - Colored LogLevels and Highlighting
        console_fmt = "[%(name)-30s][$LVLS%(levelname)-8s$LVLR]  %(message)s ($BOLD%(filename)s$RESET:%(lineno)d)"
        #console_format = self.formatter_message(console_fmt,True)
        self.console_formatter = ColoredFormatter(console_fmt)
        
        self.configure_root_logger()

    # Remove any handlers which may have been added by other libraries
    # Attach a StreamHandler (console) and FileHanlder (log files)
    def configure_root_logger(self):
        
        root_logger = pylogging.getLogger()
        
        while root_logger.hasHandlers():
            root_logger.removeHandler(root_logger.handlers[0])

        # Add console output Handler
        # StreamHandler appears to default to writing output to STDERR
        console_handler = pylogging.StreamHandler(stdout)       
        console_handler.addFilter(self.console_pattern_filter)
        console_handler.setFormatter(self.console_formatter)
        root_logger.addHandler(console_handler)
        
        # Add File Handler
        file_format = '%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
        #root_logger_formatting = '%(asctime)s ' + self.name + '.%(name)s %(levelname)s %(message)s'
        file_formatter = pylogging.Formatter(file_format)
        file_handler = pylogging.FileHandler('%s.log' % app_basenamenosuff)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # GELF Logger
        gelf_handler = GelfUdpHandler(host=logging_gelf_udp_host, 
            port=logging_gelf_udp_port, debug=True, compress=False)
        gelf_handler.addFilter(self.gelf_pattern_filter)
        root_logger.addHandler(gelf_handler)

        # default color highlighting in console
        self.add_highlighter("level","error","highlight_line",termcolor.BG_ERROR)
        self.add_highlighter("level","critical","highlight_line",termcolor.BG_ERROR)
        self.add_highlighter("level","warning","highlight_line",termcolor.BG_WARNING)

        # By default the root logger is set to WARNING and all loggers you define
        # Setting the root logger to DEBUG (lowest possible value) will result in 
        #  all log messages being processed - debug and above
        # We do not inted on using the root logger directly so this allows us to
        #  have full control over logging levels at the component level
        root_logger.setLevel(pylogging.DEBUG)

        self.root_logger = root_logger
        
        self.log = self.get_logger("domain")
        
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
    def register_logger(self.log):
        self._loggers.append(logger)
        
    def get_logger(self,name):
        logger = pylogging.getLogger(name)
        self.register_logger(logger)
        return logger
