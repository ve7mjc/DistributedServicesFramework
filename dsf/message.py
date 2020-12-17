from lxml import etree #, objectify
from io import BytesIO # StringIO, 

from enum import Enum

from datetime import datetime, timezone

import json

# application/json
# text/csv
# text/xml
# text/plain

# Example HTTP Header with Content-Type:
# Content-Type: text/html; charset=UTF-8\r\n

class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class MessageType():
    
    _code = None
    _name = None
    _description = None

    def __init__(self,typecode=None,name=None,description=None):
        self._code = typecode
        self._name = name
        self._description = description
    
    @property
    def name(self): return self._name
    
    @property
    def code(self): return self._code
        
    @property
    def description(self): return self._description
        
    def __str__(self):
        return "%s, code=<%s>, description=%s" % (self._name, self._code, self._description)


class Message():

    # Message Properties with defaults
    _data = {}
    _valid = False
    _problems = {}
    
    # Base / Original Data
    _source_data = None
    _source_content_type = None
    
    # intended to be the datetime of the data created
    _content_created_time = None

    def __init__(self,**kwargs):
        
        if "data" in kwargs and type(kwargs["data"]) is dict:
            self._data = kwargs["data"]
            self._type = MessageType("dict")

        if "data" in kwargs and type(kwargs["data"]) is list:
            self._data = kwargs["data"]
            self._type = MessageType("dict")
            
        typecode = kwargs.get("typecode",None)
        if typecode:
            self._type = MessageType(typecode,kwargs.get("typename",typecode))

        # Container Concept
        if not hasattr(self,"_type"):
            #self.log.warning("declare a message type prior to calling Message.__init__(..)")
            self._type = MessageType()
    
    # if no timestamp passed, use this time
    def set_content_created_time(self,timestamp=None):
        if timestamp: 
            self._content_created_time = timestamp
        else:
            self._content_created_time = datetime.now(timezone.utc)

    # Child Factory Class Method
    # Produce child from XML string
    @classmethod
    def from_xml(cls, data):
        if not data: return False
        newobject = cls()
        newobject._source_data = data
        if (type(data) is str):
            # convert to bytes if a string is based
            newobject._source_data = newobject._source_data.encode("utf-8")
        newobject.parse_xml()
        return newobject
    
    # common parsing of XML data to be shared amongst extended classes
    def parse_xml(self):
        self.set_source_content_type("text/xml")
        try:
            tree = etree.parse(BytesIO(self.source_data))
            self._source_data_xml_root = tree.getroot()
            # rewrite default namespace of NoneType to str "default"
            self._source_data_xml_nsmap = {k if k is not None else 'default':v for k,v in root.nsmap.items()}
        except Exception as e: # parsing error
            self.register_processing_error("xml parsing error - %s" % e)
        self.process()

    # FACTORY METHODS FOR COMMON SOURCE DATA INPUTS
    # These methods are intended to perform the common data input tasks
    # such as accepting an XML or CSV string, or opening a file on disk
    # They will check for a "process()" method in the extended class and
    # call it prior to returning an instance of the new Message
    #  ExtendedClass.process() called after data is placed in self._source_data
    #  ExtendedClass.set_source_data_content_type({type if known})
    
    # Class method to create Child Class
    # from XML read from supplied filename
    @classmethod
    def from_xml_file(cls, filename):
        data = open(filename,"rb").read()
        return cls.from_xml_data(data)
        
    # Return instance of Child Class from supplied data
    # Accepts: CSV string or bytes line
    @classmethod
    def from_csv_line(cls,data,delimiter=','):
        if not data: return None
        newobject = cls()
        if hasattr(newobject,"process"):
            newobject._source_data = data
            newobject.process()
        else: newobject._source_data = data.split(delimiter)
        newobject.set_source_content_type("text/csv")
        return newobject

    # Convenience method to from_csv_line but from a file
    @classmethod
    def from_csv_file(cls,filename,delimiter=','):
        data = open(filename,"rb").read()
        return cls.from_csv_line(data,delimiter)

    def set_source_content_type(self,content_type):
        self._source_content_type = content_type

    @property
    def content_type(self):
        return self._content_type
        
    @property
    def type_code(self): return self._type.code
        
    @property
    def data(self): return self._data
        
    @property
    def source_data(self): return self._source_data
    
    @property
    def valid(self): return self._valid
    
    @property
    def type(self):
        return self._type

    def set_type(self,**kwargs):
        if not self._type:
            self._type = MessageType()
        if "name" in kwargs:
            self._type.set_name(kwargs.get("name"))
        
    def set_invalid(self,reason=None):
        self._valid = False

    def __str__(self):
        str_rep = self.data.__str__()
        return str_rep
        
    @property
    def problems(self):
        return self._problems

    def register_missing_element(self,element_name,critical=False):
        if "missing_elements" not in self._problems:
            self._problems["missing_elements"] = []
        entry = { "name" : element_name, "critical" : "unk" }
        self._problems["missing_elements"].append(entry)
        if critical: self._valid = False
    
    # Child Message processor may call this method to register that a problem
    # occured during processing and that this Message is invalid
    def register_processing_error(self,message="Processing Error"):
        if "processing_errors" not in self._problems:
            self._problems["processing_errors"] = []
        self._problems["processing_errors"].append(message)
        self._valid = False
        
        
    def to_json(self):
        if self.type_code == "dict":
            # kludge? convert datetime objects to ISO strings
            return json.dumps(self.data, cls=JsonEncoder)