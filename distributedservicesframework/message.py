
    
# application/json
# text/csv
# text/xml
# text/plain

# Example HTTP Header with Content-Type:
# Content-Type: text/html; charset=UTF-8\r\n

class MessageType():
    
    def __init__(self):
        return
    
    @property
    def name(self):
        return self._name
    _name = None

class Message():
    
    _type = MessageType()
    _content_type = None
    _xml = None
    _data = {}
    #_organization = None

    # load from an XML file
    # call child class method to process (parse) it
    def from_xml(self, data):
        self._xml = data
        if not hasattr(self,"process_from_xml"):
            raise Exception("no process_from_xml method defined!")
        self.process_from_xml()

    @property
    def content_type(self):
        return self._content_type
        
    @property
    def type_code(self):
        return self._type_code
        
    @property
    def data(self):
        return self._data
        
    @property
    def valid(self):
        return self._valid
    _valid = False # default
    
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