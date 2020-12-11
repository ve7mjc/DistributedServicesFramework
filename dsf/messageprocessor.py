import dsf.domain

from dsf.component import Component

class MessageProcessor(Component):
    
    # these must be declared and not None in child classes
    _processor_name = None
    _processor_version = None
    
    _input_message_format = None
    _output_message_format = None
    
    _routing_keys_accepted = []
    _routing_keys_rejected = []
    
    _message_rejected = False
    _message_rejected_reason = None
    
    # one of a number of checks the pipeline will make when dynamically
    # loading and instantiating this Class
    _valid = True
    _constructor_called = False
    
    # reference to MessageProcessingPipeline instance (Parent)
    __message_pipeline_hdl = None
    
    def __init__(self,**kwargs):
        
        # pass in reference to parent (Pipeline) instance
        self._pipeline_hdl = kwargs.get("pipeline_hdl", None)
        if not self._pipeline_hdl: 
            self.log_info("pipeline handle not supplied")
        
        # Set logger name here as the name specified in the child 
        # MessageProcessor Class beats the name of the "processor" class
        self._logger_name = self._processor_name
        
        # Component constructor
        super().__init__(**kwargs)

    @property
    def pipeline(self):
        return self._pipeline_hdl

    def add_accepted_amqp_routing_key_pattern(self, routing_key_pattern):
        self._routing_keys_accepted.append(routing_key_pattern)

    # invalidate this Class instance. Flip self._valid to False
    # and log a message
    def set_invalid(self, reason):
        processor_name = getattr(self,self._processor_name,"unknown")
        self.logger.error("Processor (%s) invalidated: %s" % (processor_name,reason))
        self._valid = False

    # return True and set self.valid=False if this child fails a minimum declaration
    # return False and set self.valid=True if passed checks
    def do_checks(self):
        
        # basic Class implementation issues
        if not self._processor_name: self.set_invalid("_processor_name not set")
        if not self._processor_version: self.set_invalid("_processor_version not set")
        if not self.initialized: self.set_invalid("MessageProcessor constructor \"__init__()\" was not called!")
        
        # Call self.do_tests() which will call Child.tests() if it exists
        if self.do_tests(): self.set_invalid("Processor::tests() failed")
        
        # check for method declarations
        if not hasattr(self,"process_message"): self.set_invalid("missing process_message(message) method in implementation!")
        if not hasattr(self,"tests"): self.set_invalid("missing tests() method in implementation!")
        
        # if self._valid is False, it was found to be invalid
        if self._valid is None: self._valid = True
        return not self._valid # the inverse
    
    # a method interface which is guaranteed to be present in Child classes
    def do_tests(self):
        if hasattr(self,"tests"): return self.tests()
        else: 
            # return False to indicate we did NOT fail a test we did not do
            return False 
            
    def call_process_message(self, amqp_message):
        
        self._message_rejected = False
        self._message_rejected_reason = None
        
    
    # this method is checked after processor is dynamically loaded to check
    # to see if we have a valid instance of MessageProcessor
    # self._valid starts as None before being proven to be True or False
    @property
    def valid(self):
        if self._valid: return True
        return False
  
    # a usable name. We will fail this component on this check at a later time
    @property
    def processor_name(self):
        if self._processor_name: 
            return self._processor_name
        return self._name
       
    @property
    def version(self):
        return self._processor_version
        
    # called when a message is rejected from further processing
    # some design patterns will require this to be used as part of a normal
    # process.
    # ack - do we want to acknowledge this message for delivery?
    def reject(self,reason=None,ack=True):
        self._message_rejected = True
        self._message_rejected_reason = reason
        self.log_debug("rejecting message for reason: %s" % reason)
        
    @property
    def rejected(self):
        return self._message_rejected