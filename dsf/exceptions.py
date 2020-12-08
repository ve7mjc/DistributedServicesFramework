import dsf.domain


class ComponentStartFailed(Exception):
    pass
    
class ComponentStartTimeout(Exception):
    pass

#
# Message Processing Exceptions

class MessageProcessorException(Exception):
    ack = None
    pass

# message processing has been attempted on this message and unexpectedly failed
class MessageProcessingFailedException(MessageProcessorException):
    ack = False
    pass

# a message has been passed to the processor which has determined
# it does not have the ability to decode
class MessageTypeUnsupportedException(MessageProcessorException):
    ack = True
    pass

# Message is received but not wanted.. we may ACK the delivery
class MessageIgnoredException(MessageProcessorException):
    ack = True
    pass

#class MessageRejectedException(MessageProcessorException):
#    pass


## MESSAGE ADAPTERS

# Message Adapter has Timed out getting to ready on startup
class MessageAdapterStartupTimeout(Exception):
    pass
    
# Message Adapter has claimed itself failed during startup
class MessageAdapterStartupFailed(Exception):
    pass
    
class MessageAdapterStopTimeout(Exception):
    pass
    
    