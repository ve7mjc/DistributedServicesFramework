import dsf.domain

#
# Message Processing Exceptions

# a message has been passed to the processor which has determined
# it does not have the ability to decode
class MessageTypeUnsupportedError(Exception):
    pass

# message processing has been attempted on this message and unexpectedly failed
class MessageProcessingFailedError(Exception):
    pass

# do we really want this?
class MessageTypeIgnored(Exception):
    pass

# Message Adapter has Timed out getting to ready on startup
class MessageAdapterStartupTimeout(Exception):
    pass
    
# Message Adapter has claimed itself failed during startup
class MessageAdapterStartupFailed(Exception):
    pass
    
class MessageAdapterStopTimeout(Exception):
    pass