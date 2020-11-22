
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

