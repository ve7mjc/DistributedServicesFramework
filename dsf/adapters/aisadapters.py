import dsf.domain
from dsf.messageadapters import UdpLineMessageSource

from aismessage import AisMessage

from time import time
from queue import Queue, Empty

class AisAivdmStream():
    
    _streams = {}
    _accept_filters = []
    _messages = Queue()
    
    def __init__(self,**kwargs):
        self._accept_filters = kwargs.get("accept",None)
    
    @property
    def messages(self):
        return self._messages

    def line_accepted(self,line):
        
        if not self._accept_filters:
            return True
            
        for acc_filter in self._accept_filters:
            if line.startswith(acc_filter):
                return True
                
        return False
        
    def clear(self):
        self._messages.clear()
    
    @property
    def message(self):
        try:
            return self._messages.get_nowait()
        except Empty:
            return None
    
    def add(self,lines,receiver_id,sender_id):

        if receiver_id not in self._streams:
            self._streams[receiver_id] = {}
            
        if sender_id not in self._streams[receiver_id]:
            self._streams[receiver_id][sender_id] = {}

        # remove lines which are not of an accepted type
        remove_lines = []
        for i in range(len(lines)):
            if not self.line_accepted(lines[i]):
                remove_lines.append(i)
        
        if len(remove_lines) == len(lines): 
            # all lines need to be removed, so return
            return
            
        elif len(remove_lines):
            new_lines = []
            # we need to remove some but not all lines
            for i in len(lines):
                if i not in remove_lines:
                    new_lines.append(lines[i])
            lines = new_lines

        # determine if we are a multi-part message and do not have
        #  both parts present in this message
        parts = lines[0].split(",")
        if parts[1] == "2" and len(lines) < 2:
            
            if parts[2] == "1":
                #if self._streams[receiver_id][sender_id]["multi"]:
                self._streams[receiver_id][sender_id]["multi"] = lines[0]
                return
                
            elif parts[2] == "2" and "multi" in self._streams[receiver_id][sender_id]:
                lines.insert(0,self._streams[receiver_id][sender_id]["multi"])
                del self._streams[receiver_id][sender_id]["multi"]
                
            else:
                # we have received a second-piece but do not have 
                #  first queued
                return
                
        # build a message

        message = AisMessage.from_aivdm(lines)
        message.data["sender_addr"] = sender_id
        message.data["receive_port"] = receiver_id

        # place it

        self._messages.put(message)


# If needto to write a AivdmTcpStreamReceiver
#class AivdmTcpReceiver(MessageSource):
#    pass

# Receive 
# !AIVDM,1,1,,B,34eH@`h001G<MQnL=ko4C0;6017A,0*37
# not yet accounting for out-of-order or multi-part messages arriving
# in different datagrams. 
# AISDispatcher sends multi-part messages in one datagram with a 
#  cr+lf at the end of each message - eg. {msg_1}\r\n{msg_2}\r\n
class AivdmUdpReceiver(UdpLineMessageSource):

    _message_out_types = ["AisMessage"]

    def __init__(self,**kwargs):
        self.stream = AisAivdmStream(**kwargs,accept=["!AIVDM"])
        super().__init__(**kwargs,delimiter="CRLF",remove_delimiters=True)

    def on_lines(self,lines,**kwargs):
        
        receive_port = kwargs.get("receive_port")
        sender_addr = kwargs.get("sender_addr")
                
        # decode bytes and remove crlf from end if present
        self.stream.add(lines,receive_port,sender_addr)
        
        message = self.stream.message
        if message:
            self._write_message(message)
        