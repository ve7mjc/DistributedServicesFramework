import dsf.domain

from dsf.component import Component

from dsf import exceptionhandling
from dsf.event import *

from dsf.adapters.amqpconsumer import AmqpConsumer
from dsf.adapters.amqpproducer import AmqpProducer

from .message import Message

import threading
import logging

import socket

from datetime import datetime, timezone, timedelta
from math import floor, ceil
from copy import copy

from time import sleep

# Message Adapters

# Abstract Message Input and Output Adapters
# We are choosing to leave the Component base class out of Adapter for now as
# it is present in the data adapters so far

# start()
# stop()
# keep_working - shut down was issued
# is_stopped() - adapter is stopped
# @properties
# adapter_type - return descriptive short name, eg: AmqpConsumer
# direction - "in" or "out"

# abstract class
class MessageAdapter(Component): # Component
    
    # ability for a data adapter to arbitrarily throttle the rate of
    # message flow.
    _throttle_messages_sec = None
    _adapter_type = None # declare in child class; eg. FileWriter
    _adapter_type_str = None
    
    _message_in_types = []
    _message_out_types = []
    
    def __init__(self,**kwargs):
        
        if not self._adapter_type:
            self._adapter_type = self.__class__.__name__
        if not self._adapter_type_str:
            self._adapter_type_str = self._adapter_type
        
        # NO LOGGING UNTIL SUPER.__INIT__
        self._pipeline_hdl = kwargs.get("pipeline_hdl",None)
        self._throttle_messages_sec = kwargs.get("throttle",None)
        
        super().__init__(**kwargs)
        if not self._pipeline_hdl: 
            self.log.debug("pipeline handle not supplied")
    
    @property
    def pipeline(self):
        return self._pipeline_hdl

    # "in" or "out"
    @property
    def direction(self):
        return self._message_direction
    
    # Return type of MessageAdapter, eg. FileWriter, AmqpConsumer
    @property
    def adapter_type(self):
        return self._adapter_type
    
    @property
    def message_in_types(self):
        return self._message_in_types
    
    @property
    def message_out_types(self):
        return self._message_out_types
        
    # allow us to create messages from applications without
    #  requiring type knowledge of Message
    def create_message(self,**kwargs):
        try:
            return Message(**kwargs)
        except Exception as e:
            self.log.exception()
            return None

    
# abstract class
class MessageSource(MessageAdapter):
    
    _message_direction = "in"
    
    # defaults
    _adapter_type = "GenericInput"
    _message_queue = None
    
    _receiving = False
    _receive_timeout_secs = 0
    _last_message_receive_time = 0
    _receive_timedout = False
    
    _queue_highwater_warning = False
    _queue_highwater_level = 300
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def set_message_queue(self,handle):
        self._message_queue = handle
        
    def reset_receive_timeout(self):
        self._last_message_receive_time = dsf.domain.utc_timestamp()
        
    def check_queue_highwater(self):
        if self._queue_highwater_level and not self._queue_highwater_warning:
            if self._message_queue.qsize() >= self._queue_highwater_level:
                self._queue_highwater_warning = True
                self.log.warning("queue length highwater level of %s reached!" 
                    % self._queue_highwater_level)
        
    def check_timeout(self):
        if not self._receive_timeout_secs or self._receive_timedout:
            return
            
        if not self._last_message_receive_time:
            self.reset_receive_timeout()
        else:
            now = dsf.domain.utc_timestamp()
            if (now-self._last_message_receive_time) >= self._receive_timeout_secs:
                self.log.info("receive data timeout!")
                self._receive_timedout = True
                self._receiving = False

    def _write_message(self,message):
        self.reset_receive_timeout()
        self.check_queue_highwater()

        if not self._receiving:
            self.log.info("receiving data!")
            self._receiving = True

        if self._message_queue:
            self._message_queue.put(message)
        else:
            print("no message queue for input adapter!")
    
    def nack_message(self,message_id=None,consumer_id=None):
        pass
        
    # extend me!
    def ack_message(self,message_id=None,consumer_id=None):
        pass

# Double check MRO concept here!
class MessageSourceAmqpConsumer(AmqpConsumer,MessageSource):
    
    _adapter_type = "AMQPConsumer"
    _message_types = ["AmqpConsumerMessage"]
    
    def __init__(self,**kwargs):
        self.config_init() # inject config!
        #kwargs["queue"] = self.config.get("queue",None)
        super().__init__(**kwargs)
    
    # Received an AmqpMessage and will enqueue it for the pipeline
    def on_message(self,amqp_message):
        self._write_message(amqp_message)
        
# 
# Receive lines contained in UDP Datagrams
# - Configurable delimited. Default is CR+LF "\r\n" or 0x0d+0x0a
# - Remove delimiter from output if directed
# - Defaults to listen on all IPv4 interfaces ("0.0.0.0")
class UdpLineMessageSource(MessageSource):
    
    _start_required = True
    _threaded = True
    _message_in_types = ["line"]
    
    _mirrors = []
    
    _add_timestamp = False
    
    def __init__(self,**kwargs):
        
        super().__init__(**kwargs)
        
        #self.config_init(**kwargs)
        
        self._listen_addr = "0.0.0.0"
        self._listen_port = int(self.config.get("listen_port"))
        
        self._delimiter = self.config.get("delimiter","\r\n")
        self._remove_delimiter = self.config.get("remove_delimiters",False)
        
        self._add_timestamp = self.config.get("add_timestamp",False)
        
        mirror = self.config.get("mirror",None)
        if mirror:
            for target in mirror.split(','):
                host,port = mirror.split(":")
                port = int(port)
                if port > 0:
                    self._mirrors.append((host,port))

        # process delimiter designators
        self._delimiter = self._delimiter.replace("CR","\r").replace("LF","\n")
        


    def run(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            sock.bind((self._listen_addr, self._listen_port))
        except OSError as e:
            self.set_failed("unable to bind port; already in use! %s" % e)
            return True
        
        self.log.info('listening on udp/%s' % self._listen_port)
        
        self.set_ready()

        while not self.stop_requested:
            try:
                # If we receive multiple lines in this datagram, we will 
                #  send it along in a list as this may be intentional
                #print("getting data")
                data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
                (sender_addr, sender_port) = addr
                
                for host,port in self._mirrors:
                    sock.sendto(data, (host, port))
                
                # decode bytes and remove delimiter from end if present
                lines = data.decode().strip().split(self._delimiter)
                
                # add delimiters back if required
                if not self._remove_delimiter:
                    lines = self._delimiter.join(lines)
                
                if hasattr(self,"on_lines"):
                    self.on_lines(lines,
                        receive_port=self._listen_port,
                        sender_addr=sender_addr,sender_port=sender_port)
                else:
                    data = {}
                    data["lines"] = lines
                    if self._add_timestamp:
                        data["timestamp"] = datetime.now(timezone.utc)
                    message = Message(data=data,typecode="stringlist")
                    self._write_message(message)
                
            except OSError as e:
                if e.errno == 11:
                    sleep(0.05)
                # we are timing out or other underlying socket problem
                
            except Exception as e:
                self.log.exception()
                self.set_failed(e)

            if hasattr(self,"loop_call"):
                self.loop_call()
                
        sock.close()


class SimulatedMessageSource(MessageSource):
    
    _adapter_type = "SimulatedMessageInput"
    _message_out_types = ["Message"]
    _messagE_in_types = ["Simulated"]
    _threaded = True

    def __init__(self,**kwargs):
        kwargs["logger_name"] = "simulatedinput"
        super().__init__(**kwargs)
        
    def run(self):
        self.set_ready()
        while self.keep_working:
            self.powernap(500)
            self.write("testing one two")


#class UdpDatagramReceiver(MessageSource):
#    from socket import AF_INET, SOCK_DGRAM, SOCK_STREAM, socket
#    pass

# Receive measurement samples asynchronously via lines in UDP datagrams
# Collects and timesteamps samples, and sends samples in a bundle
#  for specified report period
# report_period_secs - time period to collect and report samples
# report_period_align - align time period to higher time reference
#  eg. if time period == 60 seconds, consider this to be all samples
#  occurring during a minute from 0 to 59 seconds
# Examples for current time of 11:21:51:
#  - 5 second periods, next is due 11:21:55
#  - 60 second periods, next report is due 11:22:00
#  - 3600 second periods, next report is due 12:00:00
#  - 300 second periods, next report is due 11:25:00
#
class AsynchronousSamplesReceiver(UdpLineMessageSource):

    def __init__(self,**kwargs):
    
        self._period_secs = kwargs.get("report_period_secs", None)
        self._period_align = kwargs.get("report_period_align", False)
        
        if self._period_secs and self._period_align:
            if not self.period_secs_alignment_check(self._period_secs):
                self.set_failed("requested period alignment and supplied %d "
                    "which does not align" % self._period_secs)
        
        self._samples = []
        
        self._last_report = None
        self._next_report = None
        
        super().__init__(**kwargs)

    # check if specified period length in seconds will align into a repeating
    #  pattern. 
    # - 1,2,3,4,5,6,10,12,15,20,30,60 seconds
    def period_secs_alignment_check(self,period_secs):
        divisible = True
        if 60 % period_secs != 0: # divisible_minutes
            divisible = False
        if period_secs > 60:
            if 3600 % period_secs != 0: # divisible_hours
                divisible = False
        return divisible
        
    def next_periodic_time(self,period_secs,period_align=False):

        # next whole second at least
        next_time = datetime.now(timezone.utc)+timedelta(seconds=1)
        next_time = next_time.replace(microsecond=0)

        if not period_align:
            # simple projection forward
            next_time = next_time + timedelta(seconds=period_secs)
        else:
            if period_secs <= 60:
                next_period = ceil(next_time.second/period_secs)
                next_second = next_period * period_secs
                if next_second == 60:
                    next_time = next_time + timedelta(minutes=1)
                    next_second = 0                   
                next_time = next_time.replace(second=next_second)
            elif period_secs <= 3600:
                next_period = ceil(next_time.minute/(period_secs/60))
                next_minute = int(next_period * (period_secs/60))
                if next_minute == 60:
                    next_time = next_time + timedelta(hours=1)
                    next_minute = 0
                next_time = next_time.replace(minute=next_minute,second=0)

            return next_time

    def set_next_report_time(self):
        self._next_report = self.next_periodic_time(self._period_secs,self._period_align)

    def do_report(self):
        
        report = {}
        
        # confirm all messages are from one sender
        senders = []
        for sample in self._samples:
            sender = sample["sender_addr"]
            if sender not in senders:
                senders.append(sender)
            del sample["sender_addr"]

        report["receive_port"] = self._listen_port
        report["sender"] = senders
        report["period_start"] = self._next_report - timedelta(seconds=self._period_secs)
        report["period_secs"] = self._period_secs
        report["samples"] = copy(self._samples)
        self._write_message(self.create_message(data=report))
        self._samples.clear()

    # called from threaded run loop so we may do asynchronous work
    def loop_call(self):
        
        if self._period_secs and not self._next_report:
            self.set_next_report_time()

        if datetime.now(timezone.utc) >= self._next_report:
            self.do_report()         
            self.set_next_report_time()

    def on_lines(self,lines,**kwargs):
        sample = {}
        sample["timestamp"] = datetime.now(timezone.utc)
        sample["data"] = lines
        sample["sender_addr"] = kwargs.get("sender_addr")
        self._samples.append(sample)


"""
##########
# MESSAGE OUTPUT ADAPTERS
##########
"""


# Output Adapters
# call a class method with a message as parameter (best way to do blocking!)
# - pass a reference to a message queue on adapter initialization so the 
#   producer may get messages as able
# - call a class method with a message as parameter
#   helpful for simple blocking publishes with

class MessageSink(MessageAdapter):
    
    _adapter_type = "GenericOutput"
    _message_direction = "out"
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    # Write Message to Adapter
    def write_message(self, message):
        raise Exception(
            "write not implemented in MessageSink child class!")

class MessageSinkAmqpProducer(AmqpProducer,MessageSink):
    
    _adapter_type = "AmqpProducer"
    
    _publish_routing_key = None
    _blocking = True
    
    _message_in_types = ["AmqpProducerMessage"]
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def write_message(self, amqp_message):
        self.log.debug("Writing message %s; blocking=%s" 
            % (amqp_message.routing_key,self._blocking))
        return self.publish_message(amqp_message,blocking=self._blocking)

# Message --> Console Writer
# blocking? we can consider the console stream rate to be insignificant
# fairly sure it is non-blocking anyways -- eg write to a buffer and flush
class MessageSinkConsoleWriter(MessageSink):
    
    _adapter_type = "ConsoleWriter"
    
    _message_in_types = [str]
    _message_out_types = []

    def __init__(self,**kwargs):
        # Component, then MessageSink
        kwargs["logger_name"] = "consolewriter"
        super().__init__(**kwargs)
        self.set_ready()
        
    def write_message(self,message):
        print("ConsoleWriter: %s" % message)
        self.report_event(OutputAdapterEvent.Delivered)
        return "ack"