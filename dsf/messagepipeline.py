# Data Processing Pipeline
# AMQP Consumer(s) -> Processors -> AMQP Publisher(s)
#
# Future - add other Reader and Writer classes to increase adapter flexibility 
#
# Objectives:
# - Data integrity assurance from input to output (no data loss)
#   Transactional approach to message processing whereby Queue Message Delivery 
#   Acknowledgement mechanisms are used to ensure that messages retrieved from 
#   the input source are not permanently removed (Acknowledged) until they 
#   have been processed correctly and have receievd positive confirmation from 
#   the destination recipient that they have been received.
# - High service availability
#   Asynchronous components sharing data through thread-safe Queues,
#   fault-tolerant compartmentalization, and service status monitoring for
#   corrective action at the earliest opportunity
#
# Message Flow:
# Input (Message Queue) -> Assigner -> Processor(s) -> 
# Shared Output Queue -> Published Message Output
#
# Matthew Currie - Nov 2020
#
# Connect:
# Producer (upstream) Publish Message Delivery Ack event
# to a 
# Consumer (Source) Consumer Channel Message Ack
# The Distributed Services Framework was developed specially for this project

import dsf.domain

from queue import Queue, Empty
import time # for sleep and sleep only, zzz
import importlib # dynamic library loading

from dsf.services import Service
from dsf.messageadapters import *
from dsf.exceptions import *

from dsf import exceptionhandling, utilities

from dsf.message import Message

# for type checking
#from dsf.amqp.amqpmessage import AmqpMessage
from dsf.messageprocessor import MessageProcessor

# Stay asyncronous - stay frosty
class MessagePipeline(Service):

    # messages from consumers will be placed here to be picked
    # up by data processing workers
    _message_sources = []
    _message_source_queue = Queue()
    
    # processed and completed messages will be placed here by
    # data processing workers to be picked up by producers for publishing
    # this queue may be bypassed when there is a single processing worker
    _message_sinks = []
    _message_sink_queue = Queue()
    
    # Registered message types for assigning appropriate processors
    # Instead of message_types, instead for now - we will use a message
    #  acceptance filter, as it is not important that the processor
    #  manages a list of known types!
    #__message_types = {}
    
    # format: routing_key_pattern, processor obj
    _processor_acceptance_filters = []
    
    # dict of message processors
    _message_processors = {}
    
    _shared_data = {}
    
    def __init__(self, **kwargs):
        self._logger_name = "MessagePipeline"
        super().__init__(**kwargs)

    # Initialize a Message Processor
    # - Query Input and Output Capabilities
    # - Message Types
    # - Acceptance and Rejection Filters
    def initialize_message_processor(self, processor_name):
        if processor_name not in self._message_processors:
            self.log.error(("requested processor by name \"%s\""
                "does not exist or is not loaded!" % processor_name))
            return True
        
        processor = self._message_processors[processor_name]
        
        # AMQP Routing Key specific!
        if hasattr(processor,"_routing_keys_accepted"):
            for amqp_acceptance_filter in processor._routing_keys_accepted:
                self._processor_acceptance_filters.append({
                    "routing_key_pattern":amqp_acceptance_filter,
                    "processor":processor })
                self.log.info("%s registered acceptance filter: "
                    "amqp_routing_key=%s" 
                        % (processor_name,amqp_acceptance_filter))

        return False

    # Enable processors and message adapters to share data between themselves
    #  - most useful for data produced in inits but needed during starts
    def put_shared_data(self,component,key,data):
        if not component in self._shared_data:
            self._shared_data[component] = {}
        self._shared_data[component][key] = data
        
    def get_shared_data(self,component,key):
        if component in self._shared_data:
            if key in self._shared_data[component]:
                return self._shared_data[component][key]
        return None

    # Load Class or Function from Module
    #  module_name - (str) of "module_name" or "module_name.class_name"
    #  class_name - (str) "class_name" or blank if supplied via kwargs or dot
    #   notation in module_name
    # kwargs.class = name of class
    def _load_class(self,module_name,class_name=None, **kwargs):
    
        if self.is_failed():
            self.log.debug("refusing to load module '%s'"
                " as we are failed!" % module_name)
            return
            
        if not isinstance(module_name,str):
            self.log.error("_load_class(arg) expecting a String!")
            return

        try:
            # Determine module_name and class_name
            if "." in module_name:
                parts = module_name.split('.')
                if len(parts) == 2:
                    module_name = parts[0]
                    class_name = parts[1]

            if not class_name: class_name = kwargs.get("class","Processor")

            self.log.debug("attempting to load %s from %s" 
                % (class_name,module_name))
            
            # Returns the Type, and not an instance
            # Case-insentitive search for a class or method
            module = importlib.import_module(module_name)
            for attrib in module.__dict__:
                if attrib.lower() == class_name.lower():
                    #self.log.debug("Matched %s to %s!" % (attrib,class_name))
                    return getattr(module,attrib)
            raise Exception("Class or method %s not found in module %s!" 
                % (class_name,module_name))
    
        except ModuleNotFoundError as e:
            #self.log.exception()
            self.log.error("%s: %s" % (adapter_type_str, e))
        
        except Exception as e:
            self.log.exception()
            self.set_failed("loading module.class %s.%s" % (module_name,class_name))
            return None

            
    # Dynamically load, check, and register MessageProcessors
    # - call MessageProcessor::do_checks()
    # - can call MessageProcessor::valid any time for invalidated conditions
    def add_message_processor(self,module_name,class_name=None,**kwargs):
        
        if self.is_failed():
            self.log.debug("refusing to add_message_processor(%s,%s) "
                "as we are failed!" % (module_name,class_name))
            return
        try:
        
            if isinstance(module_name,str):
                try:
                    processor = self._load_class(module_name,class_name,**kwargs)
                    processor = processor(pipeline_hdl=self)
                except Exception as e:
                    self.log.exception(exc_stackback=1)
                    self.set_failed(e)
            elif isinstance(module_name,MessageProcessor):
                processor = module_name
                class_name = module_name.__class__.__name__ # for logging
                if hasattr(processor,"set_pipeline_hdl"):
                    processor.set_pipeline_hdl = self
            else:
                raise Exception("add_message_processor(module_name,class_name)"
                    " types are %s,%s" 
                        % (type(module_name).__name__,type(class_name).__name__))
            
            processor_name = processor.name

            if processor:
                # todo - consider problem that initialization could fail which
                #  would leave the processor instance in the registry
                self._message_processors[processor.processor_name] = processor
                self.registerchild(processor)
                self.initialize_message_processor(processor.processor_name)
                self.log.info("Loaded message processor '%s' of class type '%s'"
                    " from %s" % (processor_name,class_name,module_name))
                return processor
            else:
                self.log.error("Error loading and validating Message "
                    "Processor %s" % name)

        except Exception as e:
            self.log.exception()
            self.set_failed(e)
 
    @property
    def message_processors(self):
        return self._message_processors
        
    @property
    def message_processor(self):
        if len(self._message_processors) != 1:
            self.log.error("message_processor called but there are "
                "%s processor(s)" % len(self._message_processors))
        return self._message_processors[list(self._message_processors.keys())[0]]

    
    ## Message Adapters ##
    
    # adapter_type - can be an instance of a class which inherits 
    #  MessageSource or MessageSink, or a string containing
    #  a MessageAdapter known to this class
    # if 'class_name' kwarg is passed, it will load a different
    def add_message_adapter(self,module_name,class_name=None,**kwargs):

        self.log.debug("add_message_adapter(module_name=%s,class_name=%s)" % (module_name,class_name))

        if self.is_failed() or self.stop_requested:
            self.log.info("Refusing to add message adapter as we are failed or stopped!")
            return
        
        adapter_direction = None
        adapter_type_str = ""
        adapter = None
        
        kwargs["pipeline_hdl"] = self
        
        try:

            # if module_name is a class type - we will instantiate it
            if isinstance(module_name,type):
                self.log.debug("creating instance of adapter from passed class")
                module_name = module_name()

            # Instance of MessageSource or MessageSink Class
            if (isinstance(module_name,MessageSource) or 
                    isinstance(module_name,MessageSink)):
                adapter_type_str = type(adapter).__name__
                adapter = module_name
                if hasattr(adapter,"set_pipeline_hdl"):
                    adapter.set_pipeline_hdl(self)
#                adapter.set_logger_name("adapter.%s" 
#                    % adapter_type_str.lower())

            # Name (String) of Module
            elif isinstance(module_name,str):
                adapter_type_str = module_name
                if "logger_name" not in kwargs:
                    kwargs["logger_name"] = ("adapter.%s" 
                        % adapter_type_str)
                if module_name == "amqpconsumer": # AmqpConsumer
                    kwargs["message_queue"] = self._message_source_queue
                    #kwargs["statistics"] = self.statistics
                    adapter = MessageSourceAmqpConsumer(**kwargs)
                elif module_name == "amqpproducer": # AmqpProducer
                    # attach Statistics and ServiceMonitor hooks?                
                    # kwargs["message_queue"] = self._message_source_queue
                    # kwargs["statistics"] = self.statistics
                    adapter = MessageSinkAmqpProducer(**kwargs)
                elif module_name == "consolewriter":
                    adapter = MessageSinkConsoleWriter(**kwargs)
                else:
                    # attempt to load a module and class
                    adapter = self._load_class(module_name,class_name)
                    adapter = adapter(pipeline_hdl=self)
                    if not isinstance(adapter, MessageAdapter):
                        raise Exception("%s adapter must derive from class "
                            "Message{Input|Output}Adapter!" 
                                % type(adapter).__name__)

            # Check the Message Adapter instance
            if adapter and isinstance(adapter,MessageSource):
                adapter.set_message_queue(self._message_source_queue)
                self._message_sources.append(adapter)
                adapter_direction = "Input"
            elif adapter and isinstance(adapter,MessageSink):
                self._message_sinks.append(adapter)
                adapter_direction = "Output"
            elif adapter is None:
                raise Exception("MessageAdapter failed to create")
            else:
                raise Exception("unknown problem. %s is not an instance of "
                    "MessageAdapter!" % type(adapter))
            
            # Message Adapter composition and callbacks
            self.registerchild(adapter)
            self.log.debug("Added <%s> Message %s Adapter"
                % (adapter_type_str,adapter_direction))
            return adapter

        except Exception as e:
            self.log.exception()
            self.log.warning("unable to load message adapter %s.%s" 
                % (module_name,class_name))
            self.set_failed("failed to add Message Adapter \"%s\": %s" 
                % (adapter_type_str,e.__str__()) )

    @property
    def message_sinks(self): 
        return self._message_sinks
    
    @property
    def message_sources(self): 
        return self._message_sources

    # Start Message Input and Output Adapters
    # Call MessageAdapter.start() method on each
    # return: number of Message Adapters that have started
    # blocking - default(False) if True, will block this call until all of the 
    #   adapters have been started or one has declared itself failed, whichever 
    #   comes first.
    #   watches MessageAdapter.is_ready() and MessageAdapter.is_failed()
    # timeoutsecs - number of seconds we will wait until all of the started
    #   adapters are ready. Default 3.
    def start_message_sources(self,blocking=True,timeoutsecs=3):
        self.start_message_adapters("sink",blocking,timeoutsecs)
        
    def start_message_sinks(self,blocking=True,timeoutsecs=3):
        self.start_message_adapters("source",blocking,timeoutsecs)
        
    def start_message_adapters(self,adapters_type="all",blocking=False,timeoutsecs=3):
        
        if self.is_failed() or self.stop_requested:
            self.log.info("Refusing to Start Adapter(s) as we are failed or stopped!")
            return
        
        adapters = []
        
        if adapters_type == "sink" or adapters_type == "all":
            adapters = adapters + self._message_sinks
            
        if adapters_type == "source" or adapters_type == "all":
            adapters = adapters + self._message_sources

        # return if we have not produced adapters
        if not len(adapters):
            self.log.warning("start_message_adapters(%s) called but "
                "no adapters to start!" % adapters_type)
            return 0

        # We now have a collection of various subclasses of MessageAdapter
        for adapter in adapters:
            adapter.start()
        started_time = utilities.utc_timestamp()
        
        # leave now unless we intend on blocking
        if not blocking: 
            return len(adapters)
        
        total_adapters_ready = 0
        adapters_ready = 0
        while (utilities.utc_timestamp() - started_time) < timeoutsecs:
            adapters_ready = 0 # clear every round
            for adapter in adapters:
                if adapter.is_ready(): adapters_ready += 1
                if adapter.is_failed():
                    raise MessageAdapterStartupFailed(type(adapter).__name__)
            if adapters_ready == len(adapters):
                return adapters_ready
            time.sleep(0.01) # test for impact without
        
        # we are timed out if we reached this code
        self.log.error("%s Message Adapter(s) have timed out during startup!" 
            % (len(adapters)-adapters_ready))
        raise MessageAdapterStartupTimeout("%s adapter(s)" 
            % (len(adapters)-adapters_ready))

    # Request all MessageAdapters(Component) stop()
    # If blocking=True, we will block in this method until all of the adapters 
    # report they have been stopped or we have timed out - whichever is first
    # Adapters must inherit/composition from the 'Component' Class
    #  which provides stop() and @property.stopped
    def stop_message_adapters(self):
        blocking = True       
        self.log.debug(("Requesting %s Message Source(s) and "
            "%s Message Sink(s) stop" 
                % (len(self._message_sources), len(self._message_sinks))))
        
        adapters = self._message_sources + self._message_sinks
        for adapter in adapters:
            if hasattr(adapter,"stop"): adapter.stop()
            else: self.log.warning(("uh-oh! adapter %s does not have a "
                "stop() method!" % type(adapter).__name__))
        
        if not blocking: 
            return len(adapters)
        
        started_timestamp = utilities.utc_timestamp()
        while (utilities.utc_timestamp()-started_timestamp) < 3:
            
            # if adapter is threaded?
            num_adapters_stopped = 0
            for adapter in adapters:
                if adapter.is_stopped():
                    num_adapters_stopped += 1
                else:
                    # do not be tempted to call join() without a timeout
                    # as we could be stuck here forever - instead we could
                    # escalate to a more forceful thread kill if we timeout
                    time.sleep(0.001)
                
            if num_adapters_stopped == len(adapters):
                self.log.info("%s Message Adapter(s) have stopped!" % num_adapters_stopped)
                return num_adapters_stopped

        # Time out. Build a better log message indicating which adapters have 
        # timed out
        timedout_adapters_string = ""
        for adapter in adapters:
            if not adapter.is_stopped():
                timedout_adapters_string = ("%s, %s" 
                    % (adapter.adapter_type, timedout_adapters_string))
        
        raise MessageAdapterStopTimeout("Timed out waiting for %s Message "
            "Adapter(s) to stop: %s" 
                % (len(adapters)-num_adapters_stopped,timedout_adapters_string))

    # pass keyword arguments straight in to message_type dict
    # examples: bind_pattern, processor
#    def add_message_type(self, message_type, **kwargs):
#        self.__message_types[message_type] = {}
#        for key in kwargs.keys():
#            self.log.debug("adding message_type['%s']['%s'] = %s" % (message_type, key, kwargs[key]))
#            self.__message_types[message_type][key] = kwargs[key]

    @property
    def valid_message_processors_loaded_num(self):
        num_valid = 0
        for processor_key in self._message_processors:
            if self._message_processors[processor_key].valid: num_valid += 1
        return num_valid

    # Get message_type for supplied routing_key or None if no match
    # match provided Message routing_key against list of known product 
    # binding_patterns and return the type if matched or None if no match
#    def routing_key_message_type(self, routing_key):
#        for message_type in self.__message_types:
#            if "bind_key" in self.__message_types[message_type]:
#                if amqputilities.match_routing_key(routing_key, self.__message_types[message_type]['bind_key']):
#                    return message_type
#        return None # if nothing found
    
    # Return a loaded Message Processor which claims to be able to process 
    # a message with provided routing_key
    # If there are no defined routing keys, return the only processor
    def processor_for_routing_key(self, routing_key):
        for acc_filter in self._processor_acceptance_filters:
            if amqputilities.match_routing_key(routing_key, acc_filter["routing_key_pattern"]):
                return acc_filter["processor"]
        self.log.debug("no processor found for routing_key=%s; providing default" % routing_key)
        if len(self._message_processors) > 0:
            for processor in self._message_processors:
                return self._message_processors[processor]

    # we are assured the message_type is a valid message type or None
    # however a known message_type may not have a processor
    def _message_type_processor(self, message_type):
        if "processor" in self.__message_types[message_type]:
            if hasattr(self.__message_types[message_type]["processor"], "process"):
                return self.__message_types[message_type]["processor"].process
            else: return self.__message_types[message_type]["processor"]
        
    # Return True if we have a callable method specified in the configuration
    # for the supplied message_type
    # type(process_reference.process) == <class 'method'>
    # prefer processor is a reference to a class which has a process() method
    # but default to processor being a callable method
    def message_type_has_processor(self, message_type):
        if message_type == None: return False
        if "processor" in self.__message_types[message_type]:
            if hasattr(self.__message_types[message_type]["processor"], "process"):
                return callable(self.__message_types[message_type]["processor"].process)
            else:
                return callable(self.__message_types[message_type]["processor"])
        return False
    
    # Message Pre-processing
    # Generally processing of a message which may be common to all message types
    def _pre_process(self, input_message, message_type):
        if hasattr(self,"pre_process_message"):
            try: # dont assume this subclass method will behave
                return self.pre_process_message(input_message, message_type)
            except Exception as e:
                self.log.error("message processing failed in pre-processing, see next.")
                self.log.exception()
                

    # wrapper function to accomodate future threads or redirection
    def _do_amqp_publish(self, amqpmessage):
        try:
            producer_response = self._amqp_producers[0].publish(message=amqpmessage, blocking=True)
            if producer_response == "ack":
                return False
        except Exception as e:
            self.log.exception()
            raise Exception("_do_amqp_publish error: %s" % e)
        return True

    def publish_message(self, message):
        for adapter in self._message_sinks:
            if message.type_code == "amqpmessage":
                self.log.debug("Writing AMQP Message; routing_key=%s to %s" % (message.routing_key,adapter.name))
            return adapter.write_message(message)
            
    # THREADED WORK METHOD
    # Do a single message in, process, and message out
    # Called from while.keep_working thread.run() worker
    # Mind the thread-safety of calls made from this method
    def _do_message_process_pass(self):
        try:
            
            # todo, return a tuple or object with deliver information
            
            # generic consumer message ack
            inputmsg_message_id = None
            inputmsg_consumer_id = None
            inputmsg_routing_key = None
            
            # block on thread-safe message queue with a small timeout to ensure
            # the loop rate remains high in case we wanted to do other tasks
            message = self._message_source_queue.get(block=False)
            
            if not isinstance(message, Message):
                raise MessageProcessingFailedException("input adapter has provided an unexpected data type of %s" % type(message))
            
            processor = None
            processor_output = None
            
            if isinstance(message,Message):
                if message.type.code == "amqp_c":
                    # Note consumer and delivery tags so we follow up with message
                    # acknowledgements to the source AMQP Consumer
                    inputmsg_message_id = message.method.delivery_tag
                    inputmsg_consumer_id = message.method.consumer_tag
                    inputmsg_routing_key = message.method.routing_key
                    
                    # Retrieve a MessageProcessor which advertises it can process this Message
                    processor = self.processor_for_routing_key(inputmsg_routing_key)
                    if not processor:
                        raise MessageTypeUnsupportedException("unknown and unsupported message type; message_type=None; routing_key=%s" % inputmsg_routing_key)
                    
                    #if not type(outmsg).__name__.lower() == "amqpmessage"
                else:
                    # Message, not of subtype AmqpConsumerMessage
                    processor = self.message_processor
                    
            if processor:
                message_out = processor.process_message(message)
            else:
                self.log.warning("processor is not available")

            if not message_out:
                raise MessageProcessingFailedException("Processor output is None")
            if not isinstance(message_out,Message):
                raise MessageProcessingFailedException("Processor output is of type %s and not a child of class Message" % type(message_out).__name__)

            if len(self.message_sinks) != 1: 
                print("CONFUSED. we have zero or more than one output adapter!")

            publish_response = self.publish_message(message_out) # blocking publish
            if publish_response == "ack":
                self._message_sources[0].ack_message(inputmsg_message_id, inputmsg_consumer_id)
                self.log.debug("processed and published message; sent ack.")
                self.report_event(PipelineEvent.CompletedMessage)
            elif publish_response == "nack":
                raise Exception("message output adapter NACK")
            else:
                raise Exception("message output adapter output: %s" % publish_response)

#            # blocking publish output! new messages will be arriving in the input while we block
#            if self._do_amqp_publish(output_amqp_message):
#                self.statistics.metric("messages_publish_failed")
#                self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
#                return # leave this call
#            
#            # this concludes a complete trip for a message! ourprimary goal
#            self._amqp_consumers[0].ack_message(input_message_delivery_tag,False,input_message_consumer_tag)
#            self.statistics.metric("messages_processed_total_completely")
#            self.statistics.metric("messages_processed_type_%s_completely" % input_message_type)

        except Empty: # there was no messages for us, back to waiting we go
            time.sleep(0.05)
            return False
            
        except MessageTypeUnsupportedException as e:
            self.log.warning("Message type unsupported! %s" % e)
            self._message_sources[0].ack_message(inputmsg_message_id, inputmsg_consumer_id)
            self.report_event(PipelineEvent.UnsupportedMessage)
            self.log.info("Message type unsupported; Sending ACK; %s" % inputmsg_routing_key)
            #self.statistics.metric("messages_processed_unknown_type")
            
        except MessageIgnoredException as e:
            self.log.info("Message type ignored; Sending ACK; %s" % inputmsg_routing_key)
            self.report_event(PipelineEvent.IgnoredMessage)
            self._message_sources[0].ack_message(inputmsg_message_id, inputmsg_consumer_id)

        except MessageProcessingFailedException as e:
            self.log.warning("Message processing failed for msg with routing_key=%s: %s" % (inputmsg_routing_key,e.__repr__()))
            self._message_sources[0].nack_message(inputmsg_message_id, inputmsg_consumer_id)
            self.report_event(MessageProcessingFailedException)
            #self.statistics.metric("messages_processed_failed_error")
        
        except Exception as e:
            self.log.exception()
            self.log.error("unexpected exception, see above")
            self.report_event(MessageProcessingFailedException)

        self.log.debug("leaving _do_message_process_pass()")

    # we make these methods so we may better return or raise exceptions as 
    # there are conditions where a critical exception should result cesation
    # of the pipeline setup stages
    # Stop the process by calling self.stop(reason) and returning
    def _startup_pipeline(self):
        try:
            # Check for one or more valid MessageProcessors. This is a 
            # crude check as we may have attempted to start a number of different
            # message producers - some of which may have failed
            if not self.valid_message_processors_loaded_num:
                self.stop("not enough Message Processors - cannot continue")
                return

            # Must register statistics and metrics as messages may arrive
            # immediately after starting adapters
            #self.monitor.add_statistics_metric_watchdog("messages_processed_total_completely", 5)

            # Start Message Adapters
            # Calls will return the number of adapters now ready or raise an 
            # exception on detection of a single adapter failure or timeout
            self.start_message_sinks(blocking=True)
            
            self.start_message_sources(blocking=True)

        except MessageAdapterStartupTimeout as e:
            self.set_failed("Message Adapter Startup Timeout: %s" % e)

        except MessageAdapterStartupFailed as e:
            self.set_failed("Message Adapter Startup Failure: %s" % e)

        except Exception as e:
            self.log.exception()
            self.set_failed("critical exception in pipeline setup")

    # Cleanly shut down the pipeline
    # Methods should block until the work has been confirmed
    # Make sure we exit this method cleanly or we will foul up the run() 
    #  loop and prevent Thread clean-up among other undesirables
    def _shutdown_pipeline(self):
 
        self.log.debug("Starting orderly shut down of pipeline")
        
        # Statistics and Monitoring
        try:
            self.stop_message_adapters()
        except Exception as e:
            self.log.exception(stacklevel=2)

    # Component::do_run()
    # Called via super(component).run() which was called by self.thread.start()
    # We do our work in a wrapped call so our parent may perform cleanup
    def run(self):

        self._startup_pipeline()
        # single threaded processor - this could be multithreaded
        # we could call a child or parent class method work loop here also
        
        if self.keep_working:
            self.log.info("Pipeline started")
        else:
            self.log.warning("Pipeline did not start correctly. Shutting down.")
        
        # The work loop
        while self.keep_working:
                self._do_message_process_pass()

        self._shutdown_pipeline()

        self.log.info("Pipeline shut down completed")