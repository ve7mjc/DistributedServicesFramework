# Data Processing Pipeline
# AMQP Consumer(s) -> Processors -> AMQP Publisher(s)
#
# Future - add other Reader and Writer classes to increase adapter flexibility 
#
# Design Objectives:
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
from distributedservicesframework.service import Service
from distributedservicesframework import exceptionhandling, utilities
from distributedservicesframework.amqp import amqputilities
from distributedservicesframework.amqp.amqpmessage import AmqpMessage
from distributedservicesframework.exceptions import *
from distributedservicesframework.messageprocessor import MessageProcessor # for subclass checks
from distributedservicesframework.dataadapters import *

from queue import Queue, Empty
import time # for sleep and sleep only, zzz
import importlib # dynamic library loading

# Stay asyncronous; Stay thread-safe!
class MessageProcessingPipeline(Service):

    # messages from consumers will be placed here to be picked
    # up by data processing workers
    _input_adapters = []
    _input_message_queue = Queue()
    
    # processed and completed messages will be placed here by
    # data processing workers to be picked up by producers for publishing
    # this queue may be bypassed when there is a single processing worker
    _output_adapters = []
    _output_message_queue = Queue()
    
    # Registered message types for assigning appropriate processors
    # disused? = throws an exception if removed tho
    __message_types = {}
    
    # dict of message processors
    _message_processors = {}
    
    def __init__(self, **kwargs):        
        # get logger, service monitor, name, etc
        super().__init__(**kwargs)

    # Initialize a Message Processor
    # - Query Input and Output Capabilities
    # - Message Types
    # - Acceptance and Rejection Filters
    def initialize_message_processor(self, processor_name):
        if processor_name in self._message_processors:
            processor = self._message_processors[processor_name]
            for amqp_acceptance_filter in processor._amqp_input_message_routing_key_acceptance_filters:
                self.logger.info("Message Processor %s registered acceptance filter: amqp_routing_key=%s" % (processor_name,amqp_acceptance_filter))
        else:
            self.logger.error("requested processor by name \"%s\" does not exist or is not loaded!" % processor_name)

    # Dynamically load message processor Module.Processor(MessageProcessor)
    # class from a supplied module
    # - Instantiate the class, do checks (including tests), and return the
    #   handle to the processor or None on any failure
    def load_message_processor(self, module_name):
        try:
            Processor = getattr(importlib.import_module(module_name), "Processor") # module.submodule
            if not issubclass(Processor,MessageProcessor):
                self.logger.info("%s.Processor() is not a subclass of MessageProcessor!" % module_name)
                return None
            processor = Processor(self) # instantiate - pass self in for parent
            if processor.do_checks(): 
                self.logger.info("%s.Processor.do_checks() has failed!" % module_name)
                return None
            return processor
        except Exception as e:
            self.logger.error(exceptionhandling.traceback_string())
            return None
    
    # Dynamically load, check, and register MessageProcessors
    # - call MessageProcessor::do_checks()
    # - can call MessageProcessor::valid any time for invalidated conditions
    def add_message_processor(self, module_name):
        processor = self.load_message_processor(module_name)
        if processor:
            self._message_processors[processor.processor_name] = processor
            self.logger.info("Loaded Message Processor %s.Processor, name='%s'" % (module_name,processor.processor_name))
            self.initialize_message_processor(processor.processor_name) 
        else:
            self.logger.error("Error loading and validating Message Processor %s" % module_name)
    
    ## Message Adapters ##
    
    def add_message_input_adapter(self,adapter_type,**kwargs):
        adapter = None
        try:
            if adapter_type == "amqp": # AmqpConsumer
                kwargs["message_queue"] = self._input_message_queue
                kwargs["statistics"] = self.statistics
                adapter = MessageInputAdapterAmqpConsumer(**kwargs)
            else:
                raise Exception("adapter type %s is unknown" % adapter_type)
        except Exception as e:
            self.stop("unable to add MessageInputAdapter type %s - %s" % (adapter_type,e))
            return

        self.logger.info("added %s type input adapter" % adapter_type)
        if adapter: self._input_adapters.append(adapter)

    # Add Message Output Adapter
    def add_message_output_adapter(self,adapter_type,**kwargs):
        adapter = None
        try:
            if adapter_type == "amqp": # AmqpProducer
                # attach Statistics and ServiceMonitor hooks?                
                # kwargs["message_queue"] = self._input_message_queue
                # kwargs["statistics"] = self.statistics
                adapter = MessageOutputAdapterAmqpProducer(**kwargs)
            elif adapter_type == "console":
                adapter = MessageOutputAdapterConsoleWriter(**kwargs)
            else:
                raise Exception("adapter type %s is unknown" % adapter_type)
        except Exception as e:
            self.stop("unable to add MessageOutputAdapter type %s - %s" % (adapter_type,e))

        if adapter: # if created
            self._output_adapters.append(adapter)
            self.logger.info("added %s type output adapter" % adapter_type)

    # Start Message Input and Output Adapters
    # Call MessageAdapter.start() method on each
    # return: number of Message Adapters that have started
    # blocking - default(False) if True, will block this call until all of the 
    #   adapters have been started or one has declared itself failed, whichever 
    #   comes first.
    #   watches MessageAdapter.is_ready() and MessageAdapter.is_failed()
    # timeoutsecs - number of seconds we will wait until all of the started
    #   adapters are ready. Default 3.
    def start_message_input_adapters(self,blocking=True,timeoutsecs=3):
        self.start_message_adapters("input",blocking,timeoutsecs)
        
    def start_message_output_adapters(self,blocking=True,timeoutsecs=3):
        self.start_message_adapters("output",blocking,timeoutsecs)
        
    def start_message_adapters(self,adapters_type="all",blocking=False,timeoutsecs=3):
        
        adapters = None
        if adapters_type=="output" or adapters_type=="all":
            adapters = self._output_adapters
        if adapters_type=="input" or adapters_type=="all":
            adapters = self._input_adapters
        
        if not len(adapters):
            self.logger.warning("start_message_adapters(%s) called but no adapters to start" % adapters_type)
            return 0
        #if adapters_type != "all" or adapters_type != "input" or adapters_type != "output":
        #    assert("MessageProcessingPipeline.start_message_adapters(self,adapters_type={all|output|input})")
        
        # We now have a collection of various subclasses of MessageAdapter
        for adapter in adapters:
            adapter.start()
            adapter.set_loglevel("debug")
        started_time = utilities.utc_timestamp()
        
        # leave now unless we intend on blocking
        if not blocking: return len(adapters)
        
        total_adapters_ready = 0
        adapters_ready = 0
        while (utilities.utc_timestamp() - started_time) < timeoutsecs:
            adapters_ready = 0 # clear every round
            for adapter in adapters:
                if adapter.is_ready(): adapters_ready += 1
                if adapter.is_failed():
                    raise MessageAdapterStartupFailed(adapter_type)
            if adapters_ready == len(adapters):
                return adapters_ready
            time.sleep(0.01) # test for impact without
        
        # we are timed out if we reached this code
        self.logger.error("%s adapter(s) have timed out during startup" % (len(adapters)-adapters_ready))
        raise MessageAdapterStartupTimeout("%s adapter(s)" % (len(adapters)-adapters_ready))

#        # fickly maneuvres to be flexible in what the adapters list type
#        # may be passed in: a single adapter, a list of adapters, or or a list 
#        # of a list of adapters which will be common
#        if not adapters_list_list: adapters_list_list = [self._amqp_producers,self._amqp_consumers]
#        if not isinstance(adapters_list_list,list): 
#            # if we happened to pass a single adapter - place it in a list of list
#            adapters_list_list = [[adapters_lists]]
#        if len(adapters_list_list) and not isinstance(adapters_list_list[0],list): 
#            # if we are simply a list of adapters, make into a list of list
#            adapters_list_list = [adapters_list_list]
#
#        # Start the adapters - adapters
#        total_adapters_started = 0
#        adapters_started = {}
#        for adapters_list in adapters_list_list:
#            adapter_type = type(adapters_list[0]).__name__.split(".")[-1]
#            adapters_started[adapter_type] = 0
#            #print(dir(type(adapters_list[0])))
#            num_started = 0
#            for adapter in adapters_list:
#                # type(adapter)
#                # <class 'distributedservicesframework.amqp.asynchronousproducer.AsynchronousProducer'>
#                adapter.start()
#                num_started += 1
#                
#            total_adapters_started += num_started
#            adapters_started[adapter_type] = num_started
#
#        # Return now if we were called non-blocking (default)
#        if not blocking:
#            for adapter_type in adapters_started:
#                self.logger.debug("Started %s adapters of type %s" % (adapters_started[adapter_type],adapter_type))
#            return
#        
#        # Check the adapters
#        # block until all clients in all supplied adapters lists are ready or
#        # timeout - whichever comes first
#        started_timestamp = utilities.utc_timestamp()
#        total_adapters_ready = 0
#        
#        while (utilities.utc_timestamp()-started_timestamp) < ready_timeout_secs:
#        
#            # time constrained loop through all adapters in all of the 
#            # lists until number ready matches number started
#            adapters_ready = 0
#            
#            for adapters_list in adapters_list_list:
#                #adapters_ready = 0 # adapters type is ?
#                # apapters is now a list of one particular type of adapter
#                for adapter in adapters_list:
#                    if adapter.is_ready(): adapters_ready += 1
#                    if adapter.is_failed():
#                        adapter_type = type(adapter).__name__.split(".")[-1]
#                        raise MessageInputAdapterFailedStartup(adapter_type)
#
#            # we have met our target of number of adapters started!
#            if adapters_ready == total_adapters_started:
#                for adapter_type in adapters_started:
#                    self.logger.info("%s %s adapter(s) ready!"
#                        % (adapters_started[adapter_type],adapter_type))
#                return total_adapters_ready
#            
#            time.sleep(0.01) # 10 ms of sanity
#        
#        # timed out and (adapters_ready < adapters_started)
#        raise MessageInputAdapterStartupTimeout()

        
    # Request all MessageAdapters(Component) stop()
    # If blocking=True, we will block in this method until all of the adapters 
    # report they have been stopped or we have timed out - whichever is first
    # Adapters must inherit/composition from the 'Component' Class
    #  which provides stop() and @property.stopped
    def stop_message_adapters(self,blocking=True):
        
        self.logger.debug("Stopping %s Message Input Adapters(s) and %s Message Output Adapters(s)" 
            % (len(self._input_adapters), len(self._output_adapters)))
        
        adapters = self._input_adapters + self._output_adapters
        for adapter in adapters:
            if hasattr(adapter,"stop"): adapter.stop()
            else: self.logger.warning("adapter %s does not have a stop() method!" % type(adapter).__name__)
        
        if not blocking: return False
        
        started_timestamp = utilities.utc_timestamp()
        while (utilities.utc_timestamp()-started_timestamp) < 3:
            # if adapter is threaded?
            num_adapters_stopped = 0
            for adapter in adapters:
                if not adapter.stopped:
                    # do not be tempted to call join() without a timeout
                    # as we could be stuck here forever - instead we could
                    # escalate to a more forceful thread kill if we timeout
                    time.sleep(0.001)
                num_adapters_stopped += 1
            if num_adapters_stopped == len(adapters):
                self.logger.debug("all (%s) adapters have stopped!" % num_adapters_stopped)
                return False


    # pass keyword arguments straight in to message_type dict
    # examples: bind_pattern, processor
    def add_message_type(self, message_type, **kwargs):
        self.__message_types[message_type] = {}
        for key in kwargs.keys():
            self.logger.debug("adding message_type['%s']['%s'] = %s" % (message_type, key, kwargs[key]))
            self.__message_types[message_type][key] = kwargs[key]

    @property
    def valid_message_processors_loaded_num(self):
        num_valid = 0
        for processor_key in self._message_processors:
            if self._message_processors[processor_key].valid: num_valid += 1
        return num_valid

    # Get message_type for supplied routing_key or None if no match
    # match provided Message routing_key against list of known product 
    # binding_patterns and return the type if matched or None if no match
    def routing_key_message_type(self, routing_key):
        for message_type in self.__message_types:
            if "bind_key" in self.__message_types[message_type]:
                if amqputilities.match_routing_key(routing_key, self.__message_types[message_type]['bind_key']):
                    return message_type
        return None # if nothing found
    
    # Return a loaded Message Processor which claims to be able to process 
    # a message with provided routing_key
    def processor_for_amqp_routing_key(self, routing_key):
        try:
            for processor_key in self._message_processors:
                processor = self._message_processors[processor_key]
                for pattern in processor._amqp_input_message_routing_key_acceptance_filters:
                    if amqputilities.match_routing_key(routing_key, pattern):
                        #self.logger.debug("returning processor '%s' for routing_key %s" % (processor.processor_name,routing_key))
                        return processor
            self.logger.info("no processor found for routing_key")
            return None
        except Exception as e:
            self.logger.error(": %s" % exceptionhandling.traceback_string(e))

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
                self.logger.error("message processing failed in pre-processing: %s" % exceptionhandling.traceback_string(e))

    # wrapper function to accomodate future threads or redirection
    def _do_amqp_publish(self, amqp_message):
        try:
            producer_response = self._amqp_producers[0].publish(message=amqp_message, blocking=True)
            if producer_response == "ack":
                return False
        except Exception as e:
            raise Exception("_do_amqp_publish error: %s" % e)
        return True

    def write_message_out(self,message):
        self.message_output_adapters[0].write(message)

    # Retrieve a message from input message queue, 
    # process message against subclass methods, and
    # push result to output message queue
    def _do_message_processing(self):
        try: # except queue.Empty, (unexpected) Exception
            
            input_message = self._input_message_queue.get(block=True, timeout=0.01)
            
            # Note consumer and delivery tags so we follow up with message
            # acknowledgements to the source AMQP Consumer. We are making an 
            # assumption here that we are working with a valid AMQP message
            input_message_routing_key = input_message.routing_key
            input_message_consumer_tag = input_message.basic_deliver.consumer_tag
            input_message_delivery_tag = input_message.basic_deliver.delivery_tag
            input_message_type = self.routing_key_message_type(input_message.routing_key)
    
            # We expect to throw Exceptions on any reason why we are unable to 
            # process this messsage.
            try:
                # Reject now if the input_message_type is unknown as we do not 
                # have knowledge of the type nor a matching processor
                
                # Retrieve a MessageProcessor which advertises it can process this Message
                processor = self.processor_for_amqp_routing_key(input_message_routing_key)
                if not processor and not input_message_type:
                    raise MessageTypeUnsupportedError("unknown and unsupported message type; message_type=None; routing_key=%s" % input_message_routing_key)
                
                # Process Message and perform follow-on actions
                #processor_output = self._process_message(input_message, input_message_type)
                
                # Message Pre-processing
                # Generally processing of a message which may be common to all message types
                #intermediary_data = self._pre_process(input_message, input_message_routing_key)
                # bypass preprocessor

                # Final processing with the MessageProcessor
                # Note we are adding a proces_method(data) method -- this is an assumption
                try:
                    processor_output = processor.process_message(input_message)
                except Exception as e:
                    raise MessageProcessingFailedError(e)
            
                # Pipeline test mode
#                if self.test_mode("pipeline_no_output"):
#                    self.logger.info("test_mode(pipeline_no_output) is set, no output!")
#                    self.logger.info(processor_output)
#                    return

                if len(self.message_output_adapters) != 1: print("CONFUSED. we have zero or more than one output adapter!")

                self.write_message_out(processor_output)

#                if processor_output is None:
#                    raise MessageProcessingFailedError("processer output is NoneType!")
#            
#                # Create AmqpMessage for flight
#                if type(processor_output) == 'AmqpMessage':
#                    output_amqp_message = processor_output
#                else:
#                    output_amqp_message = AmqpMessage(body=processor_output)
#                
#                # we are allowing the processor to specify the publish exchange by
#                # supplying an exchange property of the passed Amqp.Message object
#                output_amqp_message.exchange = "test" #output_message.exchange
#                output_amqp_message.routing_key = "test"
#
#                # blocking publish output! new messages will be arriving in the input while we block
#                if self._do_amqp_publish(output_amqp_message):
#                    self.statistics.metric("messages_publish_failed")
#                    self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
#                    return # leave this call
#                
#                # this concludes a complete trip for a message! ourprimary goal
#                self._amqp_consumers[0].ack_message(input_message_delivery_tag,False,input_message_consumer_tag)
#                self.statistics.metric("messages_processed_total_completely")
#                self.statistics.metric("messages_processed_type_%s_completely" % input_message_type)
                
            except MessageTypeUnsupportedError as e:
                self.logger.info(e)
                self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
                self.statistics.metric("messages_processed_unknown_type")
                
            except MessageProcessingFailedError as e:
                self.logger.warning("Message processing failed for msg with routing_key=%s: %s" % (input_message_routing_key,e))
                self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
                self.statistics.metric("messages_processed_failed_error")
                
            except Exception as e:
                # what Message Acknowledgement shall we perform?
                self.logger.error(exceptionhandling.traceback_string(e))

        except Empty:
            pass # there was no messages for us, back to waiting we go
            
        except Exception as e:
            self.logger.error("unexpected error in message processing loop: %s" % exceptionhandling.traceback_string(e))
            
        # this round of work has completed
        return 

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
                self.set_failed("not enough Message Processors - cannot continue")
                return

            # Must register statistics and metrics as messages may arrive
            # immediately after starting adapters
            self.service_monitor.add_statistics_metric_watchdog("messages_processed_total_completely", 5)

            # Start Message Adapters
            # Calls will return the number of adapters now ready or raise an 
            # exception on detection of a single adapter failure or timeout
            self.start_message_output_adapters(blocking=True)
            self.start_message_input_adapters(blocking=True)

        except MessageAdapterStartupTimeout as e:
            self.set_failed("Message Adapter Startup Timeout: " % e)

        except MessageAdapterStartupFailed as e:
            self.set_failed("Message Adapter Startup Failure: " % e)

        except Exception as e:
            self.set_failed("critical exception in pipeline setup")
            self.logger.error(exceptionhandling.traceback_string(e))
            exceptionhandling.print_full_traceback_console()
            
        self.logger.debug("startup_pipeline() completed")

    def _shutdown_pipeline(self):
        self.logger.info("Shutting down pipeline")
        # Statistics and Monitoring
        self.stop_message_adapters(blocking=True)
        self.logger.debug("shutdown_pipeline() completed")

    # Service::do_run()
    # Called via Thread::run() and when we complete, our parent will cleanup
    def do_run(self):
        
        self._startup_pipeline()
       
        # single threaded processor - this could be multithreaded
        # we could call a child or parent class method work loop here also
        
        # The work loop
        while self.keep_working:
            self._do_message_processing()
        
        self._shutdown_pipeline()

        
    @property
    def message_output_adapters(self):
        return self._output_adapters
    
    @property
    def message_input_adapters(self):
        return self._input_adapters