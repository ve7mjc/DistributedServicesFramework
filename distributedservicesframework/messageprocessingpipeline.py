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
from distributedservicesframework import exceptionhandling, utilities
from distributedservicesframework.service import Service
from distributedservicesframework.amqp.asynchronousconsumer import AsynchronousConsumer
from distributedservicesframework.amqp.asynchronousproducer import AsynchronousProducer
from distributedservicesframework.amqp import amqputilities
from distributedservicesframework.amqp.amqpmessage import AmqpMessage
from distributedservicesframework.exceptions import *
from distributedservicesframework.messageprocessor import MessageProcessor # for subclass checks

from queue import Queue, Empty
import time # for sleep and sleep only, zzz
import importlib # dynamic library loading
#
# Notes on extending this class:
#
#  - Call this constructor if __init__ redeclared, you may pass
#    keyword arguments which will be passed upwards. Naming convention makes
#    an effort to place the destination component in the beginning. eg.
#  - Reimplement (Optional):
#    def __init__(self): be sure to call super().__init__() and keyword
#     arguments may be passed in which are passed up the chain. Examples:
#     - amqp_publish_exchange
#     - amqp_input_message_routing_key_prefix
#    def pre_process_message(self, msc_amqp_message, message_type=None):
#      if you wish to pre-process messages before passing them to message 
#      type specific processors
#  - Methods:
#    self.add_message_type("{friendly_name}", bind_key="{amqp bind pattern}", 
#       processor={reference to callable message processor method})
#    add_amqp_consumer(**kwargs)
#    add_amqp_producer(**kwargs)
#    <more> see below
#

# Stay asyncronous; Stay thread-safe!
class MessageProcessingPipeline(Service):
    
    # <class 'amqp.asynchronousconsumer'>
    _amqp_consumers = []
    _min_amqp_consumers_required = 1
    _amqp_input_message_routing_key_prefix = None
    
    # <class 'amqp.asynchronousproducer'>
    _amqp_producers = []
    _min_amqp_producers_required = 1
    
    # messages from consumers will be placed here to be picked
    # up by data processing workers
    _input_message_queue = Queue()
    
    # processed and completed messages will be placed here by
    # data processing workers to be picked up by producers for publishing
    # this queue may be bypassed when there is a single processing worker
    _output_message_queue = Queue()
    
    # Default Exchange for Publish
    _amqp_publish_exchange = None
    #_publish_queue = None
    
    # Registered message types for assigning appropriate processors
    __message_types = {}
    
    # dict of message processors
    _message_processors = {}
    
    def __init__(self, **kwargs):
        
        self._amqp_publish_exchange = kwargs.get("amqp_publish_exchange", None)
        self._amqp_publish_queue = kwargs.get("queue", None)
        
        # get logger, service monitor, name, etc
        super().__init__(**kwargs)
        
        self.logger.debug("MessageProcessingPipeline constructor called and completed")
    
    # Initialize Message Processor
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
        self.logger.debug("loading Processor class from module %s" % module_name)
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
            
    # Create an instance of 'amqp.AsynchronousConsumer' passing in
    # - keyword arguments
    # - message_queue: pass reference to our local message Queue so it may 
    #   place messages directly into this Queue
    # - statistics: pass reference to our statistics instance so the adapter
    #   may participate in statistics
    def add_amqp_consumer(self, **kwargs):
        
        self._amqp_input_message_routing_key_prefix = kwargs.get("routing_key_prefix", None)
        
        kwargs["message_queue"] = self._input_message_queue
        kwargs["statistics"] = self.statistics
        consumer = AsynchronousConsumer(**kwargs)
        
        self._amqp_consumers.append(consumer)
        

    # Create an instance of 'amqp.asynchronousproducer' passing in
    # - keyword arguments
    # - exchange: if supplied to this class instance
    # - statistics: pass reference to our statistics instance so the adapter
    #   may participate in statistics
    def add_amqp_producer(self, **kwargs):
        
        self._publish_exchange = kwargs.get("exchange", None)          

        kwargs["statistics"] = self.statistics
        producer = AsynchronousProducer(**kwargs)
        
        self._amqp_producers.append(producer)


    # Start up Input and Output Adapters
    # blocking - default(False) will block this call until all of the adapters
    #  have been started or one has failed, whichever comes first.
    # ready_timeout_secs - number of seconds we will wait until all of the started
    #  adapters are ready. Default 3.
    def start_adapters(self,adapters_list_list=None,blocking=False,ready_timeout_secs=3):
        
        # fickly maneuvres to be flexible in what the adapters list type
        # may be passed in: a single adapter, a list of adapters, or or a list 
        # of a list of adapters which will be common
        if not adapters_list_list: adapters_list_list = [self._amqp_producers,self._amqp_consumers]
        if not isinstance(adapters_list_list,list): 
            # if we happened to pass a single adapter - place it in a list of list
            adapters_list_list = [[adapters_lists]]
        if len(adapters_list_list) and not isinstance(adapters_list_list[0],list): 
            # if we are simply a list of adapters, make into a list of list
            adapters_list_list = [adapters_list_list]

        # Start the adapters - adapters
        total_adapters_started = 0
        adapters_started = {}
        for adapters_list in adapters_list_list:
            adapter_type = type(adapters_list[0]).__name__.split(".")[-1]
            adapters_started[adapter_type] = 0
            #print(dir(type(adapters_list[0])))
            num_started = 0
            for adapter in adapters_list:
                # type(adapter)
                # <class 'distributedservicesframework.amqp.asynchronousproducer.AsynchronousProducer'>
                adapter.start()
                num_started += 1
                
            total_adapters_started += num_started
            adapters_started[adapter_type] = num_started

        # Return now if we were called non-blocking (default)
        if not blocking:
            for adapter_type in adapters_started:
                self.logger.debug("Started %s adapters of type %s" % (adapters_started[adapter_type],adapter_type))
            return
        
        # Check the adapters
        # block until all clients in all supplied adapters lists are ready or
        # timeout - whichever comes first
        started_timestamp = utilities.utc_timestamp()
        total_adapters_ready = 0
        
        while (utilities.utc_timestamp()-started_timestamp) < ready_timeout_secs:
        
            # time constrained loop through all adapters in all of the 
            # lists until number ready matches number started
            adapters_ready = 0
            
            for adapters_list in adapters_list_list:
                #adapters_ready = 0 # adapters type is ?
                # apapters is now a list of one particular type of adapter
                for adapter in adapters_list:
                    if adapter.is_ready(): adapters_ready += 1
                    if adapter.is_failed():
                        adapter_type = type(adapter).__name__.split(".")[-1]
                        raise MessageInputAdapterFailedStartup(adapter_type)

            # we have met our target of number of adapters started!
            if adapters_ready == total_adapters_started:
                for adapter_type in adapters_started:
                    self.logger.info("%s %s adapter(s) ready!"
                        % (adapters_started[adapter_type],adapter_type))
                return total_adapters_ready
            
            time.sleep(0.01) # 10 ms of sanity
        
        # timed out and (adapters_ready < adapters_started)
        raise MessageInputAdapterStartupTimeout()

        
    # return number of AMQP Producers started
    # If blocking=True is passed, this method will block until all clients
    # are ready but raise an exception on timeout or client failed status
    #def start_amqp_producers(self,blocking=False,ready_timeout_secs=3):

    # Request a clean shutdown of each AMQP Client
    # Effectively call AmqpClient::stop() method
    def stop_amqp_clients(self):
        
        num_amqp_consumers = len(self._amqp_consumers)
        num_amqp_producers = len(self._amqp_producers)

        if not num_amqp_consumers and not num_amqp_producers:
            return # nothing to do
            
        self.logger.debug("Stopping %s AMQP Producer(s) and %s AMQP Consumer(s)" % (num_amqp_producers, num_amqp_consumers))

        for producer in self._amqp_producers:
            producer.stop()
        
        for consumer in self._amqp_consumers:
            consumer.stop()

    # Calls Thread::join() on each AMQP Client thread to wait for clients to 
    # finish up and exit their ::run() loops
    # We cannot call join on a Thread which has not ran. Thread::is_alive is set
    # at call to run() and shortly after shutdown
    def block_until_amqp_clients_finish(self):
        self.logger.debug("Waiting for AMQP Producers and Consumers to finish")
        for consumer in self._amqp_consumers:
            if consumer.is_alive(): consumer.join()
        for producer in self._amqp_producers:
            if producer.is_alive(): producer.join()

    # called from AMQP Producer -- what is this?
    def post_publish_confirmation(self):
        pass
    
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

    @property
    def amqp_input_message_routing_key_prefix(self):
        return self._amqp_input_message_routing_key_prefix
            
    # Get message_type for supplied routing_key or None if no match
    # match provided Message routing_key against list of known product 
    # binding_patterns and return the type if matched or None if no match
    def routing_key_message_type(self, routing_key):
        routing_key = amqputilities.remove_routing_key_prefix(routing_key, self.amqp_input_message_routing_key_prefix)
        for message_type in self.__message_types:
            if "bind_key" in self.__message_types[message_type]:
                if amqputilities.match_routing_key(routing_key, self.__message_types[message_type]['bind_key']):
                    return message_type
        return None # if nothing found
    
    # Return a loaded Message Processor which claims to be able to process 
    # a message with provided routing_key
    def processor_for_amqp_routing_key(self, routing_key):
        try:
            routing_key = amqputilities.remove_routing_key_prefix(routing_key, self.amqp_input_message_routing_key_prefix)
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
            input_message_type = self.routing_key_message_type(input_message_routing_key)
    
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
                intermediary_data = self._pre_process(input_message, input_message_routing_key)
                
                # Final processing with the MessageProcessor
                # Note we are adding a proces_method(data) method -- this is an assumption
                try:
                    processor_output = processor.process_message(intermediary_data)
                except Exception as e:
                    raise MessageProcessingFailedError(e)
            
                if processor_output is None:
                    raise MessageProcessingFailedError("processer output is NoneType!")
            
                # Create AmqpMessage for flight
                if type(processor_output) == 'AmqpMessage':
                    output_amqp_message = processor_output
                else:
                    output_amqp_message = AmqpMessage(body=processor_output)
                
                # we are allowing the processor to specify the publish exchange by
                # supplying an exchange property of the passed Amqp.Message object
                output_amqp_message.exchange = "test" #output_message.exchange
                output_amqp_message.routing_key = "test"

                # blocking publish output! new messages will be arriving in the input while we block
                if self._do_amqp_publish(output_amqp_message):
                    self.statistics.metric("messages_publish_failed")
                    self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
                    return # leave this call
                
                # this concludes a complete trip for a message! ourprimary goal
                self._amqp_consumers[0].ack_message(input_message_delivery_tag,False,input_message_consumer_tag)
                self.statistics.metric("messages_processed_total_completely")
                self.statistics.metric("messages_processed_type_%s_completely" % input_message_type)
                
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

            # all paths lead here except unhandled exceptions!

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

            # Start Message IO Adapters
            # call will return the number of adapters ready or raise an 
            # exception on a single adapter failure or timeout
            self.start_adapters(self._amqp_producers,blocking=True)
            self.start_adapters(self._amqp_consumers,blocking=True)

            self.service_monitor.add_statistics_metric_watchdog("messages_processed_total_completely", 5)

        except Exception as e:
            self.set_failed("critical exception in pipeline setup")
            self.logger.error(exceptionhandling.traceback_string(e))
            
        self.logger.info("pipeline started")
    
    # we make these methods so we may better return or raise exceptions
    def _shutdown_pipeline(self):
        # clean up before we shut down
        self.logger.info("Shutting down pipeline")
        self.stop_amqp_clients()
        self.block_until_amqp_clients_finish()

    # Service::do_run()
    # Called via Thread::run() and when we complete, parent will cleanup
    def do_run(self):
        
        self._startup_pipeline()
       
        # single threaded processor - this could be multithreaded
        # we could call a child or parent class method work loop here also
        
        # The work loop
        while self.keep_working:
            self._do_message_processing()
        
        self._shutdown_pipeline()