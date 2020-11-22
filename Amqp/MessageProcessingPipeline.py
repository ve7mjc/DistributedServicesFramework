#
# Data Adapter / Data Processing Pipeline
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

# The Distributed Services Framework and the accompanied libraries were 
# developed for this project
from DistributedServicesFramework import ExceptionHandling
from DistributedServicesFramework.Service import Service
from DistributedServicesFramework.Amqp.AsynchronousConsumer import AsynchronousConsumer
from DistributedServicesFramework.Amqp.AsynchronousProducer import AsynchronousProducer
from DistributedServicesFramework.Amqp import AmqpUtilities
from DistributedServicesFramework.Amqp.AmqpMessage import AmqpMessage

from DistributedServicesFramework.Exceptions import *

from queue import Queue, Empty
import time # for sleep and sleep only, zzz

# must be asyncronous and thread-safe
class MessageProcessingPipeline(Service):
    
    # <class 'Amqp.AsynchronousConsumer'>
    _amqp_consumers = []
    _min_amqp_consumers_required = 1
    
    # <class 'Amqp.AsynchronousProducer'>
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
    __amqp_input_message_routing_key_prefix = None
    __message_types = {}
    
    def __init__(self, **kwargs):
        
        self._amqp_publish_exchange = kwargs.get("amqp_publish_exchange", None)
        self.__amqp_input_message_routing_key_prefix = kwargs.get("amqp_input_message_routing_key_prefix", None)
        #self._publish_queue = kwargs.get("queue", None)
        kwargs["loglevel"] = self.loglevel_debug()
        
        # get loggers, etc
        super().__init__(**kwargs)
        
        self.logger.info("started")

    # Create an instance of 'Amqp.AsynchronousConsumer' passing in
    #  - keyword arguments
    #  - reference to our local message Queue
    def add_amqp_consumer(self, **kwargs):
        kwargs["message_queue"] = self._input_message_queue
        kwargs["statistics"] = self.statistics
        consumer = AsynchronousConsumer(**kwargs)
        self._amqp_consumers.append(consumer)

    # Create an instance of 'Amqp.AsynchronousProducer' passing in
    # - keyword arguments
    # - "exchange" if supplied to this class instance
    def add_amqp_producer(self, **kwargs):
        if "exchange" in kwargs:
            self._publish_exchange = kwargs.get("exchange")
        kwargs["statistics"] = self.statistics
        producer = AsynchronousProducer(**kwargs)
        self._amqp_producers.append(producer)

    # return number of AMQP Consumers started
    def start_amqp_consumers(self):
        num_started = 0
        for consumer in self._amqp_consumers:
            # apply relevent test modes
            for test_mode in self.enabled_test_modes:
                if test_mode.startswith("amqp_"):
                    consumer.enable_test_mode(test_mode, self.test_mode(test_mode))
            consumer.start()
            num_started += 1
        self.logger.debug("Started %s consumers" % num_started)
        return num_started

    # return number of AMQP Producers started
    def start_amqp_producers(self):
        num_started = 0
        for producer in self._amqp_producers:
            producer.start()
            num_started += 1
        self.logger.debug("Started %s AMQP Producers" % num_started)
        return num_started
        
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
    def block_until_amqp_clients_finish(self):
        self.logger.debug("Waiting for AMQP Producers and Consumers to finish")
        for consumer in self._amqp_consumers:
            consumer.join()
        for producer in self._amqp_producers:
            producer.join()

    # called from AMQP Producer -- what is this?
    def post_publish_confirmation(self):
        pass
    
    # pass keyword arguments straight in to message_type dict
    # examples: bind_pattern, processor
    def add_message_type(self, message_type, **kwargs):
        self.__message_types[message_type] = {}
        for key in kwargs.keys():
            self.logger.debug("Adding self.__message_types['%s']['%s'] = %s" % (message_type, key, kwargs[key]))
            self.__message_types[message_type][key] = kwargs[key]
            
    @property
    def amqp_input_message_routing_key_prefix(self):
        return self.__amqp_input_message_routing_key_prefix
            
    # Get message_type from routing_key
    # match provided Message routing_key against list of known product 
    # binding_patterns and return the type if matched or None if no match
    def routing_key_message_type(self, routing_key):
        routing_key = AmqpUtilities.remove_routing_key_prefix(routing_key, self.__amqp_input_message_routing_key_prefix)
        self.logger.debug("searching len(self.__message_types)=%s for routing_key=%s" % (len(self.__message_types), routing_key))
        for message_type in self.__message_types:
            if "bind_key" in self.__message_types[message_type]:
                if AmqpUtilities.match_routing_key(routing_key, self.__message_types[message_type]['bind_key']):
                    self.logger.debug("returning %s for key %s" % (message_type,routing_key))
                    return message_type
        return None # if nothing found

    # we are assured the message_type is a valid message type or None
    # however a known message_type may not have a processor
    def _message_type_processor(self, message_type):
        if "processor" in self.__message_types[message_type]:
            return self.__message_types[message_type]["processor"]
        
    # Return True if we have a callable method specified in the configuration
    # for the supplied message_type
    # type(process_method) == <class 'method'>
    def message_type_has_processor(self, message_type):
        if message_type == None: return False
        if "processor" in self.__message_types[message_type]:
            return callable(self.__message_types[message_type]["processor"])
        return False
    
    # redeclare me in child classes!
    def process_message(self):
        self.logger.error("processor::process_message() method not declared in child class!")

    # Message Pre-processing
    # Generally processing of a message which may be common to all message types
    def _pre_process(self, input_message, message_type):
        if hasattr(self,"pre_process_message"):
            try: # dont assume this subclass method will behave
                return self.pre_process_message(input_message, message_type)
            except Exception as e:
                self.logger.error("message processing failed in pre-processing: %s" % ExceptionHandling.traceback_string(e))

    # No reaction to the processing success or Exceptions are performed in
    # this method. We signal the outcome by passing the output data from the
    # processor or raise an Exception that reflects the nature of the problem
    def _process_message(self, message, message_type):
        
        # routing_key based processing as no other method is written yet

        # Check if we have a suitable processor available for this data type
        # before proceeding
        if not self.message_type_has_processor(message_type):
            raise MessageTypeUnsupportedError() # ("message type %s is unsupported" % message_type)

        # Message Pre-processing
        # Generally processing of a message which may be common to all message types
        intermediary_data = self._pre_process(message, message_type)

        # Final processing
        try:
            # Call appropriate Processor method for our message_type, sanity 
            # check the output and return 
            processor_output = self._message_type_processor(message_type)(intermediary_data) # strange, Eh?
            if processor_output is None:
                raise MessageProcessingFailedError("output of processer is NoneType!")
            return processor_output
        except Exception as e:
            raise MessageProcessingFailedError(e)

    def _do_amqp_publish(self, amqp_message):
        producer_response = self._amqp_producers[0].publish(message=amqp_message, blocking=True)
        if producer_response == "ack":
            return False
        return True

    # Retrieve a message from input message queue, 
    # process message against subclass methods, and
    # push result to output message queue
    def _do_message_processing(self):
        try:
            # retrieve a message of type AmqpMessage from input queue
            input_message = self._input_message_queue.get(block=True, timeout=0.01)
            
            # Make record of consumer and delivery tags so we can act on the
            # outcome of the processing of this message
            input_message_routing_key = input_message.routing_key
            input_message_consumer_tag = input_message.basic_deliver.consumer_tag
            input_message_delivery_tag = input_message.basic_deliver.delivery_tag
            input_message_type = self.routing_key_message_type(input_message_routing_key)
    
            try:
                # Process Message and perform follow-on actions
                processor_output = self._process_message(input_message, input_message_type)

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
                self.logger.info("Unsupported message type: %s" % e)
                self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
                self.statistics.metric("messages_processed_unknown_type")
                
            except MessageProcessingFailedError as e:
                self.logger.warning("Message processing failed for msg with routing_key=%s: %s" % (input_message_routing_key,e))
                self._amqp_consumers[0].nack_message(input_message_delivery_tag, input_message_consumer_tag)
                self.statistics.metric("messages_processed_failed_error")
                
            except Exception as e:
                # what Message Acknowledgement shall we perform?
                self.logger.error(ExceptionHandling.traceback_string(e))

            # all paths lead here except unhandled exceptions!

        except Empty:
            pass # there was no messages for us, back to waiting we go
        except Exception as e:
            self.logger.error(ExceptionHandling.traceback_string(e))
            
        # this round of work has completed
        return 

    # Thread::run() - Method representing the thread’s activity
    # The standard run() method invokes the callable object passed to the 
    # object’s constructor as the target argument, if any, with sequential and 
    # keyword arguments taken from the args and kwargs arguments, respectively.
    # Thread::start() - Start the thread’s activity.
    # It must be called at most once per thread object. It arranges for the 
    # object’s run() method to be invoked in a separate thread of control.
    def run(self):

        # Start AMQP Producers and Consumers 
        # at least one of each is required or flag for shutdown if not able
        # to get at least one of each
        if self.start_amqp_producers() >= self._min_amqp_producers_required:
            if self.start_amqp_consumers() < self._min_amqp_consumers_required:
                self.stop("Not able to start minimum required %s AMQP Consumer(s)" % self._min_amqp_consumers_required)
        else: 
            self.stop("Not able to start minimum required %s AMQP Producers(s)" % self._min_amqp_producers_required)
        
        # single threaded processor - this could be multithreaded
        # we could call a child or parent class method work loop here also
        
        # The work loop
        # will not execute and pass by if required message input and output
        # mechanisms are not in place
        while self.keep_working():
                self._do_message_processing()
        
        # clean up before we shut down
        self.stop_amqp_clients()
        self.block_until_amqp_clients_finish()   

        self.logger.debug("leaving MessageProcessingPipeline::run() ..")
        # released to exit
        return
    