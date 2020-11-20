# connect:
# Producer (upstream) Publish Message Delivery Ack event
# to a 
# Consumer (Source) Consumer Channel Message Ack

from DistributedServicesFramework.Service import Service
from DistributedServicesFramework.Amqp.AsynchronousConsumer import AsynchronousConsumer
from DistributedServicesFramework.Amqp.AsynchronousProducer import AsynchronousProducer

from queue import Queue, Empty

import time # for sleep and sleep only
#import threading

# must be asyncronous and thread-safe
class MessageProcessingPipeline(Service):
    
    _amqp_consumers = []
    _amqp_producers = []
    
    _consumer_message_out_queue = Queue()
    _producer_message_in_queue = Queue()
    
    _publish_exchange = None
    _publish_queue = None
    
    def __init__(self, **kwargs):
        
        self._publish_exchange = kwargs.get("publish_exchange", None)
        self._publish_queue = kwargs.get("queue", None)
        kwargs["loglevel"] = self.loglevel_warning()
        
        # get loggers, etc
        super().__init__(**kwargs)
        
        self.logger.info("started.")
        
    def set_publish_exchange(self, exchange):
        self._publish_exchange = exchange
        
    def add_consumer(self, **kwargs):
        kwargs["message_queue"] = self._consumer_message_out_queue
        consumer = AsynchronousConsumer(**kwargs)
        self._amqp_consumers.append(consumer)
        
    def add_producer(self, **kwargs):
        if "exchange" in kwargs: self._publish_exchange = kwargs.get("exchange")
        producer = AsynchronousProducer(**kwargs)
        self._amqp_producers.append(producer)
        
    # called from AMQP Producer
    def post_publish_confirmation(self):
        pass
    
    def run(self):
        
        self._shutdown_requested = False
        
        # start up producers
        for producer in self._amqp_producers:
            producer.start()
        # TODO - do not continue if we could not start producers?
        
        for consumer in self._amqp_consumers:
            consumer.start()
        
        # single threaded processor - this could be multithreaded
        while not self._shutdown_requested:
            try:
                input_message = self._consumer_message_out_queue.get(block=True, timeout=0.01)
                consumer_tag = input_message.basic_deliver.consumer_tag
                delivery_tag = input_message.basic_deliver.delivery_tag
                
                out_msg = self.process_message(input_message)
                
                if out_msg:
                    # a processor could specify a different exchange to publish to
                    # blocking publish output! new messages will be arriving in the input while we block
                    if out_msg.exchange:
                        #exchange = self._publish_exchange
                        response = self._amqp_producers[0].publish(message=out_msg, blocking=True)
                        if response == "ack":
                            self._amqp_consumers[0].ack_message(delivery_tag,False,consumer_tag)
                            self.logger.info("processed, published, and acknowledged message with routing_key=%s" % (out_msg.routing_key))
                        else:
                            # we will be stuck here unless we send it back 
                            self._amqp_consumers[0].nack_message(delivery_tag, consumer_tag)
                    else:
                        self.logger.error("unable to publish. exchange not set")
                else:
                    self._amqp_consumers[0].nack_message(delivery_tag, consumer_tag)
                    self.logger.debug("unable to process message - returning to queue")
                # self.logger.debug(rcv)
            except Empty:
                pass
            except (KeyboardInterrupt, SystemExit):
                break
            except Exception as e:
                self.logger.error(e)

            # END while not shutdown_requested
        
        self.logger.debug("Stopping AMQP Asynchronous Consumers and Producers")
        
        # indicate a clean shutdown to each client
        for consumer in self._amqp_consumers:
            consumer.stop()
            
        for producer in self._amqp_producers:
            producer.stop()
        
        self.logger.debug("Waiting for AMQP clients to stop..")
        
        # wait for threads to finish in each client
        for consumer in self._amqp_consumers:
            consumer.join()
            
        for producer in self._amqp_producers:
            producer.join()
            
        # released to exit
        return
    
    def stop(self):
        self.logger.debug("MessageProcessingPipeline::stop() called; shutting down")
        self._shutdown_requested = True
        
