# connect:
# Producer (upstream) Publish Message Delivery Ack event
# to a 
# Consumer (Source) Consumer Channel Message Ack

from DistributedServicesFramework.Amqp.AsynchronousConsumer import AsynchronousConsumer
from DistributedServicesFramework.Amqp.AsynchronousProducer import AsynchronousProducer

from queue import Queue

# must be asyncronous and thread-safe

class MessageProcessingPipeline():
    
    _amqp_consumers = []
    _amqp_producers = []
    
    consumer_message_out_queue = Queue()
    producer_message_in_queue = Queue()
    
    def __init__(self):
        if not self.logger:
            raise Exception("I need a logger")
        
    def connect_consumer(self, consumer):
        self.logger.debug("adding consumer to consumers")
        self._amqp_consumers.append(consumer)
        pass
        
    def create_consumer(self, **kwargs):
        consumer = AsynchronousConsumer(**kwargs)
        consumer.start()
        self.connect_consumer(consumer)
        pass
        
    def connect_producer(self, producer):
        pass
        
    def create_producer(self, **kwargs):
        pass
        
    # called from AMQP Producer
    def post_publish_confirmation(self):
        pass
    
    def stop(self):
        
        self.logger.info("MessagePipeline starting clean shutdown")
        
        # indicate a clean shutdown to each client
        for consumer in self._amqp_consumers:
            consumer.stop()
        for producer in self._amqp_producers:
            producer.stop()
        
        # wait for threads to finish in each client
        for consumer in self._amqp_consumers:
            consumer.join()
        for producer in self._amqp_producers:
            producer.join()
        