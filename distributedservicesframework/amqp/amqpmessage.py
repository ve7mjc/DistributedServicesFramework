
# for data type checks
import pika.spec
from pika.spec import BasicProperties, Basic

class AmqpMessage():
    
    __exchange = None
    __routing_key = None
    __body = None
    __properties = None
    
    def __init__(self, **kwargs):
        
        # pass message parameters as a tuple in the order Pika calls the
        # on_message() callback method
        # This is likely a message received from a consumer
        if "pika_tuple" in kwargs:
            
            (basic_deliver, properties, body) = kwargs.get("pika_tuple")
            
            if not (isinstance(basic_deliver, pika.spec.Basic.Deliver)):
                raise Exception("type(basic_deliver)==%s but should be <class 'pika.spec.Basic.Deliver'>" % (type(basic_deliver)))
                
            if not (isinstance(properties, pika.spec.BasicProperties)):
                raise Exception("type(properties)==%s but should be <class 'pika.spec.BasicProperties'>" % (type(properties)))

            # <class 'pika.spec.Basic.Deliver'>
            # Basic.Deliver has 'consumer_tag', 'decode', 'delivery_tag', 'encode', 'exchange', 'get_body', 'get_properties', 'redelivered', 'routing_key', 'synchronous'
            self.__basic_deliver = basic_deliver

            # <class 'pika.spec.BasicProperties'>
            # BasicProperties has: 'app_id', 'cluster_id', 'content_encoding', 
            # 'content_type', 'correlation_id', 'decode', 'delivery_mode', 
            # 'encode', 'expiration', 'headers', 'message_id', 'priority', 
            # 'reply_to', 'timestamp', 'type', 'user_id'
            self.__properties = properties

            # body is <class 'bytes'>
            self.__body = body
            
            self.__routing_key = basic_deliver.routing_key
        
        else:
            self.__exchange = kwargs.get("exchange", None)
            self.__routing_key = kwargs.get("routing_key", None)
            self.__body = kwargs.get("body", None)
            self.__properties = kwargs.get("properties", None)

    @property
    def exchange(self):
        return self.__exchange
        
    @exchange.setter
    def exchange(self,value):
        self.__exchange = value
        
    @property
    def routing_key(self):
        return self.__routing_key
        
    @routing_key.setter
    def routing_key(self,value):
        self.__routing_key = value
        
    @property
    def body(self):
        return self.__body
        
    @body.setter
    def body(self,value):
        self.__body = value
        
    @property
    def properties(self):
        if not self.__properties:
            return BasicProperties()
        else:
            return self.__properties

    @properties.setter
    def properties(self,value):
        self.__properties = value
        
    @property
    def basic_deliver(self):
        return self.__basic_deliver
        
    def __str__(self):
        output = "exchange=%s, routing_key=%s, body=%s, properties=%s" % (self.exchange, self.routing_key, self.body, self.properties)
        return output

#    @basic_deliver.setter
#    def basic_deliver(self,value):
#        self.__basic_deliver = value