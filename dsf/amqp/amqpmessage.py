import pika.spec
from pika.spec import BasicProperties, Basic

from dsf.amqp import amqputilities
from dsf.message import Message, MessageType

class AmqpMessage(Message):
    
#    _name = None
    
    _exchange = None
    _routing_key = None
    _body = None
    _properties = None
    _method = None

    def __init__(self, **kwargs):
        self._type = MessageType("amqp","AMQPMessage")
        super().__init__(**kwargs)
        
#    @property
#    def name(self): return self._name

    @property
    def exchange(self):
        return self._exchange
        
    @exchange.setter
    def exchange(self,value):
        self._exchange = value
        
    @property
    def routing_key(self):
        return self._routing_key
        
    @routing_key.setter
    def routing_key(self,value):
        self._routing_key = value
        
    def remove_routing_key_prefix(self,prefix):
        self._routing_key = amqputilities.remove_routing_key_prefix(self._routing_key,prefix)
        
    @property
    def body(self):
        return self._body
        
    @body.setter
    def body(self,value):
        self._body = value
        
    @property
    def properties(self):
        if not self._properties:
            return BasicProperties()
        else:
            return self._properties

    @properties.setter
    def properties(self,value):
        self._properties = value
        
    @property
    def method(self):
        return self._method
    
    # no method.setter to avoid confusion
    def set_method(self,method):
        if method and hasattr(method,"routing_key"):
            self._routing_key = method.routing_key
        self._method = method
        
    def __str__(self):
        output = "exchange=%s, routing_key=%s, body=%s, properties=%s" % (self.exchange, self.routing_key, self.body, self.properties)
        return output

    @classmethod
    def from_kwargs(cls,**kwargs):
        message = AmqpMessage()
        if "routing_key" in kwargs: message.routing_key = kwargs["routing_key"]
        message.exchange = kwargs.get("exchange", None)
        message.set_method(kwargs.get("basic_deliver", None))
        message.body = kwargs.get("body", None)
        message.properties = kwargs.get("properties", None)
        return message

    @classmethod
    def from_pika(cls,method,properties,body):

        if not (isinstance(method,pika.spec.Basic.Deliver)):
            raise Exception("type(basic_deliver)==%s but should be <class 'pika.spec.Basic.Deliver'>" % (type(basic_deliver)))
        if not (isinstance(properties,pika.spec.BasicProperties)):
            raise Exception("type(properties)==%s but should be <class 'pika.spec.BasicProperties'>" % (type(properties)))

        try:
            message = cls()
            #message.amqp = {}

            # <class 'pika.spec.Basic.Deliver'>
            # Basic.Deliver has 'consumer_tag', 'decode', 'delivery_tag', 'encode', 
            #  'exchange', 'get_body', 'get_properties', 'redelivered', 
            #  'routing_key', 'synchronous'
            message.set_method(method)
            
            # <class 'pika.spec.BasicProperties'>
            # BasicProperties has: 'app_id', 'cluster_id', 'content_encoding', 
            # 'content_type', 'correlation_id', 'decode', 'delivery_mode', 
            # 'encode', 'expiration', 'headers', 'message_id', 'priority', 
            # 'reply_to', 'timestamp', 'type', 'user_id'
            message.properties = properties

            # body is <class 'bytes'>
            message.body = body
            
            return message
        except Exception as e:
            raise Exception(e)
