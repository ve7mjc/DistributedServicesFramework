from dsf.amqp import amqputilities
from dsf.message import Message, MessageType

import pika.spec

"""
For reference:
From Consumer:
pika channel _on_message_callback(channel,method,properties,body)
  channel: pika.Channel 
  method: pika.spec.Basic.Deliver 
  properties: pika.spec.BasicProperties 
  body: bytes

pika.spec.BasicProperties
  content_type
  content_encoding
  headers
  delivery_mode
  priority
  correlation_id
  reply_to
  expiration
  message_id
  timestamp
  type
  user_id
  app_id
  cluster_id

pika.spec.Basic.Deliver
  consumer_tag
  delivery_tag
  redelivered
  exchange
  routing_key

To Producer

pika.channel.publish(exchange, routing_key, body, properties=None, mandatory=False)
  exchange - exchange to publish to - can be blank for default exchange
  properties - pika.spec.Basic.properties
  mandatory bit - This flag tells the server how to react if the message cannot be
  routed to a queue. If this flag is set, the server will return an unroutable
  message with a Return method. If this flag is zero, the server silently drops
  the message. 
  
def __repr__(self):
    items = list()
    for key, value in self.__dict__.items():
        if getattr(self.__class__, key, None) != value:
            items.append('%s=%s' % (key, value))
    if not items:
        return "<%s>" % self.NAME
    return "<%s(%s)>" % (self.NAME, sorted(items))
  

"""

# The only common attributes to an AMQP message between the Consumer and
#  producer is the body and BasicProperties
class AmqpMessage(Message):

    _body = None
    _properties = None
    
    def __init__(self, **kwargs):
        self._body = kwargs.get("body",None)
        self.set_properties(kwargs.get("properties",None))
        super().__init__(**kwargs)

    @property
    def body(self):
        return self._body
        
    def set_body(self,value):
        self._body = value

# Message received during consuming from a Consumer
class AmqpConsumerMessage(AmqpMessage):

    _method = None
    # properties in parent class
    # body in parent class
    
    def __init__(self, **kwargs):
        self._type = MessageType("amqp_c","AmqpConsumerMessage")
        super().__init__(**kwargs)

    @property
    def method(self): 
        return self._method

    def set_method(self,method):
        self._method = method
        
    @property
    def properties(self):
        return self._properties
        
    def set_properties(self,properties):
        self._properties = properties

    @classmethod
    def from_pika(cls,method,properties,body):
        
        if not (isinstance(method,pika.spec.Basic.Deliver)):
            raise Exception("type(basic_deliver) is %s; but should be "
                "<class 'pika.spec.Basic.Deliver'>" % type(basic_deliver))
        if not (isinstance(properties,pika.spec.BasicProperties)):
            raise Exception("type(properties) is %s; but should be "
                "<class 'pika.spec.BasicProperties'>" % type(properties))
        try:
            obj = cls()
            #print(type(cls.__name__))
            obj.set_method(method)
            obj.set_properties(properties)
            obj.set_body(body)
            return obj

        except Exception as e:
            raise Exception(e)


# Message constructed to be published by Producer
class AmqpProducerMessage(AmqpMessage):
    
    _exchange = None
    _routing_key = None
    # body in parent class
    # properties in parent class
    _mandatory = False
    _persistent = True
    
    def __init__(self, **kwargs):
        self._type = MessageType("amqp_p","AmqpProducerMessage")
        super().__init__(**kwargs)
    
    @property
    def exchange(self): 
        return self._exchange

    @property
    def routing_key(self): 
        return self._routing_key
        
    @property
    def properties(self):
        if not self._properties:
            self.set_properties()
        return self._properties
        
    def set_properties(self,properties=None):
        
        if properties and not isinstance(properties,pika.spec.BasicProperties):
            self.log.error("AmqpMessage.set_properties() passed %s" % 
                type(properties))
            return True
            
        if properties:
            self._properties = properties
        else:
            self._properties = pika.spec.BasicProperties()

        if self._persistent:
            self._properties.delivery_mode = 2

    @property
    def mandatory(self):
        return self._mandatory

    def set_exchange(self,value):
        self._exchange = value
    
    def set_routing_key(self,value): 
        self._routing_key = value
    
    def set_mandatory(self,value=True):
        self._mandatory = value

    @classmethod
    def from_kwargs(cls,**kwargs):
        obj = cls(**kwargs)
        obj._persistent = kwargs.get("persistent", True)
        obj.set_exchange(kwargs.get("exchange", None))
        obj.set_routing_key(kwargs.get("routing_key", None))
        obj.set_properties(kwargs.get("properties", None))
        obj.set_mandatory(kwargs.get("mandatory", True))
        return obj