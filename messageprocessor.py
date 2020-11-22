from distributedservicesframework.component import Component

class MessageProcessorException(Exception):
    pass

class MessageProcessor(Component):
    
    def __init__(self):

        super().__init__()
        
    def process(self, data, **kwags):
        return
        