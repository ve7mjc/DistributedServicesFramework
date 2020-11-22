# Distributed Services Framework - Python Edition!

# add this module path to the search path so we can access modules and members
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__)))

# Bring these Classes into scope only
from DistributedServicesFramework.Service import Service
from DistributedServicesFramework import Exceptions
import DistributedServicesFramework.Amqp

# redef of Logging LogLevels for child applications
class LogLevel():
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0