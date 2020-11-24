# convenience for tracebacks for debugging

from os import path
from sys import stdout, exc_info
from traceback import format_exc
from inspect import stack

#class exceptionhandling:

# import exceptions # ?

# class exceptionhandling

# return Traceback "oneliner" for logging and other compact uses
# Format: in {filename}:{line_no}; {exception_type} - {exception_message}
def traceback_string(exception_string=""):
    
    # sys.exc_info() returns (type, value, traceback)
    # - type gets the type of the exception being handled (a subclass of 
    #   BaseException); 
    # - value gets the exception instance (an instance of the exception type)
    # - traceback gets a traceback object which encapsulates the call stack 
    #   at the point where the exception originally occurred.
    exc_type, exc_obj, exc_tb = exc_info()
    
    # exc_tb <class 'traceback'>
	#   tb_frame - frame object at this level
	#   tb_lasti - index of last attempted instruction in bytecode
	#   tb_lineno - current line number in Python source code
	#   tb_next - next inner traceback object (called by this level)

    # exc_tb.tb_frame <class 'frame'>
    #   f_back - next outer frame object (this frame’s caller)
	#   f_builtins - builtins namespace seen by this frame
	#   f_code - code object being executed in this frame
	#   f_exc_traceback - traceback if raised in this frame, or None
	#   f_exc_type - exception type if raised in this frame, or None
	#   f_exc_value - exception value if raised in this frame, or None
	#   f_globals - global namespace seen by this frame
	#   f_lasti - index of last attempted instruction in bytecode
	#   f_lineno - current line number in Python source code
	#   f_locals - local namespace seen by this frame
	#   f_restricted - 0 or 1 if frame is in restricted execution mode
	#   f_trace - tracing function for this frame, or None
    
    # tb_frame.f_code <class 'code'>
    #    co_argcount number of arguments (not including * or ** args)
    #    co_code string of raw compiled bytecode
    #    co_consts tuple of constants used in the bytecode
    #    co_filename name of file in which this code object was created
    #    co_firstlineno number of first line in Python source code
    #    co_flags bitmap: 1=optimized | 2=newlocals | 4=*arg | 8=**arg
    #    co_lnotab encoded mapping of line numbers to bytecode indices
    #    co_name name with which this code object was defined
    #    co_names tuple of names of local variables
    #    co_nlocals number of local variables
    #    co_stacksize virtual machine stack space required
    #    co_varnames tuple of names of arguments and local variables
    filename = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    method_name = exc_tb.tb_frame.f_code.co_name

    # print full traceback to console
    #print("FULL TRACEBACK:\r\n%s" % format_exc())
    # return one-line string to caller
    return "in {filename}->{method_name}() line {line_no}: {exc_type}: {msg}".format(
        exc_type=exc_type.__name__, msg=exception_string, filename=filename, line_no=exc_tb.tb_lineno, method_name=method_name)

def traceback_dict():
    exc_type, exc_obj, exc_tb = exc_info()
    fname = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return { "type" : exc_type, "filename" : fname, "lineno" : exc_tb.tb_lineno }

def print_full_traceback_console():
    print(format_exc())