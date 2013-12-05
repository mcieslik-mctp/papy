""" 
:mod:`papy.util.config`
=======================

Configures logging to monitor the execution of **PaPy** pipelines and  
OS-dependent defaults for different variables.

"""
import time
import os, socket
import logging.handlers

from numap import imports

@imports(['os', 'socket'])
def get_defaults():
    """
    Returns a dictionary of variables and their possibly os-dependent defaults.
    
    """
    DEFAULTS = {}
    # Determine the run-time pipe read/write buffer.
    if 'PC_PIPE_BUF' in os.pathconf_names:
        # unix
        x, y = os.pipe()
        DEFAULTS['PIPE_BUF'] = os.fpathconf(x, "PC_PIPE_BUF")
    else:
        # in Jython 16384
        # on windows 512
        # in jython in windows 512
        DEFAULTS['PIPE_BUF'] = 512

    # Determine the run-time socket buffers.
    # Note that this number is determine on the papy server
    # and inherited by the clients.
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    DEFAULTS['TCP_SNDBUF'] = tcp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_SNDBUF)
    DEFAULTS['TCP_RCVBUF'] = tcp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_RCVBUF)
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    DEFAULTS['UDP_SNDBUF'] = udp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_SNDBUF)
    DEFAULTS['UDP_RCVBUF'] = udp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_RCVBUF)

    # check the ip visible from the world.
    DEFAULTS['WHATS_MYIP_URL'] = \
    'http://www.whatismyip.com/automation/n09230945.asp'
    return DEFAULTS

def start_logger(log_to_file=False, \
                 log_to_stream=False, \
                 log_to_file_level=logging.INFO, \
                 log_to_stream_level=logging.INFO, \
                 log_filename=None, \
                 log_stream=None, \
                 log_rotate=True, \
                 log_size=524288, \
                 log_number=3):
    """
    Configures and starts a logger to monitor the execution of a **PaPy** 
    pipeline.

    Arguments:

      - log_to_file(``bool``) [default: ``True``] Should we write logging 
        messages into a file?
      - log_to_stream(``bool`` or ``object``) [default: ``False``] Should we 
        print logging messages to a stream? If ``True`` this defaults to 
        ``stderr``. 
      - log_to_file_level(``int``) [default: ``INFO``] The minimum logging 
        level of messages to be written to file. 
      - log_to_screen_level(``int``) [default: ``ERROR``] The minimum logging 
        level of messages to be printed to the stream.
      - log_filename(``str``) [default: ``"PaPy_log"`` or ``"PaPy_log_$TIME$"``]
        Name of the log file. Ignored if "log_to_file" is ``False``.
      - log_rotate(``bool``) [default: ``True``] Should we limit the number of 
        logs? Ignored if "log_to_file" is ``False``.
      - log_size(``int``) [default: ``524288``] Maximum number of ``bytes`` 
        saved in a single log file. Ignored if "log_to_file" is ``False``.
      - log_number(``int``) [default: ``3``] Maximum number of rotated log files
        Ignored if "log_to_file" is ``False``.
        
    """
    if log_rotate:
        log_filename = log_filename or 'PaPy_log'
    else:
        run_time = "_".join(map(str, time.localtime()[0:5]))
        log_filename = 'PaPy_log_%s' % run_time

    root_log = logging.getLogger()
    formatter = logging.Formatter(
            "%(levelname)s %(asctime)s,%(msecs).3d [%(name)s] - %(message)s", \
            datefmt='%H:%M:%S')
    root_log.setLevel(logging.DEBUG)
    if log_to_file:
        if log_rotate:
            file_handler = logging.handlers.RotatingFileHandler(log_filename, \
                                    maxBytes=log_size, backupCount=log_number)
        else:
            file_handler = logging.FileHandler(log_filename, 'w')
        file_handler.setLevel(log_to_file_level)
        file_handler.setFormatter(formatter)
        root_log.addHandler(file_handler)
    if log_to_stream:
        stream_handler = logging.StreamHandler(log_stream)
        stream_handler.setLevel(log_to_stream_level)
        stream_handler.setFormatter(formatter)
        root_log.addHandler(stream_handler)
