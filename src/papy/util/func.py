# -*- coding: utf-8 -*-
""" 
:mod:`papy.util.func`
=====================

A collection of core functions to use in Worker instances includes functions for
dealing with inputs/outputs of a pipeline or ``Pipers``. In general these 
functions are used to connect ``Pipers`` to external inputs/outputs (these are 
the pipeline input/outputs i.e. streams) or to connect them to other ``Pipers`` 
(via items i.e. transformed elements of the input streams). Based on that 
distinction two types of functions are provided:

  - stream function load or save the input stream from or into a single 
      file, therefore they can only be used at the beginnings or ends of a 
      pipeline. Stream loaders are not worker functions, as they are called once
      (e.g. with the input file name as the argument) and create the input 
      stream in the form of a generator of input items.

  - item functions load, save, process or display data items. These are 
    ``Worker`` functions and should be used within ``Pipers``. 

No method of interprocess communication, besides the default inefficient is 
supported on all platforms. Even among UNIX implementation details forking 
and can differ.

"""
# all top-level imports in this module have to be injected to remote RPyC
# connections if the defined functions should be callable remotely. 
from numap import imports # provided by IMap
from papy.util.config import get_defaults # provided by worker._inject
from papy.util.runtime import get_runtime # provided by worker._inject
PAPY_DEFAULTS = get_defaults() # init by worker._inject
PAPY_RUNTIME = get_runtime()   # init by worker._inject
# worker function imports
import cPickle, json, gc, time, tempfile, errno, os, socket, urllib, stat, \
       signal, mmap, warnings, threading

def plugger(inbox):
    """
    Returns nothing.
    
    """
    return None

def ipasser(inbox, i=0):
    """
    Passes the "i"-th input from inbox. By default passes the first input.

    Arguments:

      - i(``int``) [default: ``0``]
      
    """
    return inbox[i]

def npasser(inbox, n=None):
    """
    Passes "n" first inputs from inbox. By default passes the whole inbox.

    Arguments:

      - n(``int``) [default: ``None``]
      
    """
    return inbox[:n]

def spasser(inbox, s=None):
    """
    Passes inputs with indecies in s. By default passes the whole inbox.

    Arguments:

      - s(sequence) [default: ``None``] The default translates to a range for
        all inputs of the "inbox" i.e. ``range(len(inbox))``
    
    """
    seq = (s or range(len(inbox)))
    return [input_ for i, input_ in enumerate(inbox) if i in seq]

def nzipper(inbox, n=None):
    """
    Zips the "n" first inputs from inbox. By default zips thee whole inbox.

    Arguments:

      - n(``int``) [default: ``None``] The default translates to 
        ``zip(*inbox[:])``
    
    """
    return zip(*inbox[:n])[0]

def szipper(inbox, s=None):
    """
    Zips inputs from inbox with indicies in "s". By default zips the whole 
    inbox (all indices).

    Arguments:

      - s(sequence) [default: ``None``]    
    
    """
    #TODO: test szipper
    return zip(*[input_ for i, input_ in enumerate(inbox) if i in s])

def njoiner(inbox, n=None, join=""):
    """
    String joins and returns the first "n" inputs.
    
    Arguments:

     - n(``int``) [default: ``None``] All elements in the inbox smaller then 
       this number will be joined.
     - join(``str``) [default: ``""``] String which will join the elements of 
       the inbox i.e. ``join.join()``.
    
    """
    return join.join(inbox[:n])

def sjoiner(inbox, s=None, join=""):
    """ 
    String joins input with indices in s.

    Arguments:

      - s(sequence) [default: ``None``] ``tuple`` or ``list`` of indices of the
        elements which will be joined.
      - join(``str``) [default: ``""``] String which will join the elements of 
        the inbox i.e. ``join.join()``.

    """
    return join.join([input_ for i, input_ in enumerate(inbox) if i in s])

#
# LOGGING
# 
def print_(inbox):
    """
    Prints the first element of the inbox.
    """
    print inbox[0]
#
# INPUT/OUTPUT
#
# STREAMS
def dump_stream(inbox, handle, delimiter=None):
    """
    Writes the first element of the inbox to the provided stream (file handle) 
    delimiting the input by the optional delimiter string. Returns the name of
    the file being written. 
    
    Note that only a single process can have access to a file handle open for 
    writing. Therefore this function should only be used by a non-parallel 
    ``Piper``.

    Arguments:

      - handle(``file``) File handle open for writing.
      - delimiter(``str``) [default: ``None``] A string which will seperate the
        written items. e.g: ``"END"`` becomes ``"\\nEND\\n"`` in the output 
        stream. The default is an empty string which means that items will be 
        seperated by a blank line i.e.: ``"\\n\\n"`` (two new-line characters).
        
    """
    handle.write(inbox[0])
    delimiter = '\n%s\n' % (delimiter or '')
    handle.write(delimiter)

def load_stream(handle, delimiter=None):
    """
    Creates a string generator from a stream (file handle) containing data 
    delimited by the delimiter strings. This is a stand-alone function and 
    should be used to feed external data into a pipeline.

    Arguments:

      - hande(``file``) A file handle open for reading.
      - delimiter(``str``) [default: ``None``] The default means that items will
        be separated by two new-line characters i.e.: ``"\\n\\n"``.
    
    """
    delimiter = (delimiter or "") + "\n"
    while True:
        item = []
        while True:
            line = handle.readline()
            if line == "":
                raise StopIteration
            elif line == delimiter:
                if item:
                    break
            elif line != '\n':
                item.append(line)

        yield "".join(item)

@imports(['cPickle'])
def load_pickle_stream(handle):
    """
    Creates an object generator from a stream (file handle) containing data
    in pickles. To be used with the ``dump_pickle_stream``
    
    File handles should not be read by different processes.
    
    Arguments:
    
      - handle(``file``) A file handle open for reading.
    
    """
    while True:
        try:
            yield cPickle.load(handle)
        except EOFError:
            raise StopIteration

def dump_pickle_stream(inbox, handle):
    """
    Writes the first element of the inbox to the provided stream (data handle) 
    as a pickle. To be used with the ``load_pickle_stream`` function.

    Arguments:
    
      - handle(``file``) A file handle open for writing.    
    
    """
    cPickle.dump(inbox[0], handle, -1)

# ITEMS
@imports(['tempfile', 'os', 'errno', 'mmap', 'signal', 'socket', 'urllib', \
          'threading'])
def dump_item(inbox, type='file', prefix=None, suffix=None, dir=None, \
              timeout=320, buffer=None):
    """
    Writes the first element of the inbox as a file of a specified type. The 
    type can be 'file', 'fifo' or 'socket' corresponding to typical  files, 
    named pipes (FIFOs). FIFOs and TCP sockets and are volatile i.e. exists only
    as long as the Python process, which created them. FIFOs are local i.e.
    allow to communicate processes only on the same computer.

    This function returns a semi-random name of the file written. By default 
    creates files and fifos in the default temporary directory. To use named 
    pipes the operating system has to support both forks and fifos (not 
    Windows). Sockets should work on all operating systems.

    This function is useful to efficently communicate parallel ``Pipers`` 
    without the overhead of using queues.

    Arguments:

      - type('file', 'fifo', 'socket') [default: 'file'] Type of the created 
        file-like object.
      - prefix(``str``) [default: ``"tmp_papy_%type%"``] Prefix of the file to 
        be created. Should probably identify the ``Worker`` and ``Piper`` type. 
      - suffix(``str``) [default: ``''``] Suffix of the file to be created. 
        Should probably identify the format of the serialization protocol e.g. 
        ``"pickle"`` or deserialized data e.g. ``"nubox"``.
      - dir(``str``) [default: ``tempfile.gettempdir()``] Directory to safe the
        file to. (can be changed only for types ``"file"`` and ``"fifo"``)
      - timeout(``int``) [default: ``320``] Number of seconds to keep the 
        process at the write-end of the ``"socket"`` or ``"pipe"`` alive.
    
    """
    # get a random filename generator
    names = tempfile._get_candidate_names()
    names.rng.seed() # re-seed rng after the fork
    # try to own the file
    if type in ('file', 'fifo'):
        prefix = prefix or 'tmp_papy_%s' % type
        suffix = suffix or ''
        dir = dir or tempfile.gettempdir()
        while True:
            # create a random file name
            file = prefix + names.next() + suffix#IGNORE:W0622
            if type in ('file', 'fifo'):
                file = os.path.join(dir, file)
                try:
                    if type == 'file':
                        fd = os.open(file, tempfile._bin_openflags, 0600)
                        tempfile._set_cloexec(fd) # ?, but still open
                    elif type == 'fifo':
                        os.mkfifo(file)
                    file = os.path.abspath(file)
                    break
                except OSError, excp:
                    # first try to close the fd
                    try:
                        os.close(fd)
                    except OSError, excp_:
                        if excp_.errno == errno.EBADF:
                            pass
                        # strange error better raise it
                        raise excp_
                    if excp.errno == errno.EEXIST:
                        # file exists try another one
                        continue
                    # all other errors should be raise
                    raise excp

    # the os will create a random socket for us.
    elif type in ('socket',):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # try to bind to a port
        try:
            host = socket.gethostbyaddr(socket.gethostname())[0] # from /etc/hosts
        except socket.gaierror:
            host = urllib.urlopen(PAPY_DEFAULTS['WHATS_MYIP_URL']).read()
        sock.bind(('', 0))           # os-chosen free port on all interfaces 
        port = sock.getsockname()[1] # port of the socket
    else:
        raise ValueError("type: %s not undertood" % type)

    if type == 'file':
        # no forking
        handle = open(file, 'wb')
        os.close(fd) # no need to own a file twice!
        handle.write(inbox[0])
        handle.close() # close handle
        file = (type, file, 0)
    else:
        # forking mode - forks should be waited
        if type == 'fifo':
            pid = os.fork()
            if not pid:
                # we set an alarm for 5min if nothing starts to read 
                # within this time the process gets killed.
                signal.alarm(timeout)
                fd = os.open(file, os.O_EXCL & os.O_CREAT | os.O_WRONLY)
                signal.alarm(0)
                os.write(fd, inbox[0])
                os.close(fd)
                os._exit(0)
            file = (type, file, pid)
        elif type == 'socket':
            sock.listen(1)
            pid = os.fork()
            if not pid:
                # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                signal.alarm(timeout)
                rsock, (rhost, rport) = sock.accept() # blocks until client connects
                signal.alarm(0)
                rsock.sendall(inbox[0])               # returns if all data was sent
                # child closes all sockets and exits
                rsock.close()
                sock.close()
                os._exit(0)
            # parent closes server socket
            sock.close()
            file = (type, host, port)

        # 0. get pid list and methods for atomic operations
        # 1. add the child pid to the pid list
        # 2. try to wait each pid in the list without blocking:
        #    if success remove pid if not ready pass if OSError child not exists
        #    another thread has waited this child.
        # 0.
        tid = threading._get_ident()
        try:
            pids = PAPY_RUNTIME[tid]
        except KeyError:
            PAPY_RUNTIME[tid] = []
            pids = PAPY_RUNTIME[tid]
        add_pid = pids.append   # list methods are atomic
        del_pid = pids.remove
        # 1.
        add_pid(pid)
        # 2. 
        for pid in pids:
            try:
                killed, status = os.waitpid(pid, os.WNOHANG)
                if killed:
                    del_pid(pid)
            except OSError, excp:
                if excp.errno == os.errno.ECHILD:
                    continue
                raise
    # filename needs still to be unlinked
    return file

@imports(['mmap', 'os', 'stat', 'warnings'])
def load_item(inbox, type="string", remove=True, buffer=None):
    """
    Loads data from a file. Determines the file type automatically ``"file"``,
    ``"fifo"``, ``"socket"``, but allows to specify the representation type 
    ``"string"`` or ``"mmap"`` for memory mapped access to the file. Returns 
    the  loaded item as a ``str`` or ``mmap`` object. Internally creates an item
    from a ``file``.

    Arguments:

      - type(``"string"`` or ``"mmap"``) [default: ``"string"``] Determines the
        type of ``object`` the worker returns i.e. the ``file`` is read as a 
        string or a memmory map. FIFOs cannot be memory mapped. 
      - remove(``bool``) [default: ``True``] Should the file be removed from the
        filesystem? This is mandatory for FIFOs and sockets. Only Files can be 
        used to store data persistantly.
    
    """
    is_file, is_fifo, is_socket = False, False, False

    file = inbox[0]
    try:
        file_type = file[0]
    except:
        raise ValueError("invalid inbox item")
    if file_type == "file":
        is_file = os.path.exists(file[1])
    elif file_type == "fifo":
        is_fifo = stat.S_ISFIFO(os.stat(file[1]).st_mode)
    elif file_type == "socket":
        # how to test is valid socket?
        is_socket = True
    else:
        raise ValueError("type: %s not undertood" % file_type)


    if (is_fifo or is_socket) and (type == 'mmap'):
        raise ValueError("mmap is not supported for FIFOs and sockets")
    if (is_fifo or is_socket) and not remove:
        raise ValueError("FIFOs and sockets have to be removed")

    # get a fd and start/stop
    start = 0
    if is_fifo or is_file:
        stop = os.stat(file[1]).st_size - 1
        fd = os.open(file[1], os.O_RDONLY)
        BUFFER = (buffer or PAPY_DEFAULTS['PIPE_BUF'])
    elif is_socket:
        host, port = socket.gethostbyname(file[1]), file[2]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        stop = -1
        fd = sock.fileno()
        BUFFER = (buffer or PAPY_DEFAULTS['TCP_RCVBUF'])
    else:
        raise ValueError("got unknown inbox: %s" % (repr(inbox)))

    # get the data
    if type == 'mmap':
        offset = start - (start % (getattr(mmap, 'ALLOCATIONGRANULARITY', None)\
                                   or getattr(mmap, 'PAGESIZE')))
        start = start - offset
        stop = stop - offset + 1
        try:
            data = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset=offset)
        except TypeError:
            # we're on Python 2.5
            data = mmap.mmap(fd, stop, access=mmap.ACCESS_READ)
        data.seek(start)

    elif type == 'string':
        data = []
        if stop == -1:
            while True:
                buffer_ = os.read(fd, BUFFER)
                if not buffer_:
                    break
                data.append(buffer_)
            data = "".join(data)
            # data = sock.recv(socket.MSG_WAITALL) 
            # this would read all the data from a socket
        else:
            os.lseek(fd, start, 0)
            data = os.read(fd, stop - start + 1)
    else:
        raise ValueError('type: %s not understood.' % type)

    # remove the file or close the socket
    if remove:
        if is_socket:
            # closes client socket
            sock.close()
        else:
            # pipes and files are just removed
            os.close(fd)
            os.unlink(file[1])
    else:
        os.close(fd)
    # returns a string or mmap
    return data


# FILES
@imports(['time'])
def make_lines(handle, follow=False, wait=0.1):
    """ 
    Creates a line generator from a stream (file handle) containing data in
    lines.

    Arguments:

      - follow(``bool``) [default: ``False``] If ``True`` follows the file after
        it finishes like 'tail -f'.
      - wait(``float``) [default: ``0.1``] time to wait in seconds between file
        polls.
    
    """
    while True:
        line = handle.readline()
        if line:
            yield line
        elif follow:
            time.sleep(wait)
        else:
            raise StopIteration

#
# SERIALIZATION
#
# cPickle
@imports(['cPickle', 'gc'])
def pickle_dumps(inbox):
    """
    Serializes the first element of the input using the pickle protocol using
    the fastes binary protocol.
    
    """
    # http://bugs.python.org/issue4074
    gc.disable()
    str_ = cPickle.dumps(inbox[0], cPickle.HIGHEST_PROTOCOL)
    gc.enable()
    return str_

@imports(['cPickle', 'gc'])
def pickle_loads(inbox):
    """
    Deserializes the first element of the input using the pickle protocol.
    
    """
    gc.disable()
    obj = cPickle.loads(inbox[0])
    gc.enable()
    return obj

# JSON
@imports(['json', 'gc'], forgive=True)
def json_dumps(inbox):
    """
    Serializes the first element of the input using the JSON protocol as 
    implemented by the ``json`` Python 2.6 library.
    
    """
    gc.disable()
    str_ = json.dumps(inbox[0])
    gc.enable()
    return str_

@imports(['json', 'gc'], forgive=True)
def json_loads(inbox):
    """
    Deserializes the first element of the input using the JSON protocol as 
    implemented by the ``json`` Python 2.6 library.
    
    """
    gc.disable()
    obj = json.loads(inbox[0])
    gc.enable()
    return obj

