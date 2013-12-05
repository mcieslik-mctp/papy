# -*- coding: utf-8 -*-
"""
:mod:`papy.core`
================

This module provides classes and functions to construct and run a **PaPy** 
pipeline.

"""
#

from types import FunctionType
from inspect import isbuiltin, getsource
from itertools import izip, imap, chain, repeat, tee
from threading import Thread, Event, Lock
from multiprocessing import TimeoutError
from collections import defaultdict
from logging import getLogger
from time import time
import itertools, os, sys

from numap.NuMap import _inject_func, imports, _Weave

# self-imports
from graph import DictGraph
from util.codefile import I_SIG, L_SIG, P_LAY, P_SIG, W_SIG
from util.config import start_logger, get_defaults
from util.runtime import get_runtime


class WorkerError(Exception):
    """
    Exceptions raised or related to ``Worker`` instances.
    
    """
    pass


class PiperError(Exception):
    """
    Exceptions raised or related to ``Piper`` instances.
    
    """
    pass


class DaggerError(Exception):
    """
    Exceptions raised or related to ``Dagger`` instances.
    
    """
    pass


class PlumberError(Exception):
    """
    Exceptions raised or related to ``Plumber`` instances.
    
    """
    pass


class Dagger(DictGraph):
    """
    The ``Dagger`` is a directed acyclic graph. It defines the topology of a 
    ``PaPy`` pipeline / workflow. It is a subclass of ``DictGraph``. 
    ``DictGraph`` edges are called within the ``Dagger`` pipes and have an 
    inverted direction which reflects dataflow not dependency. Edges can be 
    thought of as dependencies, while pipes as dataflow between ``Pipers`` or 
    nodes of the graph.

    Arguments:
    
      - pipers(sequence) [default: ``()``]  A sequence of valid ``add_piper`` 
        inputs (see the documentation for the ``add_piper`` method).
      - pipes(sequence) [default: ``()``] A sequence of valid ``add_pipe`` 
        inputs  (see the documentation for the ``add_piper`` method).
    
    """

    def __init__(self, pipers=(), pipes=(), xtras=None):
        self.log = getLogger('papy')
        self.log.info('Creating %s from %s and %s' % \
                      (repr(self), pipers, pipes))
        self.add_pipers(pipers, xtras)
        self.add_pipes(pipes)


    def __repr__(self):
        """
        A short but unique representation.
        """
        return 'Dagger(%s)' % id(self)

    def __str__(self):
        """
        A long descriptive representation.
        """
        return repr(self) + "\n" + \
               "\tPipers:\n" + \
               "\n".join(('\t\t' + repr(p) + ' ' for p in self.postorder())) + '\n'\
               "\tPipes:\n" + \
               "\n".join(('\t\t' + repr(p[1]) + '>>>' + \
                          repr(p[0]) for p in self.edges()))

    def children_after_parents(self, piper1, piper2):
        """
        Custom compare function. Returns ``1`` if the first ``Piper`` instance 
        is upstream of the second ``Piper`` instance, ``-1`` if the first 
        ``Piper`` is downstream of the second ``Piper`` and ``0`` if the two 
        ``Pipers`` are independent.
        
        Arguments:
        
          - piper1(``Piper``) ``Piper`` instance.
          - piper2(``Piper``) ``Piper`` instance.
          
        """
        if piper1 in self[piper2].deep_nodes():
            return 1
        elif piper2 in self[piper1].deep_nodes():
            return - 1
        else:
            return 0

    def resolve(self, piper, forgive=False):
        """
        Given a ``Piper`` instance or the ``id`` of the ``Piper``. Returns the
        ``Piper`` instance if it can be resolved else raises a ``DaggerError`` 
        or returns ``False`` depending on the "forgive" argument. 
        
        Arguments:
    
        - piper(``Piper`` or id(``Piper``)) a ``Piper`` instance or its id to be
          found in the ``Dagger``. 
        - forgive(``bool``) [default: ``False``] If "forgive" is ``False`` a 
          ``DaggerError`` is raised whenever a  ``Piper`` cannot be resolved in 
          the ``Dagger``. If "forgive" is ``True`` then ``False`` is returned.
          
        """
        try:
            if piper in self:
                resolved = piper
            else:
                resolved = [p for p in self if id(p) == piper][0]
        except (TypeError, IndexError):
            resolved = False
        if resolved:
            self.log.info('%s resolved a piper from %s' % (repr(self), piper))
        else:
            self.log.info('%s could not resolve a piper from %s' % \
                          (repr(self), repr(piper)))
            if not forgive:
                raise DaggerError('%s could not resolve a Piper from %s' % \
                                  (repr(self), repr(piper)))
            resolved = False
        return resolved

    def connect(self, datas=None):
        """
        Connects ``Pipers`` in the order input -> output. See ``Piper.connect``.
        According to the pipes (topology). If "datas" is given will connect the
        input ``Pipers`` to the input data see: ``Dagger.connect_inputs``.
        
        Argumensts:
        
          - datas(sequence) [default: ``None``] valid sequence of input data.
            see: ``Dagger.connect_inputs``.
          
        """
        # if data connect inputs
        if datas:
            self.connect_inputs(datas)
        # connect the remaining pipers
        postorder = self.postorder()
        self.log.info('%s trying to connect in the order %s' % \
                      (repr(self), repr(postorder)))
        for piper in postorder:
            if not piper.connected and self[piper].nodes():
                # 1. sort inputs by index in postorder
                inputs = [p for p in postorder if p in self[piper].nodes()]
                # 2. sort postorder so that all parents come before children
                # mind that the top of a pipeline is a child!
                inputs.sort(cmp=self.children_after_parents)
                # 2. branch age sorted inputs
                piper.connect(inputs)
        self.log.info('%s succesfuly connected' % repr(self))

    def connect_inputs(self, datas):
        """
        Connects input ``Pipers`` to "datas" input data  in the correct order 
        determined, by the ``Piper.ornament`` attribute and the ``Dagger._cmp`` 
        function.

        It is assumed that the input data is in the form of an iterator and
        that all inputs have the same number of input items. A pipeline will
        **deadlock** otherwise. 
        
        Arguments:
        
          - datas (sequence of sequences) An ordered sequence of inputs for 
            all input ``Pipers``.
        
        """
        start_pipers = self.get_inputs()
        self.log.info('%s trying to connect inputs in the order %s' % \
                      (repr(self), repr(start_pipers)))
        for piper, data in izip(start_pipers, datas):
            piper.connect([data])
        self.log.info('%s succesfuly connected inputs' % repr(self))

    def disconnect(self, forced=False):
        """
        Given the pipeline topology disconnects ``Pipers`` in the order output 
        -> input. This also disconnects inputs. See ``Dagger.connect``,
        ``Piper.connect`` and ``Piper.disconnect``. If "forced" is ``True``
        ``NuMap`` instances will be emptied.
        
        Arguments:
        
          - forced(``bool``) [default: ``False``] If set ``True`` all tasks from
            all ``NuMaps`` instances used in the ``Dagger`` will be removed even
            if they did not belong to this ``Dagger``.
        
        """
        reversed_postorder = reversed(self.postorder())
        self.log.info('%s trying to disconnect in the order %s' % \
                      (repr(self), repr(reversed_postorder)))
        for piper in reversed_postorder:
            if piper.connected:
                # we don't want to trigger an exception
                piper.disconnect(forced)
        self.log.info('%s succesfuly disconnected' % repr(self))

    def start(self):
        """
        Given the pipeline topology starts ``Pipers`` in the order input -> 
        output. See ``Piper.start``. ``Pipers`` instances are started in two 
        stages, which allows them to share ``NuMaps``.
        
        """
        # top - > bottom of pipeline
        pipers = self.postorder()
        # 
        for piper in pipers:
            piper.start(stages=(0, 1))
        for piper in pipers:
            piper.start(stages=(2,))

    def stop(self):
        """
        Stops the ``Pipers`` according to pipeline topology.
        
        """
        self.log.info('%s begins stopping routine' % repr(self))
        self.log.info('%s triggers stopping in input pipers' % repr(self))
        inputs = self.get_inputs()
        for piper in inputs:
            piper.stop(forced=True)
        self.log.info('%s pulls output pipers until stop' % repr(self))
        outputs = self.get_outputs()
        while outputs:
            for piper in outputs:
                try:
                    # for i in xrange(stride)?
                    piper.next()
                except StopIteration:
                    outputs.remove(piper)
                    self.log.debug("%s stopped output piper: %s" % \
                                   (repr(self), repr(piper)))
                    continue
                except Exception, excp:
                    self.log.debug("%s %s raised an exception: %s" % \
                                   (repr(self), piper, excp))
        self.log.debug("%s stops the remaining pipers" % repr(self))
        postorder = self.postorder()
        for piper in postorder:
            if piper not in inputs:
                piper.stop(ends=[0])
        self.log.debug("%s finishes stopping of input pipers" % repr(self))
        for piper in inputs:
            if hasattr(piper.imap, 'stop'):
                piper.imap.stop(ends=[0])
        self.log.info('%s finishes stopping routine' % repr(self))

    def get_inputs(self):
        """
        Returns ``Piper`` instances, which are inputs to the pipeline i.e. have 
        no incoming pipes (outgoing dependency edges). 
        
        """
        start_p = [p for p in self.postorder() if not self.outgoing_edges(p)]
        self.log.info('%s got input pipers %s' % (repr(self), start_p))
        return start_p

    def get_outputs(self):
        """
        Returns ``Piper`` instances, which are outputs to the pipeline i.e. have
        no outgoing pipes (incoming dependency edges). 
        
        """
        end_p = [p for p in self.postorder() if not self.incoming_edges(p)]
        self.log.info('%s got output pipers %s' % (repr(self), end_p))
        return end_p

    def add_piper(self, piper, xtra=None, create=True, branch=None):
        """
        Adds a ``Piper`` instance to this ``Dagger``, but only if the ``Piper`` 
        is not already there. Optionally creates a new ``Piper`` if the "piper"
        argument is valid for the ``Piper`` constructor. Returns a ``tuple``
        (new_piper_created, piper_instance) indicating whether a new ``Piper`` 
        has been created and the instance of the added ``Piper``. Optionally 
        takes "branch" and "xtra" arguments for the topological node in the 
        graph.
        
        Arguments:
        
          - piper(``Piper``, ``Worker`` or id(``Piper``)) ``Piper`` instance or 
            object which will be converted to a ``Piper`` instance.
          - create(``bool``) [default: ``True``] Should a new ``Piper`` be 
            created if "piper" cannot be resolved in this ``Dagger``?
          - xtra(``dict``) [default: ``None``] Dictionary of  ``graph.Node``  
            properties.
        
        """
        self.log.info('%s trying to add piper %s' % (repr(self), piper))
        piper = (self.resolve(piper, forgive=True) or piper)
        if not isinstance(piper, Piper):
            if create:
                try:
                    piper = Piper(piper)
                except PiperError:
                    self.log.error('%s cannot resolve or create a piper from %s' % \
                                   (repr(self), repr(piper)))
                    raise DaggerError('%s cannot resolve or create a piper from %s' % \
                                      (repr(self), repr(piper)))
            else:
                self.log.error('%s cannot resolve a piper from %s' % \
                               (repr(self), repr(piper)))
                raise DaggerError('%s cannot resolve a piper from %s' % \
                                  (repr(self), repr(piper)))
        new_piper_created = self.add_node(piper, xtra, branch)
        if new_piper_created:
            self.log.info('%s added piper %s' % (repr(self), piper))
        return (new_piper_created, piper)

    def del_piper(self, piper, forced=False):
        """
        Removes a ``Piper`` from the ``Dagger`` instance.

        Arguments:
        
          - piper(``Piper``  or id(``Piper``)) ``Piper`` instance or ``Piper`` 
            instance id.
          - forced(bool) [default: ``False``] If "forced" is ``True``, will not 
            raise a ``DaggerError`` if the ``Piper`` hase outgoing pipes and 
            will also remove it.
            
        """
        self.log.info('%s trying to delete piper %s' % \
                      (repr(self), repr(piper)))
        try:
            piper = self.resolve(piper, forgive=False)
        except DaggerError:
            self.log.error('%s cannot resolve piper from %s' % \
                           (repr(self), repr(piper)))
            raise DaggerError('%s cannot resolve piper from %s' % \
                              (repr(self), repr(piper)))
        if self.incoming_edges(piper) and not forced:
            self.log.error('%s piper %s has down-stream pipers (use forced =True to override)' % \
                           (repr(self), piper))
            raise DaggerError('%s piper %s has down-stream pipers (use forced =True to override)' % \
                              (repr(self), piper))
        self.del_node(piper)
        self.log.info('%s deleted piper %s' % (repr(self), piper))

    def add_pipe(self, pipe, branch=None):
        """
        Adds a pipe (A, ..., N) which is an N-``tuple`` tuple of ``Pipers`` 
        instances. Adding a pipe means to add all the ``Pipers`` and connect 
        them in the specified left to right order.
        
        The direction of the edges in the ``DictGraph`` is reversed compared to 
        the left to right data-flow in a pipe.

        Arguments:
        
          - pipe(sequence) N-``tuple`` of ``Piper`` instances or objects which 
            are valid  ``add_piper`` arguments. See: ``Dagger.add_piper`` and 
            ``Dagger.resolve``.

        """
        #TODO: Check if consume/spawn/produce is right!
        self.log.info('%s adding pipe: %s' % (repr(self), repr(pipe)))
        for i in xrange(len(pipe) - 1):
            edge = (pipe[i + 1], pipe[i])
            edge = (self.add_piper(edge[0], create=True, branch=branch)[1], \
                    self.add_piper(edge[1], create=True, branch=branch)[1])
            if edge[0] in self.dfs(edge[1], []):
                self.log.error('%s cannot add the %s>>>%s edge (introduces a cycle)' % \
                                (repr(self), edge[0], edge[1]))
                raise DaggerError('%s cannot add the %s>>>%s edge (introduces a cycle)' % \
                                (repr(self), edge[0], edge[1]))
            self.add_edge(edge)
            self.clear_nodes() #dfs
            self.log.info('%s added the %s>>>%s edge' % \
                          (repr(self), edge[0], edge[1]))

    def del_pipe(self, pipe, forced=False):
        """
        Deletes a pipe (A, ..., N) which is an N-``tuple`` of ``Piper`` 
        instances. Deleting a pipe means to delete all the connections between 
        ``Pipers`` and to delete all the ``Pipers``. If "forced" is ``False`` 
        only ``Pipers`` which are not used anymore (i.e. have not downstream 
        ``Pipers``) are deleted.

        The direction of the edges in the ``DictGraph`` is reversed compared to 
        the left to right data-flow in a pipe.

        Arguments:

          - pipe(sequence) N-``tuple`` of ``Piper`` instances or objects which 
            can be resolved in the ``Dagger`` (see: ``Dagger.resolve``). The 
            ``Pipers`` are removed in the order from right to left.
            
          - forced(``bool``) [default: ``False``] The forced argument will be 
            given to the ``Dagger.del_piper`` method. If "forced" is ``False`` 
            only ``Pipers`` with no outgoing pipes will be deleted.

        """
        self.log.info('%s removes pipe%s forced: %s' % \
                      (repr(self), repr(pipe), forced))
        pipe = list(reversed(pipe))
        for i in xrange(len(pipe) - 1):
            edge = (self.resolve(pipe[i]), self.resolve(pipe[i + 1]))
            self.del_edge(edge)
            self.log.info('%s removed the %s>>>%s edge' % \
                          (repr(self), edge[0], edge[1]))
            try:
                self.del_piper(edge[0], forced)
                self.del_piper(edge[1], forced)
            except DaggerError:
                pass

    def add_pipers(self, pipers, *args, **kwargs):
        """
        Adds a sequence of ``Pipers`` instances to the ``Dagger`` in the 
        specified order. Takes optional arguments for ``Dagger.add_piper``.
        
        Arguments:
        
          - pipers(sequence of valid ``add_piper`` arguments) Sequence of 
            ``Pipers`` or valid ``Dagger.add_piper`` arguments to be added to 
            the ``Dagger`` in the left to right order.
            
        """
        for piper in pipers:
            self.add_piper(piper, *args, **kwargs)

    def del_pipers(self, pipers, *args, **kwargs):
        """
        Deletes a sequence of ``Pipers`` instances from the ``Dagger`` in the
        reverse of the specified order. Takes optional arguments for 
        ``Dagger.del_piper``.
        
        Arguments:
        
          - pipers (sequence of valid ``del_piper`` arguments) Sequence of 
            ``Pipers`` or valid ``Dagger.del_piper`` arguments to be removed 
            from the ``Dagger`` in the right to left order.

        """
        pipers.reverse()
        for piper in pipers:
            self.del_piper(piper, *args, **kwargs)

    def add_pipes(self, pipes, *args, **kwargs):
        """
        Adds a sequence of pipes to the ``Dagger`` in the specified order. 
        Takes optional arguments for ``Dagger.add_pipe``.
        
        Arguments:
        
          - pipes(sequence of valid ``add_pipe`` arguments) Sequence of pipes 
            or other valid ``Dagger.add_pipe`` arguments to be added to the 
            ``Dagger`` in the left to right order.
           
        """

        for pipe in pipes:
            self.add_pipe(pipe, *args, **kwargs)

    def del_pipes(self, pipes, *args, **kwargs):
        """
        Deletes a sequence of pipes from the ``Dagger`` in the specified order. 
        Takes optional arguments for ``Dagger.del_pipe``.
        
        Arguments:
        
          - pipes(sequence of valid ``del_pipe`` arguments) Sequence of pipes or 
            other valid ``Dagger.del_pipe`` arguments to be removed from the 
            ``Dagger`` in the left to right order.
        
        """
        for pipe in pipes:
            self.del_pipe(pipe * args, **kwargs)


class Plumber(Dagger):
    """
    The ``Plumber`` is a subclass of ``Dagger`` and ``Graph`` with added 
    run-time methods and a high-level interface for working with ``PaPy`` 
    pipelines. 
    
    Arguments:
    
      - dagger(``Dagger`` instance) [default: ``None``] An optional ``Dagger`` 
        instance. if ``None`` a new one is created.
    
    """

    def _finish(self, ispausing):
        """
        (internal) executes when last output piper raises ``StopIteration``.
        """
        if ispausing:
            self.log.info('%s paused' % repr(self))
        else:
            self._finished.set()
            self.log.info('%s finished' % repr(self))

    @staticmethod
    def _plunge(tasks, pausing, finish):
        """
        (internal) calls the next method of weaved tasks until they are finished
        or The ``Plumber`` instance is stopped see: ``Dagger.chinkup``.
        """
        # If no result received either not started or start & stop
        # could have been called before the plunger thread
        while True:
            if pausing():
                tasks.stop()
            try:
                tasks.next()
            except StopIteration:
                finish(pausing())
                break

    def __init__(self, logger_options={}, **kwargs):
        self._started = Event() # after start till stop
        self._running = Event() # after run till pause
        self._pausing = Event() # during pause
        self._finished = Event() # after finishing the input

        start_logger(**logger_options)
        self.log = getLogger('papy')

        # init
        self.filename = None
        #TODO: check if this works with and the stats attributes are correctly
        # set for a predefined dagger.
        Dagger.__init__(self, **kwargs)

    def _code(self):
        """
        (internal) generates imports, code and runtime calls to save a pipeline.
        
        """
        icode, tcode = '', '' # imports, task code
        icall, pcall = '', '' # imap calls, piper calls
        tdone, idone = [], [] # task done, imap done

        for piper in self:
            p = piper
            w = piper.worker
            i = piper.imap
            in_ = i.name if hasattr(i, 'name') else False
            if in_ and in_ not in idone:
                icall += I_SIG % (in_, i.worker_type, i.worker_num, i.stride, \
                                  i.buffer, i.ordered, i.skip, in_)
                idone.append(in_)
            ws = W_SIG % (",".join([t.__name__ for t in w.task]), w.args, w.kwargs)
            pcall += P_SIG % (p.name, ws, in_, p.consume, p.produce, p.spawn, \
                              p.timeout, p.branch, p.debug, p.name, p.track)
            for t in w.task:
                if (t in tdone) or not t:
                    continue
                tm, tn = t.__module__, t.__name__
                if (tm == '__builtin__') or hasattr(p, tn):
                    continue
                if tm == '__main__' or tm == self.filename:
                    tcode += getsource(t)
                else:
                    icode += 'from %s import %s\n' % (tm, tn)
                tdone.append(t)

        pipers = [p.name for p in self]
        pipers = '[%s]' % ", ".join(pipers)
        pipes = [L_SIG % (d.name, s.name) for s, d in self.edges()]
        pipes = '[%s]' % ", ".join(pipes)                           # pipes
        xtras = [str(self[p].xtra) for p in self]
        xtras = '[%s]' % ",".join(xtras)                            # node xtra
        return (icode, tcode, icall, pcall, pipers, xtras, pipes)

    def __repr__(self):
        """
        A short but unique representation.
        
        """
        return "Plumber(%s)" % super(Plumber, self).__repr__()

    def __str__(self):
        """
        A long descriptive representation.
        
        """
        return super(Plumber, self).__str__()

    def save(self, filename):
        """
        Saves pipeline as a Python source code file.
        
        Arguments:
        
          - filename(``path``) Path to save the pipeline source code.
          
        """
        handle = open(filename, 'wb')
        handle.write(P_LAY % self._code())
        handle.close()

    def load(self, filename):
        """
        Instanciates (loads) pipeline from a source code file.
        
        Arguments:
        
          - filename(``path``) location of the pipeline source code.
          
        """
        dir_name = os.path.dirname(filename)
        mod_name = os.path.basename(filename).split('.')[0]
        self.filename = mod_name
        sys.path.insert(0, dir_name)
        mod = __import__(mod_name)
        sys.path.remove(dir_name) # do not pollute the path.
        pipers, xtras, pipes = mod.pipeline()
        self.add_pipers(pipers, xtras)
        self.add_pipes(pipes)

    def start(self, datas):
        """
        Starts the pipeline by connecting the input ``Pipers`` of the pipeline 
        to the input data, connecting the pipeline and starting the ``NuMap``
        instances.
        
        The order of items in the "datas" argument sequence should correspond 
        to the order of the input ``Pipers`` defined by ``Dagger._cmp`` and 
        ``Piper.ornament``.

        Arguments:
        
          - datas(sequence) A sequence of external input data in the form of 
            sequences or iterators.

        """
        if not self._started.isSet() and \
           not self._running.isSet() and \
           not self._pausing.isSet():
            # Plumber statistics
            self.stats = {}
            self.stats['start_time'] = None
            self.stats['run_time'] = None
            # connects input pipers to external data
            self.connect_inputs(datas)
            # connects pipers within the pipeline
            self.connect()
            # make pointers to results collected for pipers by imaps
            self.stats['pipers_tracked'] = {}
            for piper in self.postorder():
                if hasattr(piper.imap, '_tasks_tracked') and piper.track:
                    self.stats['pipers_tracked'][piper] = \
                    [piper.imap._tasks_tracked[t.task] for t in piper.imap_tasks]

            self.stats['start_time'] = time()
            # starts the Dagger
            # this starts Pipers and NuMaps
            super(Plumber, self).start()
            # transitioning to started state
            self._started.set()
            self._finished.clear()
        else:
            raise PlumberError

    def run(self):
        """
        Executes a started pipeline by pulling results from it's output 
        ``Pipers``. Processing nodes i.e. ``Pipers`` with the ``track`` 
        attribute set ``True`` will have their returned results stored within 
        the ``Dagger.stats['pipers_tracked']`` dictionary. A running pipeline 
        can be paused.
        
        """
        # remove non-block results for end tasks
        if self._started.isSet() and \
           not self._running.isSet() and \
           not self._pausing.isSet() and \
           not self._finished.isSet():
            stride = 1 # FIXME
            tasks = self.get_outputs()
            wtasks = _Weave(tasks, repeats=stride)
            self._plunger = Thread(target=self._plunge, args=(wtasks, \
                            self._pausing.isSet, self._finish))
            self._plunger.deamon = True
            self._plunger.start()
            self._running.set()
        else:
            raise PlumberError

    def wait(self, timeout=None):
        """
        Waits (blocks) until a running pipeline finishes.
        
        Arguments:
        
          - timeout(``int``) [default: ``None``] Specifies the timeout, 
            ``RuntimeError`` will be raised. The default is to wait indefinetely
            for the pipeline to finish.

        """
        if self._started.isSet() and \
           self._running.isSet() and \
           not self._pausing.isSet():
            self._finished.wait(timeout)
        else:
            raise PlumberError

    def pause(self):
        """
        Pauses a running pipeline. This will stop retrieving results from the 
        pipeline. Parallel parts of the pipeline will stop after the ``NuMap`` 
        buffer is has been filled. A paused pipeline can be run or stopped. 
        
        """
        # 1. stop the plumbing thread by raising a StopIteration on a stride 
        #    boundary
        if self._started.isSet() and \
           self._running.isSet() and \
           not self._pausing.isSet():
            self._pausing.set()
            self._plunger.join()
            del self._plunger
            self._pausing.clear()
            self._running.clear()
        else:
            raise PlumberError

    def stop(self):
        """
        Stops a paused pipeline. This will a trigger a ``StopIteration`` in the
        inputs of the pipeline. And retrieve the buffered results. This will
        stop all ``Pipers`` and ``NuMaps``. Python will not terminate cleanly 
        if a pipeline is running or paused.
        
        """
        if self._started.isSet() and \
           not self._running.isSet() and \
           not self._pausing.isSet():
            # stops the dagger
            super(Plumber, self).stop()
            # disconnects all pipers
            self.disconnect()
            self.stats['run_time'] = time() - self.stats['start_time']
            self._started.clear()
        else:
            raise PlumberError
#
#
class Piper(object):
    """
    Creates a new ``Piper`` instance. The ``Piper`` is an ``object`` that acts
    a a processing node in a PaPy pipeline. 
    
    A ``Piper`` can be created from a ``Worker`` instance another ``Piper`` 
    instance or a sequence of functions or ``Worker`` instances in every case a 
    new ``Piper`` instance is created.
    
    ``Piper`` instances evaluate functions in parallel if they are created 
    with a ``NuMap`` instance provided otherwise they use the ``itertools.imap`` 
    function.

    The "produce" and "consume" arguments allow for different than 1:1 mappings
    between the number of input and output items, while "spawn" allows 
    accomodate a ``Piper`` to handle additional outputs. Additional outputs are
    created from the elements of the sequence returned by the wrapped ``Worker``
    instance.
    
    The product of "produce" and "spawn" of the upstream ``Piper`` has to equal 
    the product of "consume" and "spawn" of the downstream ``Piper``, for 
    **each** pair of pipers connected.
    
    The "branch" argument sets the "branch" attribute of a ``Piper`` instance. 
    If two ``Pipers`` have no upstream->downstream relation they will be sorted 
    according to their "branch" attributes. If neither of them has a "branch"
    attribute or both are identical their sort order will be semi-random. 
    ``Pipers`` will implicitly inherit the "branch" of an up-stream ``Piper``, 
    thus it is only necessary to sepcify the branch of a ``Piper`` if it is the
    first one after a branch point.
    
    It is possible to construct pipelines without specifying branches if 
    ``Pipers`` which are connected to multiple up-stream ``Pipers`` 
    (the order of which is by default semi-random) use ``Workers`` that act 
    correctly regardless of the order of results in their inbox.
    
    If "debug" is ``True`` exceptions are raised on all errors. This will most 
    likely hang the Python interpreter after the error occurs. Use during 
    development only!
       
    Arguments:

      - worker(``Worker``, ``Piper`` or sequence of functions or``Workers``)
      - parallel(``False`` or ``NuMap``) [default: ``False``] If parallel is 
        ``False`` the ``Piper`` instance will not process data-items in parallel
      - consume(``int``) [default: ``1``] The number of input items consumed 
        from **all** directly connected upstream ``Pipers`` per one evaluation.
      - produce(``int``) [default: 1] The number of results to generate for each  
        evaluation result. 
      - spawn(``int``) [default: 1] The number of times this `Piper`` is 
        implicitly  added to the pipeline to consume the specified number of 
        results.
      - timeout(``int``) [default: ``None``] Time to wait till a result is 
        available. Otherwise a ``PiperError`` is **returned** not raised.
      - branch(``object``) [default: ``None``] This affects the order of 
        ``Pipers`` in the ``Dagger``. ``Piper`` instances are sorted according 
        to the data-flow upstream->downstream and their "branch" attributes.
        The argument can be any object which can be used by the ``cmp`` 
        built-in function. If necessary they can override the ``__cmp__``  
        method. 
      - debug(``bool``) [default: ``False``] Verbose debugging mode. Raises a 
        ``PiperError`` on ``WorkerErrors``.
      - name(``str``) [default: ``None``] A string to identify the ``Piper``.
      - track(``bool``) [default: ``False``] If ``True`` results of this 
        ``Piper`` will be tracked by the ``NuMap`` (ignored if ``Piper`` is 
        linear).
      - repeat(``bool``) [default: ``False``] If ``True``and "produce" is 
        larger than ``1`` the evaluated results will be repeated. If ``False``
        it assumes that the evaluated results are sequences and produce will
        iterate over that ``list`` or ``tuple``. 

    """
    @staticmethod
    def _cmp(x, y):
        """
        (internal) compares pipers by ornament.
        
        """
        return cmp(x.ornament, y.ornament)

    def __init__(self, worker, parallel=False, consume=1, produce=1, \
                 spawn=1, timeout=None, branch=None, debug=False, \
                 name=None, track=False, repeat=False):
        self.inbox = None
        self.outbox = None
        self.connected = False
        self.started = False
        self.finished = False
        self.imap_tasks = []

        self.consume = consume
        self.spawn = spawn
        self.produce = produce
        self.timeout = timeout
        self.debug = debug
        self.track = track
        self.repeat = repeat

        self.tee_locks = [Lock()]
        self.tee_num = 0
        self.tees = []

        self.log = getLogger('papy')
        self.log.info('Creating a new Piper from %s' % repr(worker))

        self.imap = parallel if parallel else imap # this is itetools.imap

        #self.cmp = cmp if cmp else None
        self.branch = (None or branch)

        is_p, is_w, is_f, is_ip, is_iw, is_if = _inspect(worker)
        if is_p:
            self.worker = worker.worker
        elif is_w:
            self.worker = worker
        elif is_f or is_if or is_iw:
            self.log.info('Creating new worker from %s' % worker)
            try:
                self.worker = Worker(worker)
                self.log.info('Created a new worker from %s' % worker)
            except Exception, excp:
                self.log.error('Could not create a new Worker from %s' % \
                                worker)
                raise PiperError('Could not create a new Worker from %s' % \
                                 worker, excp)
        else:
            self.log.error('Do not know how to create a Piper from %s' % \
                           repr(worker))
            raise PiperError('Do not know how to create a Piper from %s' % \
                             repr(worker))

        # initially return self by __iter__
        self._iter = self
        self.name = name or "piper_%s" % id(self)
        self.log.info('Created Piper %s' % self)

    def __iter__(self):
        """
        (internal) returns a ``Piper._iter``, which should be overwritten
        after each ``itertools.tee``.
        
        """
        return self._iter

    def __repr__(self):
        return "%s(%s)" % (self.name, repr(self.worker))

    def start(self, stages=None):
        """
        Makes the ``Piper`` ready to return results. This involves starting the 
        the provided ``NuMap`` instance. If multiple ``Pipers`` share a 
        ``NuMap`` instance the order in which these ``Pipers`` are started is 
        important. The valid order is upstream before downstream. The ``NuMap`` 
        instance can only be started once, but the process can be done in 2 
        stages. This methods "stages" argument is a ``tuple`` which can contain
        any the numbers ``0`` and/or ``1`` and/or  ``2`` specifying which stage
        of the start routine should be carried out:
        
          - stage 0 - creates the needed ``itertools.tee`` objects.
          - stage 1 - activates ``NuMap`` pool. A call to ``next`` will block..
          - stage 2 - activates ``NuMap`` pool managers.
        
        If this ``Piper`` shares a ``NuMap`` with other ``Pipers`` the proper 
        way to start them is to start them in a valid postorder with stages 
        ``(0, 1)`` and ``(2,)`` separately. 
           
        Arguments:
        
          - stages(tuple) [default: ``(0,)`` if linear; ``(0,1,2)`` if parallel]
            Performs the specified stages of the start of a ``Piper`` instance. 
            Stage ``0`` is necessary and sufficient to start a linear ``Piper`` 
            which uses an ``itertools.imap``. Stages ``1`` and ``2`` are 
            required to start any parallel ``Piper`` instance.
            
        """
        # defaults differ linear vs. parallel
        stages = stages or ((0,) if self.imap is imap else (0, 1, 2))
        if not self.connected:
            self.log.error('Piper %s is not connected.' % self)
            raise PiperError('Piper %s is not connected.' % self)

        if not self.started:
            if 0 in stages:
                self.tees.extend(tee(self, self.tee_num))

        if hasattr(self.imap, 'start'):
            # parallel piper
            self.imap.start(stages)
            if 2 in stages:
                self.log.info('Piper %s has been started using %s' % \
                              (self, self.imap))
                self.started = True
        else:
            # linear piper
            self.log.info('Piper %s has been started using %s' % \
                          (self, self.imap))
            self.started = True


    def connect(self, inbox):
        """
        Connects the ``Piper`` instance to its upstream ``Pipers`` that should
        be given as a sequence. This connects this ``Piper.inbox`` with the
        upstream ``Piper.outbox`` respecting any "consume", "spawn" and 
        "produce" arguments. 
        
        Arguments:
        
          - inbox(sequence) sequence of ``Piper`` instances.
        
        """
        if self.started:
            self.log.error('Piper %s is started and cannot connect to %s.' % \
                           (self, inbox))
            raise PiperError('Piper %s is started and cannot connect to %s.' % \
                             (self, inbox))
        elif self.connected:
            self.log.error('Piper %s is connected and cannot connect to %s.' % \
                           (self, inbox))
            raise PiperError('Piper %s is connected and cannot connect to %s.' % \
                             (self, inbox))
        elif hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s cannot connect (NuMap is started).' % \
                           self)
            raise PiperError('Piper %s cannot connect (NuMap is started).' % \
                           self)
        else:
            # not started and not connected and NuMap not started
            self.log.info('Piper %s connects to %s' % (self, inbox))
            # determine the stride with which result will be consumed from the
            # input.
            stride = self.imap.stride if hasattr(self.imap, 'stride') else 1

            # Tee input iterators. The idea is to create a promise object for a
            # tee. The actual teed iterator will be created on start. Each tee
            # is protected with a seperate lock the reasons for this are:
            # - tee objects are as a collection not thread safe
            # - tee objects might be next'ed from different threads, a single
            #   lock will not guarantee that a thread might be allowed to finish
            #   it's stride. (How it works that a thread releases the next 
            #   thread only if it finished a stride
            teed = []
            for piper in inbox:
                if hasattr(piper, '_iter'): # isinstance Piper?
                    piper.tee_num += 1
                    tee_lock = Lock()
                    tee_lock.acquire()
                    piper.tee_locks.append(tee_lock)
                    piper = _TeePiper(piper, piper.tee_num - 1, stride)
                teed.append(_InputIterator(piper, self))

            # set how much to consume from input iterators.
            self.inbox = _Zip(*teed) if self.consume == 1 else\
                 _Consume(_Zip(*teed), n=self.consume, stride=stride)

            # set how much to 
            for i in xrange(self.spawn):
                self.imap_tasks.append(\
                    self.imap(self.worker, self.inbox) \
                        if self.imap is imap else \
                    self.imap(self.worker, self.inbox, timeout=self.timeout, \
                              track=self.track))
            # chain the results together.
            outbox = _Chain(self.imap_tasks, stride=stride)
            # Make output
            #prd = ProduceFromSequence if self.produce_from_sequence else Produce
            if self.produce == 1:
                self.outbox = outbox
            elif self.repeat:
                self.outbox = _Repeat(outbox, n=self.produce, stride=stride)
            else:
                self.outbox = _Produce(outbox, n=self.produce, stride=stride)
            self.connected = True

        return self # this is for __call__

    def stop(self, forced=False, **kwargs):
        """
        Attempts to cleanly stop the ``Piper`` instance. A ``Piper`` is 
        "started" if its  ``NuMap`` instance is "started". Non-parallel 
        ``Pipers`` do not have to be started or stopped. An ``NuMap`` instance
        can be stopped by triggering its stopping procedure and retrieving 
        results from the ``NuMaps`` end tasks. Because neither the ``Piper`` nor
        the ``NuMap`` "knows" which tasks i.e. ``Pipers`` are the end tasks 
        they have to be specified::
        
            end_task_ids = [0, 1]    # A list of NuMap task ids
            piper_instance.stop(ends =end_task_ids)        
        
        results in::
        
            NuMap_instance.stop(ends =[0,1])
            
        If the ``Piper`` did not finish processing the data before the 
        stop method is called the "forced" argument has to be ``True``::
        
            piper_instance.stop(forced =True, ends =end_task_ids)
            
        If the ``Piper`` (and consequently ``NuMap``) is part of a ``Dagger`` 
        graph the ``Dagger.stop`` method should be called instead. See: 
        ``NuMap.stop`` and ``Dagger.stop``.
        
        # verify this:
        # If "forced" is set ``True`` but the ends ``NuMap`` argument is not
        # given. The ``NuMap`` instance will not try to retrieve any results and
        # will not call the ``NuMap._stop`` method.
        
        Arguments:
        
          - forced(``bool``) [default: ``False``] The ``Piper`` will be forced 
            to stop the ``NuMap`` instance.
            
        Additional keyworded arguments are passed to the ``Piper.imap`` 
        instance.
            
        """
        # ends valid only if forced specified.
        if not self.started:
            self.log.error('Piper %s has not yet been started.' % self)
            raise PiperError('Piper %s has not yet been started.' % self)
        elif not self.finished and not forced:
            msg = 'Piper %s has not finished. Use forced =True' % self
            self.log.error(msg)
            raise PiperError(msg)
        else:
            # started and (finished or forced)
            if hasattr(self.imap, 'stop'):
                self.imap.stop(forced=forced, **kwargs)
            self.started = False
            self.log.info('Piper %s stops (finished: %s)' % \
                          (self, self.finished))


    def disconnect(self, forced=False):
        """
        Disconnects the ``Piper`` instance from its upstream ``Pipers`` or 
        input data if the ``Piper`` is the input node of a pipeline.
        
        Arguments:
        
          - forced(``bool``) [default: ``False``] If ``True`` the ``Piper`` will
            try to forcefully remove all tasks (including the spawned ones) from
            the ``NuMap`` instance.
             
        """
        if not self.connected:
            self.log.error('Piper %s is not connected and cannot be disconnected' % self)
            raise PiperError('Piper %s is not connected and cannot be disconnected' % self)
        elif self.started:
            self.log.error('Piper %s is started and cannot be disconnected (stop first)' % self)
            raise PiperError('Piper %s is started and cannot be disconnected (stop first)' % self)
        elif hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s cannot disconnect as its NuMap is started' % self)
            raise PiperError('Piper %s cannot disconnect as its NuMap is started' % self)
        else:
            # connected and not started
            if hasattr(self.imap, '_started'):
                if self.imap._tasks == []:
                    # fully stopped
                    pass
                elif self.imap_tasks[-1].task == len(self.imap._tasks) - 1:
                    # the last task of this piper is the last task in the NuMap
                    self.imap.pop_task(number=self.spawn)
                elif forced:
                    # removes all tasks from the NuMap can be called multiple 
                    # times.
                    self.imap.pop_task(number=True)
                else:
                    msg = 'Piper %s is not the last Piper added to the NuMap' % \
                            self
                    self.log.error(msg)
                    raise PiperError(msg)
            self.log.info('Piper %s disconnected from %s' % (self, self.inbox))
            self.imap_tasks = []
            self.inbox = None
            self.outbox = None
            self.connected = False

    def __call__(self, *args, **kwargs):
        """
        This is just a convenience mapping to the ``Piper.connect`` method.
        """
        return self.connect(*args, **kwargs)

    def next(self):
        """
        Returns the next result. If no result is availble within the specified 
        (during construction) "timeout" then a ``PiperError`` which wraps a  
        ``TimeoutError`` is **returned**.

        If the result is a ``WorkerError`` it is also wrapped in a 
        ``PiperError`` and is returned or raised if "debug" mode was specified 
        at initialization. If the result is a ``PiperError`` it is propagated.
        
        """
        try:
            next = self.outbox.next()
        except StopIteration, excp:
            self.log.info('Piper %s has processed all jobs (finished)' % self)
            self.finished = True
            # We re-raise StopIteration as part of the iterator protocol.
            # And the outbox should do the same.
            raise excp
        except (AttributeError, RuntimeError), excp:
            # probably self.outbox.next() is self.None.next()
            self.log.error('Piper %s has not yet been started.' % self)
            raise PiperError('Piper %s has not yet been started.' % self, excp)
        except IndexError, excp:
            # probably started before connected
            self.log.error('Piper %s has been started before connect.' % self)
            raise PiperError('Piper %s has been started before connect.' % self, excp)
        except TimeoutError, excp:
            self.log.error('Piper %s timed out waited %ss.' % \
                           (self, self.timeout))
            next = PiperError(excp)
            # we do not raise TimeoutErrors so they can be skipped.
        if isinstance(next, WorkerError):
            # return the WorkerError instance returned (not raised) by the
            # worker Process.
            self.log.error('Piper %s generated %s"%s" in func. %s on argument %s' % \
                     (self, type(next[0]), next[0], next[1], next[2]))
            if self.debug:
                # This makes only sense if you are debugging a piper as it will 
                # most probably crash papy and python NuMap worker processes 
                # threads will hang.
                raise PiperError('Piper %s generated %s"%s" in func %s on argument %s' % \
                            (self, type(next[0]), next[0], next[1], next[2]))
            next = PiperError(next)
        elif isinstance(next, PiperError):
            # Worker/PiperErrors are wrapped by workers
            if self.debug:
                raise next
            self.log.info('Piper %s propagates %s' % (self, next[0]))
        return next


class Worker(object):
    """
    The ``Worker`` is an ``object`` that composes sequences of functions. When 
    called these functions are evaluated from left to right. The function on the
    right will receive the return value from the function on the left. 
    
    The constructor takes optionally sequences of positional and keyworded 
    arguments for none or all of the composed functions. Positional arguments 
    should be given in a ``tuple``. Each element of this ``tuple`` should be a 
    ``tuple`` of positional arguments for the corresponding function. If a 
    function does not take positional arguments its corresponding element in the
    arguments ``tuple`` should be an empty ``tuple`` i.e. ``()``. Keyworded 
    arguments should also be given in a ``tuple``. Each element of this 
    ``tuple`` should be a dictionary of arguments for the corresponding 
    function. If a function does not take any keyworded arguments
    its corresponding element in the keyworded arguments ``tuple`` should be an 
    empty ``dict`` i.e. ``{}``. If none of the functions takes arguments of a 
    given type the positional and/or keyworded arguments ``tuple`` can be 
    omitted.
    
    All exceptions raised by the functions are caught, wrapped and returned 
    **not** raised. If the ``Worker`` is called with the first argument being a
    sequence which contains an ``Exception`` no function is evaluated and the 
    ``Exception`` is re-wrapped and returned.

    A ``Worker`` instance can be constructed in a variety of ways:
    
      - with a sequence of functions and a optional sequences of positional and
        keyworded arguments e.g.::
        
          Worker((func1,         func2,    func3), 
                ((arg11, arg21), (arg21,), ()),
                ({},             {},       {'arg31':arg31}))
        
      - with another ``Worker`` instance, which results in their functional 
        equivalence e.g.::
        
          Worker(worker_instance)
        
      - with multiple ``Worker`` instances, where the functions and arguments of
        the ``Workers`` are combined e.g.::
        
          Worker((worker1, worker2))
        
        this is equivalent to::
        
          Worker(worker1.task + worker2.task, \
                 worker1.args + worker2.args, \
                 worker1.kwargs + worker2.kwargs)
        
      - with a single function and its arguments in a tuple e.g.::
        
          Worker(function, (arg1, arg2, arg3))
        
        this is equivalent to::
        
          Worker((function,),((arg1, arg2, arg3),))
          
    """
    def __init__(self, functions, arguments=None, kwargs=None, name=None):
        is_p, is_w, is_f, is_ip, is_iw, is_if = _inspect(functions)
        if is_f:
            self.task = (functions,)
            if arguments is not None:
                self.args = (arguments,)
            else:
                self.args = ((),)
            if kwargs is not None:
                self.kwargs = (kwargs,)
            else:
                self.kwargs = ({},)
        elif is_w: # copy from other
            self.task = functions.task
            self.args = functions.args
            self.kwargs = functions.kwargs
        elif is_if:
            self.task = tuple(functions)
            if arguments is not None:
                self.args = arguments
            else:
                self.args = tuple([() for i in self.task])
            if kwargs is not None:
                self.kwargs = kwargs
            else:
                self.kwargs = tuple([{} for i in self.task])
        elif is_iw:
            self.task = tuple(chain(*[w.task for w in functions]))
            self.args = tuple(chain(*[w.args for w in functions]))
            self.kwargs = tuple(chain(*[w.kwargs for w in functions]))
        else:
            # e.g. is piper
            raise TypeError("The Worker expects an iterable of functions or" + \
                            " workers got: %s" % functions)
        if len(self.task) != len(self.args) or len(self.task) != len(self.args):
            raise TypeError("The Worker expects the arguents as ((args1) " + \
                            "... argsN)) and keyword arguments as " + \
                            "({kwargs}, ... ,{kwargs.}) got: %s" % \
                            repr(arguments))
        # for representation
        self.__name__ = ">".join([f.__name__ for f in self.task]) or name
        # for identification
        self.name = "%s_%s" % (self.__name__, id(self))

    def __repr__(self):
        """
        Functions within a worker e.g. (f, g, h) are evaluated from left to 
        right i.e.: h(g(f(x))) thus their representation f>g>h.
        
        """
        return "%s(%s)" % (self.__name__, id(self))

    def __hash__(self):
        """
        ``Worker`` instances are not hashable.
        """
        raise TypeError('Worker instances are not hashable')

    def __eq__(self, other):
        """
        Custom ``Worker`` equality comparison. ``Worker`` instances are 
        functionally equivalent if they evaluate the same functions, in the same
        order and have the same positional and keyworded arguments. Two 
        different ``Worker`` instances (objects with different ids) can be 
        equivalent if their functions have been initialized with the same 
        arguments.
        
        """
        return  (self.task == getattr(other, 'task', None) and
                 self.args == getattr(other, 'args', None) and
                 self.kwargs == getattr(other, 'kwargs', None))

    def _inject(self, conn):
        """
        (internal) inject/replace all functions into a rpyc connection object.
        
        """
        # provide PAPY_DEFAULTS remotely
        # provide PAPY_RUNTIME remotely
        if not 'PAPY_INJECTED' in conn.namespace:
            _inject_func(get_defaults, conn)
            _inject_func(get_runtime, conn)
            conn.execute('PAPY_DEFAULTS = get_defaults()')
            conn.execute('PAPY_RUNTIME = get_runtime()')
            conn.execute('PAPY_INJECTED = True')
        # inject all functions
        for func in self.task:
            _inject_func(func, conn)
        # create list of functions called TASK
        # and inject a function comp_task which 
        _inject_func(_comp_task, conn)
        conn.execute('TASK = %s' % \
                   str(tuple([i.__name__ for i in self.task])).replace("'", ""))
                    # ['func1', 'func2'] -> "(func1, func2)"
        # inject compose function, will ...
        self.task = [conn.namespace['_comp_task']]
        self.args = [[self.args]]
        self.kwargs = [{'kwargs':self.kwargs}]
        # instead of multiple remote back and the combined functions is
        # evaluated remotely.
        return self

    def __call__(self, inbox):
        """
        Evaluates the function(s) and argument(s) with which the ``Worker`` 
        instance has been initialized given the input data i.e. "inbox".

        If an ``Exception`` is raised by the function the ``Worker`` returns a 
        ``WorkerError`` instance. Typically a raised ``WorkerError`` should be 
        wrapped into a ``PiperError`` by the ``Piper`` instance which wraps this
        ``Worker`` instance. If any of the data in the "inbox" is a 
        ``PiperError`` instance then no function is called and the ``Worker`` 
        instance propagates the ``Exception`` (i.e. ``PiperError`` instance) to
        its ``Piper`` instance. The originial ``Exception`` travels along as the
        first argument of the innermost ``Exception``.

        Arguments:

          - inbox(sequence) A sequence of items to be evaluated by the 
            function ``f(sequence)`` is ``f((data1, data2, ..., data2))``. 
            
        """
        outbox = inbox # we save the input to raise a better exception.
        exceptions = [e for e in inbox if isinstance(e, PiperError)]
        if not exceptions:
            # upstream did not raise exception, running functions
            try:
                for func, args, kwargs in \
                zip(self.task, self.args, self.kwargs):
                    outbox = (func(outbox, *args, **kwargs),)
                outbox = outbox[0]
            except Exception, excp:
                # an exception occured in one of the f's do not raise it
                # instead return it.
                outbox = WorkerError(excp, func.__name__, inbox)
        else:
            # if any of the inputs is a PiperError just propagate it.
            outbox = PiperError(*exceptions)
        return outbox


def _inspect(instance):
    """
    (internal) Determines the type of a given ``object``. Discriminates between 
    ``Piper``, ``Worker``, ``FunctionType`` and  ``Iterable`` instances. It 
    returns  a  ``tuple`` of boolean variables i.e: (is_piper, is_worker, 
    is_function,  is_iterable_of_pipers, is_iterable_of_workers, 
    is_iterable_of_functions).
    
    """
    is_piper = isinstance(instance, Piper)
    is_function = isinstance(instance, FunctionType) or isbuiltin(instance)
    is_worker = isinstance(instance, Worker)
    is_iterable = getattr(instance, '__iter__', False) and not \
                 (is_piper or is_function or is_worker)
    is_iterable_p = is_iterable and isinstance(instance, Piper)
    is_iterable_f = is_iterable and (isinstance(instance[0], FunctionType) or \
                                     isbuiltin(instance[0]))
    is_iterable_w = is_iterable and isinstance(instance[0], Worker)
    return (is_piper, is_worker, is_function, is_iterable_p, is_iterable_w, \
            is_iterable_f)

@imports(['itertools'])
def _comp_task(inbox, args, kwargs):
    """
    (internal) Composes a sequence of functions in the global variable TASK. The
    resulting composition is given the input "inbox" and arguments "args", 
    "kwargs".
    
    """
    # Note. this function uses a global variable which must be defined on the 
    # remote host.
    for func, args, kwargs in itertools.izip(TASK, args, kwargs):
        inbox = (func(inbox, *args, **kwargs),)
    return inbox[0]


class _Consume(object):
    """
    (internal) iterator-wrapper consumes "n" results from the input iterator and 
    weaves the results together in "strides". If the result is an exception it 
    is **not** raised, but is returned.
    
    Arguments:
    
      - iterable(iterable) a sequence or other iterable
      - n(``int``) number of results to consume.
      - stride(``int``) see documentation
      
    """
    def __init__(self, iterable, n=1, stride=1):
        self.iterable = iterable
        self.stride = stride
        self._stride_buffer = None
        self.n = n

    def __iter__(self):
        return self

    def _rebuffer(self):
        batch_buffer = defaultdict(list)
        self._stride_buffer = []
        for i in xrange(self.n):                        # number of consumed 
            for stride in xrange(self.stride):               # results
                try:
                    res = self.iterable.next()
                except StopIteration:
                    continue
                except Exception, res:
                    pass
                batch_buffer[stride].append(res)

        for stride in xrange(self.stride):
            batch = batch_buffer[stride]
            self._stride_buffer.append(batch)
        self._stride_buffer.reverse()

    def next(self):
        """
        Returns the next sequence of results, given stride and n.
        
        """
        try:
            results = self._stride_buffer.pop()
        except (IndexError, AttributeError):
            self._rebuffer()
            results = self._stride_buffer.pop()
        if not results:
            raise StopIteration
        return results


class _Zip(object):
    """
    (internal) replacement for ``itertools.zip``, which pulls from all iterators
    regardless of a raised ``StopIteration``.
    
    """
    def __init__(self, *iterables):
        self.iterables = [iter(itr) for itr in iterables]

    def __iter__(self):
        return self

    def next(self):
        results = []
        stop = False
        for iter in self.iterables:
            try:
                results.append(iter.next())
            except StopIteration:
                stop = True
        if stop:
            raise StopIteration
        else:
            return results


class _Chain(object):
    """ 
    (internal) This is a generalization of the ``itertools.izip`` and 
    ``itertools.chain`` functions. If "stride" is ``1`` it behaves like 
    ``itertools.izip``, if  "stride" is ``len(iterable)`` it behaves like 
    ``itertools.chain`` in any other case it zips iterables in strides e.g::

        a = Chain([iter([1,2,3]), iter([4,5,6], stride =2)
        list(a)
        >>> [1,2,4,5,3,6]
        
    It is further resistant to exceptions i.e. if one of the iterables
    raises an exception the ``Chain`` does not end in a ``StopIteration``, but 
    continues with other iterables.
    
    """
    def __init__(self, iterables, stride=1):
        self.iterables = iterables
        self.stride = stride
        self.l = len(self.iterables)
        self.s = self.stride
        self.i = 0

    def __iter__(self):
        return self

    def next(self):
        """
        Returns the next result from the chained iterables given ``"stride"``.
        
        """
        if self.s:
            self.s -= 1
        else:
            self.s = self.stride - 1
            self.i = (self.i + 1) % self.l # new iterable
        return self.iterables[self.i].next()


class _Repeat(object):
    """ 
    (very internal) This iterator-wrapper returns n-times each result from the 
    wrapped iterator. i.e. if n =2 and the input iterators results are (1, 
    Exception, 2) then the ``__Produce`` instance will return 6 (i.e. ``2 * 3``) 
    results in the order [1, 1, Exception, Exception, 2, 2] if the stride =1. 
    If stride =2 the output will look like this: [1, Exception, 1, Exception, 
    2, 2]. Note that ``StopIteration`` is also an exception, and the Produce 
    iterator might return values after a ``StopIteration`` is raised. 
    
    """
    def __init__(self, iterable, n=1, stride=1):
        self.iterable = iterable
        self.stride = stride
        self._stride_buffer = None
        self._repeat_buffer = None
        self.n = n             # times the results in the buffer are repeated

    def __iter__(self):
        return self

    def _rebuffer(self):
        """
        (very internal) refill the repeat buffer
        """
        results = []
        exceptions = []
        for i in xrange(self.stride):
            try:
                results.append(self.iterable.next())
                exceptions.append(False)
            except Exception, excp:
                results.append(excp)
                exceptions.append(True)
        self._repeat_buffer = repeat((results, exceptions), self.n)

    def next(self):
        """
        (very internal) returns the next result, given ``"stride"`` and ``"n"``.
        
        """
        try:
            res, excp = self._stride_buffer.next()
        except (StopIteration, AttributeError):
            try:
                self._stride_buffer = izip(*self._repeat_buffer.next())
            except (StopIteration, AttributeError):
                self._rebuffer()
                self._stride_buffer = izip(*self._repeat_buffer.next())
            res, excp = self._stride_buffer.next()
        if excp:
            raise res
        else:
            return res


class _Produce(_Repeat):
    """
    (internal) This iterator wrapper is an iterator, but it returns elements 
    from the  sequence returned by the wrapped iterator. The number of returned 
    elements is defined by n and should not be smaller then the sequence 
    returned by the wrapped iterator. 
    
    For example if the wrapped iterator results are ((11, 12), (21, 22), 
    (31, 32)) then n **should** equal ``2``. For "stride" ``1`` the result will 
    be: [11, 12, 21, 22, 31, 32]. For "stride" ``2``, [11, 21, 12, 22, 31, 32]. 
    Note that ``StopIteration`` is also an exception!
    
    """
    def _rebuffer(self):
        """
        (internal) refill the repeat buffer
        
        """
        # collect a stride worth of results(result lists) or exceptions
        results = []
        exceptions = []
        for i in xrange(self.stride):
            try:
                results.append(self.iterable.next())
                exceptions.append(False)
            except Exception, excp:
                results.append(excp)
                exceptions.append(True)
        # un-roll the result lists
        res_exc = []
        for rep in xrange(self.n):
            flat_results = []
            for i in xrange(self.stride):
                result_list, exception = results[i], exceptions[i]
                if not exception:
                    flat_results.append(result_list[rep])
                else:
                    flat_results.append(result_list)
            res_exc.append((flat_results, exceptions))
        # make an iterator (like repeat)
        self._repeat_buffer = iter(res_exc)


class _InputIterator(object):
    """
    (internal) wraps a piper and iterator together.
    
    """
    def __init__(self, iterator, piper):
        self.iterator = iter(iterator)
        self.piper = piper

    def __iter__(self):
        return self

    def next(self):
        """
        (internal) returns the next result from the iterator if the piper is 
        started.
        
        """
        if self.piper.imap is imap and \
           self.piper.started is False:
            raise StopIteration
        else:
            return self.iterator.next()


class _TeePiper(object):
    """
    (internal) This is wrapper around a ``Piper`` instance if another ``Piper`` 
    instance connects to it. The actual call to ``itertools.tee`` is delayed and
    happens  on a call to ``Piper.start``. A ``TeePiper`` instance protects the 
    ``itertools.tee`` object with a ``threading.Lock``. This lock is held for a
    stride. If a ``StopIteration`` exception occurs the next ``TeePiper`` is 
    released and subsequent calls to the ``next`` method of this ``TeePiper`` 
    will not involve acquiring a lock and calling the ``next`` method of the 
    wrapped  tee object. This guarantees that the ``next`` method of a ``Piper``
    will yield a ``StopIteration`` only once. This is required because the 
    ``NuMap`` will finish a task after the first ``StopIteration`` and will not
    call ``Piper.next`` any more and will automatically raise ``StopIterations``
    for subsequent calls to ``NuMap.next``.    
    
    Arguments:
    
      - piper(``Piper``) ``Piper`` instance to be tee'd
      - i(``int``) index of the ``itertools.tee`` object within ``Piper.tees``.
      - stride(``int``) The stride of the ``Piper`` downstream of the wrapped 
        ``Piper``. In a pipeline they should be the same or compatible 
        (see manual).
        
    """
    def __init__(self, piper, i, stride):
        self.finished = False
        self.piper = piper
        self.stride = stride
        self.i = i
        self.s = 1

    def __iter__(self):
        return self

    def next(self):
        """
        (internal) returns the next result from the ``itertools.tee`` object for
        the wrapped ``Piper`` instance or re-raises an ``Exception``.
        
        """
        # do not acquire lock if NuMap is not finished.
        if self.finished:
            raise StopIteration
        # get per-tee lock
        self.piper.tee_locks[self.i].acquire()
        # get result or exception
        exception = True
        try:
            result = self.piper.tees[self.i].next()
            exception = False
        except StopIteration, result:
            self.finished = True
        except Exception, result:
            pass
        # release per-tee lock either self or next
        if self.s == self.stride or self.finished:
            self.s = 1
            self.piper.tee_locks[(self.i + 1) % len(self.piper.tees)].release()

        else:
            self.s += 1
            self.piper.tee_locks[self.i].release()
        if exception:
            raise result
        else:
            return result
