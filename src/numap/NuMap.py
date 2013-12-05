# -*- coding: utf-8 -*-
"""
:mod:`numap.NuMap`
==================

This module provides a parallel (local or remote), buffered, multi-task, lazy
map function, which can use threads and processes.

"""

try:
    from multiprocessing import Process, cpu_count, TimeoutError
    from multiprocessing.queues import SimpleQueue
    HASMP = True
except ImportError:
    HASMP = False
try:
    import rpyc
    HASRP = True
except ImportError:
    HASRP = False

# Threading and Queues
from threading import Thread, Semaphore, Event
from threading import Lock as tLock
from Queue import Queue, Empty
# for PriorityQueue
from heapq import heappush, heappop
# Misc.
from itertools import izip, repeat
from inspect import getsource, isbuiltin, isfunction
# sets-up logging
from logging import getLogger
log = getLogger(__name__)
import warnings


class NuMap(object):
    """
    ``NuMap`` is a parallel (thread- or process-based, local or remote), 
    buffered, multi-task, ``itertools.imap`` or ``multiprocessing.Pool.imap`` 
    function replacment. Like ``imap`` it evaluates a function on elements of a
    sequence or iterable, and it does so lazily. Laziness can be adjusted via 
    the "stride" and  "buffer" arguments. Unlike ``imap``, ``NuMap`` supports 
    **multiple pairs** of function and iterable **tasks**. The **tasks** are 
    **not** queued rather they are **interwoven** and share a pool or **worker**
    "processes" or "threads" and a memory "buffer".
    
    
    *Pool*
 
    The pool is a set of managed **worker** processes or threads. The choice of 
    the "worker_type" has a **fundamental** impact on the performance of the 
    map. As a general rule use "process" if you have multiple CPUs or CPU-cores
    and  your task functions are cpu-bound. Use "thread" if your function is 
    IO-bound e.g. retrieves data from the Web. Increasing the number of 
    **workers** above the number of CPUs makes sense only if these are "thread"
    based **workers** and the evaluated functions are IO-bound. Some CPU-bound 
    tasks might evaluate faster if the number of **worker** processes equals the
    number of CPUs + 1. For "thread" based  ``NuMaps`` a larger number of 
    **workers** of might improve performance. The "worker_num" argument must not
    **not** include workers needed to run remote processes and can be equal 
    ``0`` for a purely remote ``NuMaps``.


    *Iteration*
 
    Results are retrieve through iteration. A single ``NuMap`` instance supports
    iteration over results from many **tasks**. This means that it supports 
    multiple end-points. The default is to iterate over the results from the 
    first task. An iterator for a single **task** is returned by the 
    ``NuMap.get_task`` method.
    
    
    *Order*
 
    The tasks can be interdependent i.e. the results from one **task** being the
    input to a second **task**. The order in which **tasks** are added to the 
    ``NuMap`` instance is important. It affects the order of evaluation and 
    consequently the order in which results should be retrieved. If the 
    **tasks** are chained then the "order" must be a valid topological sort 
    (reverse topological order). If the ``NuMap`` is ordered the n-th result for
    a specific **task** the will be always be available before the n+1-th 
    result. If "order" is ``False`` the results will be available in the order
    they are calculated.
    
    
    *Skipping*

    The "skipping" argument determines how to respond to ``TimeoutErrors`` it is
    ignored if no "timeout" value is given to the ``NuMap.next`` method. If 
    "skipping" is ``True`` results, which are not calculated on time will be 
    omitted. If "skip" ``False`` an exception will be raised, but the result can
    be retrieved later. If **tasks** are chained a ``TimeoutError`` will 
    collapse the ``NuMap`` evaluation. Do **not* specify timeouts in for
    chained **tasks**.

    
    *Parallel evaluation*
    
    The parallelism of the evaluation is strictly defined by the "stride", 
    "buffer" and the total number of **workers** in the pool. The **worker** 
    number is obviously the upper bound of concurrently evaluated elements.
    The maximum number of elements from a single **task** evauluated in 
    parallel is defined by "stride". The "buffer" limits the maximum number of
    pending results for all **tasks** it is a function of "stride", but also 
    of the topology of dependencies between the tasks. A long "stride" improves 
    parallelism, but increases "buffer" memory requirements. It should not be 
    smaller than the number of pool **workers**, because some will be idle. The
    size of the "buffer"  is larger or equal to "stride" because a **task** 
    might depend on results from multiple up-stream **tasks**.
  
    The minimum "stride" and "buffer is ``1`` therefore the results from the 
    "buffer" must be removed in the same order as input elements are evaluated.
    Otherwise the ``NuMap`` might dead-lock. 
    
    An element is buffered until it is returned by the ``NuMap.next`` method. 
    Starting the ``NuMap`` will cause one element (the first from the first 
    **task** to be submitted to the pool. For "stride" equal to ``1``, the next
    queued element is the first from the second **task**, which can enter the 
    pool only if either the first result is retrieved (i.e. ``NuMap.next`` 
    returns) or the "buffer" is larger then the "stride". If the "buffer" is 
    ``n`` then ``n`` tasklets can enter the pool. A "stride" of ``n`` requires 
    at least ``n`` elements to enter the pool, therefore "buffer" cannot be 
    smaller then "stride". The "minimum" buffer is the maximum possible number
    of queued results. This number depends on the interdependencies between 
    **tasks** and the "stride". The default is conservative and sufficient for 
    all topologies.  If the **tasks** are chained i.e. the output from one is 
    consumed by another thenat most one i-th element from each chained **task**
    is at a given moment in the pool. In those cases the minimum "buffer" to 
    satisfy the worst case number of queued results is lower then the safe 
    default.
    
    
    *Stopping*
    
    The ``NuMap`` can be stopped at any time, however some buffered results 
    might be lost and up to 2 * "stride" additional input elements comsumed.
    If pending buffered results are not retrieved the ``NuMap`` might not shut
    down properly. 
    
    
    Arguments:
    
      - func (callable) [default: ``None``] If the ``NuMap`` is given a function
        it is used to define the first and only task of the ``NuMap``
      - iterable (iterable) [default: ``None``] a sequence of first arguments 
        for "func" required if "func" is given
      - args (``tuple``) [default: ``None``] optional, see: ``NuMap.add_task`` 
      - kwargs (``dict``) [default: ``None``] optional, see: ``NuMap.add_task``
      - worker_type(``'process'`` or ``'thread'``) [default: ``'process'``] 
        Defines the type of internally spawned pool workers. For 
        ``multiprocessing.Process`` based worker choose 'process' for 
        ``threading.Thread`` workers choose 'thread'.
      - worker_num(int) [default: number of CPUs, min: 1] The number of workers 
        to spawn locally. Defaults to the number of availble CPUs, which is a 
        reasonable choice for process-based  ``NuMaps``.
      - worker_remote(iterable) [default: ``None``] A sequence of "remote host"
        "remote worker_num" tuples e.g.
        ``(('localhost', 2]), ('127.0.0.1', 2))`` "remote worker_num" is the 
        number of workers processes per remote host. A custom ``TCP`` port can 
        be specified ``(('localhost:6666',2),)``.
      - stride(``int``) [default: automatic] number of elements from a **task**
        evaluated in parallel
      - buffer(``int``) [default: automatic] total number number of elements 
        (inputs and results) in the ``NuMap`` instance
      - ordered(``bool``) [default: ``True``] If ``True`` the output of all 
        **tasks** will be ordered see: order.
      - skip(``bool``) [default: ``False``] Should we skip a result if trying to
        retrieve it raised a  ``TimeoutError``? 
      - name(``str``) [default: "imap_id(id(object))"] an optional name to 
        associate with this ``NuMap`` instance. It should be unique. Useful for 
        code generation.            

    Restrictions:
    
      - a completely lazy i.e. "buffer-free" evaluation is not supported
      - if remote workers are enabled, "worker_type" has to be the default 
        "process".
        
    """

    @staticmethod
    def _pool_put(pool_semaphore, tasks, put_to_pool_in, pool_size, id_self, \
                  is_stopping):
        """ 
        (internal) Intended to be run in a seperate thread. Feeds tasks into 
        to the pool whenever semaphore permits. Finishes if self._stopping is 
        set.
        """
        log.debug('NuMap(%s) started pool_putter.' % id_self)
        last_tasks = {}
        for task in xrange(tasks.lenght):
            last_tasks[task] = -1
        stop_tasks = []
        while True:
            # are we stopping the Weaver?
            if is_stopping():
                log.debug('NuMap(%s) pool_putter has been told to stop.' % \
                           id_self)
                tasks.stop()
            # try to get a task
            try:
                log.debug('NuMap(%s) pool_putter waits for next task.' % \
                           id_self)
                task = tasks.next()
                log.debug('NuMap(%s) pool_putter received next task.' % id_self)
            except StopIteration:
                # Weaver raised a StopIteration
                stop_task = tasks.i # current task
                log.debug('NuMap(%s) pool_putter caught StopIteration from task %s.' % \
                                                                  (id_self, stop_task))
                if stop_task not in stop_tasks:
                    # task raised stop for the first time.
                    log.debug('NuMap(%s) pool_putter task %s first-time finished.' % \
                                                           (id_self, stop_task))
                    stop_tasks.append(stop_task)
                    pool_semaphore.acquire()
                    log.debug('NuMap(%s) pool_putter sends a sentinel for task %s.' % \
                                                           (id_self, stop_task))
                    put_to_pool_in((stop_task, None, last_tasks[stop_task]))
                if len(stop_tasks) == tasks.lenght:
                    log.debug('NuMap(%s) pool_putter sent sentinels for all tasks.' % \
                                                                        id_self)
                    # all tasks have been stopped
                    for _worker in xrange(pool_size):
                        put_to_pool_in(None)
                    log.debug('NuMap(%s) pool_putter sent sentinel for %s workers' % \
                                                           (id_self, pool_size))
                    # this kills the pool_putter
                    break
                # multiple StopIterations for a tasks are ignored. 
                # This is for stride.
                continue

            # got task
            last_tasks[tasks.i] = task[-1][0] # last valid result
            log.debug('NuMap(%s) pool_putter waits for semaphore for task %s' % \
                       (id_self, task))
            pool_semaphore.acquire()
            log.debug('NuMap(%s) pool_putter gets semaphore for task %s' % \
                       (id_self, task))
            #gc.disable()
            put_to_pool_in(task)
            #gc.enable()
            log.debug('NuMap(%s) pool_putter submits task %s to worker.' % \
                       (id_self, task))
        log.debug('NuMap(%s) pool_putter returns' % id_self)

    @staticmethod
    def _pool_get(get, results, next_available, task_next_lock, to_skip, \
                  task_num, pool_size, id_self):
        """ 
        (internal) Intended to be run in a separate thread and take results from
        the pool and put them into queues depending on the task of the result. 
        It finishes if it receives termination-sentinels from all pool workers.
        """
        log.debug('NuMap(%s) started pool_getter' % id_self)
        # should return when all workers have returned, each worker sends a 
        # sentinel before returning. Before returning it should send sentinels to
        # all tasks but the next available queue should be released only if we 
        # know that no new results will arrive.
        sentinels = 0
        result_ids, last_result_id, very_last_result_id = {}, {}, {}
        for i in xrange(task_num):
            last_result_id[i] = -1
            very_last_result_id[i] = -2
            result_ids[i] = set()

        while True:
            try:
                log.debug('NuMap(%s) pool_getter waits for a result.' % id_self)
                #gc.disable()
                result = get()
                #gc.enable()
            except (IOError, EOFError):
                log.error('NuMap(%s) pool_getter has a pipe problem.' % id_self)
                break

            # got a sentinel?
            if result is None:
                sentinels += 1
                log.debug('NuMap(%s) pool_getter got a sentinel.' % id_self)
                if sentinels == pool_size:
                    log.debug('NuMap(%s) pool_getter got all sentinels.' % \
                              id_self)
                    # here we are escaping.
                    break
                else:
                    # waiting for more sentinels or results to come.
                    continue

            # got a sentinel for a task?
            # only one sentinel per task will be received
            if result[1] is None:
                task = result[0]
                very_last_result_id[task] = result[2]
                if last_result_id[task] == very_last_result_id[task]:
                    results[task].put(('stop', False, 'stop'))
                    next_available[task].put(True)
                    log.debug('NuMap(%s) pool_getter sent sentinel for task %s.'\
                                                            % (id_self, task))
                continue

            # got some result for some task, which might be an exception
            task, i, is_valid, real_result = result
            # locked if next for this task is in 
            # the process of raising a TimeoutError
            task_next_lock[task].acquire()
            log.debug('NuMap(%s) pool_getter received result %s for task %s)' % \
                      (id_self, i, task))

            if to_skip[task]:
                log.debug('NuMap(%s) pool_getter skips results: %s' % (id_self, \
                range(last_result_id[task] + 1, last_result_id[task] + \
                      to_skip[task] + 1)))
                last_result_id[task] += to_skip[task]
                to_skip[task] = 0

            if i > last_result_id[task]:
                result_ids[task].add(i)
                results[task].put((i, is_valid, real_result))
                log.debug('NuMap(%s) pool_getter put result %s for task %s to queue' % \
                          (id_self, i, task))
            else:
                log.debug('NuMap(%s) pool_getter skips result %s for task %s' % \
                          (id_self, i, task))

            # this releases the next method for each ordered result in the queue
            # if the NuMap instance is ordered =False this information is 
            # ommitted.
            while last_result_id[task] + 1 in result_ids[task]:
                next_available[task].put(True)
                last_result_id[task] += 1
                log.debug('NuMap(%s) pool_getter released task: %s' % \
                          (id_self, task))
            if last_result_id[task] == very_last_result_id[task]:
                results[task].put(('stop', False, 'stop'))
                next_available[task].put(True)
            # release the next method
            task_next_lock[task].release()
        log.debug('NuMap(%s) pool_getter returns' % id_self)

    def __init__(self, func=None, iterable=None, args=None, kwargs=None, \
                 worker_type=None, worker_num=None, worker_remote=None, \
                 stride=None, buffer=None, ordered=True, skip=False, \
                 name=None):

        self.name = (name or 'numap_%s' % id(self))
        log.debug('%s %s starts initializing' % (self, self.name))
        if worker_type == 'process' and not HASMP:
            log.error('worker_type process requires multiprocessing')
            raise ImportError('worker_type process requires multiprocessing')
        self.worker_type = (worker_type or 'process')
        if worker_remote and not HASRP:
            log.error('worker_remote requires RPyC')
            raise ImportError('worker_remote requires RPyC')

        self._tasks = []
        self._tasks_tracked = {}
        self._started = Event()         # (if not raise TimeoutError on next)
        self._stopping = Event()        # (starting stopping procedure see stop)
        # pool options
        if worker_num is None:
            self.worker_num = stride or cpu_count()
        else:
            self.worker_num = worker_num
        self.worker_remote = (worker_remote or [])    # [('host', #workers)]
        self.stride = stride or \
                      self.worker_num + sum([i[1] for i in self.worker_remote])
        self.buffer = buffer            # defines the maximum number
        # of jobs which are in the input queue, pool and output queues
        # and next method

        # next method options
        self.ordered = ordered
        self.skip = skip

        # make pool input and output queues based on worker type.
        if self.worker_type == 'process':
            self._inqueue = SimpleQueue()
            self._outqueue = SimpleQueue()
            self._putin = self._inqueue._writer.send
            self._getout = self._outqueue._reader.recv
            self._getin = self._inqueue.get
            self._putout = self._outqueue.put
        elif self.worker_type == 'thread':
            self._inqueue = Queue()
            self._outqueue = Queue()
            self._putin = self._inqueue.put
            self._getout = self._outqueue.get
            self._getin = self._inqueue.get
            self._putout = self._outqueue.put

        # combine tasks into a weaved queue
        self._next_available = {}   # per-task boolean queue 
                                    # releases next to get a result
        self._next_skipped = {}     # per-task int, number of results
                                    # to skip (locked)
        self._task_next_lock = {}   # per-task lock around _next_skipped
        self._task_finished = {}    # a per-task is finished variable
        self._task_results = {}     # a per-task queue for results

        log.debug('%s finished initializing' % self)

        if bool(func) ^ bool(iterable):
            log.error('%s either, both or none func and iterable' % self + \
                      'have to be specified.')
            raise ValueError('%s either, both or none func and iterable' % self + \
                             'have to be specified.')
        elif bool(func) and bool(iterable):
            self.add_task(func, iterable, args, kwargs)
            self.start()

    def __call__(self, *args, **kwargs):
        return self.add_task(*args, **kwargs)

    def __str__(self):
        return "NuMap(%s)" % id(self)

    def __iter__(self):
        return self

    def _start_managers(self):
        """
        (internal) starts input and output pool queue manager threads.
        """
        self._task_queue = _Weave(self._tasks, self.stride)
        # here we determine the size of the maximum memory consumption
        self._semaphore_value = (self.buffer or (len(self._tasks) * self.stride))
        self._pool_semaphore = Semaphore(self._semaphore_value)


        # start the pool getter thread
        self._pool_getter = Thread(target=self._pool_get, args=(self._getout, \
                self._task_results, self._next_available, \
                self._task_next_lock, self._next_skipped, len(self._tasks), \
                len(self.pool), id(self)))
        self._pool_getter.deamon = True
        self._pool_getter.start()

        # start the pool putter thread
        self._pool_putter = Thread(target=self._pool_put, args=\
                (self._pool_semaphore, self._task_queue, self._putin, \
                len(self.pool), id(self), self._stopping.isSet))
        self._pool_putter.deamon = True
        self._pool_putter.start()

    def _start_workers(self):
        """
        (internal) starts **worker pool** threads or processes.
        """
        # creating the pool of worker process or threads
        log.debug('%s starts a %s-pool of %s workers.' % \
                  (self, self.worker_type, self.worker_num))
        self.pool = []
        for host, worker_num in \
                           [(None, self.worker_num)] + list(self.worker_remote):
            for _worker in range(worker_num):
                __worker = Thread(target=_pool_worker, args=\
                                  (self._inqueue, self._outqueue, host)) \
                                  if self.worker_type == 'thread' else \
                           Process(target=_pool_worker, args=\
                                  (self._inqueue, self._outqueue, host))
                self.pool.append(__worker)
        for __worker in self.pool:
            __worker.daemon = True
            __worker.start()
        log.debug('%s started the pool' % self)

    def _stop(self):
        """
        (internal) stops input and output pool queue manager threads.
        """
        if self._started.isSet():
            # join threads
            self._pool_getter.join()
            self._pool_putter.join()
            for worker in self.pool:
                worker.join()
            # remove threads  
            del self._pool_putter
            del self._pool_getter
            del self.pool
            # remove results
            self._tasks = []
            self._tasks_tracked = {}
            # virgin variables
            self._stopping.clear()
            self._started.clear()

    def add_task(self, func, iterable, args=None, kwargs=None, timeout=None, \
                block=True, track=False):
        """ 
        Adds a **task** to evaluate. A **task** is jointly a function or 
        callable an iterable with optional arguments and keyworded arguments.
        The iterable can be the result iterator of a previously added **task**
        (to the same or to a different ``NuMap`` instance).

        Arguments:
        
          - func (callable) this will be called on each element of the 
            "iterable" and supplied with arguments "args" and keyworded 
            arguments "kwargs"
          - iterable (iterable) this must be a sequence of *picklable* objects 
            which will be the first arguments passed to the "func"
          - args (``tuple``) [default: ``None``] A ``tuple`` of optional 
            constant arguments passed to the callable "func" after the first
            argument from the "iterable"
          - kwargs (``dict``) [default: ``None``] A dictionary of keyworded 
            arguments passed to "func" after the variable argument from the 
            "iterable" and the arguments from "args"
          - timeout (``bool``) see: ``_NuMapTask``
          - block (``bool``) see: ``_NuMapTask``
          - track (``bool``) [default: ``False``] If ``True`` the results 
            (or exceptions) of a **task** are saved within:
            ``self._tasks_tracked[%task_id%]`` as a ``{index:result}`` 
            dictionary. This is only useful if the callable "func" creates 
            persistant data. The dictionary can be used to restore the correct 
            order of the data
            
        """
        if not self._started.isSet():
            task = izip(repeat(len(self._tasks)), repeat(func), \
                        repeat((args or ())), repeat((kwargs or {})), \
                        enumerate(iterable))
            task_id = len(self._tasks)
            self._tasks.append(task)
            if track:
                self._tasks_tracked[task_id] = {} # result:index
            self._next_available[task_id] = Queue()
            self._next_skipped[task_id] = 0
            self._task_finished[task_id] = Event()
            self._task_next_lock[task_id] = tLock()
            # this locks threads not processes
            self._task_results[task_id] = _PriorityQueue() if self.ordered \
                                                          else Queue()
            return self.get_task(task=task_id, timeout=timeout, block=block)
        else:
            log.error('%s cannot add tasks (is started).' % self)
            raise RuntimeError('%s cannot add tasks (is started).' % self)

    def pop_task(self, number):
        """
        Removes a previously added **task** from the ``NuMap`` instance.
        
        Arguments:
        
          - number (``int`` or ``True``) A positive integer specifying the 
            number of **tasks** to pop. If  number is set ``True`` all **tasks**
            will be popped.
        
        """
        if not self._started.isSet():
            if number is True:
                self._tasks = []
                self._tasks_tracked = {}
            elif number > 0:
                last_task_id = len(self._tasks) - 1
                for i in xrange(number):
                    self._tasks.pop()
                    self._tasks_tracked.pop(last_task_id - i, None)
        else:
            log.error('%s cannot delete tasks (is started).' % self)
            raise RuntimeError('%s cannot delete tasks (is started).' % self)

    def get_task(self, task=0, timeout=None, block=True):
        """
        Returns an iterator which results are limited to one **task**. The 
        default iterator the one which e.g. will be used in a for loop is the
        iterator for the first task (task =0). The returned iterator is a 
        ``_NuMapTask`` instance.
        
        Compare::

            for result_from_task_0 in imap_instance:
                pass

        with::

            for result_from_task_1 in imap_instance.get_task(task_id =1):
                pass

        a typical use case is::

            task_0_iterator = imap_instance.get_task(task_id =0)
            task_1_iterator = imap_instance.get_task(task_id =1)

            for (task_1_res, task_0_res) in izip(task_0_iterator, task_1_iterator):
                pass
        
        """
        return _NuMapTask(self, task=task, timeout=timeout, block=block)

    def start(self, stages=(1, 2)):
        """
        Starts the processes or threads in the internal pool and the threads, 
        which manage the **worker pool** input and output queues. The 
        **starting mode** is split into **two stages**, which can be initiated 
        seperately. After the first stage the **worker pool** processes or 
        threads are  started and the ``NuMap._started`` event is set ``True``.
        A call to the ``NuMap.next`` method **will** block. After the **second 
        stage** the ``NuMap._pool_putter`` and ``NuMap._pool_getter`` threads
        will be running. The ``NuMap.next`` method should only be called **after** 
        this method returns.

        Arguments:
        
          - stages (``tuple``) [default: ``(1, 2)``] Specifies which stages of 
            the start process to execute, by default both stages.

        """
        if 1 in stages:
            if not self._started.isSet():
                self._start_workers()
                self._started.set()
        if 2 in stages:
            if not hasattr(self, '_pool_getter'):
                self._start_managers()

    def stop(self, ends=None, forced=False):
        """
        Stops an ``NuMap`` instance. If the list of end tasks is specified *via*
        the "ends" argument a call to ``NuMap.stop`` will block the calling 
        thread and retrieve (discards) a maximum of 2 * stride of results. This 
        will stop  the worker pool and the threads which manage its input and 
        output queues respectively. 
                
        If the "ends" argument is not specified, but the "forced" argument is 
        the method does not block and the ``NuMap._stop`` has to be called after
        **all** pending results have been retrieved. Calling ``NuMap._stop`` with
        pending results **will** dead-lock.
        
        Either "ends" or "forced" has to be ``True``.
        
        Arguments:

          - ends (``list``) [default: ``None``] A list of task ids which are not
            consumed within the ``NuMap`` instance.
          - forced (``bool``) [default: ``False``] If "ends" is not ``None`` 
            this argument is ignored. If "ends" is ``None`` and "forced" is 
            ``True`` the ``NuMap`` instance will trigger *stopping mode*.
            
        """
        if self._started.isSet():
            if ends:
                self._stopping.set()
                # if _stopping is set the pool putter will notify the weave 
                # generator that no more new results are needed. The weave 
                # generator will stop _before_ getting the first result from 
                # task 0 in the next stride.
                log.debug('%s begins stopping routine' % self)
                to_do = ends[:] # if ends else ends
                # We continue this loop until all end tasks
                # have raised StopIteration this stops the pool
                while to_do:
                    for task in to_do:
                        try:
                            #for i in xrange(self.stride):
                            self.next(task=task)
                        except StopIteration:
                            to_do.remove(task)
                            log.debug('%s stopped task %s' % (self, task))
                            continue
                        except Exception, excp:
                            log.debug('%s task %s raised exception %s' % \
                                      (self, task, excp))
                # stop threads remove queues 
                self._stop()
                log.debug('%s finished stopping routine' % self)
            elif forced:
                self._stopping.set()
                log.debug('%s begins triggers stopping' % self)
                # someone has to retrieve results and call the _stop_managers
            else:
                # this is the default
                msg = '%s is started, but neither ends nor forced was set.' % \
                        self
                log.error(msg)
                raise RuntimeError(msg)

    def next(self, timeout=None, task=0, block=True):
        """
        Returns the next result for the given **task**. Defaults to ``0``, which
        is the first **task**. If multiple chained tasks are evaluated then the
        next method of only the last should be called directly. 
                
        Arguments:
        
          - timeout (``float``) Number of seconds to wait until a 
            ``TimeoutError`` is raised.
          - task (``int``) id of the task from the ``NuMap`` instance
          - block (``bool``) if ``True`` call will block until result is 
            available
            
        """
        # check if any result is expected (started and not finished)
        if not self._started.isSet():
            log.debug('%s has not yet been started' % self)
            raise RuntimeError('%s has not yet been started' % self)
        elif self._task_finished[task].isSet():
            log.debug('%s has finished task %s' % (self, task))
            raise StopIteration

        # try to get a result
        try:
            log.debug('%s waits for a result: ordered %s, task %s' % \
                      (self, self.ordered, task))
            if self.ordered:
                _got_next = \
                    self._next_available[task].get(timeout=timeout, block=block)
                log.debug('%s has been released for task: %s' % (self, task))
                result = self._task_results[task].get()
            else:
                result = \
                    self._task_results[task].get(timeout=timeout, block=block)
        except Empty:
            self._task_next_lock[task].acquire()
            log.debug('%s timeout for result: ordered %s, task %s' % \
                      (self, self.ordered, task))
            # the threads might have switched between the exception and the 
            # lock.acquire during this switch several items could have been 
            # submited to the queue if one of them is the one we are waiting 
            # for we get it here immediately, but lock the pool getter not to
            # submit more results.
            try:
                if self.ordered:
                    _got_next = self._next_available[task].get(block=False)
                    log.debug('%s has been released for task: %s' % \
                              (self, task))
                    result = self._task_results[task].get()
                else:
                    result = self._task_results[task].get(block=False)
            except Empty:
                if self.skip:
                    self._next_skipped[task] += 1
                    self._pool_semaphore.release()
                self._task_next_lock[task].release()
                raise TimeoutError('%s timeout for result: ordered %s, task %s' % \
                                   (self, self.ordered, task))
        log.debug('%s got a result: ordered %s, task %s, %s' % \
                  (self, self.ordered, task, result))
        # return or raise the result
        index, is_valid, real_result = result
        if index == 'stop':
            # got the stop sentinel
            self._task_finished[task].set()
            log.debug('%s releases semaphore after StopIteration of task %s' % \
                      (self, task))
            self._pool_semaphore.release()
            log.debug('%s has finished task %s for the first time' % \
                      (self, task))
            raise StopIteration
        if task in self._tasks_tracked:
            self._tasks_tracked[task][index] = real_result
        if is_valid:
            log.debug('%s returns %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            return real_result
        else:
            log.debug('%s raises %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            raise real_result


def imports(modules, forgive=False):
    """
    Should be used as a decorator to *attach* import statments to function
    definitions. These imports are added to the global (i.e. module-level of 
    the decorated function) namespace.
        
    Two forms of import statements are supported (in the following examples
    ``foo``, ``bar``, ``oof, and ``rab`` are modules not classes or functions)::

        import foo, bar              # -> @imports(['foo', 'bar'])
        import foo.oof as oof            
        import bar.rab as rab        # -> @imports(['foo.oof', 'bar.rab'])
        
    It provides support for alternatives::
    
        try:
            import foo
        except ImportError:
            import bar
            
    which is expressed as::
    
        @imports(['foo,bar'])
        
    or alternatively::
    
        try:
            import foo.oof as oof
        except ImportError:
            import bar.rab as oof
            
    becomes::
    
        @imports(['foo.oof,bar.rab'])
        
    
    This import is available in the body of the function as ``oof`` All needed
    imports should be attached for every function (even if two function are in
    the same module and have the same ``globals``)

    Arguments:

        - modules (``list``) A list of modules in the following forms 
          ``['foo', 'bar', ..., 'baz']`` or  
          ``['foo.oof', 'bar.rab', ..., 'baz.zab']``
        - forgive (``bool``) [default: ``False``] If ``True`` will not raise 
          `ImportError``
    """
    def wrap(f):
        if modules:
            # attach import to function
            setattr(f, 'imports', modules)
            for alternatives in modules:
                # alternatives are comma seperated
                alternatives = alternatives.split(',')
                # we import the part of the import X.Y.Z -> Z
                mod_name = alternatives[0].split('.')[-1]
                for mod in alternatives:
                    mod = mod.strip().split('.')

                    try:
                        if len(mod) == 1:
                            module = __import__(mod[0])
                        else:
                            module = getattr(__import__('.'.join(mod[:-1]), \
                                            fromlist=[mod[-1]]), mod[-1])
                        f.func_globals[mod_name] = module
                        break # import only one
                    except ImportError:
                        pass
                else:
                    if forgive: # no break -> no import
                        warnings.warn('Failed to import %s' % alternatives)
                    else:
                        raise ImportError('Failed to import %s' % alternatives)
        return f
    return wrap


class _Weave(object):
    """
    (internal) Weaves a sequence of iterators, which can be stopped if the same 
    number of results has been consumed from all iterators (repeats boundaries).

    Arguments:
    
      - iterators (iterable) A sequence of objects supporting the iterator 
        protocol.
      - repeats (``int``) [default: ``1``] A positive integer defining the 
        number of results to retrieve from one iterator before the next iterator
        in the sequence.
        
    """
    def __init__(self, iterators, repeats=1):
        self.iterators = iterators          # sequence of iterators
        self.lenght = len(self.iterators)
        self.i = 0                          # index of current iterators
        self.repeats = repeats     # number of repeats from an iterator (stride)
        self.r = 0                 # current repeat
        self.stopping = False      # if True stop at i==0, r==0
        self.stopped = False

    def __iter__(self):
        # support for the iterator protocol
        return self

    def stop(self):
        """
        Triggers stopping. ``_Weave`` will stop at repeats boundaries.
        """
        # stopping is a one-way process
        # log.debug('Weave(%s) told to stop.' % id(self))
        self.stopping = True

    def next(self):
        """
        Returns the next element or raises ``StopIteration`` if stopped.
        """
        # need new iterable?
        if self.r == self.repeats:
            self.i = (self.i + 1) % self.lenght
            self.r = 0

        self.r += 1
        if self.stopping and self.i == 0 and self.r == 1:
            self.stopped = True
        if self.i == 0 and self.stopped:
            raise StopIteration
        else:
            iterator = self.iterators[self.i]
            return iterator.next()


class _NuMapTask(object):
    """
    (internal) the ``_NumMapTask`` is an object-wrapper of ``NuMap`` instaces.
    It is an interator over the results from a single **task**.
    
    Arguments:
    
        - iterator (``NuMap``) instance to be wrapped, usually initialization 
          is done by the ``NuMap.get_task``

    """
    def __init__(self, iterator, task, timeout, block):
        self.timeout = timeout
        self.block = block
        self.task = task
        self.iterator = iterator

    def __iter__(self):
        return self

    def next(self):
        """
        Returns a result if availble within "timeout" else raises a 
        ``TimeoutError`` exception. See documentation for ``NuMap.next``.
        """
        return self.iterator.next(task=self.task, timeout=self.timeout,
                                                    block=self.block)


class _PriorityQueue(Queue):
    """
    (internal) a priority queue using a heap on a list. This queue can be used 
    with "thread", but not "process" ``NuMap`` instances.
    """
    def _init(self, maxsize):
        self.maxsize = maxsize
        self.queue = []

    def _put(self, item):
        return heappush(self.queue, item)

    def _get(self):
        return heappop(self.queue)


def _pool_worker(inqueue, outqueue, host=None):
    """
    (internal) Function which is executed by worker pool processes or threads.
    It waits for tasks (function, data, arguments) at the input queue "inqueue"
    evaluates the result and passes it to the output queue "outqueue". It 
    optionally evaluates the function on a remote host.
    """
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()
    if host:
        host_port = host.split(':')
        try:
            host_port = [host_port[0], int(host_port[1])]
        except IndexError:
            pass
        conn = rpyc.classic.connect(*host_port)
        conn.execute(getsource(imports)) # provide @imports on server

    while True:
        try:
            #gc.disable()
            task = get()
            #gc.enable()
        except (EOFError, IOError):
            break

        if task is None:
            put(None)
            break

        if task[1] is None:
            put(task)
            continue

        job, func, args, kwargs, (i, data) = task
        if host:
            func = func._inject(conn) if hasattr(func, '_inject') else\
                   _inject_func(func, conn)
        try:
            ok, result = (True, func(data, *args, **kwargs))
        except Exception, excp:
            ok, result = (False, excp)
            #gc.disable(), gc.enable()
        put((job, i, ok, result))

def _inject_func(func, conn):
    """
    (internal) injects a function object into a RPyC connection object.
    """
    name = func.__name__
    if not name in conn.namespace:
        if isbuiltin(func):
            inject_code = '%s = %s' % (name, name)
        elif isfunction(func):
            inject_code = getsource(func)
        conn.execute(inject_code)
    return conn.namespace[name]

