"""
ThreadPool module for StarCluster based on WorkerPool
"""
import time
import Queue
#import workerpool
from gevent import pool

#from starcluster import exception
from starcluster import progressbar
from starcluster.logger import log


class AintJava(Exception):
    pass

class ThreadPool(object):
    def __init__(self, size=1, disable_threads=False, job_timeout=600):
        self.disable_threads = disable_threads
        self.threads_queue = Queue.Queue()
        self.results_queue = Queue.Queue()
        self._progress_bar = None
        if self.disable_threads: size = 0
        self.pool = pool.Pool(size)
        self.job_timeout = job_timeout
        self.unfinished_tasks = 0

    @property
    def progress_bar(self):
        if not self._progress_bar:
            widgets = ['', progressbar.Fraction(), ' ',
                       progressbar.Bar(marker=progressbar.RotatingMarker()),
                       ' ', progressbar.Percentage(), ' ', ' ']
            pbar = progressbar.ProgressBar(widgets=widgets, maxval=1,
                                           force_update=True)
            self._progress_bar = pbar
        return self._progress_bar

    def simple_job(self, method, args=None, kwargs=None, jobid=None):

        if args is None:
            args = []

        if kwargs is None:
            kwargs = {}

        if isinstance(args, list) or isinstance(args, tuple):
            if isinstance(kwargs, dict):
                thread = self.pool.spawn(method, *args, **kwargs)
            else:
                thread = self.pool.spawn(method, *args)
        elif args is not None and args is not []:
            if isinstance(kwargs, dict):
                thread = self.pool.spawn(method, args, **kwargs)
            else:
                thread = self.pool.spawn(method, args)
        else:
            thread = self.pool.spawn(method)

        self.task_start(thread)

        if self.threads_queue:
            return self.threads_queue.put(thread)
        return thread

    def get_results(self):
        results = []
        qsize = self.threads_queue.qsize()
        log.debug("Thread queue size at: %s" % (qsize) )
        for i in range(qsize):
            log.debug("Collecting output for queue at: %s" % (i))
            thread = self.threads_queue.get()
            log.debug("Found a thread in the queue")
            output = thread.value
            log.debug("Output from thread is: %s" % (output))
            results.append(output)
        return results

    def map(self, fn, *seq):
        self.pool.map(fn, *seq)
        self.pool.join()

    def store_exception(self, e):
        self._exception_queue.put(e)

    def shutdown(self):
        log.info("Shutting down threads...")
        self.pool.kill(timeout=self.job_timeout)

    def task_start(self, thread):
        thread.link(self.task_done)
        self.unfinished_tasks += 1

    def task_done(self, thread):
        self.results_queue.put(thread)
        self.unfinished_tasks -= 1

    def wait(self, numtasks=None, return_results=True):
        pbar = self.progress_bar.reset()
        pbar.maxval = self.unfinished_tasks
        if numtasks is not None:
            pbar.maxval = max(numtasks, self.unfinished_tasks)
        while self.unfinished_tasks != 0:
            finished = pbar.maxval - self.unfinished_tasks
            pbar.update(finished)
            log.debug("unfinished_tasks = %d" % self.unfinished_tasks)
            time.sleep(1)
        if pbar.maxval != 0:
            pbar.finish()
        self.pool.join(timeout=self.job_timeout)
#        if self._exception_queue.qsize() > 0:
#            raise exception.ThreadPoolException(
#                "An error occurred in ThreadPool", self._exception_queue.queue)
        if return_results:
            return self.get_results()

    def join(self):
        log.debug('join called in threadpool')
        self.pool.join(timeout=self.job_timeout)
#        self.pool.joinall(self.results_queue, self.job_timeout)

    def __del__(self):
        log.debug('del called in threadpool')
        self.shutdown()
        self.join()

def get_thread_pool(size=10, worker_factory=None,
                    disable_threads=False):
    return ThreadPool(size=size, disable_threads=disable_threads)
