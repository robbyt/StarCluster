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
        self.results_queue = Queue.Queue()
        self._progress_bar = None
        if self.disable_threads: size = 0
        self.pool = pool.Pool(size)
        self.job_timeout = job_timeout

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
                r = self.pool.spawn(method, *args, **kwargs)
            else:
                r = self.pool.spawn(method, *args)
        elif args is not None and args is not []:
            if isinstance(kwargs, dict):
                r = self.pool.spawn(method, args, **kwargs)
            else:
                r = self.pool.spawn(method, args)
        else:
            r = self.pool.spawn(method)

        if self.results_queue:
            return self.results_queue.put(r)
        return r


    def get_results(self):
        results = []
        for i in range(self.results_queue.qsize()):
            results.append(self.results_queue.get().value())
        return results

    def map(self, fn, *seq):
        self.pool.map(fn, *seq)

    def store_exception(self, e):
        self._exception_queue.put(e)

    def shutdown(self):
        log.info("Shutting down threads...")
        self.pool.kill(timeout=self.job_timeout)

    @property
    def unfinished_tasks(self):
        return self.results_queue.qsize()

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
        self.pool.join(timeout=self.job_timeout)
#        self.pool.joinall(self.results_queue, self.job_timeout)

    def __del__(self):
        log.debug('del called in threadpool')
        self.shutdown()
        self.join()

def get_thread_pool(size=10, worker_factory=None,
                    disable_threads=False):
    return SimpleJob(size=size, disable_threads=disable_threads)
