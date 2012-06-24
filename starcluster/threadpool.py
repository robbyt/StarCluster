"""
ThreadPool module for StarCluster based on WorkerPool
"""
import time
import Queue
from gevent import pool

from starcluster import exception
from starcluster import progressbar
from starcluster.logger import log


class AintJava(Exception):
    pass


class SimpleJob(object):
    def __init__(self, method, args=[], kwargs={}, jobid=None,
                 results_queue=None, thread_timeout=240, job_timeout=600):
        """ method : a single function call to the job that should be run
            args : any args for the function, as a list
            kwargs : any optional kwargs, as a dict
            jobid : a way to find the job, currently not required for gevent
            results_queue : status will be available here when jobs finish
            thread_timeout : timeout(secs) for a single thread
            job_timeout : once all jobs have been submitted and running,
                          python will block for N seconds before killing
                          any background threads
        """
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.jobid = jobid
        self.results_queue = results_queue
        self.pool = pool.Pool(thread_timeout)
        self.job_timeout = job_timeout

    def run(self):
        self.results_queue.append(self.pool.spawn(self.method,
                                                  *self.args,
                                                  **self.kwargs))

    def join(self):
        self.pool.gevent.joinall(self.results_queue, self.job_timeout)

def get_thread_pool(*args, **kwargs):
    msg = 'Please convert this object call from get_thread_pool to ThreadPool'
    raise AintJava(msg)
