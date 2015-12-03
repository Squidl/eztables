import threading
import sys
import traceback
import multiprocessing
from collections import Iterator

class StreamList:
    def __init__(self,source=None,closeafter=True,async=True,transform=None,callback=None):
        self.data=[]
        self.closed=False
        self.error=None
        self.stream_lock=threading.Condition(threading.Lock())
        if not source is None:
            if async:
                stream_worker(self, target=self.addmany, args=(source,closeafter,transform,callback) )
            else:
                self.addmany(source,transformation)
    def __iter__(self):
        return StreamList_Iter(self)
    def add(self,newdata):
        self.stream_lock.acquire()
        self.data.append(newdata)
        self.stream_lock.notifyAll()
        self.stream_lock.release()
    def close(self,error=None):
        self.stream_lock.acquire()
        self.closed=True
        self.error=error
        self.stream_lock.notifyAll()
        self.stream_lock.release()
    def geterror(self):
        err=None
        self.stream_lock.acquire()
        err=self.error
        self.stream_lock.release()
        return err
    def addmany(self,newdata,closeafter=True,transform=None,callback=None):
        for x in newdata:
            if transform is None:
                self.add(x)
            else:
                self.add(transform(x))
        if not closeafter is None:
            self.close()
        if not callback is None:
            callback()

class StreamList_Iter:
    __slots__=("source","offset")
    def __init__(self,source,offset=0):
        self.source=source
        self.offset=offset
    def next(self):
        source=self.source
        err=source.geterror()
        if err is not None:
            raise err
        source.stream_lock.acquire()
        if (len(source.data)>self.offset):
            result=self.source.data[self.offset]
            self.offset=self.offset+1
            source.stream_lock.release()
            return result
        if source.closed:
            source.stream_lock.release()
            raise StopIteration
        while len(self.source.data)<=self.offset and not self.source.closed:
                source.stream_lock.wait()
        source.stream_lock.release()
        return self.next()

stream_workers={}
def stream_worker(stream,target=None,args=None,callback=None):
    if not isinstance(stream,list):
        stream=[stream]
    thread=threading.Thread( target=perform_work, args= (stream,target,args,callback))
    thread.start()

import atexit
@atexit.register
def stopall_stream_workers(exc=None):
    [[stream.close(exc) for stream in stream_workers[k]] for k in stream_workers.keys()]

def perform_work(streams,target,args,callback):
    stream_workers[threading.current_thread().getName()]=streams
    try:
        target(*args)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        exc_tb=exc_tb.tb_next
        exc=TransposeException(exc_type,exc_value,exc_tb)
        stopall_stream_workers(exc)
    del stream_workers[threading.current_thread().getName()]
    if callback is not None:
		callback()

class TransposeException(Exception):
    def __init__(self,exc_type,exc_value,exc_tb):
        self.e_type=exc_type
        self.e_val=exc_value
        self.e_tb=exc_tb
    def __str__(self):
        return "\n"+("".join(traceback.format_exception(self.e_type, self.e_val, self.e_tb)))

def filter_Async(source,dest,wherefunc):
    for x in source:
        if wherefunc(x):
            dest.add(x)
    dest.close()

def filter_where(source,wherefunc,async=True):
    output=StreamList()
    if async:
        stream_worker(output,target=filter_Async, args=(source,output,wherefunc))
    else:
        filter_Async(source,output,wherefunc)
    return output

def filter_split_Async(source,outputs,splitfunc):
    for x in source:
        index=splitfunc(x)
        if index>len(outputs):
            print("ERROR with filter split")
        outputs[index].add(x)
    for y in outputs:
        y.close()

def filter_split(source,splitfunc,bins=2,async=True):
    outputs=[]
    for x in range(bins):
        outputs.append(StreamList())
    if async:
        stream_worker(outputs,target = filter_split_Async, args=(source,outputs,splitfunc))
    else:
        filter_split_Async(source,outputs,splitfunc)
    return tuple(outputs)
