#!/usr/bin/env python
from __future__ import division

import sys,os
from multiprocessing import Pool, Manager
from multiprocessing import dummy as mpthreaded
from math import ceil
import itertools

from . mrjobs import MapperError, OrderedMap
import pbsutils, mrutils
from . mrutils import allow_method_pickles

__all__ = ["MapReduce"]

allow_method_pickles()

class MapReduce(object):
    def __init__(self,data=None,n=None,mode="smp"):
        """Input:
        n number of processes to start"""
        self.n = n      # If None then uses all processors it can find in it's mode.
        self.data = data
        self.mode = mode
        self.autobatch = False
        self.autobatchfactor = 2
        self.mapfailures = []
        self.reducefailures = []
                
    def reset(self,data=None):
        if data != None:
            self.data = data
        self.mapfailures = []
        self.reducefailures = []
        
    def failures(self):
        if self.mapfailures or self.reducefailures:
            return self.mapfailures,self.reducefailures
        else:
            return None
                
    def run(self,mapper,data=None): # Mode is a hook so that we can add support for the cluster architecture
        
        debug = False
        self.reset(data)
        mapper_interface = mapper.implements()
        if self.mode == "serial": # Doesn't use the multiprocessing module
            kvlist1 = list(mapper.partition(self.data))  # Split into pieces to be mapped
            
            if debug: print "About to map",len(kvlist1),"things:",kvlist1
            
            if "map" in mapper_interface:
                map_results = [mapper._map(kv) for kv in kvlist1]
                map_results, self.mapfailures = split_failures_from_results(map_results)
            else:
                map_results = kvlist1
                
            
            if debug: print "Mapped",map_results
            
            kvlist2 = mapper.shuffle(map_results)                        
            
            if "reduce" in mapper_interface:
                results = [mapper._reduce(kv) for kv in kvlist2]
                results, self.reducefailures = split_failures_from_results(results)
            else:
                results = kvlist2
                
            return mapper.gather(results)              
            
        elif self.mode=="smp" or self.mode=="threaded": #Runs everything on one machine using local cores
            pool = None
            cores = 10*self.n
            mapper.prepare_shared_data()
            if self.mode=="smp":
                pool = Pool(processes=self.n,maxtasksperchild=mapper.maxtasks)
            elif self.mode=="threaded":
                pool = mpthreaded.Pool(processes=self.n)
            
            kvlist1 = list(mapper.partition(self.data))      # Split into pieces to be mapped
            
            if "map" in mapper_interface:
                if self.autobatch:
                    batched_kvlist1 = mrutils.splitseq(kvlist1, int(ceil(len(kvlist1)/(self.autobatchfactor*cores))))
                    map_results = pool.map(mapper._maplist,batched_kvlist1)
                    map_results = list(itertools.chain(*map_results))
                else:
                    map_results = pool.map(mapper._map, kvlist1)
                map_results, self.mapfailures = split_failures_from_results(map_results)
            else:
                map_results = kvlist1
            
            
            kvlist2 = list(mapper.shuffle(map_results))    
                
            if "reduce" in mapper_interface:
                if self.autobatch:
                    batched_kvlist2 = mrutils.splitseq(kvlist2, int(ceil(len(kvlist2)/(self.autobatchfactor*cores))))
                    results = pool.map(mapper._reducelist,batched_kvlist2)
                    results = list(itertools.chain(*results))
                else:
                    results = pool.map(mapper._reduce,kvlist2)
                results, self.reducefailures = split_failures_from_results(results)
            else:
                results = kvlist2

            pool.close()
            pool.join()
            
            return mapper.gather(results)                       
    
        elif self.mode == 'pbs':
            self.autobatch = True
            
            cores,nodes = pbsutils.parseNodeFile()
            manager = Manager()
            cq = manager.Queue() # core queue
            for c in cores:
                cq.put(c)
            pool = Pool(processes = len(cores))
            
            if debug: print "I just set up a pool with",len(cores),"workers"
            kvlist1 = list(mapper.partition(self.data))
            if debug: print "About to map",len(kvlist1),"things:",kvlist1
            
            if "map" in mapper_interface:
                if self.autobatch:
                    # Heuristic: split up into 2*len(cores) chunks
                    batched_kvlist1 = mrutils.splitseq(kvlist1, int(ceil(len(kvlist1)/(2*len(cores)))))
                    print "About to send length ",len(batched_kvlist1),"batches totaling",len(batched_kvlist1)
                    map_results = pool.map(pbsutils.task, [(x, mapper._maplist, cq) for x in batched_kvlist1])
                    map_results = list(itertools.chain(*map_results))
                else:
                    map_results = pool.map(pbsutils.task, [(x, mapper._map, cq) for x in kvlist1])
                map_results, self.mapfailures = split_failures_from_results(map_results)
            else:
                map_results = kvlist1
                
            if debug: print "Mapped",map_results
            
            kvlist2 = mapper.shuffle(map_results)
            
            if "reduce" in mapper_interface:
                if self.autobatch:
                    batched_kvlist2 = mrutils.splitseq(kvlist2, int(ceil(len(kvlist2)/(2*len(cores)))))
                    results = pool.map(pbsutils.task, [(x,mapper._reducelist, cq) for x in batched_kvlist2])
                    results = list(itertools.chain(*results))
                else:
                    results = pool.map(pbsutils.task, [(x,mapper._reduce, cq) for x in kvlist2])
                results, self.reducefailures = split_failures_from_results(results)
            else:
                results = kvlist2
            
            pool.close()
            pool.join()

            return mapper.gather(results)
        else:
            raise NotImplemented("Unknown mode, %s" % self.mode)

def split_failures_from_results(results):
    """Find the (key,value) pairs that did not get processed and return a list of
        failures and a list of good results with failures removed"""
    failures = [f for f in results if isinstance(f[1],MapperError)]
    cleanresults = [f for f in results if not isinstance(f[1],MapperError)]
        
    return cleanresults,failures

#------------------------------------#
# Map Functions
#------------------------------------#
class MapFcn(OrderedMap):
    def __init__(self):
        OrderedMap.__init__(self)
        
    def map(self,(k,v)):
        r = self._mapfcn(v)
        return k,r


def mrmap(fcn,itemlist,n,mode,maxtasks=None):
    mapjob = MapFcn()
    mapjob._mapfcn = fcn
    if maxtasks:
        mapjob.maxtasks = maxtasks
    MR = MapReduce(n=n,mode=mode)
    results = MR.run(mapjob,itemlist)
    return results

def lazymap(fcn,dataitr,n,mode,maxtasks=None,m=2):
    results = []
    
    islice = itertools.islice
    slice_length = m*n
    
    mapjob = MapFcn()
    mapjob._mapfcn = fcn
    if maxtasks:
        mapjob.maxtasks = maxtasks
    MR = MapReduce(n=n,mode=mode)

    sublist = list(islice(dataitr,slice_length))
    while len(sublist) > 0:
        results.extend(MR.run(mapjob,sublist))
        sublist = list(islice(dataitr,slice_length))
        mapjob.reset()
    return results

