#!/usr/bin/env python

from collections import defaultdict
from math import ceil
import itertools, logging, sys, types, time


from . mrutils import allow_method_pickles

allow_method_pickles()

class MRJob(object):
    def __init__(self,masterinfo=None):
        self.mapping = True
        self.status = "initialized to self.mapping %s" % self.mapping
        self.marked_localdata = []
        self.marked_sharedarray = []
        self.maxtasks = None #max number of maps to do in a subprocess before process is restarted to clear memory
        self.data_len = 0

    def reset(self):
        self.mapping = True
        self.status = "initialized to self.mapping %s" % self.mapping
        #self.marked_localdata = []
        #self.marked_sharedarray = []
        self.maxtasks = None #max number of maps to do in a subprocess before process is restarted to clear memory
        self.data_len = 0        

    def __getstate__(self):
        state = self.__dict__.copy()
        for k in self.marked_localdata: 
            state[k] = None
        return state
        
    def implements(self):
        overriden = []
        for method in ("partition","map","shuffle","reduce","gather","handle_map_error","handle_reduce_error"):
            this_method = getattr(self, method)
            base_method = getattr(MRJob, method)
            if this_method.__func__ is not base_method.__func__:
                overriden.append(method)
        return overriden
    
    @staticmethod
    def _convert_to_shared_array(localarray):
        import sharedmem as shm
        arr = shm.shared_empty(localarray.shape, localarray.dtype, order="C")
        arr[...] = localarray
        return arr
        
    def mark_local_data(self,*args):
        """items marked as local data will not be pickeled and sent to other
            processes/nodes.  They will be shared if you're using threading or 
            serial processing"""
        self.marked_localdata = args
        
    def mark_shared_array(self, *args):
        """args: list of member variables to convert to shared arrays
        
        Items marked as shared arrays are converted to shared memory if arrays
        for smp processing"""
        self.marked_sharedarray = args
        
    def prepare_shared_data(self):
        """Turn items marked shared arrays into shared arrays.  Note that this means that 
        these items cannot be pickeled and run on a machine that doesn't share ram addressing 
        with the local machine
        
        Warning! If you are going to run on a cluster you should not call this function. It will not work."""
        for item in self.marked_sharedarray:
            setattr(self,item,self._convert_to_shared_array(getattr(self,item)))
        pass
                    
    def report(self,msg=""):
        """report mapper status. Not Implemented."""
        if msg: 
            print "Mapper: %s" % msg
        else:
            print "Mapper status: %s" % self.status

    def partition(self,data):
        """Input: 
            data: to be split into a list of key,value pairs
            n: number of processors we're using to be used as a hint for how to chuck the data"""
        for i,item in enumerate(data):
            self.data_len += 1
            yield (i,item)
    
    def _map(self,kv):
    	""" This wrapper around the user defined map function is what is actually called by MapReduce
        so that we can track errors and report them back.  Note that we catch all exceptions and
        silently fail"""
        try:
            return self.map(kv)
        except KeyboardInterrupt:
            raise
        except:
            return self.handle_map_error(kv)
    
    def _maplist(self,kvlist):
    	"""Like _map but used for batch processing"""
        return [self._map(kv) for kv in kvlist]
    
    def map(self,kv):
        """Input: 
            a tuple consisting of a single (key,value) to be processed
            Return: a new list of (key,value}) pairs"""
        return kv
    
    def handle_map_error(self,kv):
        logging.exception('')
        return kv,MapperError('',kv)
    
    def shuffle(self,tlist):
        """Input: 
            keyvalue:A list of (key,value) pairs returned by the map operation
            n: a number hinting at how big to partition if necessary. 
        Typically Reduce groups all the values with the same key for the reduce step
        
        d = defaultdict(list)
        for t in tlist:
            d[t[0]].append(t[1])
        return d.iteritems()"""
        return tlist

    def _reduce(self,kv):
    	"""See documentation for _map"""
        try:
            return self.reduce(kv)
        except KeyboardInterrupt:
            raise
        except:
            return self.handle_reduce_error(kv)
    
    def _reducelist(self,kvlist):
        return [self._reduce(kv) for kv in kvlist]
        
    def reduce(self,kv):
        """ Typically process list of values into value and return (key,value)"""        
        return kv #(key,value)
        
    def handle_reduce_error(self,kv):
        logging.exception('')
        return kv,MapperError('',kv)
        
    def gather(self,results):
        """Do something with the reduced results. Write them to disk perhaps. Or make a dictionary.
            return {}.update(results) etc.  The last function called by the MapReduce system"""
        return results
        
    def getmap(self):
        return self._map

class MapperError(object):
    def __init__(self,e,(key,value)):
        self.exception = e
        self.kv = (key,value)


class DistanceMatrix(MRJob):
    def __init__(self,save_intermediate=False):
        MRJob.__init__(self)
        self.save_intermediate=save_intermediate
        self.datalength = 0
    
    def partition(self,data):
        """Input:
            data: A list of points that we want to calculate the distance between.  
                Can be arbitrary objects as long as the metric can process pairs
        Returns: (key,value) where value is a list of compound pairs to process and key is  """
        self.datalength = len(data)
        for i,ci in enumerate(data):
            for j,cj in enumerate(data):
                if j >= i: break
                yield ((i,j),(ci,cj))
                
    def metric(self,c1,c2):
        raise NotImplemented("Subclass must define a metric method") 
      
    def map(self,(key,value)):
        #print("processing key: %s" % str(key))
        return (key,self.metric(value[0],value[1]))
             
    def gather(self, results):
        distances = defaultdict(dict)
        for (key,value) in results:
            distances[key[0]][key[1]] = value
        return distances
        

class OrderedMap(MRJob):
    def __init__(self):
        MRJob.__init__(self)
        
    def gather(self,results):
        r = [None]*self.data_len
        for i,item in results: r[i] = item
        return r

Map = OrderedMap

        
#
# Some dummy classes to test things
#

class PhonyDistributedMap(Map):
    def __init__(self):
        Map.__init__(self)
        
    def map(self,(k,v)):
        return v
    
class PhonyDistributedDistance(DistanceMatrix):
    def __init__(self):
        DistanceMatrix.__init__(self)
    
    def metric(self,c1,c2):
        time.sleep(0.1)
        len1,len2 = len(str(c1)),len(str(c2))
        if len1 > len2: return .0000001
        elif len2 > len1: return .0000002
        else: return .0000003


