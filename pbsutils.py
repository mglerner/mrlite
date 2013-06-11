#!/usr/bin/env python

import os
import cPickle as pickle
import time


def task(args):
    data,function,corequeue = args
    core = corequeue.get()
    fns = ('/home/abak/src/BioTop/scripts/run-function-on-data.py',
           '/home/mglerner/coding/comptop/biotop/scripts/run-function-on-data.py',
           )
    for fn in fns:
        if os.path.isfile(fn):
            break
        
    result = runViaSSH(core,fn,data,function)
    corequeue.put(core)
    return result
    
    
def parseNodeFile():
    """parse the PBS node file

    we return a list of cores and a dictionary mapping node names
    to the number of processors available on that node. e.g.
    nodes = {'as1': 8, 'as2': 8, 'as3': 4}
    The node names are whatever we find in the nodefile. Typically
    they'll be FQDNs.
    """
    nfName = os.getenv('PBS_NODEFILE')
    if nfName is None:
        raise Exception('You must have the PBS_NODEFILE environment variable defined to run in PBS mode')
    try:
        cores = [line.strip() for line in file(nfName) if line.strip()]
    except IOError:
        raise Exception('Could not read PBS nodefile %s'%nfname)
    nodes = {}
    for core in cores:
        if core in nodes:
            nodes[core] += 1
        else:
            nodes[core] = 1
    return cores,nodes
    
    
def runViaSSH(core, scriptName, data, function, pickleprotocol=-1, removePicklesWhenDone=True):
    iname,oname,fname = getPickleNames()
    # ssh as1 script.py -n 8 -i i.pickle -o o.pickle -f mapfn.pickle
    f = file(iname,'w')
    pickle.dump(data,f,pickleprotocol)
    f.close()

    f = file(fname,'w')
    pickle.dump(function,f,pickleprotocol)
    f.close()

    # ssh as1 script.py -i i.pickle -o o.pickle -f function.pickle
    command = 'ssh %s %s -i %s -o %s -f %s'%(core,scriptName,iname,oname,fname)
    os.system(command)
    f = file(oname)
    results = pickle.load(f)
    if removePicklesWhenDone:
        for fn in iname,oname,fname:
            os.remove(fn)
    return results
    
    
def getPickleNames():
    """Get names of pickle files to use.

    For simplicity, these are created in os.env('PBS_O_WORKDIR')
    and use the current PID to make them 'unique' that's not
    particularly robust, but it's good enough for us for now, and
    we don't have to worry about people stomping all over our
    directories. Separate jobs should be run in separate
    directories.

    We check that the files don't exist and then create empty
    placeholder files before returning the names.

    We return absolute paths because the results are expected to
    be passed to other nodes. 
    """
    # don't want an unbounded while because some dumb programming
    # error could make it loop forever.
    ji = os.getenv('PBS_JOBID')
    if ji is None:
        ji = 'PBSJOB1'
    wd = os.getenv('PBS_O_WORKDIR')
    if wd is None:
        wd = '/tmp/'
    def getfn(txt,i):
        return os.path.join(os.path.abspath(wd),'%s_%s_%s.pickle'%(ji,i,txt))
    for i in range(10000):
        iname,oname,fname = getfn('input',i),getfn('output',i),getfn('function',i)
        if os.path.isfile(iname) or os.path.isfile(oname):
            continue
        fi,fo,fm = file(iname,'w'), file(oname,'w'), file(fname,'w')
        fi.close()
        fo.close()
        fm.close()
        return os.path.abspath(iname),os.path.abspath(oname),os.path.abspath(fname)
    raise IOError('Could not make new pickle names')
