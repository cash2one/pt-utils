#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. the map/reduce job of hce framework
# History:
#     2. 2012/06/04 update
#         1. remove several high level step struct
#         2. add export_macros method for MRStep
#     1. 2012/02/03 created.

"""
可复用的pyhce框架的mapper/reducer代码。
"""


import sys
import datetime

try:
    # The pyhce.cc will do this for top level script automatically
    # Mannually do it here
    from hceutil import emit
except ImportError:
    pass


########################################################################
class MRStep:
    """abstract class for pyHCE map or reduce step.
    
    Three methods are list here. They are the counterparts of pyhce mapper/reducer. 
        1. step_setup
        2. step (required)
        3. step_cleanup
    Mapper/Reducer are not combined because they are totally isolated in MapReduce
    Framework.
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        
    #----------------------------------------------------------------------
    def step_setup(self):
        """ """
        pass
    
    #----------------------------------------------------------------------
    def step(self, k, v):
        """ """
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def step_cleanup(self):
        """ """
        pass
    
    #----------------------------------------------------------------------
    def export_macros(self, local, type_):
        """
        export class method into global namespace

        @type local: dict
        @param local:  the local() object from parent scope
        @type type_: string
        @param type_:  "m"/"mapper" for mapper and "r"/"reducer" for reducer
        
        """
        
        if type_ in ('m', 'mapper'):
            local['mapper_setup'] = self.step_setup
            local['mapper'] = self.step
            local['mapper_cleanup'] = self.step_cleanup
        elif type_ in ('r', 'reducer'):
            local['reducer_setup'] = self.step_setup
            local['reducer'] = self.step
            local['reducer_cleanup'] = self.step_cleanup
        else:
            raise ValueError("unknown type: %s" % type_)
            
        return       
        
        
        
    
########################################################################
class MRStepCombinedParser(MRStep):
    """
    Load a parser, read the log line by line, _compute and put the result
    in _output.
    
    usage:
    =====
    
        Derive a new class from the Base
        
        >>> class Demo(MRStepCombineParser):
        >>>    def __init__(self):
        >>>        MRStepCombineParser.__init__(self, recordz.UIlogRecord, verbose=100)
        >>>
        >>>    def _compute(self):
        >>>        do some computing
        
        
    
    """

    #----------------------------------------------------------------------
    def __init__(self, cls, verbose_n=0):
        """
        @type cls: object
        @param cls: The record class object
        @type verbose_n: int
        @param verbose_n: print a mark to stderr every verbose_n records. Do not print if n==0.
        
        """
        self._parser_cls = cls
        self.verbose_n = verbose_n
        self._verbose_counter = 0
        
    #----------------------------------------------------------------------
    def step_setup(self):
        """
        """
        self.rec = self._parser_cls()
        self._output = {}
        
    #----------------------------------------------------------------------
    def _compute(self):
        """
        Compute a complete record.
          1. You can put the results in _output as combiner
          2. or emit directly.
          
          >>> self._output[k] = v
          >>> emit(k, v)
          
        """
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def step(self, k, v):
        """
        这里有一些问题，如果是多行的log，最后一个record没有计算，在step_cleanup中计算，单行日志会重复计算。
        """
        if self.verbose_n:
            self._verbose_counter += 1
            if self._verbose_counter % self.verbose_n == 0:
                print >> sys.stderr, "%d records done ..." % self._verbose_counter
            if self.rec.parseLine(v) == self.rec.LOG_FINISHED:
                self._compute()
        else:
            try:
                if self.rec.parseLine(v) == self.rec.LOG_FINISHED:
                    self._compute()
            except (KeyError, ValueError, IndexError):
                pass
            
     
    #----------------------------------------------------------------------
    def step_cleanup(self):
        """Compute last record and emit the data.
        """
        # last record for multi-line record. The processing is duplicated for single-line record.
        self._compute()
        
        for (k,v) in self._output.iteritems():
            if type(v) == type([]):
                emit(k, "\t".join(map(str,v)))
            else:
                emit(k, str(v))
                
        

        
def reducer_cat(k,vs):
    """
    simplely cat   
    """
    for v in vs:
        emit(k,str(v))
      

#----------------------------------------------------------------------
def reducer_simplesum(k, vs):
    """
    sum the counts of the key
    """
    s = 0
    for v in vs:
        s += float(v)
    emit(k, str(s))
    
#----------------------------------------------------------------------
def reducer_listsum(k, vs):
    """
    Sum the list values of the key.
    
    length of list is calculated from the first data line.
    
    Note that vs is of type hceutil.ReduceValues, which is not subscriptable.
    
    """
    s = []
    slen = 0
    for v in vs:
        delta = map(float, v.split("\t"))
        if s :
            for i in range(slen):
                s[i] += delta[i]
        else :
            s = delta
            slen = len(s)
    
    emit(k, "\t".join(map(str, s)))
    
#----------------------------------------------------------------------
def reducer_count(k, vs):
    """
    count the occurency of the key
    """
    c = 0
    for v in vs:
        c += 1
    emit(k, str(c))
    
