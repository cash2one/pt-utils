#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# History:
#     1. 2012/3/3 created

"""
  将文本转换为内存词典或者shelve对象的一些工具函数。
  
"""

import sys
import shelve
import random


#----------------------------------------------------------------------
def flat2dict(fn, sep="\t", override=True, header=0):
    """
    Create a memory dict structure from flat file.
    From : key val ==> d[key] = val(None)

    The parameters are less but with the same meaning with function 
    create_dict_from_flat, which, with nest-structured value, will consume a 
    large amount of memory when key number increase.  

    """
    res = {}
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        for i in range(header):
            fh.next()
        for line in fh:
            fs = line.strip().split(sep, 1)
            k, v = fs[0], None
            if len(fs) == 2:
                v = fs[1]            
            if k not in res:
                res[k] = v
            else:
                if override:
                    res[k] = v
        fh.close()
    
    return res

#----------------------------------------------------------------------
def flat2dict_ext(fn, kpos=0, vpos=[], sep="\t", override=True, header=0):
    """Create a memory dict structure from flat file.
    From : key val ==> d[key] = val
    
    @type fn: list
    @param fn: list of flat file names 
    @type kpos: int
    @param kpos: the fields position of the key. default to 0
    @type vpos: list
    @param vpos: The postion list of the values. default to empty.
    @type override: bool
    @param override: Whether to override the values if the keys are duplicated.
    @type header: int
    @param header: the number of header lines (to be skipped). default to 0
    @rtype: dict
    @return: the desired dict
    
    """
    res = {}
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        for i in range(header):
            fh.next()
        for line in fh:
            fs = line.strip().split(sep)
            k = fs[kpos]
            if vpos:
                v = map(lambda x:fs[x], vpos)
            else:
                v = fs[1:]
            if k not in res:
                res[k] = v
            else:
                if override:
                    res[k] = v
        fh.close()
    
    return res



#----------------------------------------------------------------------
def flat2shelve(fn, kpos=0, vpos=[], sep="\t", override=True, shn=None):
    """
    Create a shelve object from flat file.
    From : key val ==> d[key] = val
    
    @type fn: list
    @param fn: list of flat file names
    @type kpos: int
    @param kpos: the fields position of the key. default to 0
    @type vpos: list
    @param vpos: The postion list of the values. default to empty.
    @type override: bool
    @param override: Whether to override the values if the keys are duplicated.
    @type shn: string
    @param shn: shelve file name.    
    @rtype: tuple
    @return: (shelve object, shelve file name). 
    
    """

    if type(fn) == type(""):
        fn = [fn]
    if shn is None:
        shn = "shelve_%s" % random.random()
        
    sh = shelve.open(shn, "c")
    for f in fn:
        fh = file(f)
        while True:
            line = fh.readline()
            if not line:
                break
            fs = line.strip().split(sep)
            k = fs[kpos]
            if vpos:
                v = map(lambda x:fs[x], vpos)
            else:
                v = fs[1:]
            if k not in sh:
                sh[k] = v
            else:
                if override:
                    sh[k] = v
        fh.close()
    
    return sh, shn
    

# backend compatibility
create_simple_dict_from_flat = flat2dict
create_dict_from_flat = flat2dict_ext
create_shelve_from_flat = flat2shelve

if __name__=='__main__':
    pass