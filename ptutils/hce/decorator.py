#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose:
#     1. hce框架的decorator函数
# History:
#     1. 2013/4/15

import sys
import inspect
import os

_mapper_decorators = []   # 登记mapper的decorators，避免冲突
_cleanup_decorators = []  # 登记mapper cleanup的decorators，避免冲突

_is_emit_any = False   # 保证emit2string decorator只使用一次

_lastkey = None
_section = []
_list_sum = {}


#----------------------------------------------------------------------
def _any2string_decorator_emit(emit, sep):
    """
    @type emit: callable
    @param emit: emit function.
    @type sep: string
    @param sep: separator string like "\t"
    
    """
    def wrapper(k, v):
        k = str(k)
        if type(v) == str:
            emit(k, v)
        elif type(v) == list:
            emit(k, sep.join(map(str, v)))
        else:  # int or float
            emit(k, str(v))
    return wrapper
    

def EMIT_ANY(sep="\t"):
    """
    allow emit usage like:
        emit("key", [1, 2, 3])
    @rtype: callable
    @return: the decorated emit function
    """
    global _is_emit_any
    if not _is_emit_any:
        functions = sys._getframe(1).f_globals
        # print functions
        if 'emit' not in functions:
            raise Exception("emit is not found!")
        emit = functions['emit']         
        functions['emit'] = _any2string_decorator_emit(emit, sep)
        
        _is_emit_any= True        
        return functions['emit']

        
        

#----------------------------------------------------------------------
def _mapper_lock():
    """
    prohibit calling MAPPER decorators more than once.
    """
    global _mapper_decorators
    _mapper_decorators.append(sys._getframe(1).f_code.co_name)
    if len(_mapper_decorators) > 1:
        raise Exception("duplicated mapper decorators: %s" % _mapper_decorators)

#----------------------------------------------------------------------
def _cleanup_reg():
    """
        MAPPER_OUTPUT_COMBINE()
        MAPPER_INPUT_KEYSPLIT()
        When called both, the must be in strict order.
    """
    global _cleanup_decorators
    _cleanup_decorators.append(sys._getframe(1).f_code.co_name)
    
#----------------------------------------------------------------------
def _cleanup_lock():
    """
        MAPPER_OUTPUT_COMBINE()
        MAPPER_INPUT_KEYSPLIT()
        When called both, the must be in strict order.
    """
    global _cleanup_decorators
    _cleanup_decorators.append(sys._getframe(1).f_code.co_name)
    if len(_cleanup_decorators) > 1:
        raise Exception("decorator %s must be called after combiner %s" % (_cleanup_decorators[0], _cleanup_decorators[1]))



#----------------------------------------------------------------------
def _keysplit_decorator_mapper(mapper, sep="\t", idx=0):
    """"""
    def wrapper(k, v):
        # global sep
        # global idx
        global _lastkey
        global _section
        data = v.strip().split(sep)
        if len(data) < idx:
            raise ValueError("line don not have %d fields : \n    %s" % (idx, v))
        key = data[idx]
        if key == _lastkey:
            _section.append(data)
        else:
            if _lastkey is not None:
                mapper(_lastkey, _section)
            _lastkey = key
            _section = [data]

    return wrapper

#----------------------------------------------------------------------
def _lastrec_decorator_cleanup(mapper_cleanup, mapper):
    """"""
    def wrapper(*args, **kwargs):
        # global sep
        # global idx
        global _lastkey
        global _section
        if _section:
            mapper(_lastkey, _section)
            _lastkey = None
            _section = []
        mapper_cleanup(*args, **kwargs)

    return wrapper

#----------------------------------------------------------------------
def MAPPER_INPUT_KEYSPLIT(idx=0, sep="\t"):
    """
    It must be called in the mapper_setup function.
    """
    _mapper_lock()
    _cleanup_reg()
    
    functions = sys._getframe(1).f_globals
    # print functions

    if 'mapper' not in functions:
        raise Exception("mapper is not found!")
    if 'mapper_cleanup' not in functions:
        def f():
            pass
        functions['mapper_cleanup'] = f

    raw_mapper = functions['mapper']
    functions['mapper'] = _keysplit_decorator_mapper(raw_mapper,sep=sep, idx=idx)
    raw_mapper_cleanup = functions["mapper_cleanup"]
    functions["mapper_cleanup"]= _lastrec_decorator_cleanup(raw_mapper_cleanup, raw_mapper)

#----------------------------------------------------------------------
def _flagsplit_decorator_mapper(mapper, flag=""):
    """"""
    def wrapper(k, v):
        # global sep
        # global idx
        global _lastkey
        global _section
        if v == flag:
            mapper("", _section)
            _section = []
        else:
            _section.append(v)

    return wrapper


#----------------------------------------------------------------------
def MAPPER_INPUT_FLAGSPLIT(flag=""):
    """
    """
    _mapper_lock()
    _cleanup_reg()
    functions = sys._getframe(1).f_globals

    if 'mapper' not in functions:
            raise Exception("mapper is not found!")
    if 'mapper_cleanup' not in functions:
        def f():
            pass
        functions['mapper_cleanup'] = f    

    raw_mapper = functions['mapper']
    functions['mapper'] = _flagsplit_decorator_mapper(raw_mapper, flag)
    raw_mapper_cleanup = functions["mapper_cleanup"]
    functions["mapper_cleanup"]= _lastrec_decorator_cleanup(raw_mapper_cleanup, raw_mapper)
    
#----------------------------------------------------------------------
def _combine_decorator_emit(_type=int, sep="\t"):
    """
    """
    def wrapper(k, v):
        # global sep
        # global idx
        global _list_sum
        if type(v) == list:
            pass
        elif type(v) == str:
            v = v.split(sep)
        else:
            v = [v]
        _type_v = map(_type, v)
        if k in _list_sum:
            n = len(_type_v)
            for i in xrange(n):
                _list_sum[k][i] += _type_v[i]
        else:
            _list_sum[k] = _type_v

    return wrapper


#----------------------------------------------------------------------
def _combine_decorator_cleanup(mapper_cleanup, raw_emit, sep="\t"):
    """"""
    def wrapper(*args, **kwargs):
        # global sep
        # global idx
        global _list_sum
        for k, v in _list_sum.iteritems():
            raw_emit(str(k), sep.join(map(str, v)))
        _list_sum = {}
        mapper_cleanup(*args, **kwargs)

    return wrapper

#----------------------------------------------------------------------
def MAPPER_OUTPUT_COMBINE(_type=int, sep="\t"):
    """"""

    _cleanup_lock()    
    # only decorate emit in mapper
    if os.environ['mapred_task_is_map'] == 'true':
        functions = sys._getframe(1).f_globals
        if 'mapper_cleanup' not in functions:
            def f():
                pass
            functions['mapper_cleanup'] = f
        raw_emit = functions['emit']  
        functions['emit'] = _combine_decorator_emit(_type=_type, sep=sep)
        raw_mapper_cleanup = functions["mapper_cleanup"]
        functions["mapper_cleanup"]= _combine_decorator_cleanup(raw_mapper_cleanup, raw_emit, sep)



