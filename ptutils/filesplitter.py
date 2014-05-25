#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. the tools to created iterators to split log files
# History:
#     1. 2012/5/23 created

"""
文件iterator的构建工具， 将大的日志文件根据某些终止符或者key分割为组，方便处理逻辑
"""

import sys
import re

#----------------------------------------------------------------------
def split_file_by_ender(f, ender="\t", sep="\t", stripchar=None, maxline=0):
    """
    根据以ender开头的标志，将文件分割。ender可以是字符或者正则表达式。
    
        >>> segs = split_file_by_ender("input.txt", ender="\n")
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
       
        >>> endere = re.compile(r"\t|\n")
        >>> segs = split_file_by_ender("input2.txt", endere)
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
        
    1. newcookiesort的分隔行是"\t\n", 所以ender默认值为"\t".
    2. 为了前向兼容，返回分组的每个元素是一个列表，用sep对单行进行分割。
        
    @type f: string or object
    @param f: the file name or FileHandle for the input file
    @type ender: string or re
    @param ender: the ending tag/re for splitting
    @type sep: string
    @param sep: the seperator for different files of a line
    @type stripchar: char 
    @param stripchar: the char for line.strip([char]). 
                For example, strip="\n" is OK for line sep and value like "\t"+ "" + "\t"
    @type maxline: int
    @param maxline: number limitation of max lines. default 0 means no limit.
    @rtype: iterator
    @return: the iterator for file segments
    
    """
    if type(f) == type(""):
        fh = file(f)
    else:
        fh = f
        
    if type(ender) == type(""):
        ender = re.compile(ender)
        
    res = []
    for line in fh:
        if ender.match(line):
            if res:
                yield res
                res = []
        else:
            if maxline:
                if len(res) >= maxline:
                    yield res
                    del res
                    res = []
            res.append(line.strip(stripchar).split(sep))
    if res:   # the last rec may have no ender
        yield res   

#----------------------------------------------------------------------
def split_file_by_key(f, key_pos=0, sep="\t", stripchar=None, maxline=0):
    """
    根据每一行相同的key，将文件分割。更复杂寻求key的方法参考itertools.groupby
    
        >>> segs = split_file_by_key("input.txt", key_pos=1)
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
        
    返回分组的每个元素是一个列表，用sep对单行进行分割。
    
    目前不支持在hadoop上使用。 错误信息如下：
        
        >>> for lines in segs:
        >>> File "build/bdist.linux-x86_64/egg/ubsutils/filesplitter.py", line 94, in split_file_by_key
        >>> TypeError: 'builtin_function_or_method' object is not subscriptable
        
    @type f: string or object
    @param f: the file name or FileHandle for the input file
    @type key_pos: int
    @param key_pos: the idx of key field in line list
    @type sep: string
    @param sep: the seperator for different files of a line
    @type stripchar: char 
    @param stripchar: the char for line.strip([char]). 
                For example, strip="\n" is OK for line sep and value like "\t"+ "" + "\t"
    @type maxline: int
    @param maxline: number limitation of max lines. default 0 means no limit.
    @rtype: iterator
    @return: the iterator for file segments
    
    """
    if type(f) == type(""):
        fh = file(f)
    else:
        fh = f
            
    res = []
    last_key = None
    for line in fh:
        fields = line.strip(stripchar).split(sep)
        key = fields[key_pos]
        if key != last_key:
            if res:
                yield res
            res = [fields]
            last_key = key
        else:
            if maxline:
                if len(res) >= maxline:
                    yield res
                    del res
                    res = []
            res.append(fields)
    if res:
        yield res    


def split_file_with_kv(f, sep="\t", stripchar=None, maxline=0):
    """
    参数说明参考 split_file_by_key.
    与 split_file_by_key 完全一致，假定结果只有两个字段 k, v。
    iterator迭代产生的元素是一个tuple (k, vs).
    
    """
    if type(f) == type(""):
        fh = file(f)
    else:
        fh = f
            
    vs = []
    last_key = None
    for line in fh:
        k, v = line.strip(stripchar).split(sep, 1)
        if k != last_key:
            if vs:
                yield (last_key, vs)
            vs = [v]
            last_key = k
        else:
            if maxline:
                if len(vs) >= maxline:
                    yield (last_key, vs)
                    del vs
                    vs = []
            vs.append(v)
    if vs:
        yield (last_key, vs)    
    

#----------------------------------------------------------------------
def split_list_by_key(iterable, key_pos=0):
    """
    根据每一个元素相同的key，将大list分割为若干小list。 
    更复杂的key方法可以用itertools.groupby.
    
        >>> segs = split_list_by_key([1,2,2,1,1,3,2], key_pos=0)
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
        
    @type iterable: iterable
    @param f: the list or iterable object to be splitted
    @type key_pos: int
    @param ender: the position of the key for splitting
    @rtype: iterator
    @return: the iterator for list segments
    
    """
    
    res = []
    last_key = None
    for fields in iterable:
        key = fields[key_pos]
        if key != last_key:
            if res:
                yield res
            res = [fields]
            last_key = key
        else:
            res.append(fields)    
    yield res    

if __name__=='__main__':
    pass