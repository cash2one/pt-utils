#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. the tools to created iterators to split log files
# History:
#     1. 2012/5/23 created

"""
�ļ�iterator�Ĺ������ߣ� �������־�ļ�����ĳЩ��ֹ������key�ָ�Ϊ�飬���㴦���߼�
"""

import sys
import re

#----------------------------------------------------------------------
def split_file_by_ender(f, ender="\t", sep="\t", stripchar=None, maxline=0):
    """
    ������ender��ͷ�ı�־�����ļ��ָender�������ַ�����������ʽ��
    
        >>> segs = split_file_by_ender("input.txt", ender="\n")
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
       
        >>> endere = re.compile(r"\t|\n")
        >>> segs = split_file_by_ender("input2.txt", endere)
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
        
    1. newcookiesort�ķָ�����"\t\n", ����enderĬ��ֵΪ"\t".
    2. Ϊ��ǰ����ݣ����ط����ÿ��Ԫ����һ���б���sep�Ե��н��зָ
        
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
    ����ÿһ����ͬ��key�����ļ��ָ������Ѱ��key�ķ����ο�itertools.groupby
    
        >>> segs = split_file_by_key("input.txt", key_pos=1)
        >>> for lines in segs:
        >>>    for fields in lines:
        >>>         print fields
        
    ���ط����ÿ��Ԫ����һ���б���sep�Ե��н��зָ
    
    Ŀǰ��֧����hadoop��ʹ�á� ������Ϣ���£�
        
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
    ����˵���ο� split_file_by_key.
    �� split_file_by_key ��ȫһ�£��ٶ����ֻ�������ֶ� k, v��
    iterator����������Ԫ����һ��tuple (k, vs).
    
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
    ����ÿһ��Ԫ����ͬ��key������list�ָ�Ϊ����Сlist�� 
    �����ӵ�key����������itertools.groupby.
    
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