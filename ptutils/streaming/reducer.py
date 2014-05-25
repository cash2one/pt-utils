#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. ��streaming��ʽ�µĸ���Ԥ����reducer
 History:
     1. 2014/2/7 
"""



import sys
from ubsutils.filesplitter import split_file_with_kv

def intsum(ifh=sys.stdin, ofh=sys.stdout, sep="\t", stripchar=None):
    """
    ����������.
    
    @type sep: string
    @param sep: sep char of k-v
    @type stripchar: char
    @param stripchar: char for line.strip(char)
    """
    for (k, vs) in split_file_with_kv(ifh, sep=sep, stripchar=stripchar):
        
        if len(vs) == 1:
            print >> ofh, k + sep + vs[0]
            continue
        
        s = 0
        for v in vs:
            s += int(v)
        print >> ofh, k + sep + str(s)
    

def floatsum(ifh=sys.stdin, ofh=sys.stdout, sep="\t", stripchar=None):
    """
    �ο�intsum
    """
    for (k, vs) in split_file_with_kv(ifh, sep=sep, stripchar=stripchar):
        
        if len(vs) == 1:
            print >> ofh, k + sep + vs[0]
            continue
        
        s = 0
        for v in vs:
            s += float(v)
        print >> ofh, k + sep + str(v)


def vintsum(ifh=sys.stdin, ofh=sys.stdout, sep="\t", vsep="\t", stripchar=None):
    """
    int vector sum�� ��int list���
    
    @type sep: string
    @param sep: sep char of k-v
    @type vsep: string 
    @param vsep: sep char between values
    """
    for (k, vs) in split_file_with_kv(ifh, sep=sep, stripchar=stripchar):
        n = len(vs)
        if n == 1:
            print k + sep + vs[0]
            continue
        s = map(int, vs[0].split(vsep))
        l = len(s)
        for i in range(1, n):
            v = map(int, vs[i].split(vsep))
            for j in range(l):
                s[j] += v[j]
        print >> ofh, k + sep + vsep.join(map(str, s))

def vfloatsum(ifh=sys.stdin, ofh=sys.stdout, sep="\t", vsep="\t", stripchar=None):
    """
    float vector sum�� ��float list���
    
    @type sep: string
    @param sep: sep char of k-v
    @type vsep: string 
    @param vsep: sep char between values
    """
    for (k, vs) in split_file_with_kv(ifh, sep=sep, stripchar=stripchar):
        n = len(vs)
        if n == 1:
            print k + sep + vs[0]
            continue
        s = map(float, vs[0].split(vsep))
        l = len(s)
        for i in range(1, n):
            v = map(float, vs[i].split(vsep))
            for j in range(l):
                s[j] += v[j]
        print >> ofh, k + sep + vsep.join(map(str, s))


if __name__=='__main__':
    pass