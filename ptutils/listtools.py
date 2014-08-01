#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. small list relatd tools
 History:
     1. 14-08-01 下午13:35 : listtools.py is created.
"""

def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

if __name__=='__main__':
    pass

