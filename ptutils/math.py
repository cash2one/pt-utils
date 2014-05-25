#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. small math tools of my
 History:
     1. 14-5-22 下午3:11 : math.py is created.
"""

def median(data):
    """ return the median value of data

        @type data: string
        @param data: list
        @rtype: number
        @return: the median

    """
    ordered = sorted(data)
    n = len(ordered)
    if n % 2 == 1:
        v = ordered[(n-1)/2]
    else:
        i = n/2
        v = (ordered[i-1] + ordered[i])/2
    return v


if __name__=='__main__':
    pass

