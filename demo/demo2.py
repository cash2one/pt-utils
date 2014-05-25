#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. test
 History:
     1. 2013/5/20 
"""


from ubsutils.pipe.router import router

@router("abc")
def process_abc_data(a=1, b=3, c=2):
    """
    this is my abc data, blabla bla
    """
    print a, b, c
    
@router("bcd")
def copy_bcd_data(input, output):
    """
    copy input into output
    """
    print >>file(output, "w"), file(input).read()

if __name__=='__main__':
    router.main()