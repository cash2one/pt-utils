#!/usr/bin/env python
#coding:utf-8

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. utils about to find the reducer
     2. http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Hadoop
 History:
     1. 2013/5/13 
"""



import sys
from struct import unpack

def get_reducer(key, total, type):
    """
    hadoop分配reducer的算法见：
        http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Hadoop
    注意：
        1. hash函数中，c引用key的buffer内容 int(*query). 值域在[-128, 127], 其中query是字符指针
        2. python char2int的函数是ord，但值域[0,255]. 所以计算中文query，两者结果差异很大。这里使用了struct.unpack函数。
            2.1 详见：http://stackoverflow.com/questions/15334465/how-to-map-characters-to-integer-range-128-127-in-python
        3. hive中对应的代码：(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        
    @type key: string 
    @param key: key to be partitioned
    @type total: int
    @param total: number of reducers
    @type type: int
    @param type: 0-hadoopmap, 1-keyfieldbasedpartitioner
    @rtype: int
    @return: the id of reducer in [0, total)
    
    """
    h = 1 - type
    for c in key:
        # h = 31 * h + ord(c)
        h = 31 * h + unpack("b",c)[0]

    # 2147483647 = 0b11111111111111111111 (31-bit)
    return (h & 2147483647) % total


#----------------------------------------------------------------------
def hadoopMap(key, total):
    """"""
    return get_reducer(key, total, type=0)
    
#----------------------------------------------------------------------
def KeyFieldBasedPartitioner(key, total):
    """"""
    return get_reducer(key, total, type=1)
    
    





