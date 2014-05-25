#!/usr/bin/env python
#coding:gbk

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
    hadoop����reducer���㷨����
        http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Hadoop
    ע�⣺
        1. hash�����У�c����key��buffer���� int(*query). ֵ����[-128, 127], ����query���ַ�ָ��
        2. python char2int�ĺ�����ord����ֵ��[0,255]. ���Լ�������query�����߽������ܴ�����ʹ����struct.unpack������
            2.1 �����http://stackoverflow.com/questions/15334465/how-to-map-characters-to-integer-range-128-127-in-python
        
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
def hadooMap(key, total):
    """"""
    return get_reducer(key, total, type=0)
    
#----------------------------------------------------------------------
def KeyFieldBasedPartitioner(key, total):
    """"""
    return get_reducer(key, total, type=1)
    
    





