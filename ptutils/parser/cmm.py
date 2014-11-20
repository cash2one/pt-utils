#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose: 
     1. 与cmm整理配置相关的工具和函数
 History:
     1. 2014/11/19 19:28 : cmm.py is created.
"""



import sys
import os

import yaml

def cm2category(obj):
    """
        将cmm配置的对象转换为cm到类别（cm）的映射。
        @type obj: string or k
        @param obj: x
        @rtype: string
        @return: x
    """
    kv = {}
    if type(obj) == type(""):
        obj = yaml.load(open(obj))

    for c1 in obj:
        c1_name = c1['ch_name']
        for c2 in c1['subs']:
            c2_name = c2['ch_name']
            for req in c2['reqs']:
                cm = req['cm_name']
                if c1_name and c2_name: # 不合理， 非法配置内容直接忽略
                    kv[cm] = c1_name + '-' + c2_name
    return kv


if __name__=='__main__':
    pass
