#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 操作（encode/decode) idl 文件的工具
# History:
#     1. 2011/5/16 


import sys

import re

re_char = re.compile(r"[^\w\-_]")

def dtrim(s, tag="broken", maxlen=16):
    """
    cube的维度数据类似枚举类型，但日志中有很多非法字符，造成很多无意义的维度值。将这些值转换为tag。

        >>> dtrim("ok", 'broken')
            ok
        >>> dtrim("xxkj-<script>alert()</script>", 'broken')
            broken
        >>> dtrim("abkdaslfjlsdkflsdafjls-dsalfjlsd", 'broken')
            broken    
    """
    if len(s) > maxlen:
        return tag
    if re_char.search(s):
        return tag
    return s
    
    
#----------------------------------------------------------------------
def py2idltype(t):
    """
    将python type 转换为 idl的基本数据类型（字符串)
    目前不覆盖复杂类型：数组，struct等
    """
    if t == type(1):
        return 'int32_t'
    elif t == type(1.0):
        return 'foat'
    elif t == type(''):
        return 'string'
    else:
        raise Exception
    
    

if __name__=='__main__':
    # TODO: 开发或者import一些第三方的工具类，操作idl文件
    raise Exception
    