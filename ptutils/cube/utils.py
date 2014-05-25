#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. ������encode/decode) idl �ļ��Ĺ���
# History:
#     1. 2011/5/16 


import sys

import re

re_char = re.compile(r"[^\w\-_]")

def dtrim(s, tag="broken", maxlen=16):
    """
    cube��ά����������ö�����ͣ�����־���кܶ�Ƿ��ַ�����ɺܶ��������ά��ֵ������Щֵת��Ϊtag��

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
    ��python type ת��Ϊ idl�Ļ����������ͣ��ַ���)
    Ŀǰ�����Ǹ������ͣ����飬struct��
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
    # TODO: ��������importһЩ�������Ĺ����࣬����idl�ļ�
    raise Exception
    