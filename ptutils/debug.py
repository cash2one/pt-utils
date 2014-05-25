#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. һЩdebugʹ�õ�С����
# History:
#     1. 2013/5/12 


import sys
import inspect
import re

re_errp = re.compile(r"errp\((.+?)\)")
#----------------------------------------------------------------------
def errp(*args, **kwargs):
    """��debugʱ����stdout��ӡһ���������ɱ�����ֵ��
    
    ʹ�÷�����
    ========
        >>> errp(var1)
        >>> errp(var2, var5, var7)
    ע�⣬��Ҫ��һ���϶�ε��á��ᱨ��
        >>> errp(a) and errp(b)
        
    @type args: tuple
    @param args: ������Ҫ��ӡ��postional����
    @type stdout: FileHandle
    @param stdout: ��ӡָ����ļ������Ĭ�� sys.stderr
    @type sep: string 
    @param sep: ��������ķָ�����Ĭ�ϻ���.
    
    """
    stdout=sys.stderr
    sep="\n"
    
    if "stdout" in kwargs:
        stdout = kwargs["stdout"]
    if "sep" in kwargs:
        sep = kwargs["sep"]
    frame = inspect.currentframe()
    ret = None
    try:
        # context = ["errp(var2, var5)"]
        context = inspect.getframeinfo(frame.f_back).code_context
        if not context:  # often context returns None. Maybe the stack is too deep?
            print >> stdout, "error in get code context"
            sz = map(lambda x: " VAR ==> %s " % x, args)
            print >> stdout, sep.join(sz)            
            return True
            
        caller_lines = ''.join([line.strip() for line in context])
        var_string = re_errp.findall(caller_lines)
        if len(var_string) != 1:
            print >> stdout, "error usage of errp: %s" % context
            ret = False
        else:
            # x,y,z, stdout=open("myfile.txt", "w")
            # var_list = filter(lambda x: x.find("=") == -1, var_string[0].split(","))
            n = len(args)
            var_list = var_string[0].split(",")
            
            if len(var_list) < n:
                print >> stdout, "mismatch between formal and real variable: %s and %s " % (var_list, args)
                ret = False
            else:
                var_list = var_list[:n]
                sz = []
                for i in range(n):
                    sz.append(" %s ==> %s " % (var_list[i], args[i]))
                    
                print >> stdout, sep.join(sz)
                ret = True
    finally:
        del frame    
    
    return ret
    


    
