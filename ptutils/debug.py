#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 一些debug使用的小工具
# History:
#     1. 2013/5/12 


import sys
import inspect
import re

re_errp = re.compile(r"errp\((.+?)\)")
#----------------------------------------------------------------------
def errp(*args, **kwargs):
    """在debug时，向stdout打印一个或者若干变量的值。
    
    使用方法：
    ========
        >>> errp(var1)
        >>> errp(var2, var5, var7)
    注意，不要在一行上多次调用。会报错
        >>> errp(a) and errp(b)
        
    @type args: tuple
    @param args: 任意需要打印的postional变量
    @type stdout: FileHandle
    @param stdout: 打印指向的文件句柄，默认 sys.stderr
    @type sep: string 
    @param sep: 多个变量的分隔符，默认换行.
    
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
    


    
