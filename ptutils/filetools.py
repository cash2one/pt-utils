#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# History:
#     1. 2012/4/5 created

"""
  文件（名）处理的一些小工具。
"""
import sys
import random
import os
import re
import time

#----------------------------------------------------------------------
def swap(a, b, suf=random.random()):
    """
    exchange the names of two file.
    """
    if not os.path.exists(a) or not os.path.exists(b):
        raise Exception("path %s and %s are not ready!" % (a, b))
    tmp = "%s.%s" % (a, suf)
    os.rename(a, tmp)
    os.rename(b, a)
    os.rename(tmp, b)
    
#----------------------------------------------------------------------
def _find_opening_pid(fn, pid):
    """辅助函数，  在pid进程或者所有进程（pid=None)的打开文件中寻找fn
    
    @rtype: tuple
    @return:  (real_pid, real_link), 如果没找到，返回(None)
    """
    # /path/2//my//a/ --> /path/2/my/a
    fn = os.path.normpath(fn)
    
    _dirs = []
    if pid is not None:
        d = '/proc/'+str(pid)+'/fd'
        if os.access(d,os.R_OK|os.X_OK):
            _dirs.append(d)
    else:
        for f in os.listdir('/proc'):
            d = '/proc/%s/fd' % f
            if os.access(d, os.R_OK|os.X_OK):
                _dirs.append(d)
    if not _dirs:
        return (None, None)  # 空数据
    

    # re_pipe = re.compile(r'pipe:\[\d+\]')
    # re_socket = re.compile(r'socket:\[\d+\]')
    cur_link = ''
    cur_file = ''
    for d in _dirs:
        for fds in os.listdir(d):
            # ll /proc/21263/fd
            # l-wx------  1 work work 64  5月 12 15:38 1 -> /home/work/pengtao/projects/20130130-zhixin-evaluation/bin/tmp
            for fd in fds:
                cur_link = os.path.join(d, fd)
                try:
                    cur_file = os.readlink(cur_link)
                    if cur_file == fn:
                        m = re.match("/proc/(\d+)/fd", cur_link)
                        return (m.group(1), cur_link)
                except OSError as err:
                    if err.errno == 2:     
                        continue
                    else:
                        raise(err)
    
    return (None, None)

    
#----------------------------------------------------------------------
def readline_opened_file(fn, pid=None, sleep=5):
    """读取一个正在被打开（写入）的文件（比如log），直到文件完全关闭（log输出结束）.
    
    假定只有一个进程打开目标文件，否则太复杂。
    基本方法，
        1. 打开文件，读取，直到没有数据。记录下当前的位置。
        2. 检查pid进程（None则遍历所有进程，很耗时）所有打开的文件，如果目标fn存在，说明新数据还会写入。重新打开，从上次结束点开始重新读新数据。
           2.1 fn文件不能太大，因为需要反复的读取。
        3. 重复1和2，直到文件没有被其他进程访问（已经关闭）
    
    因为依赖/proc信息，所以函数只能在unix上执行。其他平台貌似可以借助psutil实现，参考：
    http://stackoverflow.com/questions/11114492/check-if-a-file-is-not-open-not-used-by-other-process-in-python
    
    注意检查文件是否被其他进展打开的这种操作可能造成死锁。
    
    @type fn: string
    @param fn: 目标文件名
    @type sleep: int
    @param sleep: 重复检查的周期，默认5s
    @type pid: int
    @param pid: 目标进程号，如果没有，则遍历所有进程寻找目标进程
    @rtype: iterator
    @return: 给出目前已经存在数据的iterator，如果没有新数据，则阻塞当前进程。
    
    """
    fn = os.path.normpath(fn)
    (real_pid, real_link) = _find_opening_pid(fn, pid)

    pos = 0
    if real_pid :
        # 这里不检查fn是否存在，否则可能死锁。
        while os.path.exists(real_link) and os.readlink(real_link) == fn:
            fh = open(fn)
            all_lines = fh.readlines()
            for l in all_lines[pos:]:
                yield l
                pos += 1
            fh.close()


            time.sleep(sleep)

        
    fh = open(fn)
    all_lines = fh.readlines()
    for l in all_lines[pos:]:
        yield l
        pos += 1    
    fh.close()
    return 
    
    


if __name__=='__main__':
    pass
