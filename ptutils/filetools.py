#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# History:
#     1. 2012/4/5 created

"""
  �ļ������������һЩС���ߡ�
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
    """����������  ��pid���̻������н��̣�pid=None)�Ĵ��ļ���Ѱ��fn
    
    @rtype: tuple
    @return:  (real_pid, real_link), ���û�ҵ�������(None)
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
        return (None, None)  # ������
    

    # re_pipe = re.compile(r'pipe:\[\d+\]')
    # re_socket = re.compile(r'socket:\[\d+\]')
    cur_link = ''
    cur_file = ''
    for d in _dirs:
        for fds in os.listdir(d):
            # ll /proc/21263/fd
            # l-wx------  1 work work 64  5�� 12 15:38 1 -> /home/work/pengtao/projects/20130130-zhixin-evaluation/bin/tmp
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
    """��ȡһ�����ڱ��򿪣�д�룩���ļ�������log����ֱ���ļ���ȫ�رգ�log���������.
    
    �ٶ�ֻ��һ�����̴�Ŀ���ļ�������̫���ӡ�
    ����������
        1. ���ļ�����ȡ��ֱ��û�����ݡ���¼�µ�ǰ��λ�á�
        2. ���pid���̣�None��������н��̣��ܺ�ʱ�����д򿪵��ļ������Ŀ��fn���ڣ�˵�������ݻ���д�롣���´򿪣����ϴν����㿪ʼ���¶������ݡ�
           2.1 fn�ļ�����̫����Ϊ��Ҫ�����Ķ�ȡ��
        3. �ظ�1��2��ֱ���ļ�û�б��������̷��ʣ��Ѿ��رգ�
    
    ��Ϊ����/proc��Ϣ�����Ժ���ֻ����unix��ִ�С�����ƽ̨ò�ƿ��Խ���psutilʵ�֣��ο���
    http://stackoverflow.com/questions/11114492/check-if-a-file-is-not-open-not-used-by-other-process-in-python
    
    ע�����ļ��Ƿ�������չ�򿪵����ֲ����������������
    
    @type fn: string
    @param fn: Ŀ���ļ���
    @type sleep: int
    @param sleep: �ظ��������ڣ�Ĭ��5s
    @type pid: int
    @param pid: Ŀ����̺ţ����û�У���������н���Ѱ��Ŀ�����
    @rtype: iterator
    @return: ����Ŀǰ�Ѿ��������ݵ�iterator�����û�������ݣ���������ǰ���̡�
    
    """
    fn = os.path.normpath(fn)
    (real_pid, real_link) = _find_opening_pid(fn, pid)

    pos = 0
    if real_pid :
        # ���ﲻ���fn�Ƿ���ڣ��������������
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
