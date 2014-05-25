#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose:
     1. demo for the usage of router/step/MRframework
 History:
     1. 2013/5/20
"""


import sys
from ubsutils.pipe.router import router, Step
from ubsutils.pipe.mr import MRframework


@router("classjob")
class MyFunc(Step):
    """
    this is a demo of class job.
    Put more explanation here.

    """
    ifn = "default.txt"
    ofn = "helloworld.txt"

    def run(self):
        """"""
        print "in run"
        self.func1(MyFunc.ifn)
        self._inter_func2(MyFunc.ofn)

    def func1(self, p):
        print "in func1"
        print p

    def _inter_func2(self, p):
        print "in _inter_func2"
        print >> sys.stderr, p

@router("abc")
def process_abc_data(a=1, b=3, c=2):
    """
    this is my abc data, blabla bla
    """
    print "in process_abc_data"
    print a, b, c

@router("bcd")
def copy_bcd_data(input, output):
    """
    copy input into output
    """
    print "in copy_bcd_data"
    print >>file(output, "w"), file(input).read()

@router("mr")
def process_mr_tasks():
    """
    修改MRframework的参数指向你自己的mrframe路径。
    demo中的路径在cq01-2012h1-3-uptest3.vm可用，欢迎尝试。

    """
    mr = MRframework("/home/work/pengtao/bin/ubs_mrframework/bin/Start.py")
    mr.readconf(module="./demo.mod.conf", joblist="./demo.job.conf")
    mr.shell()

if __name__=='__main__':
    router.main()
