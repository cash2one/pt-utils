#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. demo3
 History:
     1. 2013/5/20 
"""


from ubsutils.pipe.router import router

@router("classjob")
class MyFunc(Step):
    """
    this is a demo of class job
    """
    ifn = ""
    ofn = ""
    def run(self):
        """"""
        self.run1(MyFunc.ifn)
        self.run2(MyFunc.ofn)    
    def run1(self, p):
        pass
    def run2(self, p):
        pass
        

if __name__=='__main__':
    router.main()