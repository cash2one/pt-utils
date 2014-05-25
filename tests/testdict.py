#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1.  test famework for cube.outputstream
# History:
#     1. 2011/5/16 创建
    
import unittest
import os
import random

from ubsutils.dicttools import *



class TestCreateDict(unittest.TestCase):
    def setUp(self):
        self._fn = "tmp%s.txt" % random.random()
        fh = file(self._fn, 'w')
        print >> fh, "xb:c:d :mef"
        print >> fh, "ab:e:f :def"
        print >> fh, "ab:c:f :def"
        fh.close()
        
    
    def tearDown(self):
        os.remove(self._fn)
    
    def test_memorydict(self):
        out = create_dict_from_flat(fn=self._fn, kpos=1, 
                                    vpos=[0,3], sep=":", override=False)
        self.assertEqual(out["c"], ["xb", "mef"])
        self.assertEqual(out["e"], ["ab", "def"])
        
    def test_shelvedict(self):
        out, shn = create_shelve_from_flat(fn=self._fn, kpos=1,
                                      vpos=[0, 2], sep=":")
        self.assertEqual(out["c"], ["ab", "f "])
        self.assertEqual(out["e"], ["ab", "f "])
        
        out.close()
        os.remove(shn)
        

        
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestCreateDict)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
    
