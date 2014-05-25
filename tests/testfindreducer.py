#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1.  test famework for hadoop.findreducer
 History:
     1. 2013/5/18 创建

"""
    
import unittest
import os
import random

from ubsutils.hadoop.findreducer import hadooMap, KeyFieldBasedPartitioner


class TestFindReducer(unittest.TestCase):
    """
    x = {"{'query': 'zzjyt'}":500,
     "{'query': 'zzombie'}":500,
     "福商物语":200,
     "福州 真师傅":200,
     "福州 杨氏":200}
for (k, v) in x.iteritems():
    print "%s\t%s" % (k, get_reducer(k, v, 0))
    print "%s\t%s" % (k, get_reducer(k, v, 1))

    """
    def setUp(self):
        pass
        
    
    def tearDown(self):
        pass
    
    def test_hadoopMap(self):
        self.assertEqual(hadooMap("{tianxiaguixin}", 500), 315)
        self.assertEqual(hadooMap("{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}", 500), 235)
        self.assertEqual(hadooMap("a860f74f0747b751", 2000), 1968)
        self.assertEqual(hadooMap("http://001uk.net/uwic/view.html?id=94631", 700), 399)
        self.assertEqual(hadooMap("0001C9B57F4402A0935873AFCC5F2E4C", 900), 300)        
        self.assertEqual(hadooMap("我们", 100), 65)
        self.assertEqual(hadooMap("发指的淘宝", 211), 179)
        
    #----------------------------------------------------------------------
    def test_KeyFieldBasedPartitioner(self):
        """"""
        self.assertEqual(KeyFieldBasedPartitioner("{tianxiaguixin}", 500), 344)
        self.assertEqual(KeyFieldBasedPartitioner("{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}{tianxiaguixin}", 500), 388)
        self.assertEqual(KeyFieldBasedPartitioner("a860f74f0747b751", 2000), 1919)
        self.assertEqual(KeyFieldBasedPartitioner("http://001uk.net/uwic/view.html?id=94631", 700), 538)
        self.assertEqual(KeyFieldBasedPartitioner("0001C9B57F4402A0935873AFCC5F2E4C", 900), 79)        
        self.assertEqual(KeyFieldBasedPartitioner("我们", 100), 44)
        self.assertEqual(KeyFieldBasedPartitioner("发指的淘宝", 211), 0)        


        
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestFindReducer)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
    
