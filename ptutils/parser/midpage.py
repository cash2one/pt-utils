#! /usr/bin/env python
#coding:utf-8

"""
 Purpose: 
     1. 中间页数据相关的parser
         1. wiki：http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Middle_page
 History:
     1. 2013/10/24 创建 <pengtao@baidu.com>
"""



import sys
from recordz import Record

########################################################################
class MergeRawSessionRecord(Record):
    """
    所有中间页数据merge，按照时间切分session的数据，基本保留了原始log的信息
    
    It's a single line parser.
    
    usage
    =====
        详细字段见readline函数。
        
            >>> rec = MergeRawSessionRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('time')
            >>>     print rec.attr('user_id')
            >>>     print rec.attr('type')
            >>>     print rec.attr('query')
            >>>     print rec.attr('ua')
    history
    =======
    1. 2013-10-20 create parser.
    """    
    def __init__(self):
        Record.__init__(self)
        
        
        self.a_time = ""
        self.a_user_id = ''
        self.a_type = ''
        self.a_query = ''
        self.a_ua = ''      

    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the weibo search line. 
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        
        """
        fields = line.strip().split('\t')
        if len(fields) < 5:
            return False

        self.a_time = fields[0]
        self.a_user_id = fields[1]
        self.a_type = fields[2]
        self.a_query = fields[3]
        self.a_ua = fields[4]
        return True
    

########################################################################
class ShortUrlRecord(Record):
    """
    新浪短链接的访问日志
    It's a single line parser.
    usage
    =====
        详细字段见readline函数。
        
            >>> rec = ShortUrlRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('time')
            >>>     print rec.attr('ip')
            >>>     print rec.attr('user_id')
            >>>     print rec.attr('short_url')
            >>>     print rec.attr('long_url')
    history
    =======
    1. 2013-10-20 create parser.
    """    
    def __init__(self):
        Record.__init__(self)
        
        
        self.a_time = ""
        self.a_ip = ''
        self.a_user_id = ''
        self.a_short_url = ''
        self.a_long_url = ''      

    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the weibo short url click line. 
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        
        """
        fields = line.strip().split('\t')
        if len(fields) < 5:
            return False

        self.a_time = fields[0]
        self.a_ip = fields[1]
        self.a_user_id = fields[2]
        self.a_short_url = fields[3]
        self.a_long_url = fields[4]
        
        return True 
        

if __name__=='__main__':
    pass
    
