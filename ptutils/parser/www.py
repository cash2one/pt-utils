#! /usr/bin/env python
#coding:gbk

"""
 Purpose: 
     1. 大搜索www后端模块日志的parser，比如us，ac等
         1. 日志大部分在ston上，路径形如： /log/121/ps_www_us_to_ston/
 History:
     1. 2013/11/11 创建 <pengtao@baidu.com>
"""



import sys

from json import loads

try:
    from recordz import Record
except ImportError:
    from ubsutils.parser.recordz import Record

########################################################################
class USRecord(Record):
    """
    US日志的解析模块
    
    http://wiki.babel.baidu.com/twiki/bin/view/Ps/Searcher/US%E6%97%A5%E5%BF%97
    目前问题： 日志利用空格分隔和等号分割k-v， query中可能含有空格和等号。
    
    
    usage
    =====
        详细字段见readline函数。
        
            >>> rec = USRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('time')
            >>>     print rec.attr('queryid')
            >>>     print rec.attr('query')
            >>>     for info in rec.attr('server_infos'):
            >>>         print info # （serverid，server 状态，server返回的结果数，merge后被采用的结果数，各服务的检索时间，cache用的时间，us 整个系统线程占用数量，后端的线程占用数量，主动关心状态，主动关心级别，服务名称）
        
    history
    =======
    1. 2013-11-11 create parser.
    """    
    def __init__(self):
        Record.__init__(self)
        
        
        self.a_date = ""
        self.a_time = ""
        self.a_delay = -1
        self.query = ""
        self.a_server_infos = []
     

    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the us notice log. 
        
        NOTICE: 09-26 13:36:46: us. * 3554 [腰椎间盘突出症的治疗方法] tn=baidu tm=368316us dy=0 ec=0 pt=12 dl=0 dt=634 ql=0 cl=0 fl=0 qt=364 da=18926 st=329346 ar=0 zt=748 dajc=0 stjc=0 (0,13,10,2,0,0,0,0,13,1,ASM)(2,5,1,1,0,413,0,0,8,0,R_VDO)(3,10,6,4,3954,0,0,0,8,0,GSS_KV)(4,10,3,2,32479,0,0,0,8,0,DICT)(15,9,0,0,14619,0,0,0,0,0,ERS)(17,10,16,0,329279,0,0,0,8,0,ASP)(20,5,1,1,0,120,0,0,8,0,IK) src=20043,20259,1,16048,10396,10,6660,28050 pos=20043(1),20259(2),1(4),16048(5),10396(6),10(7),6660(8),28050(9) lc=1,7,29,0 queryid=ae56bfef00dc8888 fc=1 waa=0 sid=3407_3388_1435_2981_ uspack_size=110308 rsNum=10 seNum=0 asp=1(8)_2(1)_213(5)_219(1)_665(1) moffset=1_2 vui_or_ui=1 account=baidu acs=459892350_3249434408 acCache=(腰椎间盘突出症的治疗方法#0#3#baidu##0#0#0#0#0#1#1#0#10#10#0#0#0) cachusedblocks=3499990 cacheusednum=-6750 asp_pk_zx_num=2, asp_pk_zx_ids=6707,20046, abd_ec=0 rn=10 pn=1
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        
        """
        i = line.find("] tn=")  # query中有空格，所以首先将此处识别出来
        if i == -1:
            return False
        part1 = line[:i]
        part2 = line[i:]
        
        fields = part1.strip().split()
        if len(fields) < 7 or fields[6][0] != "[":
            return False
        self.a_date = fields[1]
        self.a_time = fields[2]
        self.a_delay = int(fields[5])
        self.a_query = fields[6][1:]
        self.a_server_infos = []
        self.attrs = {}
        
        is_server = False
        fields = part2.strip().split()
        for i in range(1, len(fields)):
            kv = fields[i].split("=")
            if len(kv) == 2:
                self.attrs[kv[0]] = kv[1]
            else:
                val = kv[0] # ["()()()"]
                if len(val) > 2 and val[0] == '(' and val[-1] == ')':
                    if is_server:
                        # raise Exception("duplicated server info")
                        return False
                    is_server = True
                    infos = val[1:-1].split(")(")
                    for e in infos:
                        self.a_server_infos.append(e.split(","))
                else:
                    return False
        return True
    
    
########################################################################
class SugSvrRecord(Record):
    """
    Sug Server日志的解析模块

    http://wiki.babel.baidu.com/twiki/bin/view/Ps/Main/SugSvrLog
    
    usage
    =====
        详细字段见readline函数。
        
            >>> rec = SugSvrRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('time')
            >>>     print rec.attr('layer')
            >>>     print rec.attr('cookie')
            >>>     for k, v in rec.attr('sug').iteritems():
            >>>         print k, v 
        
    history
    =======
    1. 2013-11-12 create parser.
    """    
    def __init__(self):
        Record.__init__(self)
        
        self.a_preifx = ""
        self.a_date = ""
        self.a_time = ""
        self.a_layer = ""
        self.a_from = ""
        self.a_num = 0
        self.a_delay = 0
        self.a_sug = ""
        self.a_encode = ""
        self.a_cookie = ""
        
     

    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the sug server notice log. 
        
        TRACE: 02-13 21:00:00: susvr. * 1661172064　　2　　-　　10　　175　　({q:"广州 工",p:false,s:["广州工商职业技术学院","广州工商局","广州工程技术职业学院","广州工业大学","广州工作","广州工作网","广州工商局企业查询","广州工商","广州工商银行","广州工资水平"]});　　0　　27C2DD3AE94316AF85D7FBF0F0229208
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        
        """
        fields = line.strip().split("\t")
        if len(fields) != 10:
            # there are 10 fields, 8 are documented
            return False
        
        parts = fields[0].strip().split()
        if len(parts) != 6:
            return False
        self.a_prefix = fields[0]
        self.a_date = parts[1]
        self.a_time = parts[2]
        self.a_layer = fields[1]
        self.a_from = fields[2]
        self.a_num = int(fields[3])
        self.a_delay = int(fields[4])
        self.a_sug = fields[5]
        #if fields[5][0] == "(" and fields[5][-2:] == ');':
            #print >> sys.stderr, fields[5][1:-2]
            #self.a_sug = loads(fields[5][1:-2])
        #else:
            #return False
        self.a_encode = fields[6]
        self.a_cookie = fields[7]
    
        return True
    
        
    

if __name__=='__main__':
    pass
    
