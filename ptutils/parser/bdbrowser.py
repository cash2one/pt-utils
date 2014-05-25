#! /usr/bin/env python
#coding:gbk

"""
 Purpose: 
     1. �ٶ���������parser
         1. ���������־��hdfs://szwg-ston-hdfs.dmop.baidu.com:54310/log/17617/browser_openweb/20140122/*/*/*
 History:
     1. 2014/01/24 ���� <pengtao@baidu.com>
"""



import sys


try:
    from recordz import Record
except ImportError:
    from ubsutils.parser.recordz import Record

########################################################################
class OpenWebRecord(Record):
    """
    �����ҳ�������־parser
    
    usage
    =====
        ��ϸ�ֶμ�readline������
        
            >>> rec = USRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('time')
            >>>     print rec.attr('referer')
            >>>     print rec.attr('url')
            >>>     print rec.attr('pccode')
            
    history
    =======
    1. 2014-01-24 create parser.
    
    """    
    def __init__(self):
        Record.__init__(self)
        
        
        self.a_time = "" # server�˻����û���ʱ�䣿
        self.a_pccode = ""
        self.a_pccode2 = ""
        self.a_url = ""
        self.a_referer = ""
        self.a_kvs = ""

    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the us notice log. 
        
        openweb��־ʵ����
        "Op=CriticalTech Type=OpenWeb Time=2014_01_22_21_00_00 skinname=default cid=DE17E23771D5CFAC4F4356A528290D05:FG=1 referer=htt..."
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        """
        
        fields = line.strip().split()
        for e in fields:
            kv = e.split("=", 1)
            if le(kv) == 1:
                self.a_kvs[kv[0]] = ""
            else:
                self.a_kvs[kv[0]] = kv[1]
        
        self.a_time = self.a_kvs.get("servertime", "")
        self.a_pccode = self.a_kvs.get("pccode", "")
        self.a_pccode2 = self.a_kvs.get("pccode2", "")
        self.a_url = self.a_kvs.get("url", "")
        self.a_referer = self.a_kvs.get("referer", "")
        
        return True
    
    
########################################################################
class MahaoRecord(Record):
    """
    �����������ԭʼ�������ɵ��м����ݣ�ƴ��session
    
    
    usage
    =====
        ��ϸ�ֶμ�readlines������
        
            >>> rec = MahaoRecord()
            >>> for lines in split_file_by_key(fh):
            >>>     if not rec.readLines(lines):
            >>>         continue
            >>>     print rec.attr('pccode')
            >>>     actions = rec.attr("actions")
            >>>     print actions[0]['time']
            >>>     print actions[0]['referer']

            
    history
    =======
    1. 2014-01-27 create parser.
    
    """    
    def __init__(self):
        Record.__init__(self)
        
        self.a_pccode = ""
        self.a_actions = []

    #----------------------------------------------------------------------
    def readLines(self, lines):
        """
        read and parse the lines of a pccode. 
        
        ��ʽ���£�
        PcCode=C_0-D_0-M_AC220B850E08-V_04796CE5-T_20140122204641234    server_time=1390394866  ip=123.150.156.61       cid=017D2C19365E5ABEF787D54693FBC196:FG=1       url=http://www.360.cn/       referer=http://www.360buy.com/
        
        @type lines: list
        @param lines: the all logs of a pccode
        @rtype: bool
        @return: True if line is a valid log. False if not.
        """
        subfields = lines[0][0].split("=", 1)
        if len(subfields) != 2 or subfields[0] != "PcCode":
            return False
        self.a_pccode = subfields[1]
        self.a_actions = []
        
        # server_time=1390394866
        lines.sort(key=lambda x:x[1])
        
        for line in lines:
            act = {}
            for e in line:
                subfields = e.split("=", 1)
                if len(subfields) == 2:
                    act[subfields[0]] = subfields[1]
            self.a_actions.append(act)
            
        return True

if __name__=='__main__':
    pass
    
