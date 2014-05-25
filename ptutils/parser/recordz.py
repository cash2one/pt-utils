#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 文本newcookiesort和mergelog的标准parser
#     2. 基于lixin@baidu.com的record.py
# ToDo
#     1. wholebaidusession日志的parseline对最后一个记录无效，因为没有flush内部状态
# History:
#     5. 2012/06/13
#        1. bugfix BaiduWholeSessionRecord.asString
#     4. 2012/06/04
#        1. 给MergeRecord添加parseLine方法。
#     3. 2012/02/28
#        1. 添加DetailnewquerysortRecord.
#     2. 2012/02/06
#        1. 基于宇杰代码，添加 UIlogRecord 和所需的ip2str函数。
#     1. 2011/10/28 
#        1. 将MyRecord中的 MergeAtomicSessionRecord, NSAccessLogRecord,QueryLogRecord,UBSAccessLogRecord 添加到 record.py中
#        2. 把NewCookieSortRecord和相关Record中的保留字段save1~save4更新为info, prefixsug, mu和s字段
#           2.1 见 http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Clickdata_data_format#newcookiesort
#        3. 在NewCookieSortRecord中添加readLine方法，适应PyHce方式运行hadoop程序




import re
import time
import urllib2

try:
    import udf
    import myudf   
except:
    pass
try:
    from ubsutils.filesplitter import split_list_by_key
except:
    pass

def ip2str(ipdata):
    """
    Convert hex number into readable IP address.
    
    Example:
    
      >>> ip2str(1143972207)
      68.47.161.111
      
    """
    try:
        if type(ipdata) != type(1):
            return ''
        if ipdata > 0xFFFFFFFF:
            return ''
        ipf=[]
        mask=[0xFF000000,0x00FF0000,0x0000FF00,0x000000FF]
        for i in range(0,4):
            dig = (ipdata & mask[i]) >> (3-i)*8
            ipf.append(dig)
        if len(ipf) != 4:
            return ''
        return "%d.%d.%d.%d" %(ipf[0],ipf[1],ipf[2],ipf[3])
    except ValueError:
        return "0.0.0.0"
    
    
class Record:
    """Define a abstract parser base.
       
    usage
    =====
    
        - With file handle:
        
        >>> rec = Record()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('query')
        >>>     print rec.attr('search')

        - With string:
          
        >>> rec = Record()
        >>> for line in fh.readlines():
        >>>     if rec.parseLine(line) != rec.LOG_FINISHED:
        >>>         continue
        >>>     print rec.attr('query')  
        
    history
    =======
    
      1. 2012-03-08 updated by pengtao@baidu.com.
    """
    _expr_var_re = re.compile(r'@(@|\w+)')

    # Tag for parseLine function
    LOG_FINISHED = 0    # A complete record is loaded.
    LOG_UNFINISHED = 1  # The log is not a complete record
    LOG_BROKEN = 2      # Invalid log
    
    def __init__(self):
        self.env_dict = globals().copy()
        self.env_dict.update(locals())
        self.code_cache = {}
        self.attrs = {}

    def attr(self, name):
        if hasattr(self, 'a_' + name):
            return getattr(self, 'a_' + name)
        elif name in self.attrs:
            return self.attrs[name]
        else:
            return None

    def eval(self, expr):
        expr = expr.lstrip()
        expr = re.sub(self._expr_var_re,
                      lambda x: self._exprVarRepl(x), expr)
        if expr in self.code_cache:
            code = self.code_cache[expr]
        else:
            code = compile(expr, '', 'eval')
            self.code_cache[expr] = code
        return eval(code, self.env_dict)

    def _exprVarRepl(self, mo):
        s = mo.group(1)
        if s == '@':
            return '@'
        else:
            return 'self.attr("%s")' % s

    # To be overridden, return False on EOF
    def readNext(self, f):
        raise Exception
    

    #----------------------------------------------------------------------
    def parseLine(self, line):
        """
        Input a log line for this parser. 
        The EOF signal is handled outside parser itself.        
        
        @type line: string
        @param line: the inputed next line string
        """
        raise Exception, "to be overridden in base class"
        
    #----------------------------------------------------------------------
    def readLine(self, line):
        """ Input a log line.
            
        Difference with parseLine. 
          - parseLine returen LOG_FINISHED
          - readLine return True/False (user knows it's a single-line log)
            
        """
        raise Exception
        

    # To be overridden, return the string representation
    def asString(self):
        raise Exception

class TutorRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_type = ''
        self.a_query = ''
        self.a_url = ''

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        (self.a_cookie, self.a_type, self.a_query, self.a_url) = \
         line[:-1].split('\t')
        return True

class ImageLogRecord(Record):
    def __init__(self):
        Record.__init__(self)

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        fields = line[:-1].split('\t')
        for f in fields:
            (key, value) = f.split(':', 1)
            self.attrs[key] = value
        return True

class ImageQueryRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_query = ''
        self.a_type = ''
        self.a_fields = []

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        fields = line[:-1].split('\t')
        self.a_query = fields[0]
        self.a_type = fields[1]
        self.a_fields = fields[2:]
        return True

class SobarAccessRecord(Record):
    """
    define a parser for sobar urldata (raw data) log
    
    The format for sobar access is at
      - http://wiki.babel.baidu.com/twiki/bin/view/Ps/WebPM/SobarAccess
      - 丁杰的一篇介绍ppt
        - http://ecmp.baidu.com/page/site/WebABTest/document-details?nodeRef=workspace://SpacesStore/df811731-5c17-4647-b528-abc5de91910f&cursor=2&showFolders=all
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = SobarAccessRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('url')
        >>>     print rec.attr('target')
        
        - With string:
          
        >>> rec = SobarAccessRecord()
        >>> for line in fh:
        >>>     if not rec.readLine(line):
        >>>         continue
        >>>     print rec.attr('refer')
        >>>     print rec.attr('ip')      

    history
    =======
    
      1. 2012-10-24 update by pengtao@baidu.com
      
    """
    _field_re = re.compile(r'^([\w.]+?):(.*)$')

    def __init__(self):
        Record.__init__(self)
        self.a_log_time = 0  # server time
        self.a_url = ''      # log url
        self.a_target = ''   # url after redirecting
        self.a_ip = ''       

    def _parseLine(self, line):
        # Fields are seperated by tab
        fields = line[:-1].split('\t')
        assert(len(fields) >= 3)

        # Extract fixed fields
        t = int(time.mktime(time.strptime(
            fields[0][1 : -1], '%Y-%m-%d %H:%M:%S')))
        url = fields[1]
        ip = fields[-1]

        # Save other fields in map
        fdmap = {}
        for f in fields[2 : -1]:
            mo = self._field_re.search(f)
            if mo != None and len(mo.group(2)) > 0:
                fdmap[mo.group(1)] = mo.group(2)
        return (t, url, ip, fdmap)
    
    #----------------------------------------------------------------------
    def readLine(self, line):
        (self.a_log_time, self.a_url, self.a_ip, self.attrs) = \
            self._parseLine(line)
        self.a_target = self.a_url
        i = 1
        while True:
            if ('rdturl%d' % i) in self.attrs:
                self.a_target = self.attrs['rdturl%d' % i]
                i += 1
            else:
                break
        return True        

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        return self.readLine(line)
        

    def asString(self):
        s = ''
        for key in self.attrs:
            s += '%s:%s\t' % (key, self.attrs[key])
        return '[%s]\t%s\t%s%s' % (
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.a_log_time)),
            self.a_url, s, self.a_ip)

class CookieSortRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_ip = ''
        self.a_time = 0
        self.a_type = 0
        self.a_pn = 0
        self.a_pos = 0
        self.a_f = 0
        self.a_rsp = 0
        self.a_cl = 0
        self.a_F = 0
        self.a_query = ''
        self.a_url = ''
        self.a_reg = 0
        self.a_w = 0.0

    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():
                break
        fields = line[:-1].split('\t')
        assert(len(fields) == 14)
        self.a_cookie = fields[0]
        self.a_ip = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_type = int(fields[3])
        self.a_pn = int(fields[4])
        self.a_pos = int(fields[5])
        self.a_f = int(fields[6])
        self.a_rsp = int(fields[7])
        self.a_cl = int(fields[8])
        self.a_F = int(fields[9])
        self.a_query = fields[10]
        self.a_url = fields[11]
        self.a_reg = int(fields[12])
        self.a_w = float(fields[13])
        return True

    def asString(self):
        return '%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%f' % (
            self.a_cookie, self.a_ip,
            time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(self.a_time)),
            self.a_type, self.a_pn, self.a_pos, self.a_f, self.a_rsp,
            self.a_cl, self.a_F, self.a_query, self.a_url, self.a_reg, self.a_w)

class SessionRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_duration = 0
        self.a_query_num = 0
        self.a_search_num = 0
        self.a_click_num = 0
        self.a_tab_num = 0
        self.a_turn_num = 0
        self.a_rs_num = 0
        self.a_broken = 0
        self.a_rank = 0
        self.a_actions = []

    def readNext(self, f):
        self.a_actions = []
        while True:
            line = f.readline()
            if not line:
                if len(self.a_actions) > 0:
                    self._compute()
                    return True
                return False
            if line.isspace():
                if len(self.a_actions) > 0:
                    self._compute()
                    return True
                continue
            fields = line[:-1].split('\t')
            assert(len(fields) == 14)
            action = {}
            action['cookie'] = fields[0]
            action['ip'] = fields[1]
            action['time'] = int(time.mktime(time.strptime(
                fields[2], '%d/%b/%Y:%H:%M:%S')))
            action['type'] = int(fields[3])
            action['pn'] = int(fields[4])
            action['pos'] = int(fields[5])
            action['f'] = int(fields[6])
            action['rsp'] = int(fields[7])
            action['cl'] = int(fields[8])
            action['F'] = int(fields[9])
            action['query'] = fields[10]
            action['url'] = fields[11]
            action['reg'] = int(fields[12])
            action['w'] = float(fields[13])
            self.a_actions.append(action)

    def asString(self):
        s = ''
        for a in self.a_actions:
            s += '%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%f\n' % (
                a['cookie'], a['ip'],
                time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(a['time'])),
                a['type'], a['pn'], a['pos'], a['f'], a['rsp'],
                a['cl'], a['F'], a['query'], a['url'], a['reg'], a['w'])
        return s

    def _compute(self):
        # Time of last action minus time of first action
        self.a_duration = self.a_actions[-1]['time'] - \
            self.a_actions[0]['time']
        self.a_cookie = self.a_actions[0]['cookie']
        self.a_query_num = 0
        self.a_search_num = 0
        self.a_click_num = 0
        self.a_tab_num = 0
        self.a_turn_num = 0
        self.a_rs_num = 0
        self.a_broken = 0
        self.a_rank = 0
        shifen = False  # TODO: make it right
        noclick = False
        last_query = None
        click_cur = 0
        tab_cur = 0
        for action in self.a_actions:
            if action['type'] == 0:  # search
                self.a_search_num += 1
                if action['query'] != last_query or action['pn'] == -1:  # new
                    self.a_query_num += 1
                    if action['query'] != last_query and action['f'] == 1: # rs
                        self.a_rs_num += 1
                    if last_query != None and click_cur == 0 and tab_cur == 0:
                        noclick = True
                    last_query = action['query']
                    click_cur = 0
                    tab_cur = 0
                if action['pn'] != -1:  # turn page
                    self.a_turn_num += 1
            elif action['type'] == 1:  # click
                if action['query'] != last_query:
                    self.a_broken = 1
                else:
                    self.a_click_num += 1
                    click_cur += 1
                if action['pos'] == 0:
                    shifen = True
            else:  # action['type'] == 2, tab search
                if action['query'] != last_query:
                    self.a_broken = 1
                else:
                    self.a_tab_num += 1
                    tab_cur += 1
        if last_query != None and click_cur == 0 and tab_cur == 0:
            noclick = True
        if self.a_query_num > 0:
            turn_ratio = self.a_turn_num / (self.a_query_num + 0.0)
            rs_ratio = self.a_rs_num / (self.a_query_num + 0.0)
            click_ratio = (self.a_click_num + self.a_tab_num) / \
                        (self.a_query_num + 0.0)
        else:
            turn_ratio = 0.0
            rs_ratio = 0.0
            click_ratio = 0.0
        if self.a_turn_num > 0 or self.a_broken or noclick:
            qc_seq = 0
        else:
            qc_seq = self.a_query_num

        # Compute SessionRank at last
        self.a_rank = self._decideRank(turn_ratio, rs_ratio, click_ratio,
                                       qc_seq)

    # Decision tree output
    def _decideRank(self, turn_ratio, rs_ratio, click_ratio, qc_seq):
        session_time = self.a_duration
        search_num = self.a_search_num
        if qc_seq <= 0:
            if click_ratio <= 0.2:
                return 0
            else:
                if search_num <= 4:
                    if rs_ratio <= 0.125:
                        if click_ratio <= 1.25:
                            return 2
                        else:
                            return 1
                    else:
                        return 1
                else:
                    if search_num <= 14:
                        return 1
                    else:
                        return 0
        else:
            if rs_ratio <= 0.125:
                return 2
            else:
                if search_num <= 2:
                    if session_time <= 156:
                        return 1
                    else:
                        return 2
                else:
                    return 1

class QuerySortRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_query = ''
        self.a_search_num = 0
        self.a_total_w = 0.0
        self.a_other_w = 0.0
        self.a_results = []

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        fields = line[:-1].split('\t')
        assert(len(fields) == 66)
        self.a_query = fields[0]
        self.a_search_num = int(fields[1])
        self.a_total_w = float(fields[2])
        self.a_results = []
        for i in range(3, 63, 3):
            result = {}
            result['w'] = float(fields[i])
            result['F'] = int(fields[i+1])
            result['url'] = fields[i+2]
            self.a_results.append(result)
        self.a_other_w = float(fields[63])
        return True

    def asString(self):
        s = '%s\t%d\t%f' % (self.a_query, self.a_search_num, self.a_total_w)
        for i in range(20):
            s += '\t%f\t%d\t%s' % (self.a_results[i]['w'],
                                   self.a_results[i]['F'],
                                   self.a_results[i]['url'])
        s += '\t%f\t-\t0' % self.a_other_w
        return s

class DistributaryRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_ip = ''
        self.a_cookie = ''
        self.a_time = 0
        self.a_fm = ''
        self.a_pn = 0
        self.a_p1 = 0
        self.a_p2 = 0
        self.a_p3 = 0
        self.a_p4 = 0
        self.a_tn = ''
        self.a_tab = ''
        self.a_title = ''
        self.a_tp = ''
        self.a_f = 0
        self.a_rsp = 0
        self.a_F = 0
        self.a_query = ''
        self.a_url = ''
        self.a_reg = 0
        self.a_w = 0.0
        self.a_id = 0
        self.a_info = ''
        self.a_prefixsug = ''
        self.a_mu = ''
        self.a_s = ''
        self.a_oq = ''
        self.a_qid = ''
        self.a_cid = ''

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        fields = line[:-1].split('\t')
        assert(len(fields) == 28)
        self.a_ip = fields[0]
        self.a_cookie = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_fm = fields[3]
        self.a_pn = int(fields[4])
        self.a_p1 = int(fields[5])
        self.a_p2 = int(fields[6])
        self.a_p3 = int(fields[7])
        self.a_p4 = int(fields[8])
        self.a_tn = fields[9]
        self.a_tab = fields[10]
        self.a_title = fields[11]
        self.a_tp = fields[12]
        self.a_f = int(fields[13])
        self.a_rsp = int(fields[14])
        self.a_F = int(fields[15])
        self.a_query = fields[16]
        self.a_url = fields[17]
        self.a_reg = int(fields[18])
        self.a_w = float(fields[19])
        self.a_id = int(fields[20])
        self.a_info = fields[21]
        self.a_prefixsug = fields[22]
        self.a_mu = fields[23]
        self.a_s = fields[24]
        self.a_oq = fields[25]
        self.a_qid = fields[26]
        self.a_cid = fields[27]
        return True

    def asString(self):
        return '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
               '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                   self.a_ip, self.a_cookie,
                   time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(self.a_time)),
                   self.a_fm, self.a_pn, self.a_p1, self.a_p2, self.a_p3, self.a_p4,
                   self.a_tn, self.a_tab, self.a_title, self.a_tp, self.a_f,
                   self.a_rsp, self.a_F, self.a_query, self.a_url, self.a_reg,
                   self.a_w, self.a_id, self.a_info, self.a_prefixsug, self.a_mu,
                   self.a_s, self.a_oq, self.a_qid, self.a_cid)

class NewCookieSortRecord(Record):
    """Define a parser for newcookiesort.
    The format of newcookiesort is at
      - http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Clickdata_data_format#newcookiesort
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = NewCookieSortRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('query')
        >>>     print rec.attr('cookie')
        
        - With string:
          
        >>> rec = NewCookieRecord()
        >>> for line in fh.readlines():
        >>>     if rec.parseLine(line) != rec.LOG_FINISHED:
        >>>         continue
        >>>     print rec.attr('cookie')
        >>>     print rec.attr('url')  


    history
    =======
    
      1. 2012-03-08 add parseLine support.
    """    
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_ip = ''
        self.a_time = 0
        self.a_fm = ''
        self.a_pn = 0
        self.a_p1 = 0
        self.a_p2 = 0
        self.a_p3 = 0
        self.a_p4 = 0
        self.a_tn = ''
        self.a_tab = ''
        self.a_title = ''
        self.a_tp = ''
        self.a_f = 0
        self.a_rsp = 0
        self.a_F = 0
        self.a_query = ''
        self.a_url = ''
        self.a_reg = 0
        self.a_w = 0.0
        self.a_id = 0
        self.a_info = ''
        self.a_preifxsug = ''
        self.a_mu = ''
        self.a_s = ''
        self.a_oq = ''
        self.a_qid = ''
        self.a_cid = ''

    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():
                break
        fields = line[:-1].split('\t')
        assert(len(fields) == 28)
        self.a_cookie = fields[0]
        self.a_ip = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_fm = fields[3]
        self.a_pn = int(fields[4])
        self.a_p1 = int(fields[5])
        self.a_p2 = int(fields[6])
        self.a_p3 = int(fields[7])
        self.a_p4 = int(fields[8])
        self.a_tn = fields[9]
        self.a_tab = fields[10]
        self.a_title = fields[11]
        self.a_tp = fields[12]
        self.a_f = int(fields[13])
        self.a_rsp = int(fields[14])
        self.a_F = int(fields[15])
        self.a_query = fields[16]
        self.a_url = fields[17]
        self.a_reg = int(fields[18])
        self.a_w = float(fields[19])
        self.a_id = int(fields[20])
        self.a_info = fields[21]
        self.a_prefixsug = fields[22]
        self.a_mu = fields[23]
        self.a_s = fields[24]
        self.a_oq = fields[25]
        self.a_qid = fields[26]
        self.a_cid = fields[27]
        return True

    #----------------------------------------------------------------------
    def parseLine(self, line):
        """read a log line and parse it. Similar to readLine.
        Please see Record.parseLine for parameters.
        """
        if self.readLine(line):
            return self.LOG_FINISHED
        else:
            return self.LOG_UNFINISHED
        
    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        Input a log line for this parser. 
        The EOF signal is handled outside parser itself.        
        
        @type line: string
        @param line: the inputed next line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        """

        if line.isspace():
            return False

        fields = line[:-1].split('\t')
        assert(len(fields) == 28)
        self.a_cookie = fields[0]
        self.a_ip = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_fm = fields[3]
        self.a_pn = int(fields[4])
        self.a_p1 = int(fields[5])
        self.a_p2 = int(fields[6])
        self.a_p3 = int(fields[7])
        self.a_p4 = int(fields[8])
        self.a_tn = fields[9]
        self.a_tab = fields[10]
        self.a_title = fields[11]
        self.a_tp = fields[12]
        self.a_f = int(fields[13])
        self.a_rsp = int(fields[14])
        self.a_F = int(fields[15])
        self.a_query = fields[16]
        self.a_url = fields[17]
        self.a_reg = int(fields[18])
        self.a_w = float(fields[19])
        self.a_id = int(fields[20])
        self.a_info = fields[21]
        self.a_prefixsug = fields[22]
        self.a_mu = fields[23]
        self.a_s = fields[24]
        self.a_oq = fields[25]
        self.a_qid = fields[26]
        self.a_cid = fields[27]
        
        return True

    def asString(self):
        return '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
               '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                   self.a_cookie, self.a_ip,
                   time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(self.a_time)),
                   self.a_fm, self.a_pn, self.a_p1, self.a_p2, self.a_p3, self.a_p4,
                   self.a_tn, self.a_tab, self.a_title, self.a_tp, self.a_f,
                   self.a_rsp, self.a_F, self.a_query, self.a_url, self.a_reg,
                   self.a_w, self.a_id, self.a_info, self.a_prefixsug, self.a_mu,
                   self.a_s, self.a_oq, self.a_qid, self.a_cid)

class NewSessionRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_duration = 0
        self.a_query_num = 0
        self.a_search_num = 0
        self.a_click_num = 0
        self.a_tab_num = 0
        self.a_turn_num = 0
        self.a_rs_num = 0
        self.a_broken = 0
        self.a_rank = 0
        self.a_actions = []
        
        # signal for parseLine
        self._last_line_status = self.LOG_FINISHED

    def readNext(self, f):
        self.a_actions = []
        while True:
            line = f.readline()
            if not line:
                if len(self.a_actions) > 0:
                    self._compute()
                    return True
                return False
            if line.isspace():
                if len(self.a_actions) > 0:
                    self._compute()
                    return True
                continue
            fields = line[:-1].split('\t')
            assert(len(fields) == 28)
            action = {}
            action['cookie'] = fields[0]
            action['ip'] = fields[1]
            action['time'] = int(time.mktime(time.strptime(
                fields[2], '%d/%b/%Y:%H:%M:%S')))
            action['fm'] = fields[3]
            action['pn'] = int(fields[4])
            action['p1'] = int(fields[5])
            action['p2'] = int(fields[6])
            action['p3'] = int(fields[7])
            action['p4'] = int(fields[8])
            action['tn'] = fields[9]
            action['tab'] = fields[10]
            action['title'] = fields[11]
            action['tp'] = fields[12]
            action['f'] = int(fields[13])
            action['rsp'] = int(fields[14])
            action['F'] = int(fields[15])
            action['query'] = fields[16]
            action['url'] = fields[17]
            action['reg'] = int(fields[18])
            action['w'] = float(fields[19])
            action['id'] = int(fields[20])
            action['info'] = fields[21]
            action['prefixsug'] = fields[22]
            action['mu'] = fields[23]
            action['s'] = fields[24]
            action['oq'] = fields[25]
            action['qid'] = fields[26]
            action['cid'] = fields[27]
            self.a_actions.append(action)


    #----------------------------------------------------------------------
    def parseLine(self, line):
        """read a log line and parse it. Similar to readLine.
        Please see Record.parseLine for parameters.
        """

        # clear
        if self._last_line_status == self.LOG_FINISHED:
            self.a_actions = []
        
        if line.isspace() or not line: # line == ""
            if len(self.a_actions) > 0:
                self._compute()
                self._last_line_status = self.LOG_FINISHED
                return self.LOG_FINISHED
        else:
            fields = line[:-1].split('\t')
            #  assert(len(fields) == 28)
            if len(fields) != 28:
                return self.LOG_BROKEN
            action = {}
            action['cookie'] = fields[0]
            action['ip'] = fields[1]
            action['time'] = int(time.mktime(time.strptime(
                fields[2], '%d/%b/%Y:%H:%M:%S')))
            action['fm'] = fields[3]
            action['pn'] = int(fields[4])
            action['p1'] = int(fields[5])
            action['p2'] = int(fields[6])
            action['p3'] = int(fields[7])
            action['p4'] = int(fields[8])
            action['tn'] = fields[9]
            action['tab'] = fields[10]
            action['title'] = fields[11]
            action['tp'] = fields[12]
            action['f'] = int(fields[13])
            action['rsp'] = int(fields[14])
            action['F'] = int(fields[15])
            action['query'] = fields[16]
            action['url'] = fields[17]
            action['reg'] = int(fields[18])
            action['w'] = float(fields[19])
            action['id'] = int(fields[20])
            action['info'] = fields[21]
            action['prefixsug'] = fields[22]
            action['mu'] = fields[23]
            action['s'] = fields[24]
            action['oq'] = fields[25]
            action['qid'] = fields[26]
            action['cid'] = fields[27]
            self.a_actions.append(action)
            self._last_line_status = self.LOG_UNFINISHED
            return self.LOG_UNFINISHED
        
    def asString(self):
        """
        the output is not the exact: e.g. 0.5 --> 0.500000
        """
        s = ''
        for a in self.a_actions:
            aw = "%d" % a['w'] if a['w'] == round(a['w']) else "%f" % a['w']
            s += '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
              '\t%d\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                  a['cookie'], a['ip'],
                  time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(a['time'])),
                  a['fm'], a['pn'], a['p1'], a['p2'], a['p3'], a['p4'],
                  a['tn'], a['tab'], a['title'], a['tp'], a['f'], a['rsp'],
                  a['F'], a['query'], a['url'], a['reg'], aw, a['id'],
                  a['info'], a['prefixsug'], a['mu'], a['s'],
                  a['oq'], a['qid'], a['cid'])
        return s

    def _compute(self):
        # Time of last action minus time of first action
        self.a_duration = self.a_actions[-1]['time'] - \
            self.a_actions[0]['time']
        self.a_cookie = self.a_actions[0]['cookie']
        self.a_query_num = 0
        self.a_search_num = 0
        self.a_click_num = 0
        self.a_tab_num = 0
        self.a_turn_num = 0
        self.a_rs_num = 0
        self.a_broken = 0
        self.a_rank = 0
        shifen = False  # TODO: make it right
        noclick = False
        last_query = None
        click_cur = 0
        tab_cur = 0
        for action in self.a_actions:
            if action['fm'] == 'se':  # search
                self.a_search_num += 1
                if action['query'] != last_query or action['pn'] == -1:  # new
                    self.a_query_num += 1
                    if action['query'] != last_query and action['f'] == 1: # rs
                        self.a_rs_num += 1
                    if last_query != None and click_cur == 0 and tab_cur == 0:
                        noclick = True
                    last_query = action['query']
                    click_cur = 0
                    tab_cur = 0
                if action['pn'] != -1:  # turn page
                    self.a_turn_num += 1
            elif action['fm'] in ('tab', 'hint'):  # tab search
                if action['query'] != last_query:
                    self.a_broken = 1
                else:
                    self.a_tab_num += 1
                    tab_cur += 1
            else:  # click
                if action['query'] != last_query:
                    self.a_broken = 1
                else:
                    self.a_click_num += 1
                    click_cur += 1
                if action['fm'] != 'as':
                    shifen = True
        if last_query != None and click_cur == 0 and tab_cur == 0:
            noclick = True
        if self.a_query_num > 0:
            turn_ratio = self.a_turn_num / (self.a_query_num + 0.0)
            rs_ratio = self.a_rs_num / (self.a_query_num + 0.0)
            click_ratio = (self.a_click_num + self.a_tab_num) / \
                        (self.a_query_num + 0.0)
        else:
            turn_ratio = 0.0
            rs_ratio = 0.0
            click_ratio = 0.0
        if self.a_turn_num > 0 or self.a_broken or noclick:
            qc_seq = 0
        else:
            qc_seq = self.a_query_num

        # Compute SessionRank at last
        self.a_rank = self._decideRank(turn_ratio, rs_ratio, click_ratio, 
                                       qc_seq)

    # Decision tree output
    def _decideRank(self, turn_ratio, rs_ratio, click_ratio, qc_seq):
        session_time = self.a_duration
        search_num = self.a_search_num
        if qc_seq <= 0:
            if click_ratio <= 0.2:
                return 0
            else:
                if search_num <= 4:
                    if rs_ratio <= 0.125:
                        if click_ratio <= 1.25:
                            return 2
                        else:
                            return 1
                    else:
                        return 1
                else:
                    if search_num <= 14:
                        return 1
                    else:
                        return 0
        else:
            if rs_ratio <= 0.125:
                return 2
            else:
                if search_num <= 2:
                    if session_time <= 156:
                        return 1
                    else:
                        return 2
                else:
                    return 1

# All actions in one day.
class LongSessionRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_cookie = ''
        self.a_duration = 0
        self.a_actions = []
        self.next_line = None

    def readNext(self, f):
        self.a_actions = []
        last_cookie = None
        while True:
            if self.next_line != None:
                line = self.next_line
                self.next_line = None
            else:
                line = f.readline()
            if not line:
                if len(self.a_actions) > 0:
                    self.a_cookie = self.a_actions[0]['cookie']
                    self.a_duration = self.a_actions[-1]['time'] - \
                        self.a_actions[0]['time']
                    self.a_actions.sort(cmp=lambda x, y: x['time'] - y['time'])
                    return True
                return False
            if line.isspace():
                continue
            fields = line[:-1].split('\t')
            assert(len(fields) == 28)
            if last_cookie != None and fields[0] != last_cookie:
                self.next_line = line
                self.a_cookie = self.a_actions[0]['cookie']
                self.a_duration = self.a_actions[-1]['time'] - \
                    self.a_actions[0]['time']
                self.a_actions.sort(cmp=lambda x, y: x['time'] - y['time'])
                return True
            last_cookie = fields[0]
            action = {}
            action['cookie'] = fields[0]
            action['ip'] = fields[1]
            action['time'] = int(time.mktime(time.strptime(
                fields[2], '%d/%b/%Y:%H:%M:%S')))
            action['fm'] = fields[3]
            action['pn'] = int(fields[4])
            action['p1'] = int(fields[5])
            action['p2'] = int(fields[6])
            action['p3'] = int(fields[7])
            action['p4'] = int(fields[8])
            action['tn'] = fields[9]
            action['tab'] = fields[10]
            action['title'] = fields[11]
            action['tp'] = fields[12]
            action['f'] = int(fields[13])
            action['rsp'] = int(fields[14])
            action['F'] = int(fields[15])
            action['query'] = fields[16]
            action['url'] = fields[17]
            action['reg'] = int(fields[18])
            action['w'] = float(fields[19])
            action['id'] = int(fields[20])
            action['info'] = fields[21]
            action['prefixsug'] = fields[22]
            action['mu'] = fields[23]
            action['s'] = fields[24]
            action['oq'] = fields[25]
            action['qid'] = fields[26]
            action['cid'] = fields[27]
            self.a_actions.append(action)

    def asString(self):
        s = ''
        for a in self.a_actions:
            s += '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
              '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                  a['cookie'], a['ip'],
                  time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(a['time'])),
                  a['fm'], a['pn'], a['p1'], a['p2'], a['p3'], a['p4'],
                  a['tn'], a['tab'], a['title'], a['tp'], a['f'], a['rsp'],
                  a['F'], a['query'], a['url'], a['reg'], a['w'], a['id'],
                  a['info'], a['prefixsug'], a['mu'], a['s'],
                  a['oq'], a['qid'], a['cid'])
        return s

# Definition of an atomic session:
# 1) An atomic session has exactly one query;
# 2) The first action must be a search;
# 3) Subsequent searches must be turning page;
# 4) Save the search action following the last action.
class AtomicSessionRecord(LongSessionRecord):
    def __init__(self):
        LongSessionRecord.__init__(self)
        self.a_query = ''
        self.a_next_action = {}
        self.actions = []
        self.search_queries = []
        self.action_links = {}

    def readNext(self, f):
        while len(self.search_queries) == 0:
            if not LongSessionRecord.readNext(self, f):
                return False
            self.actions = self.a_actions
            self._linkActions()
        (pos, query, qids) = self.search_queries.pop()
        self.a_actions = [self.actions[pos]]
        for i in self.action_links[pos]:
            self.a_actions.append(self.actions[i])
        del self.action_links[pos]
        self.a_duration = self.a_actions[-1]['time'] - \
            self.a_actions[0]['time']
        self.a_query = query

        # Find next action
        self.a_next_action = {}
        for i in range(len(self.search_queries) - 1, -1, -1):
            action = self.actions[self.search_queries[i][0]]
            if action['time'] > self.a_actions[-1]['time']:
                self.a_next_action = action
                break
        return True

    def _linkActions(self):
        self.search_queries = []
        self.action_links = {}
        for i in range(len(self.actions)):
            action = self.actions[i]
            if action['fm'] == 'se' and action['pn'] == -1:  # new search
                self.search_queries.insert(
                    0, [i, action['query'], [action['qid']]])
                self.action_links[i] = []
            elif action['fm'] == 'se' and action['pn'] != -1:  # turn page
                for j in range(len(self.search_queries)):
                    (k, q, s) = self.search_queries[j]
                    if action['query'] == q:
                        self.action_links[k].append(i)
                        self.search_queries[j][2].append(action['qid'])
                        break
            else:  # click or tab search
                for (k, q, s) in self.search_queries:
                    if action['query'] == q and action['qid'] in s:
                        self.action_links[k].append(i)
                        break

    def asString(self):
        if len(self.a_next_action) == 0:
            s = '\t\n'
        else:
            a = self.a_next_action
            s = '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
              '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n' % (
                  a['cookie'], a['ip'],
                  time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(a['time'])),
                  a['fm'], a['pn'], a['p1'], a['p2'], a['p3'], a['p4'],
                  a['tn'], a['tab'], a['title'], a['tp'], a['f'], a['rsp'],
                  a['F'], a['query'], a['url'], a['reg'], a['w'], a['id'],
                  a['info'], a['prefixsug'], a['mu'], a['s'],
                  a['oq'], a['qid'], a['cid'])
        return LongSessionRecord.asString(self) + s


########################################################################
class SearchRecord(LongSessionRecord):
    """
    SearchRecord本质上就是一个search。
    
    与AtomicSessionRecord的差别：
      - 一个AtomicSession中包含翻页
      - SearchRecord中只有搜索，没有翻页。翻页会被当做新的search。
    """
    def __init__(self):
        LongSessionRecord.__init__(self)
        self.a_query = ''
        self.a_next_action = {}
        self.actions = []
        self.search_queries = []
        self.action_links = {}
        self.a_atomicindex = -1


    def readNext(self, f):
        while len(self.search_queries) == 0:
            if not LongSessionRecord.readNext(self, f):
                return False
            self.actions = self.a_actions
            self._linkActions()
        (pos, query, qids, atomicindex) = self.search_queries.pop()
        self.a_actions = [self.actions[pos]]
        for i in self.action_links[pos]:
            self.a_actions.append(self.actions[i])
        del self.action_links[pos]
        self.a_duration = self.a_actions[-1]['time'] - \
            self.a_actions[0]['time']
        self.a_query = query
        self.a_atomicindex = atomicindex

        # Find next action
        self.a_next_action = {}
        for i in range(len(self.search_queries) - 1, -1, -1):
            action = self.actions[self.search_queries[i][0]]
            if action['time'] > self.a_actions[-1]['time']:
                self.a_next_action = action
                break
        return True

    def _linkActions(self):
        self.search_queries = []
        self.action_links = {}
        atomicindex = 0
        for i in range(len(self.actions)):
            action = self.actions[i]
            if action['fm'] == 'se':  # new search and turn page
                if action['pn'] == -1:
                    atomicindex = 0
                else:
                    atomicindex += 1
                self.search_queries.insert(
                    0, [i, action['query'], [action['qid']], atomicindex])
                self.action_links[i] = []
            else:  # click or tab search
                for (k, q, s, a) in self.search_queries:
                    if action['query'] == q and action['qid'] in s:
                        self.action_links[k].append(i)
                        break

    def asString(self):
        if len(self.a_next_action) == 0:
            s = '\t\n'
        else:
            a = self.a_next_action
            s = '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
              '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n' % (
                  a['cookie'], a['ip'],
                  time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(a['time'])),
                  a['fm'], a['pn'], a['p1'], a['p2'], a['p3'], a['p4'],
                  a['tn'], a['tab'], a['title'], a['tp'], a['f'], a['rsp'],
                  a['F'], a['query'], a['url'], a['reg'], a['w'], a['id'],
                  a['info'], a['prefixsug'], a['mu'], a['s'],
                  a['oq'], a['qid'], a['cid'])
        return LongSessionRecord.asString(self) + s        


class AutoRankRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_query = ''
        self.a_search_num = 0
        self.a_clk_ratia_1 = 0.0
        self.a_clk_ratia_2 = 0.0
        self.a_clk_ratia_3 = 0.0
        self.a_clk_ratia_top3 = 0.0
        self.a_clk_ratia_all = 0.0
        self.a_session_num = 0
        self.a_rs_ratio = 0.0
        self.a_endclk_ratio = 0.0
        self.a_rank = 0
        self.a_cl = 0

    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            fields = line[:-1].split('\t')
            if len(fields) != 2:
                break
        assert(len(fields) == 12)
        self.a_query = fields[0]
        self.a_search_num = int(fields[1])
        self.a_clk_ratia_1 = float(fields[2])
        self.a_clk_ratia_2 = float(fields[3])
        self.a_clk_ratia_3 = float(fields[4])
        self.a_clk_ratia_top3 = float(fields[5])
        self.a_clk_ratia_all = float(fields[6])
        self.a_session_num = int(fields[7])
        self.a_rs_ratio = float(fields[8])
        self.a_endclk_ratio = float(fields[9])
        self.a_rank = int(fields[10])
        self.a_cl = int(fields[11])
        return True

    def asString(self):
        return '%s\t%d\t%f\t%f\t%f\t%f\t%f\t%d\t%f\t%f\t%d\t%d' % (
            self.a_query, self.a_search_num, self.a_clk_ratia_1,
            self.a_clk_ratia_2, self.a_clk_ratia_3, self.a_clk_ratia_top3,
            self.a_clk_ratia_all, self.a_session_num, self.a_rs_ratio,
            self.a_endclk_ratio, self.a_rank, self.a_cl)

###############################################################################
class BuildLogRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.a_urlno = 0
        self.a_url = ''
        self.a_pt = 0
        self.a_rich = 0

    def readNext(self, f):
        line = f.readline()
        if not line:
            return False
        (urlno, self.a_url, pt, rich) = line.split('\t')
        self.a_urlno = int(urlno)
        self.a_pt = int(pt)
        self.a_rich = int(rich)
        return True

    def asString(self):
        return '%d\t%s\t%d\t%d' % (self.a_urlno, self.a_url, self.a_pt,
                                   self.a_rich)
    
    
    
  
        
    
    
###################################################################
fdict = {'displayunit': ('url,0,STRING', 'urlinfo,1,OPTIONAL'),  \
         'urlinfo_OPTIONAL': {'PP': ('index', 'src', 'weight'), 'KEY_POS': 1,  \
                              'SP': ('index', 'src', 'is_cl_as', 'srcid', 'style'),  \
                              'PPIM': ('index', 'src', 'end_index', 'searchid'), \
                              'AS': ('index', 'src', 'weight', 'urlno', 'suburl_sign', 'site_sign1', 'site_sign2', 'sex', 'pol', 'cont_sign', 'match_prop', 'strategy', 'info','authority_weight','time_factor','page_type','field','srcid','style'), \
                              'Unknown': ('index', 'src'), \
                              'ADJ': ('index', 'src', 'weight', 'urlno', 'suburl_sign', 'site_sign1', 'site_sign2', 'cont_sign', 'match_prop', 'info')}, \
         'seinfo2': ('org_query,0,STRING', 'distm,1,STRING', 'pageNo,2,INT', 'asnum,3,INT', 'sample_id,4,INT','dtn,5,STRING','need_sp,6,INT','dn,7,INT','ln,8,INT','rs,9,STRING','se,10,STRING'), \
         'seinfo1': ('query,0,STRING', 'setm,1,STRING', 'pn,2,INT', 'tn,3,STRING', 'tp,4,STRING', 'f,5,INT', 'rsp,6,INT', 'bduss,7,STRING', 'id,8,INT', 'oq,9,STRING', 'cid,10,INT','info,11,STRING','statisfy,12,STRING'),  \
         'glof': ('cookie,0,STRING', 'ip,1,STRING', 'sid,2,STRING', 'qid,3,STRING', 'secnt,4,INT', 'clcnt,5,INT', 'tabcnt,6,INT', 'discnt,7,INT','se_idx,8,INT'), \
         'clickunit': ('clicktm,0,STRING', 'fw,1,STRING', 'pos1,2,INT', 'pos2,3,INT', 'pos3,4,INT', 'pos4,5,INT', 'tab,6,STRING','title,7,STRING','F,8,INT','clickurl,9,STRING', 'clickwgt,10,FLOAT', 'reserve1,11,STRING', 'reserve2,12,STRING', 'reserve3,13,STRING', 'reserve4,14,STRING', 'cl_indx,16,INT', 'tp,15,STRING')}

inner_sep='{\e}'   #seperator between items
outer_sep='{\c}'   #seperator between typelist
fpart=('glof','seinfo1','seinfo2','clickunit','displayunit')

class Click:
    def __init__(self,fld_fields):

        self.attrs={}
        self.dictname = 'clickunit'
        clickpart = fdict[self.dictname]	
        if len(fld_fields) <=1:
            return

        if len(fld_fields) ==  16:
            clickpart = ('clicktm,0,STRING', 'fw,1,STRING', 'pos1,2,INT', 'pos2,3,INT', 'pos3,4,INT', 'pos4,5,INT', 'tab,6,STRING','title,7,STRING','F,8,INT','clickurl,9,STRING', 'clickwgt,10,FLOAT', 'reserve1,11,STRING', 'reserve2,12,STRING', 'reserve3,13,STRING', 'reserve4,14,STRING', 'cl_indx,15,INT')

        for unit in clickpart:
            (key,pos,ty) = unit.split(',')
            val = fld_fields[int(pos)]
            if ty == 'INT':
                val = int(val)
            elif ty == 'FLOAT':
                val = float(val)
            elif ty == 'OPTIONAL':
                kn = "%s_%s"%(key,ty)
                kndict = fdict[kn]
                kpos = int(kndict['KEY_POS'])
                fields = val.split(':')
                tystr= fields[kpos] #type
                for i in range(0,len(kndict[typestr])):
                    kstr = key+'.'+ kndict[tystr][i]
                    vstr = fields[i]
                    if vstr.isdigit():
                        self.attrs[kstr] = int(fields[i])
                    else:
                        self.attrs[kstr] = fields[i]	    
            else:
                pass

            self.attrs[key]=val

    def attr(self,name):
        val = None
        if name in self.attrs:
            val = self.attrs[name]
        else:
            val=None
        return val 


    def toString(self):
        clickpart = fdict[self.dictname]
        seqlist=[''] * len(clickpart)
        for unit in clickpart:
            (key,pos,ty) = unit.split(',')
            if '.' in key: #remove optional
                continue	
            seqlist[int(pos)] = self.attrs[key]

        if not len(seqlist):
            return ''
        seq=''
        for s in seqlist:
            if s=='':
                continue
            seq += '%s\t' %(s)
        return  seq[:-1]

class Display:
    def __init__(self,fld_fields):

        self.attrs={}
        self.dictname = 'displayunit'
        displaypart = fdict[self.dictname]
        if len(fld_fields) <= 1:
            return
        for unit in displaypart:
            (key,pos,ty) = unit.split(',')
            val = fld_fields[int(pos)] #value array
            if ty == 'INT':
                val = int(val)
            elif ty == 'FLOAT':
                val = float(val)
            elif ty == 'OPTIONAL':
                kn = "%s_%s"%(key,ty)
                kndict = fdict[kn]
                kpos = int(kndict['KEY_POS'])
                fields = val.split(':')
                tystr= fields[kpos]
                for i in range(0,len(kndict[tystr])):
                    kstr = key+'.'+ kndict[tystr][i]
                    vstr = fields[i]
                    if vstr.isdigit():
                        self.attrs[kstr] = int(fields[i])
                    else:
                        self.attrs[kstr] = fields[i]
            else:
                pass

            self.attrs[key]=val #optional save STRING  OPTIONAL 

    def attr(self,name):
        val=None
        if name in self.attrs:
            val = self.attrs[name]
        else:
            val = None
        return val

    def toString(self):
        dispart = fdict[self.dictname]
        seqlist = [''] * len(dispart)
        for unit in dispart:
            (key,pos,ty) = unit.split(',')
            if '.' in key: #remove optional
                continue    
            seqlist[int(pos)] = self.attrs[key]

        if not len(seqlist):
            return ''
        seq=''
        for s in seqlist:
            if s=='':
                continue
            seq += '%s\t' %(s)
        return  seq[:-1]

#-----------------------------------------------------------------
class MergeRecord(Record):
    def __init__(self,args=None):
        #init 
        Record.__init__(self)
        self.attrs={} #query attr all have
        self.o_clicks= []
        self.o_displays= []
        if args != None:	
            line = args[0]
            (glof_dict,sedict0,sedict1) = (fdict['glof'],fdict['seinfo1'],fdict['seinfo2'])
            fields = line[:-1].split(outer_sep)


            #main area
            if fields[0] != '':
                glof_f = fields[0].split('\t')
                    #--------------add seid 20100727---------------
                if len(glof_f) == 8:
                    glof_dict = ('cookie,0,STRING', 'ip,1,STRING', 'sid,2,STRING', 'qid,3,STRING', 'secnt,4,INT', 'clcnt,5,INT', 'tabcnt,6,INT', 'discnt,7,INT')
            #------------------------------------------------------

                for unit in glof_dict:
                    (key,pos,ty) = unit.split(',')
                    if int(pos) > len(glof_f):
                        continue
                    val = glof_f[int(pos)]
                    if ty == 'INT':
                        val = int(val)
                    elif ty == 'FLOAT':
                        val = float(val)
                    else:
                        pass
                    self.attrs[key] = val
            if fields[1] != '':
                (se1_f,se2_f) = fields[1].split(inner_sep)
                sefields0 = se1_f.split('\t')
                sefields1 = se2_f.split('\t')
                sefields = [sefields0,sefields1]
                sedict = [sedict0,sedict1]
                for i in range(0,len(sefields)):
                    if len(sefields[i]) <= 1:
                        continue
                    sdict = sedict[i]
                    sefld = sefields[i]
                    for unit in sdict:
                        (key,pos,ty) = unit.split(',')
                        if int(pos) >= len(sefld):
                            continue

                        val = sefld[int(pos)]
                        if ty == 'INT':
                            val = int(val)
                        elif ty == 'FLOAT':
                            val = float(val)
                        else:
                            pass
                        self.attrs[key] = val
            #####click area
            c_fields = fields[2].split(inner_sep) #second level
            for fld in c_fields:
                if fld == '':
                    continue
                fld_fields = fld.split('\t')
                cf_obj = Click(fld_fields)
                self.o_clicks.append(cf_obj)
            #####display url area
            d_fields = fields[3].split(inner_sep)
            for fld in d_fields:
                if fld == '':
                    continue
                fld_fields = fld.split('\t')
                ds_obj = Display(fld_fields)
                self.o_displays.append(ds_obj) 
#-----------------------------------------------
    def readNext(self, f):
        #init 
        self.attrs={} #query attr all have
        self.o_clicks= []
        self.o_displays= []
        #deal
        while True:
            line = f.readline()
            if not line: #EOF
                return False
            if not line.isspace():
                break 

        (glof_dict,sedict0,sedict1) = (fdict['glof'],fdict['seinfo1'],fdict['seinfo2'])
        fields = line[:-1].split(outer_sep)
        #main area  
        if fields[0] != '':
            glof_f = fields[0].split('\t')
            #--------------add seid 20100727---------------
            if len(glof_f) == 8:
                glof_dict = ('cookie,0,STRING', 'ip,1,STRING', 'sid,2,STRING', 'qid,3,STRING', 'secnt,4,INT', 'clcnt,5,INT', 'tabcnt,6,INT', 'discnt,7,INT')
            #------------------------------------------------------
            for unit in glof_dict:
                (key,pos,ty) = unit.split(',')
                if int(pos) >= len(glof_f):
                    continue

                val = glof_f[int(pos)]
                if ty == 'INT':
                    val = int(val)
                elif ty == 'FLOAT':
                    val = float(val)
                else:
                    pass
                self.attrs[key] = val
        if fields[1] != '':
            (se1_f,se2_f) = fields[1].split(inner_sep)
            sefields0 = se1_f.split('\t')
            sefields1 = se2_f.split('\t')
            #------------add info , statisfy in click pub ,20100727---
            if len(sefields0) == 0:
                sedict0 = ('query,0,STRING', 'setm,1,STRING', 'pn,2,INT', 'tn,3,STRING', 'tp,4,STRING', \
                           'f,5,INT', 'rsp,6,INT', 'bduss,7,STRING', 'id,8,INT', 'oq,9,STRING', 'cid,10,INT')
            #-----------------------------------------------------------------
            sefields=[sefields0,sefields1]
            sedict=[sedict0,sedict1]
            for i in range(0,len(sefields)):
                if len(sefields[i]) <= 1:
                    continue
                sdict = sedict[i]
                sefld = sefields[i]
                for unit in sdict:
                    (key,pos,ty) = unit.split(',')
                    if int(pos) >= len(sefld):
                        continue

                    val = sefld[int(pos)]
                    if ty == 'INT':
                        val = int(val)
                    elif ty == 'FLOAT':
                        val = float(val)
                    else:
                        pass
                    self.attrs[key] = val
        #####click area
        c_fields = fields[2].split(inner_sep) #second level
        for fld in c_fields:
            if fld == '':
                continue
            fld_fields = fld.split('\t')
            cf_obj = Click(fld_fields)
            self.o_clicks.append(cf_obj)
        #####display url area
        d_fields = fields[3].split(inner_sep)
        for fld in d_fields:
            if fld == '':
                continue
            fld_fields = fld.split('\t')
            ds_obj = Display(fld_fields)
            self.o_displays.append(ds_obj)
        ######return true##########
        return True

    def attr(self,name):
        val=None
        if hasattr(self,'o_'+name):
            val = getattr(self,'o_'+name)
        elif name in self.attrs:
            val = self.attrs[name]
        else:
            pass
        return val

    def asString(self):
        (glof_dict,sedict0,sedict1) = (fdict['glof'],fdict['seinfo1'],fdict['seinfo2'])
        ##global area
        F0 = ''
        seq_f0 = ['']*len(glof_dict)
        for unit in glof_dict:
            (key,pos,ty)=unit.split(',')
            if key in self.attrs:
                seq_f0[int(pos)] = self.attrs[key]
            else:
                return 'ERROR'
        for seq in seq_f0:
            F0 += "%s\t" %(seq)
        ##se area
        F1_0 = ''
        seq_f1_0 = ['']*len(sedict0)
        for unit in sedict0:
            (key,pos,ty)=unit.split(',')
            if key in self.attrs:
                seq_f1_0[int(pos)] = self.attrs[key]
            else:
                break
        for seq in seq_f1_0:
            F1_0 += "%s\t" %(seq)
        if F1_0 != '':
            F1_0 = F1_0[:-1]
        F1_1 = ''
        seq_f1_1 = ['']*len(sedict1)
        for unit in sedict1:
            (key,pos,ty)=unit.split(',')
            if key in self.attrs:
                seq_f1_1[int(pos)] = self.attrs[key]
            else:
                break
        for seq in seq_f1_1:
            F1_1 += "%s\t" %(seq)
        if F1_1 != '':
            F1_1 = F1_1[:-1]
        F1 = "%s%s%s" % (F1_0,inner_sep,F1_1)
        #time recontruct??
        #clicks
        F2 =''
        for click in self.o_clicks:
            F2+="%s%s" % (click.toString(),inner_sep)
        F2 = F2[:0-len(inner_sep)]
        #displays
        F3 =''
        for display in self.o_displays:
            F3+="%s%s" % (display.toString(),inner_sep)
        F3 = F3[:0-len(inner_sep)]
        return  "%s%s%s%s%s%s%s" %(F0,outer_sep,F1,outer_sep,F2,outer_sep,F3) 


    #----------------------------------------------------------------------
    def parseLine(self, line):
        """
        mergelog v1 是单行一条记录，解析当前行记录
        """
        #init 
        self.attrs={} #query attr all have
        self.o_clicks= []
        self.o_displays= []
        #deal
        if line.isspace():
            return self.LOG_UNFINISHED

        try:

            (glof_dict,sedict0,sedict1) = (fdict['glof'],fdict['seinfo1'],fdict['seinfo2'])
            fields = line[:-1].split(outer_sep)
            #main area  
            if fields[0] != '':
                glof_f = fields[0].split('\t')
                #--------------add seid 20100727---------------
                if len(glof_f) == 8:
                    glof_dict = ('cookie,0,STRING', 'ip,1,STRING', 'sid,2,STRING', 'qid,3,STRING', 'secnt,4,INT', 'clcnt,5,INT', 'tabcnt,6,INT', 'discnt,7,INT')
                #------------------------------------------------------
                for unit in glof_dict:
                    (key,pos,ty) = unit.split(',')
                    if int(pos) >= len(glof_f):
                        continue
    
                    val = glof_f[int(pos)]
                    if ty == 'INT':
                        val = int(val)
                    elif ty == 'FLOAT':
                        val = float(val)
                    else:
                        pass
                    self.attrs[key] = val
            if fields[1] != '':
                (se1_f,se2_f) = fields[1].split(inner_sep)
                sefields0 = se1_f.split('\t')
                sefields1 = se2_f.split('\t')
                #------------add info , statisfy in click pub ,20100727---
                if len(sefields0) == 0:
                    sedict0 = ('query,0,STRING', 'setm,1,STRING', 'pn,2,INT', 'tn,3,STRING', 'tp,4,STRING', \
                               'f,5,INT', 'rsp,6,INT', 'bduss,7,STRING', 'id,8,INT', 'oq,9,STRING', 'cid,10,INT')
                #-----------------------------------------------------------------
                sefields=[sefields0,sefields1]
                sedict=[sedict0,sedict1]
                for i in range(0,len(sefields)):
                    if len(sefields[i]) <= 1:
                        continue
                    sdict = sedict[i]
                    sefld = sefields[i]
                    for unit in sdict:
                        (key,pos,ty) = unit.split(',')
                        if int(pos) >= len(sefld):
                            continue
    
                        val = sefld[int(pos)]
                        if ty == 'INT':
                            val = int(val)
                        elif ty == 'FLOAT':
                            val = float(val)
                        else:
                            pass
                        self.attrs[key] = val
            #####click area
            c_fields = fields[2].split(inner_sep) #second level
            for fld in c_fields:
                if fld == '':
                    continue
                fld_fields = fld.split('\t')
                cf_obj = Click(fld_fields)
                self.o_clicks.append(cf_obj)
            #####display url area
            d_fields = fields[3].split(inner_sep)
            for fld in d_fields:
                if fld == '':
                    continue
                fld_fields = fld.split('\t')
                ds_obj = Display(fld_fields)
                self.o_displays.append(ds_obj)
            ######return true##########
            return self.LOG_FINISHED
        
        except (KeyError, ValueError, IndexError):
            return self.LOG_BROKEN
            
    
        
        

#########################################################################

class MergeSessionRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.o_scookie=''
        self.o_sip=''
        self.o_stag='' #session tag
        self.o_count=0
        self.o_actions=[]

        self.lastRec = None
        self.currRec = None

    def attr(self,name):
        val=None
        if hasattr(self,'o_'+name):
            val = getattr(self,'o_'+name)
        elif name in self.attrs:
            val = self.attrs[name]
        else:
            pass 
        return val

    def sameSession(self,rec1,rec2):
        if rec1.attr('cookie')==rec2.attr('cookie') and \
           rec1.attr('ip')==rec2.attr('ip') and \
           rec1.attr('sid')==rec2.attr('sid'):
            return True
        else:
            return False

    def readNext(self, f):
        self.o_actions=[] #clear it
        self.o_count=0
        self.o_scookie=None
        self.o_sip=None
        self.o_stag=None

        if self.lastRec != None:
            self.o_actions.append(self.lastRec)
            self.o_count += 1
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')
            if not self.o_stag:
                self.o_stag = self.lastRec.attr('sid')

        while True:
            line = f.readline()
            if not line: #EOF  quit
                self.lastRec = None
                self.currRec = None
                if len(self.o_actions) > 0:
                    return True
                return False
            if line.isspace():
                continue
            args=[line,'']
            self.currRec = MergeRecord(args)

            if self.lastRec != None and not self.sameSession(self.lastRec, self.currRec):
                self.lastRec = self.currRec    #record it
                self.currRec = None
                return True

            self.lastRec = self.currRec
            self.o_actions.append(self.currRec)
            self.o_count += 1  #count
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')
            if not self.o_stag:
                self.o_stag = self.lastRec.attr('sid')
            self.currRec = None

    def asString(self):
        s=''
        for rec in self.o_actions:
            s += "%s\n" %(rec.asString())
        return s[:-1] #remove the last \n

##########################################################
class MergeBigSessionRecord(Record):
    def __init__(self):
        Record.__init__(self)
        self.o_scookie=''
        self.o_sip=''
        self.o_count=0
        self.o_actions=[]

        self.lastRec = None
        self.currRec = None

    def attr(self,name):
        val=None
        if hasattr(self,'o_'+name):
            val = getattr(self,'o_'+name)
        elif name in self.attrs:
            val = self.attrs[name]
        else:
            pass                                                        
        return val

    def sameSession(self,rec1,rec2):
        if rec1.attr('cookie') == rec2.attr('cookie') and rec1.attr('ip') == rec2.attr('ip'):
            return True
        else:
            return False

    def readNext(self, f):
        self.o_actions=[] #clear it
        self.o_count=0
        self.o_scookie=None
        self.o_sip=None

        if self.lastRec != None:
            self.o_actions.append(self.lastRec)
            self.o_count += 1
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')

        while True:
            line = f.readline()
            if not line: #EOF  quit
                self.lastRec = None
                self.currRec = None
                if len(self.o_actions) > 0:
                    return True
                return False
            if line.isspace():
                continue
            args=[line,'']
            self.currRec = MergeRecord(args)

            if self.lastRec != None and not self.sameSession(self.lastRec, self.currRec):
                self.lastRec = self.currRec    #record it
                self.currRec = None
                return True


            self.lastRec = self.currRec
            self.o_actions.append(self.currRec)
            self.o_count += 1  #count
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')
            self.currRec = None

    def asString(self):
        s=''
        for rec in self.o_actions:
            s += "%s\n" %(rec.asString())
        return s[:-1] #remove the last \n



MAX_ATOMIC_SESSION_ACTION = 1000


    
    

########################################################################
class MergeAtomicSessionRecord(Record):
    """ define the AtomicSessionRecord counterpart for mergelog"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Record.__init__(self)
        self.o_scookie=''
        self.o_sip=''
        self.o_stag='' #session tag
        self.o_count=0
        self.o_actions=[]
        self.o_next_action = None

        self.lastRec = None
        self.currRec = None        
        
    def attr(self,name):
        val=None
        if hasattr(self,'o_'+name):
            val = getattr(self,'o_'+name)
        elif name in self.attrs:
            val = self.attrs[name]
        else:
            pass 
        return val   

    def sameAtomicSession(self,rec1,rec2):
        if rec1.attr('cookie')==rec2.attr('cookie') and \
           rec1.attr('ip')==rec2.attr('ip') and \
           rec1.attr('sid')==rec2.attr('sid') and \
           rec1.attr('query') == rec2.attr('query'):
            return True
        else:
            return False
        
    def sameSession(self,rec1,rec2):
        if rec1.attr('cookie')==rec2.attr('cookie') and \
           rec1.attr('ip')==rec2.attr('ip') and \
           rec1.attr('sid')==rec2.attr('sid') :
            return True
        else:
            return False

    def readNext(self, f):
        self.o_actions=[] #clear it
        self.o_count=0
        self.o_scookie=None
        self.o_sip=None
        self.o_stag=None
        self.o_next_action = None

        if self.lastRec != None:
            self.o_actions.append(self.lastRec)
            self.o_count += 1
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')
            if not self.o_stag:
                self.o_stag = self.lastRec.attr('sid')

        while True:
            line = f.readline()
            if not line: #EOF  quit
                self.lastRec = None
                self.currRec = None
                self.o_next_action = None
                if len(self.o_actions) > 0:
                    return True
                return False
            if line.isspace():
                continue
            args=[line,'']
            self.currRec = MergeRecord(args)

            if self.currRec.attr('secnt') == 0:
                continue                 # skip the display without search ( e.g. huge spamcookie)

            if self.lastRec != None and \
               ( self.o_count >= MAX_ATOMIC_SESSION_ACTION or \
                 not self.sameAtomicSession(self.lastRec, self.currRec) ):
                if self.sameSession(self.lastRec, self.currRec):
                    self.o_next_action = self.currRec
                else:
                    self.o_next_action = None
                self.lastRec = self.currRec    #record it
                self.currRec = None
                return True

            self.lastRec = self.currRec
            self.o_actions.append(self.currRec)
            self.o_count += 1  #count
            if not self.o_scookie:
                self.o_scookie = self.lastRec.attr('cookie')
            if not self.o_sip:
                self.o_sip = self.lastRec.attr('ip')
            if not self.o_stag:
                self.o_stag = self.lastRec.attr('sid')
            self.currRec = None

    def asString(self):
        s=''
        for rec in self.o_actions:
            s += "%s\n" %(rec.asString())
        return s[:-1] #remove the last \n    
    
########################################################################
class UBSAccessLogRecord(Record):
    """define a parser for accesslog"""
    
    # most common format : r'%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"'
    _baidu_click_apache_log_format_ = r'%h %t \"%r\" %>s %{Cookie}i'  # some click server records referer
    
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Record.__init__(self)
        
        #import apachelog
        #self._apachelog = apachelog.parser(AccessLogRecord._baidu_click_apache_log_format_)
        import re
        self._parser = re.compile(r'^(\S*) \[(.+?)\] \"(.+?)\" (\S+) (.+)$')
        
        # raw string
        self._host = ''
        self._time = ''
        self._request = ''
        self._status = ''
        self._cookie = ''
        
        # parsed string
        self.a_ip = ''
        self.a_time = 0
        self.a_status = 0
        self.a_cookie = 0
        
        
    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():
                if self.parseLine(line) == self.LOG_FINISHED:
                    break
   
                
        return True       
    
    #----------------------------------------------------------------------
    def parseLine(self, line):
        """
        parse a single log line. The line may be trimmed before.
        
        """
        #the format of click log apaches are not the same
        #data = self._apachelog.parse(line)
        #self._host = data['%h']
        #self._time = data['%t']
        #self._request = data['%r']
        #self._status = data['%>s']
        #self._cookie = data['%{Cookie}i']
        if line.isspace() or not line:
            return self.LOG_UNFINISHED
        
        m = self._parser.match(line)
        if not m:
            return self.LOG_BROKEN  # skip the invalid record
        
        (self._host, self._time, self._request, self._status, self._cookie) = m.group(1, 2, 3, 4, 5)
                        
        self.a_ip = self._host
        
        try:
            self.a_time = int(time.mktime(time.strptime( self._time
                                                     , '%d/%b/%Y:%H:%M:%S +0800')))
        except ValueError:
            return self.LOG_BROKEN
        
        if self._status != '200': # vwu/(/\" HTTP/1.1" 200 BAIDUID=1C2E5782E208209A4D61329BE0285D48:FG=1 refer=
            return self.LOG_BROKEN
        else:
            self.a_status = 200
            
        # BAIDUID=0F679C81019565A0B41BC1D3620B200F:FG=1; BD_UTK_DVT=1; dmuid=ca777250-3275-11de-8d26-d5d6e5198bea; dm4d0fedfe-a743-7118-665b-4d3b05f3013a=5%7C1240902601; Hm_lvt_458=1240902601468; PASSPORTRETRYSTRING=1241534606|1241534628  
        fs = self._cookie.split("BAIDUID=", 1)
        if len(fs) == 2:
            self.a_cookie = fs[1].split(":")[0]
        else:
            self.a_cookie = '-'
            
        self.attrs = {}

        fs = self._request.split()   # ""GET /w.gif?q=xxx HTTP/1.1"
        if len(fs) != 3:
            ##DEBUG
            #sys.stderr.write("invalid request: %s IN %s" % (self._request, line))
            return self.LOG_BROKEN
        
        (method, resource, protocol) = fs 
        if method != 'GET' or protocol != 'HTTP/1.1':
            return self.LOG_BROKEN
        
        fs =  resource.split('?', 1)  # /w.gif?q=%CCxxxxx&b=xxxx&c=xxx
        if len(fs) != 2:
            return self.LOG_BROKEN
        (prefix, paras) =  fs  
        if prefix != '/w.gif':
            return self.LOG_BROKEN       
            
        for p in paras.split('&'):
            # &path=http://www.baidu.com/s?tn=sitehao123&bs=dnf%D7%D4%B6%AF%CB%B5%BB%B0%B9%A4%BE%DF&f=3
            if p.startswith('path='):
                fs = p.split("?",1)
                if len(fs) == 2:
                    fs2 = fs[1].split("=",1)
                    if len(fs2) == 2:
                        key = fs2[0]
                        val = fs2[1]
                    else:
                        key = fs[0]
                        val = ""
                    self.attrs[key] = val                        
                
            fs = p.split('=', 1)
            if len(fs) >= 2:
                (key, val) = fs[0:2]
            else:  # 4%B0&amp&tn=max2_cb&cid=0&q
                key = fs[0]   
                val = ''
                
            if key not in self.attrs:
                self.attrs[key] = val
            else:
                # TODO maybe important for tracking exceptions
                self.attrs[key] = val
                
        return self.LOG_FINISHED  # a line is properly parsed
            
        
    def asString(self):
        # r'%h %t \"%r\" %>s %{Cookie}i'
        return '%s [%s] "%s" %s %s' % (
                   self._host,
                   self._time,
                   self._request,
                   self._status,
                   self._cookie)
    
    
########################################################################
class NSAccessLogRecord(Record):
    """define a parser for NS apache accesslog"""
    
    # most common format : r'%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"'
    _baidu_click_apache_log_format_ = r'%h %t \"%r\" %>s %{Cookie}i'  # some click server records referer
    
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Record.__init__(self)
        # raw string
        self._rawlog = ''
        
        # parsed string
        self.a_ip = ''
        self.a_time = ''
        self.a_timezone = ''
        self.a_method = ''
        self.a_url = ''
        self.a_protocol = ''
        self.a_statuscode = ''
        self.a_referer = ''
        self.a_urlpre = ''
        self.a_cookie = ''
        self.a_urlfields = {}
        self.a_baiduid = ''

        #import apachelog
        #self._apachelog = apachelog.parser(AccessLogRecord._baidu_click_apache_log_format_)
        #ns 点击apache服务器的数据
        import re
        # 10.23.205.10 - - [14/Jul/2013:16:11:59 +0800] "GET /u.gif?ts=1qfx&pid=241&sid=hj3yreoqcvstj&hid=762&page=tieba-index&ver=5&p=110&px=1366*768&ref=http%3A%2F%2Fwww.baidu.com%2Findex.php%3Ftn%3D10018801_hao&xp=A4AA(home_wrap)A3AA(like_wrap)EFDB3C&g=like_box&ep=.5%2C0&ci=3&pp=522%2C375&ps=1366%2C5906&u=%2Ff%3Fkw%3D%25BA%25AB%25BA%25A3%26fr%3Dindex%26fp%3D0&txt=%E9%9F%A9%E6%B5%B7 HTTP/1.1" 200 0 + 0 "http://tieba.baidu.com/" BAIDUID=42872CB620F068D26BCFB8E789DB1ADA:FG=1; locale=zh; BDUSS=t3ZGlWZjVLUkhrejZhWHlVVmUtSXhQRXlNNnJzfkpVOXY5c2QyMW1BY1Zud2xTQUFBQUFBJCQAAAAAAAAAAAEAAAAz3NUxv8mwrrXEsabX0wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABUS4lEVEuJRS2; SSUDBTSP=1373770261; SSUDB=t3ZGlWZjVLUkhrejZhWHlVVmUtSXhQRXlNNnJzfkpVOXY5c2QyMW1BY1Zud2xTQUFBQUFBJCQAAAAAAAAAAAEAAAAz3NUxv8mwrrXEsabX0wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABUS4lEVEuJRS2 "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0"
        self._parser = re.compile(r'^(\d+\.\d+\.\d+\.\d+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}) (\+\d{4})\] \"(.*?) (.*?) (.*?)\" (.*?) .*?\"(.*?)\" (.*?)$')
        # 0071696AAF337C2619CE65A3AB2DB166%3AFG=1
        # 0071696AAF337C2619CE65A3AB2DB166:FG=1
        self._baiduid_parser = re.compile(r'BAIDUID=(.*?)[: %]')        
        

        
        
    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():
                if self.parseLine(line):
                    return True
  
    #----------------------------------------------------------------------
    def parseLine(self, line):
        """"""
        self._rawlog = line.strip()
        
        m = self._parser.match(self._rawlog)
        if not m:
            return False
        
        (_host, _time, _timezone, _request_method, _request_url, _request_protocol, _status, _referer, _cookie) = m.group(1, 2, 3, 4, 5, 6, 7, 8, 9)
                                
        self.a_ip = _host
        self.a_timesz = _time
        self.a_time = int(time.mktime(time.strptime(_time, '%d/%b/%Y:%H:%M:%S')))
        self.a_timezone = _timezone
        self.a_method = _request_method
        self.a_url = _request_url
        self.a_protocol = _request_protocol
        self.a_statuscode = _status
        self.a_referer = _referer
        self.a_cookie = _cookie
        self.a_urlfields = {}
        
        fs = _request_url.split('?', 1)
        
        self.a_urlpre = fs[0]
        if len(fs) > 1:
            fields = fs[1].split('&')
            for tmp in fields:
                try:
                    (key, val) = tmp.split('=', 1) # path=http://path?a=xxx&b=xxx
                    val = urllib2.unquote(val)
                    self.a_urlfields[key] = val
                except:
                    pass        # skip the key val pair

        
        m = self._baiduid_parser.search(_cookie)
        if m:
            self.a_baiduid = m.group(1)
        else:
            self.a_baiduid = '-'
        
        return True            
        
    def asString(self):
        # r'%h %t \"%r\" %>s %{Cookie}i'
        return self._rawlog
    
########################################################################
class QueryLogRecord(Record):
    """define a parser for the simplest querylog parser"""
    
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Record.__init__(self)
        # parsed string
        self.a_query = ''
        self.a_nSearch = 1
        self.a_nClick = 0        
        
    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():

                try:
                    query, nSearch, nClick = line.strip().split("\t")
                    self.a_query = query
                    self.a_nSearch = int(nSearch)
                    self.a_nClick = float(nClick)
                    
                except:
                    sys.stderr.write("ill formated querylog line '%s'\n" % line)
                    continue
                                
                return True       
    
    def asString(self):
        
        return "%s\t%s\t%s" % (self.a_query, self.a_nSearch, self.a_nClick)
    

######################################################################
class UIRecord(Record):
    """define a ui.log parser
    
    The UI log format is at
      - http://wiki.babel.baidu.com/twiki/bin/view/Ps/Searcher/DisllayLogFomat
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = UIRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('query')
        >>>     print rec.attr('queryinfo.cookie')
        
        - With string:
          
        >>> rec = UIRecord()
        >>> for line in fh.readlines():
        >>>     if not rec.parseLine(line):
        >>>         continue
        >>>     print rec.attr('query')
        >>>     print rec.attr('queryinfo.cookie')      
        
      2. data fields
        1. basic
        
           >>> rec.attr('time')
           >>> rec.attr('query')
           
        2. queryinfo
          1. there are current 19 fields.
        
           >>> rec.attr('queryinfo.qid')
           >>> rec.attr('queryinfo.pageNo')
           >>> ...
           >>> rec.attr('queryinfo.ext')
           
        3. urlinfo
           1. see MergeRecord for details.
           
           >>> displays = rec.attr('displays')
           >>> for d in displays:
           >>>     print d.attr('url')
           >>>     print d.attr('urlinfo.src')

    history
    =======
    
      1. 2010 first written by wangyujie@baidu.com
      2. 2012-02-06 update by pengtao@baidu.com.

    """

    # queryinfo format discription
    queryinfo_part = ('qid,0,STRING', 
                      'pageNo,1,INT',  
                      'asnum,2,INT', 
                      'sample_id,3,INT',  # 旧抽样id，已废弃
                      'ip,4,INT',
                      'cookie,5,STRING',
                      'tn,6,STRING',
                      'need_sp,7,INT',
                      'dn,8,INT',
                      'ln,9,INT',
                      'rs,10,STRING',
                      'se,11,STRING',
                      'url,12,STRING',
                      'ad_jp_num,13,INT',
                      'is_se,14,INT',
                      'app_num,15,INT',
                      'fav_num,16,INT',
                      'sampling_id,17,STRING', # 新抽样id，类似1194_1228_1225_1206_1186_1179
                      'ext,18,STRING')  
    
    def __init__(self):
        self.attrs={}
        self.a_time=None
        self.a_query=None
        self.a_displays= []
        self.a_rawstring = ''


    def readNext(self,f):
        
        while True:
            line = f.readline()
            if not line: #EOF  quit
                return False
            if line.isspace():
                continue

            if self.parseLine(line) == self.LOG_FINISHED:
                return True
            else:
                return False
        
    def asString(self):
        return self.a_rawstring

    def clearAttrs(self):
        """
        Clean the partially intialized attributes.

        Mainly on invalid log.
        
        """

        self.attrs = {}
        self.a_time = None
        self.a_query = None
        self.a_displays = []
        self.a_rawstring = None
        
    #----------------------------------------------------------------------
    def parseLine(self, line):
        """
        Parse the input record data.         
        """

        self.clearAttrs()
        self.a_rawstring = line
        
        (query_fld,qinfo_fld,disp_fld) = line.strip().split('\t',2)
        
        # example of query_fld
        # URL: 2012-02-02 22:00:08: 湖南卫视
        sub_list = query_fld.split(': ')
        self.a_time = int(time.mktime(time.strptime(sub_list[1], '%Y-%m-%d %H:%M:%S')))
        self.a_query=sub_list[2]
           
        # example of queryinfo part
        # e47...aa7:1:10:0:1143972207:2C8...D0B:baidusite:1:70581817:760:\
        # 湖南卫视在线直播|0-...-宫锁珠帘湖南卫视版|0::\
        # /s?tn=baidusite&word=%BA%FE%C4%CF%CE%C0%CA%D3:0:0:0:0:0:
        url_idx = 12
        url_tail_len = len(self.queryinfo_part) - url_idx - 1
        
        fld_fields_1= qinfo_fld.split(':',url_idx)
        fld_fields = fld_fields_1[:-1]
        # url may contains ":"
        fld_fields_2 = fld_fields_1[-1].rsplit(':', url_tail_len) 
        fld_fields += fld_fields_2

        for unit in self.queryinfo_part:
            (key,pos,ty) = unit.split(',')
            val = fld_fields[int(pos)] #value array
            if ty == 'INT':
                val = int(val)
                if pos == '4': #parse ip to *.*.*.*
                    val = ip2str(val)
                    
            elif ty == 'FLOAT':
                val = float(val)
            
            kstr = 'queryinfo.' + key
            self.attrs[kstr]=val #optional save STRING  OPTIONAL 

        ##display url area
        d_fields = disp_fld.split('\t')
        idx = 0
        while idx < len(d_fields):
            fld1 = d_fields[idx] # url
            fld2 = d_fields[idx+1] # urlinfo
            if fld1 == '' or fld2 == '':
                idx += 2
                continue
            fld_fields = [fld1,fld2]
            ds_obj = Display(fld_fields)
            self.a_displays.append(ds_obj)
            idx += 2
        return self.LOG_FINISHED

class DetailnewquerysortRecord(Record):
    """Define a parser for detailnewquerysort.
    The format of detailnewquerysort is at
      - http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Clickdata_data_format#detailnewquerysort
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = DetailnewquerysortRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('query')
        >>>     print rec.attr('search')
        
        - With string:
          
        >>> rec = DetailnewquerysortRecord()
        >>> for line in fh.readlines():
        >>>     if rec.parseLine(line) != rec.LOG_FINISHED:
        >>>         continue
        >>>     print rec.attr('urls')[0]['mu']
        >>>     print rec.attr('urls')[0]['url']      
        
      2. data fields
        1. basic
        
           >>> rec.attr('query')
           >>> rec.attr('search')
           >>> rec.attr('wgtclick')
           >>> rec.attr('click')
           
           
        2. urls
          1. The clicked urls are stored in a list, each has 10 fields
        
           >>> rec.attr('urls')[0]['fm']
           >>> rec.attr('urls')[0]['click.satify']
           >>> ...
           >>> rec.attr('urls')[0]['mu']


    history
    =======
    
      1. 2012-02-28 written by pengtao@baidu.com.
"""

    
    def __init__(self):
        Record.__init__(self)
        self.a_query = ''
        self.a_search = 0
        self.a_wgtclick = 0
        self.a_click = 0
        
        self.a_rawstring = ''
        self.a_urls = []
        self._nextline = ''  # next query
        
        self._is_last_rec = False


    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                if not self._is_last_rec:
                    self._is_last_rec = True  # prepare the last record
                    return True
                else:
                    return False
            ret = self.parseLine(line)
            if rec == self.LOG_FINISHED:
                return True
            
            
    
    #----------------------------------------------------------------------
    def _initNewRec(self):
        """Parse the summary line and be ready for followings.        
        """
        tmp = self._nextline.split("\t")
        (self.a_query, self.a_search, 
         self.a_wgtclick, self.a_click) = (tmp[0], int(tmp[1]),
                                           float(tmp[2]), int(tmp[3]))
        self.a_rawstring = self._nextline
        self.a_urls = []
        

    def parseLine(self, line):
        """Parse and add a line for current record.
        
        @type line: string
        @param line: the input line of current/next record
        @rtype: flag
        @return: flag for adding this line.
                 LOG_BROKEN: invalid log
                 LOG_UNFINISHED: current record remains
                 LOG_FINISHED: current record is finished.
        """
        fs = line.split("\t")
        n = len(fs)
        if n == 4: # summary line
            self._nextline = line
            return self.LOG_FINISHED         
        elif n == 11:  # detail line
            if self._nextline <> '':
                self._initNewRec()
                self._nextline = ''
                
            self.a_rawstring += line
            
            if fs[0] <> self.a_query:
                return self.LOG_BROKEN
            else:
                rec = {}
                #  查询串	 点击类型	 点击主位置	 是否是子链接点击	 子链接点击位置	 策略	 加权点击值	 绝对点击次数	 结尾点击次数:满意点击值	 点击的url	 子链接的主链接（默认为”-”）
                rec['fm'] = fs[1]
                rec['p1'] = int(fs[2])
                rec['sub'] = int(fs[3])
                rec['p2'] = int(fs[4])
                rec['strategy'] = fs[5]
                rec['w'] = float(fs[6])
                rec['click'] = int(fs[7])
                tmp = fs[8].split(":")
                rec['click.last'] = int(tmp[0])
                rec['click.satisfy'] = float(tmp[1])
                rec['url'] =  fs[9]
                rec['mu'] = fs[10]
                self.a_urls.append(rec)
                
                return self.LOG_UNFINISHED
        else:
            return self.LOG_BROKEN # wrong format      
            
    def asString(self):
        return self.a_rawstring
    
class BaiduWholeSessionRecord(Record):
    """
    解析全百度session日志的parser.    
    
    全百度session日志在log平台生成，详见
    http://log.baidu.com/?m=Job&a=ComplexEditor&jid=11478
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = BaiduWholeSessionRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('user')['cookie']

        
        - With string:
          
        >>> rec = BaiduWholeSessionRecord()
        >>> for line in fh.readlines():
        >>>     if rec.parseLine(line) != rec.LOG_FINISHED:
        >>>         continue
        >>>     print rec.attr('user')['uid']
        >>>     print rec.attr('user')['uname']     
        
      2. data fields
        1. user        
           >>> rec.attr('user')['cookie']
           >>> rec.attr('user')['uid']
           >>> rec.attr('user')['uname']           
           
        2. actions
           >>> for act in rec.attr('actions'):
           >>>     print act['product.pid']
           >>>     print act['product.pname']
           >>>     print act['product.aid']
           
        3. sessions  
          The sessions are list of actions
          
           >>> for session in rec.attr('sessions'):
           >>>     print act['session.id']
           >>>     for act in session:
           >>>         print act['url']
           >>>         print act['refer']
           >>>         print act['query']

    history
    =======
    
      1. 2012-05-24 written by pengtao@baidu.com.
    
    """
    def __init__(self):
        Record.__init__(self)
        self.a_user = {'cookie':'',
                       'uid':'',
                       'uname':''}
        self.a_sessions = []
        self.a_actions = []
        
        # signal for parseLine
        self._last_line_status = self.LOG_UNFINISHED

    def readNext(self, f):
        """
        wrapper of the parseLine
        """
        self.a_actions = []
        while True:
            line = f.readline()
            if not line:
                if len(self.a_actions) > 0:
                    self._compute()
                    return True
                return False
            if self.parseLine(line) == self.LOG_FINISHED:
                return True
            else:
                continue



    #----------------------------------------------------------------------
    def parseLine(self, line):
        """read a log line and parse it. Similar to readLine.
        Please see Record.parseLine for parameters.
        """
        
        fields = line.strip().split('\t')
        #  assert(len(fields) == 28)
        if len(fields) != 17:
            return self.LOG_BROKEN
        
        action = { 'session.id': fields[0],
                   'session.step': fields[1],
                   'user.cookie': fields[2],
                   'user.uid': fields[3],                   
                   'user.uname': fields[4],
                   'location.ip': fields[5],
                   'location.address': fields[6],
                   'product.pid': fields[7],
                   'product.pname': fields[8],
                   'time': fields[9],
                   'url': fields[10],
                   'refer': fields[11],
                   'method': fields[12],
                   'aid': fields[13],
                   'aname': fields[14],
                   'query': fields[15],
                   'attributes': fields[16],
                   }

        if self._last_line_status == self.LOG_FINISHED:
            self.a_user['cookie'] = self._next_action['user.cookie']
            self.a_user['uid'] = self._next_action['user.uid']
            self.a_user['uname'] = self._next_action['user.uname']
            self.a_actions = [self._next_action]
            self._last_line_status = self.LOG_UNFINISHED
            
        if action['user.cookie'] != self.a_user['cookie']:
            self._next_action = action
            if self.a_user['cookie'] :
                self._compute()
                self._last_line_status = self.LOG_FINISHED
                return self.LOG_FINISHED
            else:
                self._last_line_status = self.LOG_FINISHED
                return self.LOG_UNFINISHED
        else:
            self.a_actions.append(action)
            return self.LOG_UNFINISHED
        
    def asString(self):
        """
        the output is not the exact: e.g. 0.5 --> 0.500000
        """
        order = ['session.id',
                 'session.step',
                 'user.cookie',
                 'user.uid',                   
                 'user.uname',
                 'location.ip',
                 'location.address',
                 'product.pid',
                 'product.pname',
                 'time',
                 'url',
                 'refer',
                 'method',
                 'aid',
                 'aname',
                 'query',
                 'attributes']

                   
        s = ''
        for a in self.a_actions:
            s += "\t".join(map(lambda x: a[x], order)) + "\n"
        return s

    def _compute(self):
        """ 将 action list 截断为 session list
        """
        self.a_sessions = []
        for session in split_list_by_key(self.a_actions, key_pos='session.id'):
            self.a_sessions.append(session)
            
        return
        
class QueryStatRecord(Record):
    """Define a parser for QueryStat (strategy cube data with query dimension by chenxiaoqian).
    The data is on stoff:
      - /ps/ubs/chenxiaoqian/new_atomicsession_cube/step3.1_mergeinfo/20130324/
    The format of newcookiesort is at
      - key = part1{\c}part2{\c}part3   value1 value2
    The detailed data fields is on 
      -   http://cq01-2011q4-uptest2-1.vm.baidu.com:8080/index.html
       
    usage
    =====
    
      1. example

        - With file handle:
        
        >>> rec = QueryStatRecord()
        >>> while True:
        >>>     flag = rec.readNext(fh)
        >>>     if not flag:
        >>>         break
        >>>     print rec.attr('query')
        >>>     print rec.attr('')
        
        - With string:
          
        >>> rec = NewCookieRecord()
        >>> for line in fh.readlines():
        >>>     if rec.parseLine(line) != rec.LOG_FINISHED:
        >>>         continue
        >>>     print rec.attr('cookie')
        >>>     print rec.attr('url')  


    history
    =======
    
      1. 2013-07-07 created.
    """    
    def __init__(self):
        Record.__init__(self)
        (query, date, singlerank, search_type, 
                 following_type, sid, is_oq_se, action_num, 
                 query_searches, query_term_len, query_char_len, zhida_query) = dim_items         
        self.a_query = ''
        self.a_date = ''
        self.a_singlerank = 0
        self.a_search_type = ''
        self.a_following_type = 0
        self.a_sid = 0
        self.a_is_oq_se = 0
        self.a_action_num = 0
        self.a_query_searches = 0
        self.a_query_term_len = ''
        self.a_query_char_len = ''
        self.a_zhidao_query = ''
        self.attrs = {}

    def readNext(self, f):
        while True:
            line = f.readline()
            if not line:
                return False
            if not line.isspace():
                break
        fields = line[:-1].split('\t')
        assert(len(fields) == 28)
        self.a_cookie = fields[0]
        self.a_ip = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_fm = fields[3]
        self.a_pn = int(fields[4])
        self.a_p1 = int(fields[5])
        self.a_p2 = int(fields[6])
        self.a_p3 = int(fields[7])
        self.a_p4 = int(fields[8])
        self.a_tn = fields[9]
        self.a_tab = fields[10]
        self.a_title = fields[11]
        self.a_tp = fields[12]
        self.a_f = int(fields[13])
        self.a_rsp = int(fields[14])
        self.a_F = int(fields[15])
        self.a_query = fields[16]
        self.a_url = fields[17]
        self.a_reg = int(fields[18])
        self.a_w = float(fields[19])
        self.a_id = int(fields[20])
        self.a_info = fields[21]
        self.a_prefixsug = fields[22]
        self.a_mu = fields[23]
        self.a_s = fields[24]
        self.a_oq = fields[25]
        self.a_qid = fields[26]
        self.a_cid = fields[27]
        return True

    #----------------------------------------------------------------------
    def parseLine(self, line):
        """read a log line and parse it. Similar to readLine.
        Please see Record.parseLine for parameters.
        """
        if self.readLine(line):
            return self.LOG_FINISHED
        else:
            return self.LOG_UNFINISHED
        
    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        Input a log line for this parser. 
        The EOF signal is handled outside parser itself.        
        
        @type line: string
        @param line: the inputed next line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        """

        if line.isspace():
            return False

        fields = line[:-1].split('\t')
        assert(len(fields) == 28)
        self.a_cookie = fields[0]
        self.a_ip = fields[1]
        self.a_time = int(time.mktime(time.strptime(
            fields[2], '%d/%b/%Y:%H:%M:%S')))
        self.a_fm = fields[3]
        self.a_pn = int(fields[4])
        self.a_p1 = int(fields[5])
        self.a_p2 = int(fields[6])
        self.a_p3 = int(fields[7])
        self.a_p4 = int(fields[8])
        self.a_tn = fields[9]
        self.a_tab = fields[10]
        self.a_title = fields[11]
        self.a_tp = fields[12]
        self.a_f = int(fields[13])
        self.a_rsp = int(fields[14])
        self.a_F = int(fields[15])
        self.a_query = fields[16]
        self.a_url = fields[17]
        self.a_reg = int(fields[18])
        self.a_w = float(fields[19])
        self.a_id = int(fields[20])
        self.a_info = fields[21]
        self.a_prefixsug = fields[22]
        self.a_mu = fields[23]
        self.a_s = fields[24]
        self.a_oq = fields[25]
        self.a_qid = fields[26]
        self.a_cid = fields[27]
        
        return True

    def asString(self):
        return '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d' \
               '\t%d\t%s\t%s\t%d\t%f\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                   self.a_cookie, self.a_ip,
                   time.strftime('%d/%b/%Y:%H:%M:%S', time.localtime(self.a_time)),
                   self.a_fm, self.a_pn, self.a_p1, self.a_p2, self.a_p3, self.a_p4,
                   self.a_tn, self.a_tab, self.a_title, self.a_tp, self.a_f,
                   self.a_rsp, self.a_F, self.a_query, self.a_url, self.a_reg,
                   self.a_w, self.a_id, self.a_info, self.a_prefixsug, self.a_mu,
                   self.a_s, self.a_oq, self.a_qid, self.a_cid)