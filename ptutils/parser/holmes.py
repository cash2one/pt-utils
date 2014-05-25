#! /usr/bin/env python
#coding:utf-8

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. Holmes(百度统计)相关日志的parser
 History:
     1. 2013/8/30 创建
"""



import sys
from recordz import Record

########################################################################
class PreSessionRecord(Record):
    """
    parser for holmes_pre_session.
    It's a single line parser.
    usage
    =====
        详细字段见readline函数。
        
            >>> rec = PreSessionRecord()
            >>> for line in fh:
            >>>     if not rec.readLine(line):
            >>>         continue
            >>>     print rec.attr('baiduid')
            >>>     print rec.attr('visit_time')
            >>>     for pv in rec.attr('pv_list'):
            >>>          print pv["visit_url"]
            >>>          print pv["last_url"]
            >>>     for act in rec.attr('act_list'):
            >>>          print act["action"]
            >>>          print act["label"]
    
    history
    =======
    
    1. 2013-08-30 create parser.
    """    
    def __init__(self):
        Record.__init__(self)
        
        self._idxs = 0 # start idx for session pv and act
        
        self.a_session_id = ""
        self.a_session_field_count = 0
        self.a_pv_count = 0
        self.a_pv_col = 0
        self.a_pv_list = []        
        self.a_act_count = 0
        self.a_act_col = 0
        self.a_act_list = []
        
        self.a_log_type = ""
        self.a_visitor_id = ""
        self.a_baiduid = ""
        self.a_visit_time = ""
        self.a_last_visit_time = ""
        self.a_is_new_visitor = ""
        self.a_siteid = ""
        self.a_site_status = ""
        self.a_trade_id1 = ""
        self.a_trade_id2 = ""
        self.a_from_type = ""
        self.a_from_url = ""
        self.a_domain_type = ""
        self.a_engine_id = ""
        self.a_subengine_id = ""
        self.a_page_num = ""
        self.a_keyword = ""
        self.a_clickid = ""
        self.a_ip = ""
        self.a_province_id = ""
        self.a_city_id = ""
        self.a_isp_id = ""
        self.a_browser = ""
        self.a_os = ""
        self.a_screen = ""
        self.a_color = ""
        self.a_flash = ""
        self.a_language = ""
        self.a_cookie = ""
        self.a_java = ""
        self.a_custom_from = ""
        self.a_custom_media = ""
        self.a_custom_plan = ""
        self.a_custom_word = ""
        self.a_custom_idea = ""
        self.a_click_type = ""
        self.a_typeid = ""
        self.a_userid = ""
        self.a_planid = ""
        self.a_unitid = ""
        self.a_ideaid = ""
        self.a_wordid = ""
        self.a_cmatch = ""
        self.a_rank = ""
        self.a_clktime = ""
        self.a_variable = ""
        self.a_qpid = ""
        self.a_qcid = ""
        self.a_rgtag = ""
        self.a_bdtar = ""
        self.a_bddesc = ""
        self.a_hmclkid = ""
        self.a_mkt_adid = ""
        self.a_mkt_userid = ""
        self.a_mkt_campaignid = ""
        self.a_mkt_typeid = ""
        self.a_mkt_mediaid = ""
        self.a_mkt_property1 = ""
        self.a_mkt_property2 = ""
        self.a_mkt_property3 = ""
        self.a_mkt_property4 = ""
        self.a_searchid = ""
        self.a_loyalty = ""
        self.a_parent_siteid = ""
            
    #----------------------------------------------------------------------
    def readLine(self, line):
        """
        read and parse the holmes_pre_session line. 
        
        holmes_pre_session日志采用自描述的方法，描述信息+字段内容。
            - 描述信息一般为数组数量+数组长度。
            - 字段内容比较奇怪，rec1[0], rec2[0], rec3[0], ... rec1[1], rec2[1], rec3[1]
        具体字段如下：
            - id column_count(74), 
                - log_type, visitor_id, baiduid, ...
            - pv_count, column_count (8)
                - [view_time, view_url, last_url, close_time, life_cycle, focus_peroid, is_read_head, ..]
            - event_count, column_count(4)
                - [category, action, label, value]
            - unkown1_count, column_count(7):
                - [f1, f2, f3, f4, f5, f6, f7]
            - unkown2_count, column_count(6):
                - [f1, f2, f3, f4, f5, f6]
            - unkown3_count, column_count(3):
                - [f1, f2, f3]
                
        目前parser只处理1. 固定字段(部分), 2. pv信息， 3. act信息，更多的字段（unknown）都没有处理。
        
        @type line: string
        @param line: the inputed line string
        @rtype: bool
        @return: True if line is a valid log. False if not.
        
        """
        fields = line.strip().split('\t')
        if len(fields) < 2*3:
            return False
        idx = 0
        self.a_session_id = fields[0]
        self.a_session_field_count = int(fields[1])
        self._assign_fix(fields)        
        
        idx = 2+self.a_session_field_count
        self.a_pv_count = int(fields[idx])
        self.a_pv_col = int(fields[idx+1])
        self.a_pv_list = []
        self._assign_pv(idx+2, fields)
        
        idx += 2 + self.a_pv_count*self.a_pv_col
        self.a_act_count = int(fields[idx])
        self.a_act_col = int(fields[idx+1])
        self.a_act_list = []
        self._assign_act(idx+2, fields)
        
        # idx += 2 + self.a_act_count*self.a_act_col
        # self._assign_unkown1()
        # self._assign_unkown2()
        # self._assign_unkown3()       
        return True
    
    #----------------------------------------------------------------------
    def _assign_fix(self, fields):
        """"""
        self.a_log_type = fields[2]
        self.a_visitor_id = fields[3]
        self.a_baiduid = fields[4]
        self.a_visit_time = fields[5]
        self.a_last_visit_time = fields[6]
        self.a_is_new_visitor = fields[7]
        self.a_siteid = fields[8]
        self.a_site_status = fields[9]
        self.a_trade_id1 = fields[10]
        self.a_trade_id2 = fields[11]
        self.a_from_type = fields[12]
        self.a_from_url = fields[13]
        self.a_domain_type = fields[14]
        self.a_engine_id = fields[15]
        self.a_subengine_id = fields[16]
        self.a_page_num = fields[17]
        self.a_keyword = fields[18]
        self.a_clickid = fields[19]
        self.a_ip = fields[20]
        self.a_province_id = fields[21]
        self.a_city_id = fields[22]
        self.a_isp_id = fields[23]
        self.a_browser = fields[24]
        self.a_os = fields[25]
        self.a_screen = fields[26]
        self.a_color = fields[27]
        self.a_flash = fields[28]
        self.a_language = fields[29]
        self.a_cookie = fields[30]
        self.a_java = fields[31]
        self.a_custom_from = fields[32]
        self.a_custom_media = fields[33]
        self.a_custom_plan = fields[34]
        self.a_custom_word = fields[35]
        self.a_custom_idea = fields[36]
        self.a_click_type = fields[37]
        self.a_typeid = fields[38]
        self.a_userid = fields[39]
        self.a_planid = fields[40]
        self.a_unitid = fields[41]
        self.a_ideaid = fields[42]
        self.a_wordid = fields[43]
        self.a_cmatch = fields[44]
        self.a_rank = fields[45]
        self.a_clktime = fields[46]
        self.a_variable = fields[47]
        self.a_qpid = fields[48]
        self.a_qcid = fields[49]
        self.a_rgtag = fields[50]
        self.a_bdtar = fields[51]
        self.a_bddesc = fields[52]
        self.a_hmclkid = fields[53]
        self.a_mkt_adid = fields[54]
        self.a_mkt_userid = fields[55]
        self.a_mkt_campaignid = fields[56]
        self.a_mkt_typeid = fields[57]
        self.a_mkt_mediaid = fields[58]
        self.a_mkt_property1 = fields[59]
        self.a_mkt_property2 = fields[60]
        self.a_mkt_property3 = fields[61]
        self.a_mkt_property4 = fields[62]
        self.a_searchid = fields[63]
        self.a_loyalty = fields[64]
        self.a_parent_siteid = fields[65]    
        
    
    


    #----------------------------------------------------------------------
    def _assign_pv(self, idx, fields):
        """"""
        NR = self.a_pv_count
        NF = self.a_pv_col
        
        if self.a_pv_col < 7:# 7/8 is now known
            raise Exception()        
        self.a_pv_list = [{} for i in range(NR)]
        
        for i in range(NR):
            pv = self.a_pv_list[i]
            pv["view_time"] = fields[idx+0*NR+i]
            pv["view_url"] = fields[idx+1*NR+i]
            pv["last_url"] = fields[idx+2*NR+i]
            pv["closed_time"] = fields[idx+3*NR+i]
            pv["life_cycle"] = fields[idx+4*NR+i]
            pv["focus_peroid"] = fields[idx+5*NR+i]
            pv["is_reach_head"] = fields[idx+6*NR+i]
            # pv["unknown"] = fields[idx+7*NF+i]

        return True
    
    def _assign_act(self, idx, fields):
        """"""
        NR = self.a_act_count
        NF = self.a_act_col
        
        if self.a_act_col < 4:# 7/8 is now known
            raise Exception()        
        self.a_act_list = [{} for i in range(NR)]
        
        for i in range(NR):
            act = self.a_act_list[i]
            act["category"] = fields[idx+0*NR+i]
            act["action"] = fields[idx+1*NR+i]
            act["label"] = fields[idx+2*NR+i]
            act["value"] = fields[idx+3*NR+i]

        return True                
        

if __name__=='__main__':
    pass
    
