#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 关于各种平台上日志路径的配置
 History:
     1. 2013/6/3 
"""


hadoop_bin = {
    'stoff':'/home/work/hadoop-client-stoff/hadoop/bin/hadoop',
    'rank':'/home/work/hadoop-client-rank/hadoop/bin/hadoop'
}

para_template = {
    'date':'*', # 20130911
    'hour':'*',	# 09
    'minute':'*',						# 15
    'machine':'*',						# hz01-se-click0-0.hz01.baidu.com
    'part':"*",							# 00007
    # 'idc':'*'
}

_logpath_stoff = {
    # /log/20682/accesslog_to_stoff/20120807/0000/szwg-rank-hdfs.dmop/17/tc-se-click0-3.17.00.log.lzma
	# /log/20682/accesslog_to_stoff/20131104/0000/szwg-rank-hdfs.dmop/0900/0910/0915/hz01-se-click0-0.hz01.baidu.com_20131104090000.log
    "accesslog"           :  "/log/20682/accesslog_to_stoff/%(date)s/0000/szwg-rank-hdfs.dmop/%(hour)s00/*/%(hour)s%(minute)s/%(machine)s_%(date)s%(hour)s%(minute)s*",
    # /log/20682/bws_access/20120814/0000/szwg-ecomon-hdfs.dmop/22/bws_access_log.jx-www-pr31.jx.baidu.com.2012081422.lzma
    # /log/20682/bws_access/20130602/0000/szwg-ecomon-hdfs.dmop/0435/10.212.50.18_20130602043500.log
    "bwslog"              :  "/log/20682/bws_access/%(date)s/0000/szwg-ecomon-hdfs.dmop/%(minute)s/%(machine)s_%(date)s%(minute)s*",
    ## /log/20682/mergelog_v1/20120701/0000/szwg-rank-hdfs.dmop/part-02999.gz
    #mergelogv1_hadoop_root  =   /log/20682/mergelog_v1/
    # /log/20682/mergelog_v2_daily_to_stoff/20120701/0000/szwg-ecomon-hdfs.dmop/part-00000-A
    "mergelog"            : "/log/20682/mergelog_v2_daily_to_stoff/%(date)s/0000/szwg-ecomon-hdfs.dmop/part-%(part)s-A",
    # /log/20682/newcookiesort/20120801/0000/szwg-ecomon-hdfs.dmop/part-00284.lzma
    "newcookiesort"       : "/log/20682/newcookiesort/%(date)s/0000/szwg-ecomon-hdfs.dmop/part-%(part)s*",
    # /log/20682/ps_bz_log_dump/20130501/0100/szwg-ecomon-hdfs.dmop/0100/cq01-ps-wwwui10-t8.cq01.baidu.com_20130501010000.log
    "uilog"               : "/log/20682/ps_bz_log_dump/%(date)s/%(hour)s00/szwg-ecomon-hdfs.dmop/%(hour)s%(minute)s/%(machine)s_%(date)s%(hour)s%(minute)s*",
    # /log/20682/querylog/20130602/0000/szwg-rank-hdfs.dmop/querylog_20130602
    "querylog"            : "/log/20682/querylog/%(date)s/0000/szwg-rank-hdfs.dmop/querylog_%(date)s",
    # "/ps/ubs/chenxiaoqian/new_atomicsession_cube/step3.1_mergeinfo/20130705/part-00299.gz"
    "querystat"           : "/ps/ubs/chenxiaoqian/new_atomicsession_cube/step3.1_mergeinfo/%(date)s/part-%(part)s.gz",
    # "/log/20682/ps_ubs_nsclick_to_stoff/20130714/0000/szwg-ston-hdfs.dmop/tc-bae-static00.tc/psups_access_log.20130714"
    "nsclickubs"          : "/log/20682/ps_ubs_nsclick_to_stoff/%(date)s/0000/szwg-ston-hdfs.dmop/%(machine)s/psups_access_log.%(date)s",
    # /log/20682/nsclick_day_to_stoff/20130713/0000/2300/yf-tc-bae-static25.yf01/access_log.2013071323
    "nsclickall"          : "/log/20682/nsclick_day_to_stoff/%(date)s/0000/%(hour)s%(minute)s/%(machine)s/access_log.%(date)s%(hour)s*",
    # /log/20682/click_adjust_new_to_stoff/@date@/0000/merge_round2/part-*
    "mr2"                 : "/log/20682/click_adjust_new_to_stoff/%(date)s/0000/merge_round2/part-%(part)s",
    # /log/2295/holmes_pre_session_off_for_ps_ubs/20121013/0000/szwg-ecomon-hdfs.dmop/2300/session.2012101323-tc-hm-pre00.tc
    "holmes"              : "/log/2295/holmes_pre_session_off_for_ps_ubs/%(date)s/0000/nj01-yulong-hdfs.dmop/%(hour)s00/session.%(date)s%(hour)s-%(machine)s"
    
    ## /log/20682/click_adjust_new_to_stoff/20121013/0000/merge_round2/part-00799
    #clickadjust_round2_hadoop_root = /log/20682/click_adjust_new_to_stoff/


    ##  /log/20682/sobar_urldata_ston_to_stoff/20121017/0000/szwg-ston-hdfs.dmop/0415/yf-p2p-web07.yf01.baidu.com_20121017041500.log
    #sobar_urldata_hadoop_root = /log/20682/sobar_urldata_ston_to_stoff/
    ## ston: /app/ns/lsp/output/ubs/logdata_sobar_session_foreproc/20121023/0000/part-00399
    #sobar_logdata_session_hadoop_root = /app/ns/lsp/output/ubs/logdata_sobar_session_foreproc/    
    #hdfs_detailnewquerysort =   /log/20682/detailnewquerysort/@date@/0000/szwg-*-hdfs.dmop/part-*
    
}



# logpath['stoff'] = _logpath_stoff
# logpath['rank'] = _logpath_rank
logpath = {}

_va = dir()
for k in hadoop_bin:
    name = "_logpath_" + k
    if name in _va:
        logpath[k] = locals()[name]
