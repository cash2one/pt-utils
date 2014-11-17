#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. the tools to parser special fields in log
# History:
#     1. 2012/6/13 created

"""
日志parser使用的小工具，比如解析一些特殊的字段。

"""

import sys
from datetime import datetime, timedelta
from urlparse import urlsplit, parse_qs

# newcookiesort 字段映射
IDX = {
    "cookie":0,
    "ip":1,
    "time":2,
    "fm":3,
    "pn":4,
    "p1":5,
    "p2":6,
    "p3":7,
    "p4":8,
    "tn":9,
    "tab":10,
    "title":11,
    "tp":12,
    "f":13,
    "rsp":14,
    "F":15,
    "query":16,
    "url":17,
    "BDUSS":18,
    "weight":19,
    "ID":20,
    "info":21,
    "prefixSug":22,
    "mu":23,
    "s":24,
    "oq":25,
    "qid":26,
    "cid":27
}


#----------------------------------------------------------------------
def get_rsv_list(tp):
    """
        split the tp field and get the rsv k-v list
       
        >>> get_rsv_list("rsv_sid=12380_12371:rsv_bp=2:rsv_inter=AS223|2|3-AC|233|AB-xxx")
        {'rsv_sid':'12380_12371', 'rsv_bp':'2', 'rsv_inter':'AS223xxx'}
        
    """
    fields = tp.split(":")
    res = {}
    for f in fields:
        if f.startswith("rsv_"):
            try:
                (k, v) = f.split("=")
            except ValueError:
                # print >> sys.stderr, f
                continue
            res[k] = v
            
    return res

def get_rsv_sids(tp):
    """
        split the tp field and get the sid list
       
        >>> get_rsv_sids("rsv_sid=12380_12371:rsv_bp=2:rsv_inter=AS223|2|3-AC|233|AB-xxx")
        ["12380", "12371"]
        
    """
    kv = get_rsv_list(tp)
    if 'rsv_sid' in kv:
        return kv['rsv_sid'].split("_")
    else:
        return []


    
#----------------------------------------------------------------------
def str2time(sz):
    """
    04/Nov/2012:13:09:24 --> time.struct_time(tm_year=2012, tm_mon=11, tm_mday=4, 
                                              tm_hour=13, tm_min=9, tm_sec=24, tm_wday=6, tm_yday=309, tm_isdst=-1)
    """
    return time.strptime(sz, '%d/%b/%Y:%H:%M:%S')

#----------------------------------------------------------------------
def str2time2int(sz):
    """
    04/Nov/2012:13:09:24 --> 1352005764
    """
    return int(time.mktime(time.strptime(sz, '%d/%b/%Y:%H:%M:%S')))

#----------------------------------------------------------------------
def get_alldays_between(start, end, delta=1):
    """
    get the day string list between start and end every delta day
    
       >> get_alldays_between('20130802', '20130808', delta=2)
       ['20130802', '20130804', '20130806', '20130808']
     
    """
    dt_start = datetime.strptime(start, "%Y%m%d")
    dt_end = datetime.strptime(end, "%Y%m%d")
    dt_delta = timedelta(days=delta)
    res = []
    while dt_start <= dt_end:
        res.append(dt_start.strftime("%Y%m%d"))
        dt_start += dt_delta
    return res


#----------------------------------------------------------------------
def get_date(base=None, delta=-1):
    """
    get_date()  --> '2014-11-16' # yestoday
    get_date(delta=-2)  --> '2014-11-15' # the day before yestoday
    get_date(base='2014-11-11', delta=3)  --> '2014-11-14' # 3 day after 2014-11-11
    """
    if base is None:
        base = datetime.today()
    else:
        base = datetime.strptime(base, "%Y-%m-%d")
    target = base + timedelta(days=delta)
    return target.strftime("%Y-%m-%d")

    

def get_qs_from_url(url):
    """
        @type url: string
        @param url: the url string
        @rtype: dict
        @return: the dict of url query parameters
    """
    parts = urlsplit(url)
    return parse_qs(parts.query)

def parse_r_parameter(r):
    """
    一个复杂case
    activity-event:event_id=9467-7-400259.1_m-activity_fwindow_index:cid=1403:access_token=158c97afd98e4a4ab93becd61ba047ba:device_token=c026e06c2a7b4b5181347b4dd3bce355cdc8f7b2a8801c952       55fed28c79497b2:udid=:imei=:r2=activity-event%3Aevent_id:app_access_token=158c97afd98e4a4ab93becd61ba047ba:twitter_id=
     返回列表：
     [ ("activity-event", {"event_id": "xxx"}),
       ("1_m-activity_fwindow_index", {"cid":"1403", access_token:"xxxxx}),
       ...
       () ]
    """
    if isinstance(r, dict):
        if 'r' in r:
            r = r['r']
        else:
            return []
    if not r:
        return []
    list1 = r.split(".")
    list2 = map(lambda x: x.split(":"), list1)
    list3 = []
    for e in list2:
        kvs = {}
        src = e[0]
        for kv in e[1:]:
            fields = kv.split("=", 1)
            if len(fields) == 2:
                kvs[fields[0]] = fields[1]
            else:
                kvs[fields[0]] = ""
        list3.append((src, kvs))

    return list3






    

if __name__=='__main__':
    pass
