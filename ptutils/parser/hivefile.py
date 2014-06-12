#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 对hive文本文件的parser
 History:
     1. 2014/6/12 11:14 : hivefile.py is created.
"""



import sys
from collections import namedtuple


class LineRecord():
    """
    解析hive数据表（文本格式）的一条记录

    usage
    =====

        - parse a single line:

        >>> rec = LineReocrd(schema="cookie,ip,time,url,referer",sep="\x01")
        >>> for line in sys.stdin:
        >>>     rec.parse_line(line) # v = rec.parse_line(line)
        >>>     v = rec.contents
        >>>     print v.cookie
        >>>     print v.ip
        >>>     print v.time
        >>>     print v.url
        >>>     print v.referer

        - With file handle:

        >>> rec = LineReocrd(schema="cookie,ip,time,url,referer", sep="\x01")
        >>> while True:
        >>>     flag = rec.read_next(sys.stdin)
        >>>     if not flag:
        >>>         break
        >>>     v = rec.contents
        >>>     print v.cookie
        >>>     print v.time
        >>>     print v.referer

    history
    =======
      1. 2014-06-12 created by taopeng@meilishuo.com

    """

    def __init__(self, schema):
        """
        hive文本的一二三级分隔符默认是\x01, \x02, \x03
        """
        self.schema_string = schema
        self.tuple_cls = namedtuple(typename="hive_text_file_log_parser", field_names=self.schema_string)
        self.contents = ()  # named tuple


    def parse_line(self, line, sep="\x01", stripchar="\n"):
        """
        """
        fields = line.strip(stripchar).split(sep)
        self.contents = self.tuple_cls(*fields)

        return self.contents


    def read_next(self, fh, sep="\x01", stripchar="\n"):
        """
        """
        line = fh.readline()
        if not line:
            return False
        else:
            self.parse_line(line, sep=sep, stripchar=stripchar)
        return True


class MobileAppLogRecord(LineRecord):
    """对应hive表mobie_app_log_new的parser"""

    def __init__(self):
        """Constructor for MobileAppLogRecord"""
        LineRecord.__init__(self, "date_time,hour,minute,second,user_id,class_name,method_name,access_token,uri,client_ip,device_token,client_id,client_app,client_device,client_version,device_id,http_code,udid,imei,specific_data,query_data,post_data,memory_used,time_spent,sql_num,sql_avg_time,log_source,macid,unique_device_id")

class VisitlogsRecord(LineRecord):
    """对应hive表的visitlogs的parser"""

    def __init__(self):
        """Constructor for VisitlogsRecord"""
        LineRecord.__init__(self, "hour,minute,second,class_name,method_name,mem_use,run_time,user_id,agent,refer,type,is_search_engine,uri,visitip,poststr,sessid,channel_from,tag,http_code,refer_class_name,refer_method_name,data_size,client_id,nginx_time")


if __name__=='__main__':
    args = parse_args()
    main(args)

