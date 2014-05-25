#! /usr/bin/env python
#coding:gbk

"""
 Purpose: 
     1. udw相关的parser
 History:
     1. 2014/02/02 创建 <pengtao@baidu.com>
"""



import sys

from collections import namedtuple
from functools import partial

from ubsutils.filesplitter import split_file_by_key


class UdwLineRecord():
    """
    解析udw表格的一条记录
    
    usage
    =====
    
        - parse a single line:
        
        >>> rec = UdwLineReocrd(schema="cookie,ip,time,url,referer")
        >>> for line in sys.stdin:
        >>>     rec.parse_line(line) # v = rec.parse_line(line)
        >>>     v = rec.contents
        >>>     print v.cookie
        >>>     print v.ip
        >>>     print v.time
        >>>     print v.url
        >>>     print v.referer

        - With file handle:
          
        >>> rec = UdwLineReocrd(schema="cookie,ip,time,url,referer", prefix="")
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
      2. 2014-03-02 created by pengtao@baidu.com.
      1. 2014-02-02 created by pengtao@baidu.com.
    
    """
    def __init__(self, schema, prefix="udw_key,udw_tablename"):
        """
        com.baidu.udw.mapred.MultiTableInputFormat 给mapper的输入为
        udwkey + "\t" + tablename + sep + field1 + sep + field2 + ...
        所以默认 用 prefix + schema 做为namedtuple的字段名
        
        """
        if prefix:
            self.schema_string = prefix + "," + schema
        else:
            self.schema_string =  schema
        self.tuple_cls = namedtuple(typename="udw_log_parser", field_names=self.schema_string)
        self.contents = ()            # named tuple

        
    
    def parse_line(self, line, sep="\t", stripchar="\n"):
        """
        """
        fields = line.strip(stripchar).split(sep)
        self.contents = self.tuple_cls(*fields)
        
        return self.contents
    
    
    def read_next(self, fh, sep="\t", stripchar="\n"):
        """
        """
        line = fh.readline()
        if not line:
            return False
        else:
            self.parse_line(line, sep=sep, stripchar=stripchar)
        return True



def _map_fields_to_idx(split_fields, fields):
    """
    
    [a, b, d], [a, b, c, d, e, f]  --> [0,1,3]
    
    """
    
    if len(fields) < len(split_fields):
        raise "feilds for split is not enought : %s v.s. %s" % (split_fields, struct_fields)
    
    split_idx = map(lambda x: fields.index(x), split_fields)
    
    return split_idx


class _NestedStruct:
    """
    Objects storing the nested action struct. 
    
    The info, mission, goal, search are all _NestedStruct objects
    
       >>> for mission in info.missions:
       >>>     for goal in mission.goals:
       >>>         for search in goal.searches:
       >>>             for action in search.actions:
       >>>                 action.ps_action_name
       
    """
    def __init__(self, subname):
        self._subname = subname
        # 利用subname命名子结构list
        setattr(self, subname, [])
        # 包含所有基本action的list
        self.allactions = []
    
    def assemble(self, structs):
        setattr(self, self._subname, structs)

        basic = structs[0]
        if not hasattr(basic, "allactions"):
            self.allactions = structs
        else:
            for s in structs:
                self.allactions += s.allactions

    

def _recursive_assemble_struct(reslist, subnames, split_idx):
    """
    recursively asemble basic stuctures into high-level structure
    """
    if len(split_idx) == 0:
        newres = _NestedStruct(subnames[0])
        newres.assemble(reslist)
        return newres
                        
    current_subname = subnames[-1]
    current_split_idx = split_idx[-1]
                    
    newreslist = []
    resbuffer = []
    old_split_tag = None
                    
    for res in reslist:
        if hasattr(res, "allactions"):
            action = res.allactions[0]
        else:
            action = res
        split_tag = action[current_split_idx]
        if split_tag != old_split_tag:
            if old_split_tag is not None:
                newres = _NestedStruct(current_subname)
                newres.assemble(resbuffer)
                newreslist.append(newres)
            resbuffer = [res]
            old_split_tag = split_tag
        else:
            resbuffer.append(res)
                    
    newres = _NestedStruct(current_subname)
    newres.assemble(resbuffer)
    newreslist.append(newres)
    
    return _recursive_assemble_struct(newreslist, subnames[:-1], split_idx[:-1])
                

def split_stream(schema, struct, split, stream=sys.stdin, sep="\t", stripchar="\n", schema_prefix="udw_key,udw_tablename"):
    """
    将udw的input stream分割成嵌套的结构
    
    >>> for info in split_stream("cookie,missionid,fm,url,refer", struct="missions.actions", split="cookie,missionid")
    >>>     for mission in info.missions:
    >>>         for action in mission.actions:
    >>>             print action.fm
    >>>             print action.url
    >>>             print action.refer
    
    @type schema: string 
    @param schema: field name list for basic action
    @type struct: string 
    @param struct: names of nested structures
    @type split: string
    @param split: list of fields name for structure segmentation
    @type stream: filehandle
    @param stream: input stream. sys.stdin is the default value
    @type sep: string
    @param sep: seperator of fields
    @type stripchar: string 
    @param stripchar: char for strip(char) methods
    @type schema_preifx: string 
    @param schema_preifx: "udw_key,udw_tablename" as default
    
    """
    if schema_prefix:
        schema = schema_prefix + "," + schema
    fields = map(lambda x: x.strip(), schema.split(","))
    split_fields = map(lambda x: x.strip(), split.split(","))
    struct_fields = map(lambda x: x.strip(), struct.split("."))
    if len(split_fields) != len(struct_fields) :
        raise "length of split and struct do not match : %s v.s. %s" % (split_fields, struct_fields)
        
    split_idx = _map_fields_to_idx(split_fields, fields)
    
    action_cls = namedtuple('action', schema)
    
    for lines in split_file_by_key(stream, split_idx[0], stripchar="\n"):
        actions = map(lambda x: action_cls(*x), lines)
        info = _recursive_assemble_struct(actions, struct_fields, split_idx[1:])
        yield info




def split_ps_session_stream(stream=sys.stdin):
    """
    输入filehandle，将内容分割为missions -> goals -> searches -> actions 四级嵌套结构。

    usage:
    =====
    
        >>> for cookie_info in split_ps_session_stream():
        >>>     for mission in cookie_info.missions:
        >>>         for goal in mission.goals:
        >>>             for search in search.actions:
        >>>                 for action in goal.actions:
        >>>                     print action.ps_actionname
        >>>                     print action.ps_rsv_inter
        >>>                     print action.event_click_pos
        >>>                     print action.event_query
    
    """
    # return partial(split_stream, 
    # return yield xxx is OK
    return split_stream(stream=stream,
                   schema="event_time,event_ip,event_country,event_city,event_province,event_baiduid,event_userid,event_cookie,event_username,ps_ad_count_id,ps_actionname,event_click_pos,ps_subclick_pos,ps_relation_searchpos,event_template_name,event_page_number,ps_source_tab,ps_query_id,ps_search_type,event_click_target_url,event_click_target_title,ps_click_sub_url,event_query,ps_previousquery,ps_sugprefixword,event_useragent,ps_rsv_inter,ps_xpath,ps_sampling_id,ps_rsvmap,ps_is_satisfy,ps_satisfy_weight,ps_click_weight,ps_extendinfo,ps_session_id,ps_session_inner_id,ps_goal_id,ps_atom_sessionid,ps_cookiesort_map,ps_labels,event_log_source,event_urlparams,ps_filter_detail,event_action,event_day,event_product,ps_is_normal",
                   struct="missions.goals.searches.actions", 
                   split="event_baiduid,ps_session_id,ps_goal_id,ps_atom_sessionid"
                   )



class UdwPsclickSession:
    """
    udw中newcookiesort对应日志的结构化解析parser。可以移植海源的newcookiesort_parser, 
    """
    
    def __init__(self, schema):
        """"""
        pass
    


if __name__=='__main__':
    pass
    
