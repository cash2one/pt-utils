#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. mls的mysql数据库表dump为文本文件，对这些文本文件进行解析的parser
     2. 借用了pandas的dataframe结构
 History:
     1. 2014/12/31 11:54 : mysqlmls.py is created
"""


import pandas as pd
import sys


def t_dolphin_poster_info(file_name, sep="\t"):
    " dolphin 库中的 t_dolphin_poster_info 表 "

    names = ['poster_id', 'poster_name', 'poster_ename', 'status', 'op_date', 'op_uid', 'platform', 'catalog', 'isAutoSort']
    return pd.read_csv(file_name, sep=sep, names=names)


def t_dolphin_stat_mob_poster_for_operation(file_name, sep="\t"):
    " dolphin_stat 库中的 t_dolphin_stat_mob_poster_for_operation 表 "

    names = ['id', 'dt', 'navi_poster_id', 'navi_poster_type', 'navi_poster_name', 'navi_from', 'pid_data', 'poster_pid', 'gmv', 'doota_num', 'doota_cpc_high_quality_num', 'doota_cpc_normal_quality_num', 'doota_no_cpc_high_quality_num', 'doota_no_cpc_normal_quality_num', 'doota_unpass_num', 'doota_unexam_num', 'doota_update_num', 'doota_cpc_high_quality_update_num', 'doota_cpc_normal_quality_update_num', 'doota_no_cpc_high_quality_update_num', 'doota_no_cpc_normal_quality_update_num', 'poster_uv', 'poster_pv', 'fill_rate', 'doota_show_time', 'first_page_show_rate', 'second_page_show_rate', 'third_page_show_rate', 'single_twitter_pv', 'single_twitter_uv', 'doota_single_twitter_pv', 'doota_single_twitter_uv', 'click_btn_pv', 'click_btn_uv', 'doota_click_btn_pv', 'doota_click_btn_uv', 'order_goods_num', 'order_buyer_num', 'paid_order_goods_num', 'paid_order_buyer_num', 'paid_order_distinct_goods_num', 'hotsell_gmv_rate', 'hotsell_sku']
    return pd.read_csv(file_name, sep=sep, names=names)



if __name__=='__main__':
    pass

