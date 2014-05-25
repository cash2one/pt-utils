# 2014-01-27, 星期一 16:42

   1. pdbstreaming.py不支持readlines，非常annoying

# 2014-01-27, 星期一 15:59

   1. 设计一套将table数据导入cube，一键分析的工具，参考方伟的实现。
   2. 参考argparse的ParseResult和urlparse的SplitResult，提供行对象的高效封装。
   3. 提供cube dense相关工具，
   4. 重写general_sum_reducer.py, 作为脚本工具



# 2013-08-06, 星期二 11:30

   1. hce模式处理命令行参数的函数
   2. debug模式获取单part作为输入，input path包含文件太多时也不好使，考虑改进
   3. 在命令行添加dfs参数
   

# HISTORY


   1. 2013-09-30, 星期一 12:48
       1. input路径中包含namenode信息，在debug抽取单part时，namenode信息会被丢掉，造成问题
           1. input = hdfs://szwg-rank-hdfs.dmop.baidu.com:54310/app/ps/rank/ubs/monitor-target-out/@date@/all/newquerycube     2. input = /app/ps/rank/ubs/monitor-target-out/@date@/all/newquerycube/part-00000
		   2. 解决： 利用urlsplit 解析url，将scheme和netloc添加在path前
       2. pdbstreaming.py input output script.py -c aa -d bbb
           1. pdbstreaming.py的参数解析过强，-c或者-d会报错
		   2. 解决： 第4个以后的参数截断，不由parse_args处理
       3. 如果job文件有严重错误
           1. debug step1 20130829 不报错： (mr):debug grep-weigou-in-holmes 20130829   ==== run test mrjob in 2013-08-30 15:12:  /home/work/pengtao/bin/ubs_mrframework/bin/Start.py ../conf/weigou.mod.conf ./tmp.default.holmes.job.conf.schedule.20130830-1511-28 -t 20130829 -p grep-weigou-in-holmes -s -test 
    	   2. run step1 -test 报错 [FATAL]@initEnv
		   3. 解决：debug内部调用 -test，没有打印-test返回的stdout和stderr。将这些信息打印到sys.stderr上。

