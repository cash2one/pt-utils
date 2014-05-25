HISTORY
"""""""
	5. 2013-04-17, add debug command in ubsutils.pipe.mr and MACROS in ubsutils.hce.decorator
    4. 2013-04-02, add ubsutils.pipe.router.Step
    3. 2013-03-17, add ubsutils.pipe.mr and ubsutils.pipe.router. add pyhce.py into ubstuls
    2. 2012-06-13, add get_rsv_list in parser.utils.
    1. 2012-03-02, beta release0.0.1: 完成将文本转化为内存词典 or shelve对象的工具函数。




DESCRIPTION
"""""""""""
    此python模块是提供ubs编程的一些小工具函数。设计目的是：
    1. 纯python
      1. 不包含 c/c++ 扩展，保证linux环境的服务器和windows的个人PC都可以使用。
    2. 仅包含应用广泛的小函数
      1. 与某些特定项目相关的工具函数不要放入此模块。


INSTALL
"""""""""
    模块采用了setuptools管理安装。依赖setuptools, nose。
    1. python setup.py build
    2. python setup.py test            # 测试
    3. python setup.py -q bdist_egg    # 生成egg文件，供调用
    4. python setup.py install         # 安装入site-packages



MORE INFO
"""""""""
   1. egg文件的使用
      1. egg是python的打包文件，类似java的jar包。适合上传供streaming程序调用。
      2. 使用方式 export PYTHONPATH=/path/2/egg/ubscake-0.0.2-py2.6.egg 或者将egg文件路径加入python的sys.path变量。
   2. api文档
      1. 项目采用epydoc自动生成api文档。
      2. 在线版本见 http://bb-ps-ubs02.bb01.baidu.com:8080/ubs-api-doc/ubsutils/


TODO
""""
  1. 因为pyhce.cc和pyhce.py的设计不合理+不一致， pyhce的脚本必须from hecutils import emit和在文件最末尾使用 MAPPER_INPUT_FLAGSPLIT等，这不合理。推动pyhce.cc调整执行顺序，然后在mapper_setup中使用 MAPPER_INPUT_FLAGSPLIT, 恢复pyhce.py.
  2. 添加一些工具函数，
      1. 20131221，天和datetime对象相互转换。
	  2. median函数
  3. filesplitter中的一系列函数，split_file_by_key，在处理极端数据（比如巨大的作弊cookie）内存会爆掉，提供参数控制每次返回的数量。-1不控制。 可以按照100， 1000等控制。