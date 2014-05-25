#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1.  在分析得到某个cube数据后，利用不同的存储方式进行输出：text，schema，protobuf etc
     2.  后续可以做的很多: 比如稀疏数组的压缩表示等，从业务角度定义文件格式。
 History:
     1. 2011/5/16 创建文件
"""


import sys
import json
import utils
import time

########################################################################
class OutputStream:
    """
    outputstream的抽象类，
    """

    #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        初始化数据 文件/句柄
        包含：
            1. 数据文件/句柄
                如果fn == "stdout://", 表示向标准输出打印，与schema api保持一致，其他特殊表示（"hdfs://") 尚未支持
            2. 描述文件/句柄
            3. dimension和measure各自的名称，类型，默认值
        """
        self._descriptionFn = "outputstream-struct-tstamp-%s.txt" % (time.time())
        self._descriptionFh = None
        self._isDescribed = False
        
        self._dataFn = fn
        self._dataFh = None
        
        self._dimensionNames = []
        self._dimensionNamesPos = {}
        self._measureNames = []
        self._measureNamesPos = {}
        
        self._dimensionTypes = {}
        self._measureTypes = {}        
        self._dimensionDefaults = {}
        self._measureDefaults = {}
        
        # length of dimension and measure
        self._nd = 0
        self._nm = 0
        
    #----------------------------------------------------------------------
    def setStructFlat(self, orderedDimensionNames=[], orderedMeasureNames=[], dimensionTypes={}, measureTypes={}, dimensionDefaults={}, measureDefaults={}):
        """
        根据输入参数，设置基本输出格式信息。
        输入分为dimension和measure，每部分由name，type和default （value）三部分组成。
        
            1. orderedDimensionNames, orderedMeasureNames:
                  必须参数，list类型，取值为字符串，维度和统计信息的名称。
            2. dimensionTypes, measureTypes:
                  可选参数，dict类型，key是names字符串，value是type类型。dimension的缺省类型是<type 'str'>, measure的缺省类型是<type 'int'>
            3. dimensionDefaults, measureDefaults:         
                  可选参数，dict类型，key是names字符串，value是记录的缺省取值，dimension的缺省取值是-1(int或者float型)或者'-'（其他类型）。measure的缺省取值是0.
        
        标志位和句柄处理在派生类中完成
        """
        if self._isDescribed:
            return False
        
        if not (orderedDimensionNames and orderedMeasureNames) :
            return False
        self._dimensionNames = orderedDimensionNames
        self._nd = len(orderedDimensionNames)
        for i in range(self._nd):
            self._dimensionNamesPos[orderedDimensionNames[i]] = i
        self._measureNames = orderedMeasureNames
        self._nm = len(orderedMeasureNames)
        for i in range(self._nm):
            self._measureNamesPos[orderedMeasureNames[i]] = i
        
        
        for n in orderedDimensionNames:
            # default type is <type  'str'>
            self._dimensionTypes[n] = dimensionTypes.get(n, type(''))   

            # default value : -1 or '-'
            if n in dimensionDefaults:
                self._dimensionDefaults[n] = dimensionDefaults[n]
            else:
                if self._dimensionTypes[n] in (type(1), type(1.0)):
                    self._dimensionDefaults[n] = -1
                else:
                    self._dimensionDefaults[n] = '-'
                    
        for n in orderedMeasureNames:
            # default type is <type  'str'>
            self._measureTypes[n] = measureTypes.get(n, type(1))
            # default default valueis 0
            self._measureDefaults[n] = measureDefaults.get(n, 0)

        return True

        
    #----------------------------------------------------------------------
    def setStructFn(self, fn=None):
        """
        根据fn的文件内容，设置outputschema的输出格式
        抽象类只完成fn中的内容进行拷贝
        主体在派生类中完成
            1. 根据文件内容更新内部设置
            2. 标志位和句柄处理
        """
        if self._isDescribed:
            return False
        
        self._descriptionFh = file(self._descriptionFn, 'w')
        self._descriptionFh.write(file(fn).read())
        self._descriptionFh.close()
        
        return True
    
        
    #----------------------------------------------------------------------
    def getStruct(self):
        """
        返回文件中的struct信息
        """
        if self._descriptionFh:
            self._descriptionFh.close()
            
        self._descriptionFh = file(self._descriptionFn)
        result = self._descriptionFh.read()
        
        self._descriptionFh.close()
        return result
        

    #----------------------------------------------------------------------
    def getNames(self, way="measure"):
        """
        获取dimension/measure的成员变量名称
        way = dimension, measure
        """
        if self._isDescribed:
            if way == 'measure':
                return self._measureNames
            elif way == 'dimension':
                return self._dimensionNames
            else:
                raise Exception
        else:
            return None
        
    
    def getTypes(self, way="measure"):
        """
        获取dimension/measure的成员变量类型
        way = dimension, measure
        """
        if self._isDescribed:
            if way == 'measure':
                return map(lambda x: self._measureTypes[x], self._measureNames)
            elif way == 'dimension':
                return map(lambda x: self._dimensionTypes[x], self._dimensionNames)
            else:
                raise Exception
        else:
            return None
        
        
    
    #----------------------------------------------------------------------
    def append(self):
        """
        添加输出（一行cube记录）
        在派生类中实现
        """
        raise Exception
        
    #----------------------------------------------------------------------
    def close(self):
        """"""
        raise Exception
        
########################################################################
class TextOutputStream(OutputStream):
    """
    存储方式为文本的outputstream
    
        >>>out = TextOutputStream(fn=self._fn)
        >>>out.setStructFlat(['d1', 'd2', 'd3', 'd4'], ['v1', 'v2', 'v3'], 
        ...dimensionTypes={'d1':type(1), 'd2':type(''), 'd4':type('')}, 
        ...dimensionDefaults={'d2':'x'}, measureDefaults={'v2':-10})
        >>>out.append([1, 'a', 'b', 'cc'], [0, 1, 7])
        >>>out.append(ddict={'d3':'input'}, mdict={'v1':20})
        >>>out.close()
        
    """
    
    #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        句柄默认初始化为标准输出
        """
        OutputStream.__init__(self, fn=fn)
        if self._dataFn == "stdout://":
            self._dataFh = sys.stdout
        else:
            self._dataFh = file(self._dataFn, 'w')
        
        self._dsep = ':'
        self._msep = "\t"
        self._sep = "\t"

        
    #----------------------------------------------------------------------
    def setStructFlat(self, orderedDimensionNames=[], orderedMeasureNames=[], dimensionTypes={}, measureTypes={}, dimensionDefaults={}, measureDefaults={}, dsep=":", msep="\t", sep="\t"):
        """
        设置文本输出的格式
        参数详细解释见OutputStream.setStructFlat
        额外参数：
            dsep ： dimension的分隔符
            msep：measure的分隔符        
        """
        
        if  not OutputStream.setStructFlat(self, orderedDimensionNames, orderedMeasureNames, dimensionTypes, measureTypes, dimensionDefaults, measureDefaults):
            return False
            
        self._dsep = dsep
        self._msep = msep
        self._sep  = sep
            
        self._isDescribed = True
        
        return True 
            

        
      
    #----------------------------------------------------------------------
    def append(self, dlist=[], mlist=[], ddict={}, mdict={}):
        """
        将一条cube数据插入outputstream
        (dlist, mlist)一组，(ddict, mdict) 一组，后者优先级高于后者。
        list数据必须完整，dict可以缺失，更新list中对应的key。
        
        """
        
        ddata = []
        mdata = []
        if dlist:
            if len(dlist) != self._nd :
                raise ValueError("len does not match: nd=%d, len(dlist)=%d, nm=%d, len(mlist)=%d" % (self._nd, len(dlist)))
            ddata = dlist
        if mlist:
            if len(mlist) != self._nm :
                raise ValueError("len does not match: nm=%d, len(mlist)=%d" % (self._nm, len(mlist)))
            mdata = mlist
            
        if not ddata:
            ddata = map(lambda x: self._dimensionDefaults[x], self._dimensionNames)
        if not mdata:
            mdata = map(lambda x: self._measureDefaults[x], self._measureNames)
           
        if ddict:
            for k in ddict:
                ddata[self._dimensionNamesPos[k]] = ddict[k]
        if mdict:
            for k in mdict:
                mdata[self._measureNamesPos[k]] = mdict[k]
                            
        k = self._dsep.join(map(str, ddata))
        v = self._msep.join(map(str, mdata))
        
        # self._dataFh.write("%s\t%s\n" % (k, v))
        print >> self._dataFh, k + self._sep + v
        
        return True
        
    #----------------------------------------------------------------------
    def close(self):
        """
        close data file
        """
        self._dataFh.close()
        
        

########################################################################
class TextOutputCombineStream(TextOutputStream):
    """
    TextOutStream的派生类，对文本输出进行聚合再输出。
    适用于dimension组成很少的情况。
    
    注意，append操作不直接输出数据，在程序退出前，需要调用emit方法。
    
       >>> 
    
    """

     #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        句柄默认初始化为标准输出，用特殊字符串标识。
        
        """
        TextOutputStream.__init__(self, fn=fn)
        
        self._combinedata = {}
        
    def append(self, dlist=[], mlist=[], ddict={}, mdict={}):
        """
        将一条cube数据插入outputstream。此处具体实现是将数据进行打印而不是输出。
            
    
        @type dlist: list
        @param dlist: 以数组形式输入的dimension数据
        @type mlist: list
        @param mlist: 以数组形式输入的measure数据
        @type ddict: dict
        @param ddict: 以词典形式输入的dimension数据（缺失key取默认值）
        @type mdict: dict
        @param mdict: 以词典形式输入的measure数据（缺失key取默认值）
        @rtype: bool
        @return:  成功返回True，否则抛异常
        
        注意：
        (dlist, mlist)一组，(ddict, mdict) 一组，后者优先级高于前者。
        list数据必须完整，dict可以缺失，其余由default值补全。        
        """
        
        ddata = []
        mdata = []
        if dlist:
            if len(dlist) != self._nd :
                raise ValueError("len does not match: nd=%d, len(dlist)=%d, nm=%d, len(mlist)=%d" % (self._nd, len(dlist)))
            ddata = dlist
        if mlist:
            if len(mlist) != self._nm :
                raise ValueError("len does not match: nm=%d, len(mlist)=%d" % (self._nm, len(mlist)))
            mdata = mlist
            
        if not ddata:
            ddata = map(lambda x: self._dimensionDefaults[x], self._dimensionNames)
        if not mdata:
            mdata = map(lambda x: self._measureDefaults[x], self._measureNames)
           
        if ddict:
            for k in ddict:
                ddata[self._dimensionNamesPos[k]] = ddict[k]
        if mdict:
            for k in mdict:
                mdata[self._measureNamesPos[k]] = mdict[k]
                            
        k = self._dsep.join(map(str, ddata))
        if k in self._combinedata:
            v0 = self._combinedata[k]
            self._combinedata[k] = map(lambda x: v0[x]+mdata[x], range(self._nm))
        else:
            self._combinedata[k] = mdata
            
        return True
 

    def emit(self, is_sort=False):
        """
        将内部聚合的dimension-measure数据进行输出。
        
        @type is_sort: bool
        @param is_sort: 输出时是否按照dimension进行排序
        @rtype: bool
        @return: 生成返回True
        """
        if is_sort:
            for k in sorted(self._combinedata.keys()):
                v = self._msep.join(map(str, self._combinedata[k]))
                # self._dataFh.write("%s\t%s\n" % (k, v))
                print >> self._dataFh, k + self._sep + v
        else:
            for k in self._combinedata:
                v = self._msep.join(map(str, self._combinedata[k]))
                # self._dataFh.write("%s\t%s\n" % (k, v))
                print >> self._dataFh, k + self._sep + v
           
        return True
    
    #----------------------------------------------------------------------
    def get_merged_data(self):
        """
        get the merged data for emiting.
        
        """
        return self._combinedata
        
    
    
########################################################################
class SchemaOutputStream(OutputStream):
    """
    存储方式为schema数据文件的outputstream
    """

    #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        数据文件名称初始化为"stdout://", 表示schema writer向标准输出写。
        """
        import disql.schema as schema
        OutputStream.__init__(self, fn=fn)
        self._recordName = 'CubeRecord'
        self._writerSchema = None
        self._writer = None
        self._creator = None
        self._rec = None
        self._disqlschemaPackage = schema
        
    #----------------------------------------------------------------------
    def setStructFlat(self, orderedDimensionNames=[], orderedMeasureNames=[], dimensionTypes={}, measureTypes={}, dimensionDefaults={}, measureDefaults={}, recName=None):
        """
        1. 简单方式设置schema输出的格式
        2. 生成idl文件，初始化schema writer和creator
        """        
        if  not OutputStream.setStructFlat(self, orderedDimensionNames, orderedMeasureNames, dimensionTypes, measureTypes, dimensionDefaults, measureDefaults):
            return False
            
        if recName:
            self._recordName = recName
            
        self._updateIdlFile()
        self._InitScheaWriter()        
        self._isDescribed = True
        
        return True 
    
    #----------------------------------------------------------------------
    def setStructfn(self, fn):
        """
        根据fn(idl)文件内容，设置schema的输出格式
        注意fn中的手工idl文件必须符合cube的dimension/measure规范。见_updateIdlFile
        """
        if not OutputStream.setStructFn(fn):
            return False
        self._InitScheaWriter()        
        self._setFlatBySchema()
        self._isDescribed = True
        
        return True
    
    #----------------------------------------------------------------------
    def _InitScheaWriter(self):
        """
        初始化schema writer
        """
        self._writerSchema = self._disqlschemaPackage.load_idl_file(self._descriptionFn)
        self._writer = self._disqlschemaPackage.RecordWriter(self._dataFn, self._writerSchema, {})
        self._creator = self._disqlschemaPackage.StructCreator(self._writerSchema)
        self._rec = self._creator.create(self._recordName)
        return True
        
    
    #----------------------------------------------------------------------
    def _updateIdlFile(self):
        """
        根据flat信息，更新/生成idl文件
        """
        if not self._descriptionFh:
            self._descriptionFh =  file(self._descriptionFn, 'w')
            
        dsz = "struct dimStruct { \n"
        for d in self._dimensionNames:
            dsz += "    %s %s;\n" % (utils.py2idltype(self._dimensionTypes[d]), d)
        dsz += "};\n\n"
        
        msz = "struct meaStruct { \n"
        for d in self._measureNames:
            msz += "    %s %s;\n" % (utils.py2idltype(self._measureTypes[d]), d)
        msz += "};\n\n"
        
        sz = "struct %s {\n" % self._recordName + \
           "    dimStruct dimension; \n" + \
           "    meaStruct measure; \n" + \
           "} ; \n\n"
        
        self._descriptionFh.write(dsz+msz+sz)
        self._descriptionFh.close()
        return True
        
           

    #----------------------------------------------------------------------
    def _setFlatBySchema(self):
        """
        根据schema文件的信息，初始化OutputStream的结构信息
        """
        #self._dimensionNames = []
        #self._measureNames = []
        #self._dimensionTypes = {}
        #self._measureTypes = {}        
        #self._dimensionDefaults = {}
        #self._measureDefaults = {}
        # TODO
        raise Exception
        
        


        #----------------------------------------------------------------------
    def append(self, dlist=[], mlist=[], ddict={}, mdict={}):
        """
        将一条cube数据插入outputstream
        (dlist, mlist)一组，(ddict, mdict) 一组，后者优先级高于前者
        list数据必须完整，dict可以缺失，其余由default值补全。        
        """        
        self._rec["dimension"] = {}
        self._rec["measure"] = {}
        if dlist:
            if len(dlist) != self._nd :
                raise ValueError("len does not match: nd=%d, len(dlist)=%d, nm=%d, len(mlist)=%d" % (self._nd, len(dlist)))
            for i in range(self._nd):
                self._rec["dimension"][self._dimensionNames[i]] = dlist[i]
        if mlist:
            if len(mlist) != self._nm :
                raise ValueError("len does not match: nm=%d, len(mlist)=%d" % (self._nm, len(mlist)))
            for i in range(self._nm):
                self._rec["measure"][self._measureNames[i]] = mlist[i]
                
        if not self._rec["dimension"]:
            self._rec["dimension"].update(self._dimensionDefaults)
        if not self._rec["measure"]:
            self._rec["measure"].update(self._measureDefaults)
           
        if ddict:
            self._rec["dimension"].update(ddict)
        if mdict:
            self._rec["measure"].update(mdict)

        self.appendRec(self._rec)
        
        return True

    #----------------------------------------------------------------------
    def appendRec(self, rec):
        """
        用户保证 rec是一个合法的schema rec对象
        """
        if rec:
            self._writer.append(rec)
        else:
            raise Exception
        return True
    
    #----------------------------------------------------------------------
    def close(self):
        """"""
        self._writer.close()

    
