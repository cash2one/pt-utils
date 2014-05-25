#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1.  �ڷ����õ�ĳ��cube���ݺ����ò�ͬ�Ĵ洢��ʽ���������text��schema��protobuf etc
     2.  �����������ĺܶ�: ����ϡ�������ѹ����ʾ�ȣ���ҵ��Ƕȶ����ļ���ʽ��
 History:
     1. 2011/5/16 �����ļ�
"""


import sys
import json
import utils
import time

########################################################################
class OutputStream:
    """
    outputstream�ĳ����࣬
    """

    #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        ��ʼ������ �ļ�/���
        ������
            1. �����ļ�/���
                ���fn == "stdout://", ��ʾ���׼�����ӡ����schema api����һ�£����������ʾ��"hdfs://") ��δ֧��
            2. �����ļ�/���
            3. dimension��measure���Ե����ƣ����ͣ�Ĭ��ֵ
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
        ����������������û��������ʽ��Ϣ��
        �����Ϊdimension��measure��ÿ������name��type��default ��value����������ɡ�
        
            1. orderedDimensionNames, orderedMeasureNames:
                  ���������list���ͣ�ȡֵΪ�ַ�����ά�Ⱥ�ͳ����Ϣ�����ơ�
            2. dimensionTypes, measureTypes:
                  ��ѡ������dict���ͣ�key��names�ַ�����value��type���͡�dimension��ȱʡ������<type 'str'>, measure��ȱʡ������<type 'int'>
            3. dimensionDefaults, measureDefaults:         
                  ��ѡ������dict���ͣ�key��names�ַ�����value�Ǽ�¼��ȱʡȡֵ��dimension��ȱʡȡֵ��-1(int����float��)����'-'���������ͣ���measure��ȱʡȡֵ��0.
        
        ��־λ�;�������������������
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
        ����fn���ļ����ݣ�����outputschema�������ʽ
        ������ֻ���fn�е����ݽ��п���
        �����������������
            1. �����ļ����ݸ����ڲ�����
            2. ��־λ�;������
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
        �����ļ��е�struct��Ϣ
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
        ��ȡdimension/measure�ĳ�Ա��������
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
        ��ȡdimension/measure�ĳ�Ա��������
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
        ��������һ��cube��¼��
        ����������ʵ��
        """
        raise Exception
        
    #----------------------------------------------------------------------
    def close(self):
        """"""
        raise Exception
        
########################################################################
class TextOutputStream(OutputStream):
    """
    �洢��ʽΪ�ı���outputstream
    
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
        ���Ĭ�ϳ�ʼ��Ϊ��׼���
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
        �����ı�����ĸ�ʽ
        ������ϸ���ͼ�OutputStream.setStructFlat
        ���������
            dsep �� dimension�ķָ���
            msep��measure�ķָ���        
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
        ��һ��cube���ݲ���outputstream
        (dlist, mlist)һ�飬(ddict, mdict) һ�飬�������ȼ����ں��ߡ�
        list���ݱ���������dict����ȱʧ������list�ж�Ӧ��key��
        
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
    TextOutStream�������࣬���ı�������оۺ��������
    ������dimension��ɺ��ٵ������
    
    ע�⣬append������ֱ��������ݣ��ڳ����˳�ǰ����Ҫ����emit������
    
       >>> 
    
    """

     #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        ���Ĭ�ϳ�ʼ��Ϊ��׼������������ַ�����ʶ��
        
        """
        TextOutputStream.__init__(self, fn=fn)
        
        self._combinedata = {}
        
    def append(self, dlist=[], mlist=[], ddict={}, mdict={}):
        """
        ��һ��cube���ݲ���outputstream���˴�����ʵ���ǽ����ݽ��д�ӡ�����������
            
    
        @type dlist: list
        @param dlist: ��������ʽ�����dimension����
        @type mlist: list
        @param mlist: ��������ʽ�����measure����
        @type ddict: dict
        @param ddict: �Դʵ���ʽ�����dimension���ݣ�ȱʧkeyȡĬ��ֵ��
        @type mdict: dict
        @param mdict: �Դʵ���ʽ�����measure���ݣ�ȱʧkeyȡĬ��ֵ��
        @rtype: bool
        @return:  �ɹ�����True���������쳣
        
        ע�⣺
        (dlist, mlist)һ�飬(ddict, mdict) һ�飬�������ȼ�����ǰ�ߡ�
        list���ݱ���������dict����ȱʧ��������defaultֵ��ȫ��        
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
        ���ڲ��ۺϵ�dimension-measure���ݽ��������
        
        @type is_sort: bool
        @param is_sort: ���ʱ�Ƿ���dimension��������
        @rtype: bool
        @return: ���ɷ���True
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
    �洢��ʽΪschema�����ļ���outputstream
    """

    #----------------------------------------------------------------------
    def __init__(self, fn="stdout://"):
        """
        �����ļ����Ƴ�ʼ��Ϊ"stdout://", ��ʾschema writer���׼���д��
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
        1. �򵥷�ʽ����schema����ĸ�ʽ
        2. ����idl�ļ�����ʼ��schema writer��creator
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
        ����fn(idl)�ļ����ݣ�����schema�������ʽ
        ע��fn�е��ֹ�idl�ļ��������cube��dimension/measure�淶����_updateIdlFile
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
        ��ʼ��schema writer
        """
        self._writerSchema = self._disqlschemaPackage.load_idl_file(self._descriptionFn)
        self._writer = self._disqlschemaPackage.RecordWriter(self._dataFn, self._writerSchema, {})
        self._creator = self._disqlschemaPackage.StructCreator(self._writerSchema)
        self._rec = self._creator.create(self._recordName)
        return True
        
    
    #----------------------------------------------------------------------
    def _updateIdlFile(self):
        """
        ����flat��Ϣ������/����idl�ļ�
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
        ����schema�ļ�����Ϣ����ʼ��OutputStream�Ľṹ��Ϣ
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
        ��һ��cube���ݲ���outputstream
        (dlist, mlist)һ�飬(ddict, mdict) һ�飬�������ȼ�����ǰ��
        list���ݱ���������dict����ȱʧ��������defaultֵ��ȫ��        
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
        �û���֤ rec��һ���Ϸ���schema rec����
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

    
