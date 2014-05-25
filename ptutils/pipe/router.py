#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. pipe框架的核心代码
# History:
#     1. 2013/3/16 created


import sys
import inspect
from optparse import (OptionParser, BadOptionError, AmbiguousOptionError)

#----------------------------------------------------------------------
def _decode(sz):
    """
    脚本编码不同, 尝试各种编码。
    """
    try:
        return sz.decode("gbk")
    except UnicodeDecodeError:
        try:
            return sz.decode("utf8")
        except UnicodeDecodeError:
            try:
                return sz.decode("gb18030")
            except UnicodeDecodeError:
                return sz

########################################################################
class Step(object):
    """
        某一部数据处理的抽样基类，其派生类的实例供router统一调度。
        派生类必须override run 函数，实现功能。
        只提供若干help信息，简化 router的list_app_usage压力.
        基类保持简单，不提供自动注册。由router统一管理。
        类变量可与命令行参数交互。
        
        usage
        =====
            >> @router("step1")
            >> class MyStep(Step):
            >>     var1 = "value1"
            >>     def run(self):
            >>         Your code here
            
        
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
       
    #----------------------------------------------------------------------
    def getargs(self, prefix = ""):
        """
        get the string of class variables like:
            >> "plot --> plot_size_pos_clickratio
            >>     ifn = "xxx"
            >>     ofn = "yyy"
            >>     pic_prefix = click-ratio-hist"
        """
        classvars = [ var for var in dir(self.__class__) if not var.startswith("_") ]
        sz = ""
        for v in classvars:
            attr = getattr(self.__class__, v)
            if not callable(attr):
                sz += prefix + v + " = " + _decode(str(attr)) + "\n"
        return sz
        
   
    #----------------------------------------------------------------------
    def getdoc(self, prefix=""):
        """
        get the detailed class doc
        """
        doc = _decode(self.__class__.__doc__)
        sz = ""
        for l in doc.split("\n"):
            sz += prefix + l + "\n"
        return sz
        
               
    #----------------------------------------------------------------------
    def run(self):
        """"""
        raise TypeError(" run method is not implemented in %s" % self.__class__)
        
    
    

class Router(object):  
    """
    
    核心框架，统一管理pipeline脚本中每一个step对应的函数。根据命令行参数调用。
    
    Usage
    =====
    
      1. import
      
         >> from ubsutils.pipe.router import router
         >> if __name__ == "__main__":
         >>     router.main()
         
      2. decorate the step functions
      
        >> @router("step1")
        >> def process_xxx_data(ifn, ofn):
        >> ...
        >> @router("step2")    
        >> def process_yyy_data(a, b, c="xx", d="mm")
        >> ...
        
      3. usage on command line
        >> python script.py -h
        >> python script.py -f step1 --ifn=a.txt --ofn=b.txt
        >> python script.py -f step2 a -c la -d laaa bvalue
    
       
    """
    def __init__(self):  
        self.app_path = {}  
        
    def route(self, func, args, kwargs):  
        """
         根据func的类型，执行应用逻辑。
        """
        application = self.app_path[func]  
        if inspect.isfunction(application):
            return application(*args, **kwargs)
        elif inspect.isclass(application):
            discard = {}
            for k in kwargs:
                if hasattr(application, k):
                    setattr(application, k, kwargs[k])
                else:
                    discard[k] = kwargs[k]
            if discard:
                print >> sys.stderr, "discard paramenters: %s" % discard
            step = application()
            return step.run()
        else:
            raise TypeError("unknown type: %s" % application)
    
    def __call__(self, path):
        """
        prepare a named wrapper function. Just hook it to app_path. Don't do anything.
        
        usage:
           >> @router("step3")
           >> def process_xxxx_task()
           
        """
        def wrapper(application):
            if path in self.app_path:
                raise ValueError("duplicated function: %s" % path)
            self.app_path[path] = application
            return application
        return wrapper


    #----------------------------------------------------------------------
    def _getfuncstr(self, name, app):
        """
        get the string of a function like:
          >> "plot --> plot_size_pos_clickratio(ifn, ofn, pic_prefix=click-ratio-hist)"
        """
        spec = inspect.getargspec(app)
        varnames = list(spec.args)
        if spec.defaults:   # not None
            defaults = list(spec.defaults)                
            for i in range(-1, -len(defaults)-1, -1):
                #varnames[i] += "=%s" % _decode(defaults[i])
                varnames[i] += _decode("=%s" % defaults[i])
        sz = "%s --> %s(%s)" % (name, app.func_name, ", ".join(varnames))
        return sz
    
    #----------------------------------------------------------------------
    def _getclsstr(self, name, app):
        """
        return plot --> PlotClass
        """
        sz = "%s --> class %s " % (name, app.__name__)
        return sz

                
    
    #----------------------------------------------------------------------
    def epilog(self, func=None):
        """
        如果func == None, 打印pipeline脚本内部被router封装的函数和类.
        如果func != None, 打印对应函数的func_doc或者类变量的__doc__        
        """
        prefix = "  "
        sz = "\n"
        if func:
            if func not in self.app_path:
                raise ValueError("%s is not in script." % func)
            app = self.app_path[func]
            if inspect.isfunction(app):
                sz += prefix + self._getfuncstr(func, app) + "\n"
                prefix2 = "\n"+prefix*2
                sz += prefix*2 + prefix2.join(_decode(app.func_doc).split("\n"))
                return sz
            elif inspect.isclass(app):
                sz += prefix + self._getclsstr(func, app) + "\n"
                ins = app()
                sz += ins.getargs(prefix*2)
                sz += ins.getdoc(prefix*2)
                return sz
            else:
                pass
            
        else:  # list all functions/step
            szfunc = ""
            szclass = ""
            for name in sorted(self.app_path.keys()):
                app = self.app_path[name]
                if inspect.isfunction(app):
                    szfunc += prefix + self._getfuncstr(name, app) + "\n"
                elif inspect.isclass(app):
                    szclass += prefix + self._getclsstr(name, app) + "\n"
                    ins = app()
                    szclass += ins.getargs(prefix*2)
               
            sz = "\n\n"+"List of all possible FUNCTIONs:\n" + \
                szfunc + "\n" + \
                "List of all possible STEPs:\n" + \
                szclass + "\n"
            
            return sz

    #----------------------------------------------------------------------
    def _parse_args(self):
        """
        非常trick的实现。 分两轮parse参数。
        1. 获取--func --verbose（框架使用）, 校验，打印帮助等。
        2. 过滤--func，parse其他参数，传给下游函数。
        
        """
        class MyParser(OptionParser):
            def format_epilog(self, formatter):
                # epilog in default OptionParser will strip newlines in text.                
                return self.epilog
            
            def _process_args(self, largs, rargs, values):
                "An unknown option pass-through implementation"
                while rargs:
                    try:
                        OptionParser._process_args(self,largs,rargs,values)
                    except (BadOptionError,AmbiguousOptionError), e:
                        largs.append(e.opt_str)   
        
        # mainpyfile = sys.argv[0]
        parser = MyParser(usage="usage: %prog -f FUNC [parameters] \n" +
                              "All the parameters are passed to target function", 
                              epilog=self.epilog()
                              )     
        parser.add_option("-f","--func", help="indicator for which func to be run.")
        parser.add_option("-v","--verbose", metavar="FUNC", help="show detailed help (heredoc) for a function.")
        options, args = parser.parse_args()
        
        if options.verbose:
            parser.epilog = self.epilog(func=options.verbose)
            parser.print_help()
            sys.exit(0)

        func = options.func
        if func not in self.app_path:
            parser.print_help()
            sys.exit(0)
            
        parser2 = OptionParser()
        for a in args:
            if a.startswith("--"):
                opt = a.split("=")[0]
                parser2.add_option(opt)
            elif a.startswith("-"):
                if len(a) > 1:
                    parser2.add_option(a[:2])
        options, args = parser2.parse_args(args)

        # positional para, optional para
        return (func, args, dict(options.__dict__))    
    
    #----------------------------------------------------------------------
    def main(self):
        """
        entrance for Router object.
        
        usage:
           >> if __name__ == "__main__":
           >>     router.main()
           
        """
        func, args, kwargs = self._parse_args()
        self.route(func, args, kwargs)
        




# the Router instance for importing
router = Router()
    