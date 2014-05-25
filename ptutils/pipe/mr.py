#!/usr/bin/env python
#coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose:
#     1. ubs_mrFramework配置和启动的轻量级封装
# History:
#     1. 2013/3/16


import sys
import os
import re
import cmd
import readline
import datetime
import subprocess
import random
import time
from argparse import ArgumentParser
from ConfigParser import ConfigParser
from urlparse import urlsplit

from ubsutils.filetools import swap, readline_opened_file

class MRcmd(cmd.Cmd):
    """Simple command processor for mr jobs."""
    prompt = "(mr):"
    intro = "interactively run ubs_mrframework job."

    #----------------------------------------------------------------------
    def __init__(self, mrObj):
        """"""
        cmd.Cmd.__init__(self)
        self.mrObj = mrObj
        self.jobQueue = []
        # autocomplete will ignore "-"
        # http://mail.python.org/pipermail/python-list/2011-March/599475.html
        delims = readline.get_completer_delims().replace("-", "")
        readline.set_completer_delims(delims)

        # all status variable must begin with "v_"
        self.v_verbose = True


    #----------------------------------------------------------------------
    def do_ls(self, pattern):
        """
        list all jobs
        """
        if pattern:
            print "The available jobs with substring %s are:" % pattern
        else:
            print "The available jobs are:"

        n = len(self.mrObj._step_schedule)
        for i in range(n):
            name = self.mrObj._step_schedule[i]
            if name.find(pattern) != -1:
                print "  [%d]\t%s" % (i, name)
        base = 100
        m = len(self.mrObj._step_joblist)
        for i in range(n,m):
            name = self.mrObj._step_joblist[i]
            if name.find(pattern) != -1:
                print "  [%d]\t%s" % (base+i, name)

    #----------------------------------------------------------------------
    def help_ls(self):
        """"""
        print "\n".join(["ls [pattern]",
                         "ls all jobs with pattern substring."])

    #----------------------------------------------------------------------
    def _do_run(self, args):
        """
         ['demo-step']
         ['demo-step', '20130425']
         ['demo-step', '20130425', '-test']
         
        """
        n = len(args)
        if n == 1:
            # print "run job <%s> with time <%s>" % (job, time)
            self.mrObj.runjob(p=args[0], s=True, verbose=self.v_verbose)
        elif n == 2:
            if args[1] == '-test':
                self.mrObj.runjob(p=args[0], s=True, test=True, verbose=self.v_verbose)
            else:
                self.mrObj.runjob(p=args[0], t=args[1], s=True, verbose=self.v_verbose)
        elif n == 3 and args[2] == '-test':
            self.mrObj.runjob(p=args[0], t=args[1], s=True, test=True, verbose=self.v_verbose)
        else:
            print "wrong cmd for run"
            self.help_run()
            


    def do_run(self, line):
        """
        run job with time

        The underlying api is :
            def runjob(self, module=None, schedule=None, p=None, t=None, s=False):

        """
        args = filter(None, line.strip().split())
        self._do_run(args)


    def help_run(self):
        print "\n".join(["run jobname [time] [-test]",
                         "run the job with time parameter."])


    def complete_run(self, text, line, begidx, endidx):
        if not text:
            completions = self.mrObj._step_joblist
        else:
            completions = [ f
                            for f in self.mrObj._step_joblist
                            if f.startswith(text)
                            ]
        return completions
    
    #----------------------------------------------------------------------
    def do_queue(self, line):
        """"""
        line = line.strip()
        if not line:    
            if self.jobQueue:
                print "current jobs in queue are:"
                i = -1
                for job in self.jobQueue:
                    i += 1
                    print "    %d. %s" % (i, job)
            else:
                print "NO job in queue."
        else:
            parts = filter(None, line.split())
            if parts[0] == "clear":
                if len(parts) != 2:
                    self.help_queue()
                    return
                target = parts[1]
                if target == 'all':
                    print "clear all jobs..."
                    self.jobQueue = []
                else:
                    try:
                        target = int(target)
                        if target >= len(self.jobQueue):
                            print "no %th job in queue" % target
                            return
                        print "clear %dth job: %s" % (target, self.jobQueue[target])
                        del self.jobQueue[target]
                    except ValueError:
                        print "invalid number %s" % target
                        self.help_queue()
                        return
            elif parts[0] == "start":
                n = len(self.jobQueue)
                i = 0
                while self.jobQueue:
                    args = self.jobQueue.pop(0)
                    i += 1
                    print "==== run %d/%d jobs in queue ====\n" % (i, n)
                    self._do_run(args)
                    
            elif parts[0] == "add":
                self.jobQueue.append(parts[1:])                                
            else:
                print "unknown command %s" % parts
                self.help_queue()
                return
            
    #----------------------------------------------------------------------
    def help_queue(self):
        """"""
        print "\n".join([
            "queue usage                          : manipulate the job queue.",
            "    queue                            : show current jobs.",
            "    queue start                      : start the job queue.",
            "    queue clear [N|all]              : clear the Nth/all job. 0-based.",
            "    queue add [job [time] [-test]]   : add job into queue."
        ])
    #----------------------------------------------------------------------
    def complete_queue(self, text, line, begidx, endidx):
        """
        """
        completions = []
        parts = filter(None, line.strip().split())
        n = len(parts)
        if n == 1:
            completions = ["add", "clear", "start"]
        elif n == 2:
            if text:
                completions = [f for f in ["add", "clear", "start"] if f.startswith(text)]
            else:
                # begin with the 3rd fields
                completions = self.mrObj._step_joblist
        elif n == 3:
            completions = [ f
                            for f in self.mrObj._step_joblist
                            if f.startswith(text)
                            ]
        else:
            pass
        return completions        
        

    #----------------------------------------------------------------------
    def _debug_argparser(self, _list):
        """"""
        parser = ArgumentParser(description='mini-parser for debug commmands.')
        parser.add_argument("jobname", help="the name of job to be debugged.")
        parser.add_argument("time", nargs='?', default=None, help="the time str.")
        parser.add_argument("-n", "--part", type=int, default=1, help="number of parts. default 1")
        parser.add_argument("-m", "--map", type=int, default=0, help="number of mappers. default the value of parts.")
        parser.add_argument("-r", "--reduce", type=int, default=1, help="number of reducers. default 1.")


        # parser.parse_args # it will call sys.exit, which is not desired
        args, argv = parser.parse_known_args(_list)
        if args.map == 0:
            args.map = args.part
            
        return (args, argv)

    def do_debug(self, line):
        """
        MRcmd中的特有接口，在单步执行hadoop job时，选择hdfs上的一个part，快速执行，进行debug。
        debug hello-world time [[-n numofparts] [-m numofmapers] [-r numofreducers]]
        说明见 help_debug.
        """
        _list = line.strip().split()

        n = len(_list)
        if n == 0 :
            self.help_debug()
        else:
            (arg, other) = self._debug_argparser(_list)
            if other:
                self.help_debug()
                return
                
            config = {
                'parts':  arg.part,
                'map':   arg.map,
                'reduce':arg.reduce }
            self.mrObj.debugjob(p=arg.jobname, t=arg.time, revisions=config, verbose=self.v_verbose)

    #----------------------------------------------------------------------
    def help_debug(self):
        """"""
        print "\n".join(["debug jobname [time [-n numofparts] [-m numofmappers] [-r numofreducers]]",
                         "run the debug job.",
                         "    -n number of hdfs parts, default 1.",
                         "    -m number of mappers, default == numofparts.",
                         "    -r number of reducers, default 1."])

    complete_debug = complete_run



    def do_EOF(self, line):
        return True

    #----------------------------------------------------------------------
    def help_EOF(self):
        """"""
        print "exit the shell"

    #----------------------------------------------------------------------
    def do_quit(self, line):
        """"""
        return True
    #----------------------------------------------------------------------
    def help_quit(self):
        """"""
        print "exit the shell"

    do_q = do_quit
    #----------------------------------------------------------------------
    def help_q(self):
        """"""
        print "exit the shell"

    #----------------------------------------------------------------------
    def help_help(self):
        """"""
        print "print this help"

    #----------------------------------------------------------------------
    def emptyline(self):
        """
        The default emptyline function is to repeat last command, which will cause trouble.
        So overide it here.
        """
        self.do_ls("")

    def do_set(self, line):
        """set the status variable
        """
        vs = line.strip().split()

        if len(vs) == 2 and vs[0] == 'verbose' and vs[1] in ("True", "T", "False", 'F'):
            if vs[1].startswith("T"):
                self.v_verbose = True
                print "    now verbose = True"
            elif vs[1].startswith("F"):
                self.v_verbose = False
                print "    now verbose = False"
            else:
                pass
        else:
            self.help_set()
            
    #----------------------------------------------------------------------
    def help_set(self):
        """
        """
        print "\n".join([
            "set verbose T[rue]/F[alse].",
            "    set the status variable.",
        ])
        
    #----------------------------------------------------------------------
    def complete_set(self, text, line, begidx, endidx):
        """
        """
        workline = line[:begidx]
        parts = filter(None, workline.strip().split())
        
        n = len(parts)
        completions = []

        if n == 1:  # set xxx
            if text:  # part[1]
                completions = [f for f in ["verbose"] if f.startswith(text)]
            else:   # part[2]
                completions = ["verbose"]
        elif n == 2:  # set verbose xxx
            if parts[1] == "verbose":
                if text: 
                    completions = [f for f in ["False", "True"] if f.startswith(text)]
                else:
                    completions = ["False", "True"]
            else:  
                completions = []
        else:
            completions = []
            
        return completions   

    #----------------------------------------------------------------------
    def do_show(self, line):
        """"""
        args = filter(None, line.strip().split())
        for arg in args:
            name = "v_" + arg
            if hasattr(self, name):
                print "    %s = %s" % (arg, getattr(self, name))
            else:
                print "    %s = None" % (arg)
        if not args:
            var_list = filter(lambda x: x.startswith("v_"), dir(self))
            var_list = map(lambda x: x[2:], var_list)
            print "    Availabe variables include:"
            for var in var_list:
                print    "        %s" % var     
    

    #----------------------------------------------------------------------
    def help_show(self):
        """"""
        print "\n".join([
            "show [VAR1] [VAR2] ...",
            "   print the varaible values."
        ])
        
            
    #----------------------------------------------------------------------
    def complete_show(self, text, line, begidx, endidx):
        """"""
        var_list = filter(lambda x: x.startswith("v_"), dir(self))
        var_list = map(lambda x: x[2:], var_list)
        completion = []
        if text:
            completion = [f for f in var_list if f.startswith(text)]
        else:
            completion = var_list
            
        return completion
        
        

class ScheduleObj:
    """
    schedule.conf的mapping对象
    """
    re_job = re.compile(r"\[\s*Association:\s*([\w\-\.]+)\s*\]")
    re_sch = re.compile(r"\[\s*([\w\-\.]+)\s*\](.+)")

    #----------------------------------------------------------------------
    def __init__(self):
        """"""
        self.m_jobfile = None
        self.m_steps = []

    #----------------------------------------------------------------------
    def _parse_asso(self, line):
        """
        处理
        [Association:zhixin.joblist.conf]
        """

        m = ScheduleConf.re_job.search(lines)
        if m:
            self.m_jobfile = m.group(1)
            jobfile = prefix + "/" + m.group(1)
            if not os.path.isfile(jobfile):
                raise Exception("joblist config %s is not ready." % jobfile)
            return True

        return False

    #----------------------------------------------------------------------
    def _parse_sch(self, line):
        """"""
        m =  ScheduleConf.re_sch.search(line)
        if m:
            key = m.group(1)
            value = m.group(2).strip()
            if key == "Schedule:Serial":
                self.m_steps.append((key, value))
            elif key == "Schedule:Group":
                self.m_steps.append((key, value.split()))
            elif key == "Schedule:Parallel":
                self.m_steps.append((key, value.split()))
            else:
                raise Exception("unknown schedule format %s" % line)
            return True
        return False

    #----------------------------------------------------------------------
    def parse(self, fn):
        """
        Get jobslist file and each step from schedule.conf
        The config is like:
            [Association:zhixin.joblist.conf]
            [Schedule:Serial] newcookiesort-to-qid-data
            [Schedule:Serial] bwsfitlered-to-qid-data

        It do not support full grammer of schedule.conf

        @rtype: tuple
        @return: (joblist_file, [schedule_steps])
        """
        prefix = os.path.dirname(fn)
        lines = map(lambda x: x.strip(), file(fn).readlines()) # strip lines
        n = len(lines)
        i = 0
        while i < n:
            if lines[i]: # skip empty line
                if self._parse_asso(lines[i]):
                    break
                else:
                    raise Exception("unkown schedule line %s" % lines[i])
            i += 1
        if self.m_jobfile is None:
            raise Exception("No joblist config in file %s" % fn)
        while i < n:
            if lines[i]:
                if not self._parse_sch(lines[i]):
                    raise Exception("unknown schedule line %s" % lines[i])
            i += 1
        return (self.m_jobfile, self.m_steps)


    #----------------------------------------------------------------------
    def output(self, ofn):
        """"""
        ofh = open(ofn)
        print >> ofh, "[Association:%s] %s" % self.m_jobfile
        for s in self.m_steps:
            k, v = s
            if type(v) == type("1"):
                print >> ofh, "[%s] %s" % (k, v)
            elif type(v) == type([]):
                print >> ofh, "[%s] %s" % (k, " ".join(v))
            else:
                raise Exception("[%s] %s" % (k, v))
        ofh.close()

    #----------------------------------------------------------------------
    def set_jobfile(self, name):
        """
        set the job file name in schedule
        """
        self.m_jobfile = name


class JoblistObj:
    """
    joblist.conf的mapping对象. 后续可以直接用confparser对象。
    """
    re_name = re.compile(r"\[\s*([\w\-\.]+)\s*\]")
    n_prefix_len =  16   # default space length for job key

    #----------------------------------------------------------------------
    def __init__(self):
        """"""
        self.m_common = {}  # common job
        self.m_names = []  # job name in order
        self.m_jobs = {}  # other jobs

    #----------------------------------------------------------------------
    def parse_old20130505(self, fn):
        """
        保存历史代码。
        
        The conf file is like:
            [bwslog-to-qid-data]
            streaming       =  hce
        It do not support full grammer of mrframe joblist.conf. 

        @rtype: tuple
        @return: [job1, job2, ..., jobn]
        """
        # filter comments and blanklines
        lines = map(lambda x: x.split("#",1)[0].strip(), open(fn).readlines())
        section = None
        key = None
        for line in lines:
            if line:
                m = JoblistObj.re_name.match(line)
                if m:
                    name = m.group(1)
                    if name == "common":
                        section = self.m_common
                    else:
                        if name in self.m_jobs:
                            raise Exception("duplicated job %s" % name)
                        self.m_names.append(name)
                        self.m_jobs[name] = {}
                        section = self.m_jobs[name]
                else:
                    fs = line.split("=", 1)
                    if len(fs) == 2:
                        k, v = fs
                        k = k.strip()
                        v = v.strip()
                        if k in section:
                            section[k].append(v)
                        else:
                            section[k] = [v]
                        key = k
                    else:
                        # file = A
                        #        B
                        section[key].append(fs[0])
        return self.m_names
    
    def _kvpair2dict(self, pairs):
        """
             [("streaming", "hce"), ("input", "/path1\n/path2"), ("output", "/path2")]
        
        -->   {"streaming":"hce", "input":["/path1", "/path2"], "output":"/path2"}
        """
        res = {}
        for p in pairs:
            k, v = p
            if k not in res:
                res[k] = v.split("\n")
            else:
                res[k] += v.split("\n")
                
        return res
        
    #----------------------------------------------------------------------
    def parse(self, fn):
        """
        The conf file is like:
            [bwslog-to-qid-data]
            streaming       =  hce
        It do not support full grammer of mrframe joblist.conf. 
        This function directly invokes the configparser lib, so it does not support inline comments starting with "#". Be careful!

        @rtype: tuple
        @return: [job1, job2, ..., jobn]
        """
        cp = ConfigParser()
        cp.read(fn)
        sections = cp.sections()
        for name in sections:
            sec = self._kvpair2dict(cp.items(name))
            if name == "common":
                self.m_common = sec
            else:
                if name in self.m_jobs:
                    raise Exception("duplicated job %s" % name)
                self.m_names.append(name)
                self.m_jobs[name] = sec
                
        return self.m_names


    def _output_section(self, fh, section):
        """
        输出一个section内的k，v
        """
        for k in section:
            n = JoblistObj.n_prefix_len - len(k)

            value = section[k]
            if type(value) != type([]):
                value = [value]

            if n > 0:
                prefix = "%s%s=   " % (k, " "*n)
            else:
                prefix = "%s=   " % k
            prefix2 = " "*len(prefix)
            print >> fh, prefix + "%s" % value[0]
            for v in value[1:]:
                print >> fh,  prefix2 + "%s" % v
        return True


    #----------------------------------------------------------------------
    def output(self, ofn):
        """"""
        ofh = open(ofn, "w")
        print >> ofh, "[common]"
        self._output_section(ofh, self.m_common)
        for name in self.m_names:
            print >> ofh, ""
            print >> ofh, "[%s]" % name
            self._output_section(ofh, self.m_jobs[name])
        ofh.close()

    #----------------------------------------------------------------------
    def set_common(self, key, value):
        """
        set the k v in common section
        """
        self.m_common[key] = value

    #----------------------------------------------------------------------
    def set_job(self, name, k, v):
        """
        set the k, v in name section
        """
        section = self.m_jobs[name]
        section[k] = v

########################################################################
class MRConf:
    """

    解析mrFrame配置文件的一些工具函数。
    初期设计只是利用re更改一些文本，后续需求越来越复杂。实现了配置conf object.

    parse_xxx
    generate_simple_xxx 等功能都可以用新的ScheduleConf, JoblistConf实现

    """

    tmp_prefix = "/tmp"

    #----------------------------------------------------------------------
    def __init__(self, hadoop = None):
        """Constructor"""
        self._hadoop_bin = hadoop



    def parse_schedule(self, fn):
        """
        Get jobslist file and each step from schedule.conf
        The config is like:
            [Association:zhixin.joblist.conf]
            [Schedule:Serial] newcookiesort-to-qid-data
            [Schedule:Serial] bwsfitlered-to-qid-data

        It do not support full grammer of schedule.conf

        @rtype: tuple
        @return: (joblist_file, [schedule_steps])

        """
        re_joblist = re.compile(r"\[Association:([\w\-\.]+)\]")
        re_serial = re.compile(r"\[Schedule:Serial\]\W+([\w\-\.]+)")

        prefix = os.path.dirname(fn)
        jobfile = ""
        serial_list = []
        lines = file(fn).readlines()
        n = len(lines)
        i = 0
        while i < n:
            m = re_joblist.search(lines[i])
            i += 1
            if m:
                jobfile = prefix + "/" + m.group(1)
                break
        while i < n:
            m =  re_serial.search(lines[i])
            if m:
                serial_list.append(m.group(1))
            i += 1

        if jobfile == "":
            raise Exception("No joblist config in file %s" % fn)
        if not os.path.isfile(jobfile):
            raise Exception("joblist config %s is not ready." % jobfile)

        return (jobfile, serial_list)

    def parse_joblist(self, fn):
        """
        Get job names in joblist config file
        The conf file is like:
            [bwslog-to-qid-data]
            streaming       =  hce
        It do not support full grammer of joblist.conf

        @rtype: tuple
        @return: [job1, job2, ..., jobn]
        """
        re_job = re.compile(r"\[([\w-]+)\]")

        jobs = []
        for line in open(fn):
            # line.strip() starts with []
            m = re_job.match(line.strip())
            if m:
                name = m.group(1)
                if name != "common":
                    jobs.append(name)

        return jobs

    def generate_simple_schedule_conf(self, jobfile, schedulefile=None):
        """
        generate a serial schedule from joblist.conf

        @rtype: string
        @return: schedule file name (func will generate one when input is None)

        """
        conf = MRConf()

        if schedulefile:
            outfile = schedulefile
            outname = os.path.basename(schedulefile)
        else:
            # outfile is like zhixin.schedule.20130317-0811-57.conf
            prefix = os.path.dirname(jobfile)
            outname = "tmp.default." + os.path.basename(jobfile) + ".schedule." + datetime.datetime.now().strftime("%Y%m%d-%H%M-%S")
            if prefix:
                outfile = prefix + "/" + outname
            else:
                outfile = outname

        jobs = conf.parse_joblist(jobfile)

        fho = file(outfile,"w")
        print >> fho, "[Association:%s]" % os.path.basename(jobfile)
        for job in jobs:
            print >> fho, "[Schedule:Serial] %s" % job
        fho.close()

        return outfile

    def generate_debug_jobfile(self, jobfile, newjobfile, testout, p, revisions):
        """
        根据revision中的信息，修正jobfile文件中的p job。
        输出到newjobfile, 供下一步debug。

        @type jobfile: string
        @param jobfile: input joblist conf file
        @type newjobfile: string
        @param newjobfile: output debug joblist conf
        @type testout: string
        @param testout: stdout string from mrframe test run. for input/output/hadoopbin config
        @type p: string
        @param p: target job name
        @type revisions: dict
        @param revisions: new configurations for target job. default {}
                          example: revisions = {'parts': numberofparts ; 'map': numberofmappers, "reduce": numberofreducers}

        """
        keys = {"input":[], "hadoop_bin":[], "output":[], "mapper":[], "reducer":[]}
        self.parse_mrtest(testout, keys, p=p)
        print >> sys.stderr, "sample first %d parts from hdfs" % revisions["parts"]
        print >> sys.stderr, "    input = %s " % keys["input"]
        print >> sys.stderr, "    output = %s " % keys["output"]
        print >> sys.stderr, "    mapper = %s " % keys["mapper"]
        print >> sys.stderr, "    reducer = %s " % keys["reducer"]
        print >> sys.stderr, "    hadoop = %s " % keys["hadoop_bin"]
        parts = self.get_hdfs_parts(keys["input"], revisions["parts"], hadoop=keys["hadoop_bin"][0])
        if not parts:
            return False

        job = JoblistObj()
        job.parse(jobfile)

        job.set_common("remove_output", "1")
        job.set_job(name=p, k="input", v=parts)
        job.set_job(name=p, k="output", v=MRConf.tmp_prefix+keys["output"][0])
        job.set_job(name=p, k="mapred.reduce.tasks", v=revisions["reduce"])
        job.set_job(name=p, k="mapred.map.tasks", v=revisions["map"])
        job.set_job(name=p, k="mapred.job.priority", v="VERY_HIGH")

        job.output(newjobfile)

        return True

    #----------------------------------------------------------------------
    def parse_mrtest(self, out, keys, p=None):
        """
        parse the real config from mrframe test run output.

        dict <uilog-extract-target-query>:
            file =  /home/work/pengtao/projects/20130205-zhixin-mining/bin/hce_uilog_extract_demo_log.py
        /home/work/pengtao/projects/20130130-zhixin-evaluation/bin/hce_mmma.c
            input = /log/20682/ps_bz_log_dump/20130224/????/szwg-ecomon-hdfs.dmop/*/*.log
        /ps/ubs/pengtao/20130130-zhixin-evaluation/abcdefkkf
            mapper = "pyhce hce_uilog_extract_demo_log.py"
            output = /ps/ubs/pengtao/20130205-zhixin-mining/uilog-example-data/20130413/
            hadoop_bin = /home/work/hadoop-client-stoff/hadoop/bin/hadoop

        @type out: string
        @param out: output string of mrframe test run
        @type keys: dict
        @param keys: the target conf, like "input", "hadoop_bin"
        @rtype: string
        @return: path to the log file : /home/work/pengtao/projects/20130317-ubsutils-reload/log//20130317-ubsutils-reload/20130501//hadoopmsg.20130317-ubsutils-reload.tmp.default.test.job.conf.schedule.decorator-test1-4.20130501000000
                 None if there is no log file (e.g. in test mode)
        """
        lines = out.split("\n")
        n = len(lines)
        begidx = 0
        
#         if p:  # get the job p section
#             sz = "dict <%s>" % p
#             i = -1
#             while i < n-1:
#                 i += 1
#                 l = lines[i]
#                 if l.find(sz) != -1:
#                     begidx = i
#                     break
                
            
        for k in keys:
            re_key = re.compile(r"\t%s = (.+)" % k)
            i = begidx
            while i < n-1:
                i += 1
                l = lines[i]
                m = re_key.match(l) # line start
                if m:
                    keys[k].append(m.group(1))
                    while i < n:
                        i += 1                        
                        l = lines[i].strip()
                        if l and l.find("=") == -1:
                            keys[k].append(l)
                        else:
                            break  # keys[key][-1]
                    break  # keys[k]

        i =  n
        re_file = re.compile(r"([/\.\w\-]+/hadoopmsg.\d+.[/\.\w-]+)")
        while i > begidx:  # last several line
            i -= 1
            m = re_file.search(lines[i])
            if m:
                return m.group(1)
            
        # raise ValueError("can not find log file in cmd string:\n    %s" % out)
        return None   # no log file

    

    #----------------------------------------------------------------------
    def get_hdfs_parts(self, paths, numofparts, hadoop='/home/work/hadoop-client-stoff/hadoop/bin/hadoop'):
        """
        get the first n parts from paths. Equally divide n parts into number of paths.
        
        input is like
             ["hdfs://szwg-rank-hdfs.dmop.baidu.com:54310/app/ps/rank/ubs/monitor-target-out/@date@/all/newquerycube"]
             ["/app/ps/rank/ubs/monitor-target-out/@date@/all/newquerycube"]

        The returned paths is like
            -rw-r--r--   3 ns-lsp ns-lsp 1057134186 2013-03-02 03:41 /log/20682/newcookiesort/20130301/0000/szwg-ecomon-hdfs.dmop/part-00099
        """
        n = len(paths)
        res = []
        for i in range(n):
            size = int((i+1)*numofparts/n) - int(i*numofparts/n)
            cmds = [hadoop, "dfs", "-ls", paths[i]]
            try:
                process = subprocess.Popen(cmds, stdout=subprocess.PIPE)
            except OSError as e:
                print >> sys.stderr, "can not get hdfs parts : %s" % cmds
                print >> sys.stderr, e
                return []
            split_res = urlsplit(paths[i])
            prefix = ""
            if split_res.scheme:
                # hdfs + :// +  szwg-rank-hdfs.dmop.baidu.com:54310
                prefix = split_res.scheme + "://" + split_res.netloc
            local_res = []
            while len(local_res) < size:
                # Found xxx items # this is possible
                # --rw-r--r-- xxxx  --> /log/20682/xxx              
                line = process.stdout.readline()
                if not line:
                    print >> sys.stderr, " only %d parts in hdfs path %s" % (len(local_res), paths[i])
                    break
                if line.startswith("Found"):
                    continue
                local_res.append(prefix+line.split()[-1])
            process.terminate()
            res += local_res
            
        return res


########################################################################
class MRframework:
    """
    调用ubs_mrFramework.

    usage:
    =====

      1. basic configure
        >> mr = MRframework()
        >> mr.set_path("~/ubs_mrframework/start.py")
      2. usage 1
        >> mr.main()  # parse the command line variables and send it directly to mrFramework
      3. usage 2
        >> mr.readconf("module.conf", "joblist.conf")
        >> mr.shell()  # interactively run a single job

    整合在pipe.router脚本中的典型用法
        >> @router("mr")
        >> def process_mr_tasks(job=None, time=None)
        >>     mr = MRframework("mrStart", "20130314")
        >>     # mr.set_path("mrStart")
        >>     # mr.set_date("20130315")
        >>     mr.readconf(module="zhixin.module.conf", joblist="zhixin.joblist.conf")
        >>     if job:
        >>         mr.runjob(p=job, t=time, s=True)
        >>     else:
        >>         mr.shell()

    """

    #----------------------------------------------------------------------
    def __init__(self, path=None, time=None):
        """Constructor"""
        self._clean()
        if path:
            self.set_path(path)
        if time:
            self.set_time(time)

    #----------------------------------------------------------------------
    def _clean(self):
        """"""
        self._conf_module = None
        self._conf_joblist = None
        self._conf_schedule = None
        self._start = ""
        self._step_schedule = []  # schedule.conf中定义的jobs
        self._step_joblist = []   # joblist.conf中全部的jobs
        self._time_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
        self._hadoop_bin = ""

    def set_path(self, path):
        if os.path.isfile(path):
            self._start = path
        else:
            raise Exception("framework binary %s is not ready!" % path)
        return path

    #----------------------------------------------------------------------
    def set_time(self, time_str):
        """"""
        if not re.match(r"^\d+$", time_str):
            raise Exception("invalid time string %s" % time_str)
        self._time_str = time_str


    #----------------------------------------------------------------------
    def get_conf_schedule(self):
        """
        需要一个getter，因为schedule.conf可能是从joblist.conf自动生成的
        """
        return self._conf_schedule



    #----------------------------------------------------------------------
    def readconf(self, module, joblist=None, schedule=None):
        """
        输入并parse mrFramework的三个配置文件: module.conf, schedule.conf和joblist.conf.
        在mrFramwork的命令中没有显式指定joblist.conf，由schedule.conf关联。
        所以，
          1. 如果schedule被指定，函数会根据schedule找到joblist， 后者被忽略。
          2. 如果schedule不指定，函数会自动生成一个与joblist匹配的schedule。

          内部填充的job列表
          self._step_schedule = []
          self._step_joblist = []
          必须保证 _step_schedule是_step_joblist的子集，joblist的前n顺序与schedule严格一致。

        """
        conf = MRConf()

        if not os.path.isfile(module):
            raise Exception("conf %s is not ready" % module)
        self._conf_module = module

        if schedule:
            if not os.path.isfile(schedule):
                raise Exception("conf %s is not ready" % schedule)
            self._conf_schedule = schedule
            jobfile, self._step_schedule = conf.parse_schedule(self._conf_schedule)
            if joblist is not None:
                if joblist != jobfile:
                    raise Exception("joblist <%s> in schedule file <%s> is different from <%s>" % (jobfile, schedule, joblist))
            self._conf_joblist = jobfile

        else:
            if joblist is None :
                raise Exception("both joblist.conf and schedule.conf are None")
            if not os.path.isfile(joblist):
                raise Exception("conf %s is not ready" % joblist)
            self._conf_joblist = joblist
            self._conf_schedule = conf.generate_simple_schedule_conf(joblist)
            tmp_jobfile, self._step_schedule = conf.parse_schedule(self._conf_schedule)

        self._step_joblist = conf.parse_joblist(self._conf_joblist)
        orderdiff = []
        for job in self._step_joblist:
            if job not in self._step_schedule:
                orderdiff.append(job)
        self._step_joblist = self._step_schedule + orderdiff

        return True


    #----------------------------------------------------------------------
    def shell(self):
        """
        在配置设计后，可以启动shell，交互式的单步执行schedule中的job。
        """
        MRcmd(self).cmdloop()





    def debugjob(self, p, t=None, revisions={"parts":1,"map":1,"reduce":1}, verbose=True):
        """
        在hadoop上debug一个job，简化接口。
        revisions = {'parts': numberofparts ; 'map': numberofmappers, "reduce": numberofreducers}

        基本思路
           1. 不动mrframe的框架
           2. 生成一个新的joblist.conf，设为框架默认。单步执行目标job
           3. 单步执行完毕，恢复旧joblist.conf
        """
        process = self._runjob(p=p, t=t, s=True, test=True)
        (stdout, stderr) =  process.communicate()
        retcode = process.poll()
        if retcode:
            print >> sys.stderr, stdout
            print >> sys.stderr, stderr
            return 

        prefix = os.path.dirname(self._conf_joblist)
        outname = "tmp.debug." + os.path.basename(self._conf_joblist) + "." + datetime.datetime.now().strftime("%Y%m%d-%H%M-%S")
        tmp_job_conf = prefix + "/" + outname

        conf = MRConf()
        if conf.generate_debug_jobfile(self._conf_joblist, tmp_job_conf, stdout, p, revisions):
            swap_conf_schedule = self._conf_schedule
            self._conf_schedule = conf.generate_simple_schedule_conf(tmp_job_conf)
            self.runjob(p=p, t=t, s=True, verbose=verbose)
            self._conf_schedule = swap_conf_schedule
            
            ## swap joblist to a debug one
            #swap(self._conf_joblist, tmp_job_conf)
            #self.runjob(p=p, t=t, s=True)
            #swap(self._conf_joblist, tmp_job_conf)
            
    def _print_job_iomr(self, job, result):
        """
        @type result: dict
        @param result: input/output/mapper/reducer info from mrframe test run
        
        """

        if result["cmd"]:
            print >> sys.stderr,  "==== JOB %s highlights:" % job
            print >> sys.stderr, "     cmd = %s" % result["cmd"]
        elif result["input"]:
            print >> sys.stderr,  "==== JOB %s highlights:" % job
            print >> sys.stderr, "     input = %s" % result["input"]
            print >> sys.stderr, "     output = %s" % result["output"]
            print >> sys.stderr, "     mapper = %s" % result["mapper"]
            print >> sys.stderr, "     reducer = %s" % result["reducer"]          
        else:
            pass
    

    def runjob(self, p=None, t=None, s=False, test=False, module=None, schedule=None, verbose=True):
        """
        交互调用 mrframe
        
        调用两次start.py：
            1. 第一次按照test调用，获取并打印input/output/mapper/reducer等关键信息。
            2. 第二次真实调用，执行job
        如果指定按照test模式执行，则不做第二次调用，返回详细配置信息。
        
        @rtype: int
        @return: retcode
            
        """
        # 测试执行
        process = self._runjob(p=p, t=t, s=s, test=True, module=module, schedule=schedule)
        if process:
            (stdout, stderr) = process.communicate()
            retcode = process.poll()
            if retcode:
                print >> sys.stderr, "==== test run exit with retcode %d:" % retcode
                print >> sys.stderr, "     ", stdout
                # print >> sys.stderr, "  stderr:  ", stderr
                return retcode
        else:
            return 1
         
        iomr = {"mapper":[], "reducer":[], "input":[], "output":[], "cmd":[]}
        conf = MRConf()
        hadoopmsg_file = conf.parse_mrtest(stdout, iomr, p=p)
        self._print_job_iomr(p, iomr)

        if test is True: 
            if verbose:
                print >> sys.stderr, "==== test run detail information:\n ", stdout
                # print >> sys.stderr, stderr  # no useful info
            return retcode

        # 真实执行
        process = self._runjob(p=p, t=t, s=s, test=False, module=module, schedule=schedule)
        
        if verbose:                
            if hadoopmsg_file is not None:
                time.sleep(5)  # make sure the mrframe has already created hadoopmsg_file
                if os.path.isfile(hadoopmsg_file):
                    print >> sys.stderr, "==== echo log file : %s " % hadoopmsg_file
                    for line in readline_opened_file(hadoopmsg_file):
                        print >> sys.stderr, line, 
                else:
                    print >> sys.stderr, "hadoopmsg file is not ready after mrframe run: %s" % hadoopmsg_file
                    
            else:
                # mrframe returns allway are 0. No matters what happened.
                print >> sys.stderr, "there is NO hadoomsg log file. run in test mode to find the error"
                return 1
                
                    
        (stdout, stderr) = process.communicate()
        if stdout:
            print >> sys.stderr, stdout
        if stderr:
            print >> sys.stderr, stderr
        retcode = process.poll()
        return retcode
    
    #----------------------------------------------------------------------
    def _runjob(self, p=None, t=None, s=False, test=False, module=None, schedule=None):
        """
        The inputs are mapping to parameters of start.py.

        When a input is None, it will first search the instance defaults.

        p and s will be ignore when None.
        t will be datetime.now().strftime("%Y%m%d")
        
        @rtype: Popen
        @return: the subprocess.Popen object with mrframe running

        """
        cmds = [self._start]

        if module:
            cmds.append(module)
        elif self._conf_module:
            cmds.append(self._conf_module)
        else:
            raise Exception("No module.conf is found.")

        if schedule:
            cmds.append(schedule)
        elif self._conf_schedule:
            cmds.append(self._conf_schedule)
        else:
            raise Exception("No schedule.conf is found.")

        if t:
            cmds += ["-t", t]
        else:
            cmds += ["-t", self._time_str]

        if p:
            cmds += ["-p", p]

        if s:
            cmds.append("-s")

        if test:
            cmds.append("-test")

        is_test = ""
        if test:
            is_test = "test"
        print >> sys.stderr, "==== run %s mrjob in %s:  %s " % (is_test, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), " ".join(cmds))
        # subprocess.call(cmds)
        try:
            process = subprocess.Popen(cmds, stdout=subprocess.PIPE)
        except OSError as e:
            print >> sys.stderr, e
            return None

        return process

    #----------------------------------------------------------------------
    def _parse_args(self):
        """
        """
        parser = ArgumentParser(description='wrapper for ubs_mrframework.')
        parser.add_argument("module", help="the module configure file")
        parser.add_argument("schedule", help="the schedule configure file")
        parser.add_argument("-p", "--procedure", help="The target step in schedule conf")
        parser.add_argument("-t", "--time", help="The time string like '201303150415'")
        parser.add_argument("-s", "--single", action="store_true", help="Run single step.")
        parser.add_argument("--test", action="store_true", help="Run in test mode.")

        args = parser.parse_args()
        return args

    #----------------------------------------------------------------------
    def main(self):
        """
        """
        args = self._parse_args()
        self.runjob(args.module, args.schedule, p=args.procedure, t=args.time, s=args.single, test=args.test)


if __name__=='__main__':
    mr = MRframework()
    mr.set_path("mrStart")
    mr.main()
