#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Purpose：实现了DataX 的Restful API
# Author: Justin

'''''''''''''''''''''''''''
        API 示例
'''''''''''''''''''''''''''
# 启动一个Job，返回Job ID
# JOB_DESC=`cat xxxx.json`
# curl -d "jobDesc=$JOB_DESC&jvm=\"xxxx\"&params=\"-DHADOOP_USER_NAME=hive -Dyear=2020\"" http://localhost:9999/job
# 或
# curl -d "jobDesc=http://<dns>/path/test.json&params=\"-Dyear=2020\"" http://localhost:9999/job
# curl -d "jobDesc=<local path>/dir/test.json&params=\"-Dyear=2020\"" http://localhost:9999/job
# 
# 查询所有Job ID列表
# curl http://localhost:9999/jobs

# 查询Job状态
# curl http://localhost:9999/job/1
# curl http://localhost:9999/job/1/attr   attr可以是stdout,stderr,status,errmsg

# 中止并移除一个Job
# curl -X DELETE http://localhost:9999/job/1

import os
import subprocess
from subprocess import PIPE
import threading
import inspect
import ctypes
from flask import Flask, request
import json
import re
import tempfile
from datax import *


app = Flask(__name__)

# 全局变量，保存所有job的运行结果
jobInfos = {}
logDir = os.path.join(os.path.dirname(os.path.dirname(__file__)),'log','server')

def getjobInfo(jobid):
    jobInfo = None
    if jobInfos.has_key(jobid):
        jobInfo = jobInfos[jobid]
    return jobInfo
 
def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")

def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)

# 新job在运行在一个新的线程中
def start_job_worker(jobid,jvm, params,options,jobDesc):
    # printCopyright()
    inputArgv = []
    filepath = None

    if jobid is not None:
        inputArgv.append('--jobid={0}'.format(jobid))
    if jvm is not None:
        inputArgv.append('-j{0}'.format(jvm))
    if params is not None:
        inputArgv.append('-p{0}'.format(params))
    if options is not None:
        inputArgv.append(options)
    
    #  如果以{开头，表示输入的是描述Job的JSON，否则按json文件路径或URL处理
    if( re.match('\s*\{.*',jobDesc)):
        fd, filepath = tempfile.mkstemp()
        with open(filepath,'wb') as f:
            # ensure_ascii一定要设为False，否则返回的是ascii格式的，其中仍然包含着unicode编码文本
            jobDesc = json.loads(jobDesc,encoding='utf-8')
            str = json.dumps(jobDesc,ensure_ascii=False,encoding='utf-8')
            str = str.encode('utf-8')
            f.write(str)
            # json.dump(jobDesc,f,encoding='utf-8')
            f.close()
            inputArgv.append(filepath)
    else:
        # unicode类型转str类型，datax json文件路径只允许str类型
        jobDesc = jobDesc.encode('utf-8')
        inputArgv.append(jobDesc)
    
    # 为复用datax.py,模仿它的参数构造
    parser = getOptionParser()
    options, args = parser.parse_args(inputArgv)
    startCommand = buildStartCommand(options, args)
    print(startCommand)
    # 指定stdout管道，才会接收到控制台输出
    child_process = subprocess.Popen(startCommand, shell=True,stdout= PIPE,stderr= PIPE)

    try:
        # 阻塞，等待执行完成，如果执行过程中线程被kill，它会抛出异常
        (stdout, stderr) = child_process.communicate()
        # print(stdout)
        jobInfo = getjobInfo(jobid)

        # 日志可能较大，需要保存到磁盘文件中
        with open(os.path.join(logDir,'job-{}.log'.format(jobid)),'wb') as f:
            f.write(stdout)
            f.close()
  
        jobInfo["stderr"] = stderr
        jobInfo["return_code"] = child_process.returncode
        jobInfo.pop("jobThread")
        jobInfos[jobid] = jobInfo
    except SystemExit as e:
        jobInfo = getjobInfo(jobid)
        jobInfo["errmsg"] = 'Job {} has been killed'.format(jobid)
        jobInfo.pop("jobThread")
        jobInfos[jobid] = jobInfo

    if filepath is not None:
        os.unlink(filepath)

# 启动一个新的job
@app.route('/job', methods=['POST'])
def start_job():
    try:
        if False:
            # header Content-Type: Content-Type: application/json
            values = json.loads(request.get_data(),encoding='utf-8')
        else:
            # header Content-Type: application/x-www-form-urlencoded
            values = request.form

        jvm = values.get("jvm",None)
        params = values.get("params",None)
        options = values.get("options",None)
        jobDesc = values.get("jobDesc",None)

        jobid = str(len(jobInfos)+1)
        t = threading.Thread(   target=start_job_worker,
                                name='datax_job_{0}'.format(jobid),
                                kwargs={
                                    'jobid': jobid,
                                    'jvm': jvm,
                                    'params': params,
                                    'options': options,
                                    'jobDesc': jobDesc
                                })
        t.start()
        
        jobInfo = {}
        jobInfo["jobThread"] = t
        jobInfos[jobid] = jobInfo

        return "jobid={0}".format(jobid);
    except Exception as e:
        print(e)
        return "jobid=-1"

# 查询所有Job的ID列表
@app.route('/jobs',methods=['GET'])
def get_jobids():
    ids = []
    for id in jobInfos:
        ids.append(id)
    return json.dumps(ids)


# 查询job运行结果
@app.route('/job/<path:jobid>',methods=['GET'])
@app.route('/job/<path:jobid>/<path:attr>',methods=['GET'])
def get_job(jobid='',attr=None):

    jobInfos.has_key(jobid)
    jobInfo = getjobInfo(jobid)
    reqResult = {}

    if jobInfo is None:
        reqResult["status"] = "error"
        reqResult["errmsg"] = 'Job {} not found.'.format(jobid)
        return json.dumps(reqResult)
    else:
        if not jobInfo.has_key("return_code"):
            reqResult["status"] = "error"
            reqResult['errmsg'] = 'Job {} is running.'.format(jobid)
        else:
            reqResult["status"] = "success"            
            # reqResult["stderr"] = jobInfo["stderr"]
            reqResult["return_code"] = jobInfo["return_code"]
            
            if attr == 'stdout':
                logfile = os.path.join(logDir,'job-{}.log'.format(jobid))
                if os.path.exists(logfile):
                    with open(logfile,'rb') as f:
                        reqResult["stdout"] = f.read()
                        f.close()
                else:
                    print('Job日志文件[Job-{}.log]没找到'.format(jobid))

            if attr is None:
                return json.dumps(reqResult,ensure_ascii=False,encoding='utf-8')
            elif reqResult.has_key(attr):
                return json.dumps(reqResult[attr],ensure_ascii=False,encoding='utf-8')
            else:
                return 'Job {} has no \"{}\" attribute.'.format(jobid,attr)



# 移除一个Job，如果它正在运行则先kill掉再移除
@app.route('/job/<path:jobid>',methods=['DELETE'])
def remove_jobid(jobid=''):

    jobInfo = getjobInfo(jobid)
    reqResult = {}

    if jobInfo is None:
        reqResult["status"] = "error"
        reqResult["errmsg"] = 'Job {} not found.'.format(jobid)
    else:
        if not jobInfo.has_key("return_code"):
            try:
                t = jobInfo["jobThread"]
                stop_thread(t)
            except Exception as e:
                print(e)
            reqResult["errmsg"] = "Job {} has been killed.".format(jobid)
        else:
            reqResult["errmsg"] = 'Job {} has completed before this.'.format(jobid)
        
        # 移除该job
        jobInfos.pop(jobid)
        reqResult["status"] = "success"
    return json.dumps(reqResult)


if __name__ == '__main__':
    from argparse import ArgumentParser
    try:

        parser = ArgumentParser()
        parser.add_argument('-p',
                            '--port',
                            default=9999,
                            type=int,
                            help='port to listen on')

        parser.add_argument('--host',
                            default='127.0.0.1',
                            type=str,
                            help='bind ip address')
        parser.add_argument('--logdir',
                            default=logDir,
                            type=str,
                            help='logfile output dir')    
        args = parser.parse_args()
        port = args.port
        host = args.host
        logDir = args.logdir

        # 检查保存日志的目录
        if not os.path.exists(logDir):
            os.makedirs(logDir)
        
        # debug=True时，在VSCode中断点失效
        app.run(debug=False, host=host, port=port)
    except Exception as e:
        print(e)