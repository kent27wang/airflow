#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@author: wt
@time:  2020/9/3 13:24
'''
import logging
from functools import wraps

def fictitious_run(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        from airflow.ccmodels import taskinstance_run
        lvars = {}
        co_varnames = func.__code__.co_varnames[:func.__code__.co_argcount]
        vs_default = func.func_defaults
        for i in range(len(vs_default)):
            lvars[co_varnames[-i - 1]] = vs_default[-i - 1]
        for i in range(len(args)):
            lvars[co_varnames[i]] = args[i]
        for k,v in kwargs.items():
            lvars[k] = v
        logging.info('parameters: %s' % lvars)
        result = taskinstance_run(lvars['self'],
                         verbose=lvars['verbose'],
                         ignore_all_deps=lvars['ignore_all_deps'],
                         ignore_depends_on_past=lvars['ignore_depends_on_past'],
                         ignore_task_deps=lvars['ignore_task_deps'],
                         ignore_ti_state=lvars['ignore_ti_state'],
                         mark_success=lvars['mark_success'],
                         test_mode=lvars['test_mode'],
                         job_id=lvars['job_id'],
                         pool=lvars['pool'],
                         session=lvars['session'])
        return result

    return wrapper


class ccviews:
    @staticmethod
    def get_func(func):
        from airflow.www.ccviews import testrun, testlog, testcode, taskcode
        func_dict = { #views与ccview函数映射关系
            'testrun': testrun,
            'testlog': testlog,
            'testcode': testcode,
            'taskcode': taskcode,
        }
        return func_dict[func.__name__]

    @staticmethod
    def replace_func(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            params = {}
            co_varnames = func.__code__.co_varnames[:func.__code__.co_argcount]
            vs_default = func.func_defaults or []
            for i in range(len(vs_default)):
                params[co_varnames[-i - 1]] = vs_default[-i - 1]
            for i in range(len(args)):
                params[co_varnames[i]] = args[i]
            for k, v in kwargs.items():
                params[k] = v
            logging.info('parameters: %s' % params)
            result = ccviews.get_func(func)(params['self'])
            return result
        return wrapper


class cccli:
    @staticmethod
    def testrun(args, dag=None):
        from airflow.ccmodels import cli_testrun
        cli_testrun(args, dag)
