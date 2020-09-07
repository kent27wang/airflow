#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@author: wt
@time:  2020/9/3 17:38
'''
import copy
import os
import subprocess

import dateutil.parser
import dateutil.parser
from flask import (
    redirect)
from flask import (
    request)
from flask._compat import PY2
from flask_login import flash
from past.builtins import unicode

from airflow import configuration as conf
from airflow import models
from airflow.settings import Session
from airflow.www.forms import DateTimeForm
from airflow.www.views import dagbag


def testlog(object):
    BASE_TEST_LOG_FOLDER = os.path.expanduser(
        conf.get('core', 'BASE_LOG_FOLDER')) + '/test'
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    execution_date = request.args.get('execution_date')
    dag = dagbag.get_dag(dag_id)
    log_relative = "{dag_id}/{task_id}/{execution_date}".format(
        **locals())
    loc = os.path.join(BASE_TEST_LOG_FOLDER, log_relative)
    loc = loc.format(**locals())
    log = ""
    TI = models.TaskInstance
    session = Session()
    dttm = dateutil.parser.parse(execution_date)
    ti = session.query(TI).filter(
        TI.dag_id == dag_id, TI.task_id == task_id,
        TI.execution_date == dttm).first()
    dttm = dateutil.parser.parse(execution_date)
    form = DateTimeForm(data={'execution_date': dttm})

    if ti:
        host = ti.hostname
        log_loaded = False
        if os.path.exists(loc):
            try:
                f = open(loc)
                log += "".join(f.readlines())
                f.close()
                log_loaded = True
            except:
                log = "*** Failed to load local log file:\n{0}.\n".format(loc)
        else:
            log = "*** Failed to load local log file:\n{0} not exists.\n".format(loc)
    session.commit()
    session.close()

    if PY2 and not isinstance(log, unicode):
        log = log.decode('utf-8')
    return object.render(
        'airflow/ti_code.html',
        code=log, dag=dag, title="Test Run Log", task_id=task_id,
        execution_date=execution_date, form=form)


def testrun(object):
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    origin = request.args.get('origin')
    dag = dagbag.get_dag(dag_id)
    execution_date = request.args.get('execution_date')
    execution_date = dateutil.parser.parse(execution_date).isoformat()
    manaual_create_table = request.args.get('manaual_create_table') == "true"
    clear_history_data = request.args.get('clear_history_data') == "true"
    cmd = 'airflow testrun {} {} {}'.format(dag_id, task_id, execution_date)
    if manaual_create_table: cmd += ' --manaual_create_table'
    if clear_history_data: cmd += ' --clear_history_data'
    path = "DAGS_FOLDER/{}".format(dag.filepath)
    cmd += ' -sd %s' % path
    sp = subprocess.Popen(
        ['bash', '-c', cmd],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=copy.copy(os.environ))
    flash("TaskInstance[{}, {}] should start any moment now.".format(task_id, execution_date))
    return redirect(origin)


def testcode(object):
    from airflow.ccutils import operator
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    execution_date = request.args.get('execution_date')
    dttm = dateutil.parser.parse(execution_date)
    form = DateTimeForm(data={'execution_date': dttm})
    dag = dagbag.get_dag(dag_id)
    task = dag.get_task(task_id)
    code = operator.parse_task_code(task, 2)
    return object.render(
        'airflow/ti_code.html',
        code=code, dag=dag, title="Test Code", task_id=task_id,
        execution_date=execution_date, form=form)

def taskcode(object):
    from airflow.ccutils import operator
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    execution_date = request.args.get('execution_date')
    dttm = dateutil.parser.parse(execution_date)
    form = DateTimeForm(data={'execution_date': dttm})
    dag = dagbag.get_dag(dag_id)
    task = dag.get_task(task_id)
    code = operator.parse_task_code(task, 1)
    return object.render(
        'airflow/ti_code.html',
        code=code, dag=dag, title="Task Code", task_id=task_id,
        execution_date=execution_date, form=form)
