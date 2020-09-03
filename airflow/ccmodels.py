#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@author: wt
@time:  2020/9/2 16:17
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.standard_library import install_aliases

install_aliases()
import copy
from datetime import datetime
import os
import signal
import socket

from airflow import settings
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils.state import State

import logging

from airflow.models import  Log, Stats



def taskinstance_run(
        tiself,
        verbose=True,
        ignore_all_deps=False,
        ignore_depends_on_past=False,
        ignore_task_deps=False,
        ignore_ti_state=False,
        mark_success=False,
        test_mode=False,
        job_id=None,
        pool=None,
        session=None):

    task = tiself.task
    tiself.pool = pool or task.pool
    tiself.test_mode = test_mode
    tiself.refresh_from_db(session=session, lock_for_update=True)
    tiself.job_id = job_id
    tiself.hostname = socket.getfqdn()
    tiself.operator = task.__class__.__name__

    if not ignore_all_deps and not ignore_ti_state and tiself.state == State.SUCCESS:
        Stats.incr('previously_succeeded', 1, 1)

    queue_dep_context = DepContext(
        deps=QUEUE_DEPS,
        ignore_all_deps=ignore_all_deps,
        ignore_ti_state=ignore_ti_state,
        ignore_depends_on_past=ignore_depends_on_past,
        ignore_task_deps=ignore_task_deps)
    if not tiself.are_dependencies_met(
            dep_context=queue_dep_context,
            session=session,
            verbose=True):
        session.commit()
        return

    hr = "\n" + ("-" * 80) + "\n"  # Line break

    # For reporting purposes, we report based on 1-indexed,
    # not 0-indexed lists (i.e. Attempt 1 instead of
    # Attempt 0 for the first attempt).
    msg = "Starting attempt {attempt} of {total}".format(
        attempt=tiself.try_number % (task.retries + 1) + 1,
        total=task.retries + 1)
    tiself.start_date = datetime.now()

    dep_context = DepContext(
        deps=RUN_DEPS - QUEUE_DEPS,
        ignore_all_deps=ignore_all_deps,
        ignore_depends_on_past=ignore_depends_on_past,
        ignore_task_deps=ignore_task_deps,
        ignore_ti_state=ignore_ti_state)
    runnable = tiself.are_dependencies_met(
        dep_context=dep_context,
        session=session,
        verbose=True)

    if not runnable and not mark_success:
        # FIXME: we might have hit concurrency limits, which means we probably
        # have been running prematurely. This should be handled in the
        # scheduling mechanism.
        tiself.state = State.NONE
        msg = ("FIXME: Rescheduling due to concurrency limits reached at task "
               "runtime. Attempt {attempt} of {total}. State set to NONE.").format(
            attempt=tiself.try_number % (task.retries + 1) + 1,
            total=task.retries + 1)
        logging.warning(hr + msg + hr)

        tiself.queued_dttm = datetime.now()
        msg = "Queuing into pool {}".format(tiself.pool)
        logging.info(msg)
        session.merge(tiself)
        session.commit()
        return

    # Another worker might have started running this task instance while
    # the current worker process was blocked on refresh_from_db
    if tiself.state == State.RUNNING:
        msg = "Task Instance already running {}".format(tiself)
        logging.warn(msg)
        session.commit()
        return

    # print status message
    logging.info(hr + msg + hr)
    tiself.try_number += 1

    if not test_mode:
        session.add(Log(State.RUNNING, tiself))
    tiself.state = State.RUNNING
    tiself.pid = os.getpid()
    tiself.end_date = None
    if not test_mode:
        session.merge(tiself)
    session.commit()

    # Closing all pooled connections to prevent
    # "max number of connections reached"
    settings.engine.dispose()
    if verbose:
        if mark_success:
            msg = "Marking success for "
        else:
            msg = "Executing "
        msg += "{tiself.task} on {tiself.execution_date}"

    context = {}
    try:
        logging.info(msg.format(tiself=tiself))
        if not mark_success:
            context = tiself.get_template_context()

            task_copy = copy.copy(task)
            tiself.task = task_copy

            def signal_handler(signum, frame):
                '''Setting kill signal handler'''
                logging.error("Killing subprocess")
                task_copy.on_kill()
                raise AirflowException("Task received SIGTERM signal")

            signal.signal(signal.SIGTERM, signal_handler)

            # Don't clear Xcom until the task is certain to execute
            tiself.clear_xcom_data()

            tiself.render_templates()
            # task_copy.pre_execute(context=context)
            logging.info('Skip pre_execute.')

            # If a timeout is specified for the task, make it fail
            # if it goes beyond
            result = None
            # if task_copy.execution_timeout:
            #     try:
            #         with timeout(int(
            #                 task_copy.execution_timeout.total_seconds())):
            #             result = task_copy.execute(context=context)
            #     except AirflowTaskTimeout:
            #         task_copy.on_kill()
            #         raise
            # else:
            #     result = task_copy.execute(context=context)
            logging.info('Skip execute.')
            # If the task returns a result, push an XCom containing it
            # if result is not None:
            #     tiself.xcom_push(key=XCOM_RETURN_KEY, value=result)

            # task_copy.post_execute(context=context)
            logging.info('Skip post_execute.')
            Stats.incr('operator_successes_{}'.format(
                tiself.task.__class__.__name__), 1, 1)
        tiself.state = State.SUCCESS
    except AirflowSkipException:
        tiself.state = State.SKIPPED
    except (Exception, KeyboardInterrupt) as e:
        tiself.handle_failure(e, test_mode, context)
        raise

    # Recording SUCCESS
    tiself.end_date = datetime.now()
    tiself.set_duration()
    if not test_mode:
        session.add(Log(tiself.state, tiself))
        session.merge(tiself)
    session.commit()

    # Success callback
    # try:
    #     if task.on_success_callback:
    #         task.on_success_callback(context)
    # except Exception as e3:
    #     logging.error("Failed when executing success callback")
    #     logging.exception(e3)
    logging.info('Skip success callback.')

    session.commit()
