#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@author: wt
@time:  2020/9/3 19:14
'''
import os
import re



TMP_DB = 'cctemp'

class SqlParser:

    class Table:
        TBL_TYPE_LIST = [1, 2] #1:create table, 2:insert table

        tbl_type = None
        table_name = None
        tmp_table_name = None
        tmp_hdfs_path = None

    def __init__(self, sqlfile):
        self.sqlfile = sqlfile
        self.wrter_tables = [] #Table
        self.sql = ""
        self._wrter_tables = set()
        with open(sqlfile, 'r') as f:
            self._sql = f.read()

    @staticmethod
    def clear_comment(sql):
        res = []
        is_comment = False
        for row in sql.split("\n"):
            row = row.strip()
            if row.startswith("/*"): is_comment = True
            if row.endswith("*/"):
                is_comment = False
                continue
            if is_comment: continue
            if not row: continue
            i = row.find("--")
            if i != -1: row = row[:i]
            res.append(row)
        return "\n".join(res)

    def replace_table(self, sql=""):
        def turn_table(table_name):
            db, table = table_name.split('.')
            return '%s.%s___%s' % (TMP_DB, db, table)
        def turn_insert_body(matched):
            all = matched.group(0)
            t = SqlParser.Table()
            tbl = matched.group('table')
            tmp_tbl = turn_table(tbl)
            t.table_name, t.tmp_table_name, t.tbl_type = tbl, turn_table(tbl), t.TBL_TYPE_LIST[1]
            if t.table_name not in self._wrter_tables:
                self.wrter_tables.append(t)
                self._wrter_tables.add(t.table_name)
            return all.replace(matched.group('table'), tmp_tbl)

        if not sql: sql = self._sql.replace('`', '')
        p1 = '\sCREATE\s+TABLE\s+(?P<table>\w+\.\w+)'
        p2 = '\sINSERT\s.*?(?P<table>\w+\.\w+)'
        created_tables = re.findall(p1, sql, re.IGNORECASE)
        for ct in created_tables:
            tmp_tbl = turn_table(ct)
            sql = sql.replace(ct, tmp_tbl)
            t = SqlParser.Table()
            t.table_name, t.tmp_table_name, t.tbl_type = ct, tmp_tbl, t.TBL_TYPE_LIST[0]
            self.wrter_tables.append(t)
            self._wrter_tables.add(t.table_name)
        sql = re.sub(p2, turn_insert_body, sql, flags=re.IGNORECASE)
        return sql

    def build_test_sql(self, manaual_create_table=False):
        sql = self.clear_comment(self._sql)
        sql = self.replace_table(sql)
        need_add_sql = ""
        for wt in self.wrter_tables:
            if wt.tbl_type == self.Table.TBL_TYPE_LIST[0]:
                need_add_sql += "drop table if exists %s;\n" % wt.tmp_table_name
            if wt.tbl_type == self.Table.TBL_TYPE_LIST[1]:
                if not manaual_create_table:
                    need_add_sql += "drop table if exists %s;\n" % wt.tmp_table_name
                    need_add_sql += "create table %s like %s;\n" % (wt.tmp_table_name, wt.table_name)
        self.sql, self._sql = need_add_sql + sql, ""
        return self.sql


class operator:
    @staticmethod
    def get_file_index(cmd):
        cmdlist = re.split('\s+', cmd) if isinstance(cmd,str) else cmd
        file_index = -1
        for i in range(len(cmdlist)):
            if cmdlist[i] == '-f' and i < len(cmdlist) - 1 and os.path.exists(cmdlist[i + 1]):
                file_index = i + 1
        return file_index

    @staticmethod
    def is_spark_sql_cmd(cmd):
        is_spark = False
        if re.split('\s+', cmd)[0].upper().find("SPARK-SQL") == -1:
            pass

        return is_spark

    @staticmethod
    def bash_operator_test_execute(task, context):
        from builtins import bytes
        import os
        import logging
        from subprocess import Popen, STDOUT, PIPE
        from tempfile import gettempdir, NamedTemporaryFile
        from airflow.exceptions import AirflowException
        from airflow.utils.file import TemporaryDirectory

        bash_command = task.bash_command
        if re.split('\s+', bash_command.strip())[0].upper().find("SPARK-SQL") == -1:
            raise AirflowException(u"仅支持运行SPARK-SQL任务。")

        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            cmdlist = re.split('\s+', bash_command)
            file_index = operator.get_file_index(cmdlist)
            file_path = cmdlist[file_index]
            sp = SqlParser(file_path)
            sql = sp.build_test_sql()
            tmp_sqlfile = os.path.join(tmp_dir, os.path.basename(file_path))
            with open(tmp_sqlfile, 'w') as f:
                f.write(sql)
            cmdlist[file_index] = tmp_sqlfile
            bash_command = ' '.join(cmdlist)
            with NamedTemporaryFile(dir=tmp_dir, prefix=task.task_id) as f:
                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=task.env,
                    preexec_fn=os.setsid)
                task.sp = sp
                logging.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(task.output_encoding).strip()
                    logging.info(line)
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))
                if sp.returncode:
                    raise AirflowException("Bash command failed")
        if task.xcom_push_flag:
            return line
