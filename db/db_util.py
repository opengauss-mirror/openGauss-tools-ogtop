# -*- coding: utf-8 -*-
"""
Provide generic classes、functions、algorithms that are relevant to a database or server
"""
import json
import socket

import datetime
import psycopg2
import pandas as pd
from common import alarm_configuration as ac, shortcut_key_menu
from db.db_config import abort_sql_patch

pd.options.mode.chained_assignment = None


class OpenGaussOption(object):
    """database linking and manipulation"""

    def __init__(self, conn_config):
        """
        obtain the database link according to the configuration information
        :param conn_config: pass in the database link configuration dictionary
        """
        self.Host = conn_config["host"]
        self.Port = conn_config["port"]
        self.Database = conn_config["database"]
        self.User = conn_config["user"]
        self.Application_name = conn_config['application_name']
        if self.User != 'omm':
            try:
                self.Password = conn_config["password"]
            except Exception as e:
                print('FATAL:  Invalid username/password,login denied.')
                exit()
                return
            try:
                self.conn = psycopg2.connect(host=self.Host,
                                             database=self.Database,
                                             user=self.User,
                                             password=self.Password,
                                             port=self.Port,
                                             application_name=self.Application_name)
            except Exception as e:
                print(e)
                exit()
                return
        else:
            try:
                self.conn = psycopg2.connect(database=self.Database, port=self.Port)
            except Exception as e:
                print(e)
                exit()
                return

    def query_sql(self, sql, params):
        """
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            conn.autocommit = True
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                cur.close()
                return [''] if len(rows) == 0 else rows[0]
            except Exception as e:
                return str(e)
            except IndexError as e:
                return str(e)
            except KeyError as e:
                return str(e)

    def all_query_sql(self, sql):
        """
        :param sql: sql execute specified sql statement
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            conn.autocommit = True
            try:
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                cur.close()
                return rows
            except Exception as e:
                return str(e)
            except IndexError as e:
                return str(e)
            except KeyError as e:
                return str(e)

    def time_sql(self, sql):
        """
        :param sql: sql execute specified sql statement
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            conn.autocommit = True
            try:
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                cur.close()
                conn.close()
                return rows
            except Exception as e:
                return str(e)
            except IndexError as e:
                return str(e)
            except KeyError as e:
                return str(e)

    def params_query_sql(self, sql, params):
        """
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            conn.autocommit = True
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                cur.close()
                return rows
            except Exception as e:
                return str(e)
            except IndexError as e:
                return
            except KeyError as e:
                return

    def explain_query_sql(self, explain, prepare, sql):
        """
        :param explain: execute explain sql
        :param sql: sql execute specified sql statement
        :param prepare: execute prepare sql
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            conn.autocommit = True
            try:
                cur = conn.cursor()
                cur.execute(prepare + sql)
                cur.execute(explain)
                rows = cur.fetchall()
                cur.close()
                return rows
            except Exception as e:
                return str(e)
            except IndexError as e:
                return str(e)
            except KeyError as e:
                return str(e)

    def finish_sql(self, sign, pid=None, datname=None):
        """
        conversation killing function
        :param sign:indicates the session killing mode
        :param pid:session pid
        :param datname: use database_name
        :return:
        """
        sql_dict = {
            "force_all": "select pg_terminate_backend(pid) from pg_stat_activity where usename not in ('omm') and application_name not in ('ogtop');",
            "force_by_dbname": "select pg_terminate_backend(pid) from pg_stat_activity where datname=%s and application_name not in ('ogtop');",
            "force_idle": "select pg_terminate_backend(pid) from pg_stat_activity where state='idle in transaction' and extract(epoch from(current_timestamp - state_change)) >600 and application_name not in ('ogtop');",
            "force_by_pid": "select pg_terminate_backend (pid) from pg_stat_activity where pid=%s  and application_name not in ('ogtop');"
        }
        if sign != "":
            conn = self.conn
            conn.autocommit = True
            try:
                if sign in ["force_all", "force_by_dbname", "force_idle"]:
                    cur = conn.cursor()
                    if sign == "force_by_dbname":
                        cur.execute(sql_dict.get(sign), (datname,))
                        cur.close()
                        conn.close()
                    else:
                        cur.execute(sql_dict.get(sign))
                        cur.close()
                        conn.close()
                else:
                    cur = conn.cursor()
                    cur.execute(sql_dict.get(sign), (pid,))
                    cur.close()
                    conn.close()
            except Exception as e:
                # conn.rollback()
                '-'


def get_query(OpenGaussOption, dynamicsql_query_sql, sql_id):
    """
    get query details
    :param OpenGaussOption: database connection
    :param sql_id: data sql id or session id
    :param dynamicsql_query_sql: dynamicsql query sql
    :return:the result after processing
    """
    try:
        res = OpenGaussOption.params_query_sql(dynamicsql_query_sql, (sql_id,))
        data = res[0][0]
    except Exception as e:
        data = ''
    return data


def get_querys(session_id, page, depart):
    """
    get query details
    :param depart:display data
    :param sql_id: data session id or session id
    :return:the result after processing
    """
    try:
        data_df = pd.DataFrame(depart, columns=shortcut_key_menu.background_data_field[page])
        data_df = data_df.loc[data_df['sessionid'] == int(session_id), 'unique_query']
        data = data_df.iloc[0]
    except Exception as e:
        data = ""
    return data


def sort_query(order_column, order_type, order_data, page):
    """
    sort by the specified column
    :param order_column:column names are used when sorting
    :param order_type:the type when sorting
    :param order_data:sort usage data
    :param page:execution page name
    :return: returns sorted data
    """

    num_type = {"session": ["pid"],
                "asp": ["sessionid", "unique_query_id"],
                "table": ['relid'],
                "table_performance": ["relid", "n_live_tup", "n_dead_tup", "seq_scan", "seq_tup_read", "idx_scan",
                                      "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
                                      "heap_blks_read", "heap_blks_hit", "idx_blks_hit", "idx_blks_read",
                                      "toast_blks_read", "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit",
                                      "readtim"],
                "sharemem": ["level", "totalsize", "freesize", "usedsize"],
                "top_mem_used_session": ["pid", "init_mem", "used_mem", "peak_mem"],
                "lock": ["pid", "page", "tuple", "transactionid", "virtualxid", "objid"],
                "lock_chain": ["blocker_holder"],
                "wait_event": ["wait", "failed_wait"],
                "dynamicsql": ["unique_sql_id", ],
                "slow_sql": ['session_id', 'unique_query_id', 'debug_query_id', 'n_soft_parse', 'n_hard_parse',
                             'n_returned_rows', 'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted',
                             'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                             'parse_time', 'plan_time', 'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
                             'lock_count', 'lock_time', 'lock_wait_count', 'lock_wait_time', 'lock_max_count',
                             'lwlock_count', 'lwlock_wait_count', 'lwlock_time', 'lwlock_wait_time'],
                "redo":["redo_start_ptr"],
                }
    float_type = {
        "session": ["db_time", "cpu_time", "excete_time", "data_io_time", "xact_start_time", "query_start_time"],
        "asp": ["duration_ms"],
        "table": ["tabsize", "idxsize", "totalsize"],
        "table_performance": ["phyrds", "phywrts", "phyblkrd", "phyblkwrt", ],
        "sharemem": ["usedsize/totalsize"],
        "wait_event": ["avg_wait_time", "total_wait_time", "history_max_wait_time", "history_min_wait_time"],
        "dynamicsql": ["avg_db_time", "data_io_time", "n_calls", "db_time", "cpu_time", "execution_time", "parse_time",
                       "plan_time", "total_elapse_time", "n_returned_rows", "n_tuples_fetched", "n_tuples_returned",
                       "n_tuples_inserted", "n_tuples_updated", "n_tuples_deleted", "n_blocks_fetched", "n_blocks_hit",
                       "n_soft_parse", "n_hard_parse", "rewrite_time", "pl_execution_time", "pl_compilation_time",
                       'net_send_info_time', 'net_send_info_n_calls', 'net_send_info_size', 'net_recv_info_time',
                       'net_recv_info_n_calls', 'net_recv_info_size', 'net_stream_send_info_time',
                       'net_stream_send_info_n_calls', 'net_stream_send_info_size', 'net_stream_recv_info_time',
                       'net_stream_recv_info_n_calls', 'net_stream_recv_info_size', "sort_count", "sort_time",
                       "sort_mem_used", "hash_time", "sort_spill_count", "sort_spill_size", "hash_count",
                       "hash_mem_used", "hash_spill_count", "hash_spill_size"],
        "slow_sql": ['duration_ms', 'db_time', 'cpu_time', 'execution_time', 'data_io_time', 'net_send_info_time',
                     'net_send_info_n_calls', 'net_send_info_size', 'net_recv_info_time', 'net_recv_info_n_calls',
                     'net_recv_info_size', 'net_stream_send_info_time', 'net_stream_send_info_n_calls',
                     'net_stream_send_info_size', 'net_stream_recv_info_time', 'net_stream_recv_info_n_calls',
                     'net_stream_recv_info_size'],
        "snap_summary_more_statement": ['snap_unique_sql_id', 'snap_node_id', 'snap_user_id', 'n_calls',
                                        'total_elapse_time', 'n_returned_rows', 'n_tuples_fetched',
                                        'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated',
                                        'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit', 'n_soft_parse',
                                        'n_hard_parse', 'db_time', 'cpu_time', 'execution_time', 'parse_time',
                                        'plan_time', 'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
                                        'snap_net_send_info_time', 'snap_net_send_info_n_calls',
                                        'snap_net_send_info_size', 'snap_net_recv_info_time',
                                        'snap_net_recv_info_n_calls', 'snap_net_recv_info_size',
                                        'snap_net_stream_send_info_time', 'snap_net_stream_send_info_n_calls',
                                        'snap_net_stream_send_info_size', 'snap_net_stream_recv_info_time',
                                        'snap_net_stream_recv_info_info_n_calls',
                                        'snap_net_stream_recv_info_size', 'sort_count', 'sort_time',
                                        'sort_mem_used', 'sort_spill_count', 'sort_spill_size', 'hash_count',
                                        'hash_time', 'hash_mem_used', 'hash_spill_count', 'hash_spill_size'],
    }
    if num_type.get(page) and order_column in num_type.get(page):
        order_query = sorted(order_data, key=lambda x: int(x.get(order_column)), reverse=order_type)
    elif float_type.get(page) and order_column in float_type.get(page):
        order_query = sorted(order_data, key=lambda x: float(x.get(order_column)), reverse=order_type)
    else:
        order_query = sorted(order_data, key=lambda x: x.get(order_column), reverse=order_type)
    return order_query


def create_abort_sql_patch(OpenGaussOption, patchname, uniquesqlid):
    """
    create sql patch
    :param OpenGaussOption: database connection
    :param patchname:  sql patch name
    :param uniquesqlid:  sql uniquesqlid
    :return:
    """
    try:
        OpenGaussOption.params_query_sql(abort_sql_patch, (patchname, uniquesqlid))
    except Exception as e:
        '-'


def drop_related_data(OpenGaussOption, sql, patchname):
    """
    execute drop sql
    :param OpenGaussOption: database connection
    :param patchname: sql patch name
    :return:
    """
    try:
        a = OpenGaussOption.params_query_sql(sql, (patchname,))
        print(a)
    except Exception as e:
        print(e)
        '-'


def fuzzy_match(fuzzy_key, fuzzy_data, filed_list):
    """
    fuzzy query
    :param fuzzy_key: fuzzy query data
    :param fuzzy_data: total data for a fuzzy query
    :param filed_list: fuzzy list of fields
    :return: returns the data after the fuzzy query
    """
    fuzzy_data = pd.DataFrame(fuzzy_data, columns=filed_list).astype('str')

    data = []
    for filed in filed_list:
        index_list = fuzzy_data[fuzzy_data[filed].str.contains(fuzzy_key, regex=False) == True].index.values.tolist()
        if index_list != []:
            for ind in index_list:
                if fuzzy_data.loc[ind].tolist() in data:
                    continue
                else:
                    data.append(fuzzy_data.loc[ind].tolist())
    dfs = pd.DataFrame(data, columns=filed_list).reset_index()
    dfs.pop('index')
    data_list1 = dfs.to_json(orient='records')
    data_list = (data_list1[1:-1]).replace("},", "}!")
    data_list.split("!")
    data_list = json.loads(data_list1)
    return data_list


def progress_bar(float_num):
    """
    calculating progress bar
    :param float_num: pass in a decimal number less than or equal to 100,leaving 2 decimal places
    :return: returns a progress bar string
    """
    if type(float_num) != float:
        return '>' * float_num + "{}.00%".format(str(float_num))
    elif float_num == 100.00:
        return '>' * 50 + "100.00%"
    elif float_num == 0.00:
        return '>' * 0 + '0.00%'
    elif float_num == None:
        return ''
    else:
        n = int(float_num // 2)
        return ">" * n + str(round(float_num, 2)) + '%'


def conversion_type(dicts):
    """
    replaces all the valuses in the dictionary with string types
    :param dicts: pass in a dictionary with values of other types
    :return: returns a dictionary whose value is a string
    """
    for k, v in dicts.items():
        if v == None:
            dicts[k] = ''
    dicts = {k: str(v) for k, v in dicts.items()}
    return dicts


def get_localtime(openGau, sql):
    """
    gets the server time currently in use
    :return: return a datetime
    """
    db_host_time = openGau.time_sql(sql)
    return db_host_time


def get_host():
    """
    obtain the ip address of the database server
    :return:the obtain server ip
    """
    hostname = socket.gethostname()
    return hostname


def check_alarm_data(depart, table, alarm_flg):
    """
    检测是否告警
    :param depart: 数据
    :param table: 页面
    :param alarm_flg: 告警标志
    :return:
    """
    alarm_menu = ac.alarm_parameter.get(table)
    flg = alarm_menu.get(alarm_flg)
    symbol = flg.get("symbol")
    numerical = flg.get("numerical")
    state = "safety"
    if alarm_flg == 'last_autovacuum':
        if depart > numerical:
            state = "alarm"
            return state
    elif alarm_flg == "last_autovacuum":
        if (datetime.datetime.now() - datetime.datetime.strptime(depart.split('.')[0],
                                                                 '%Y-%m-%d %H:%M:%S.%f')).days > 7 if depart != '-' else depart:
            state = "alarm"
            return state
    else:
        if symbol == ">":
            if depart > numerical:
                state = "alarm"
                return state
        elif symbol == "<":
            if depart < numerical:
                state = "alarm"
                return state
        elif symbol == "!=":
            if depart != numerical:
                state = "alarm"
                return state
        elif symbol == "==":
            if depart == numerical:
                state = "alarm"
                return state
    return state
