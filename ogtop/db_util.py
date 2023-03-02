# -*- coding: utf-8 -*-
"""
Provide generic classes、functions、algorithms that are relevant to a database or server
"""
import json
import socket
import psycopg2
import time
import pandas as pd

pd.options.mode.chained_assignment = None


class openGauss_option(object):
    """database linking and manipulation"""

    def __init__(self, conn_config):
        """
        obtain the database link according to the configuration information
        :param conn_config: pass in the database link configuration dictionary
        """
        self.Host = conn_config["ip"]
        self.Port = conn_config["port"]
        self.Database = conn_config["database"]
        self.User = conn_config["user"]
        if self.User != 'omm':
            try:
                self.Password = conn_config["password"]
            except Exception as e:
                print("database connection failure")
                return
            try:
                self.conn = psycopg2.connect(host=self.Host, database=self.Database, user=self.User,
                                             password=self.Password,
                                             port=self.Port)
            except Exception as e:
                print("database connection failure")
                return
        else:
            try:
                self.conn = psycopg2.connect(database=self.Database, port=self.Port)
            except Exception as e:
                print("database connection failure")
                return

    def query_sql(self, sql, params):
        """
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                self.conn.commit()
                conn.commit()
                cur.close()
                return [''] if len(rows) == 0 else rows[0]
            except Exception as e:
                conn.rollback()
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
            try:
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                self.conn.commit()
                conn.commit()
                cur.close()
                return rows
            except Exception as e:
                conn.rollback()
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
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                self.conn.commit()
                conn.commit()
                cur.close()
                return rows
            except Exception as e:
                conn.rollback()
                return str(e)
            except IndexError as e:
                return
            except KeyError as e:
                return

    def explain_query_sql(self, explan, prepare, sql):
        """
        :param explan: execute explan sql
        :param sql: sql execute specified sql statement
        :param prepare: execute prepare sql
        :return: return the query result of an sql statement
        """
        if sql != "":
            conn = self.conn
            try:
                cur = conn.cursor()
                cur.execute(prepare + sql)
                cur.execute(explan)
                rows = cur.fetchall()
                self.conn.commit()
                conn.commit()
                cur.close()
                return rows
            except Exception as e:
                conn.rollback()
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
            "force_all": "select sessionid,query from pg_stat_activity where usename not in ('omm');",
            "force_by_dbname": "select pid,query from pg_stat_activity where datname=%s;",
            "force_idle": "select sessionid,query from pg_stat_activity where state='idle in transaction' and extract(epoch from(current_timestamp - state_change)) >600;",
            "force_by_pid": "select pg_terminate_backend (%s);"
        }
        if sign != "":
            conn = self.conn
            try:
                if sign in ["force_all", "force_by_dbname", "force_idle"]:
                    cur = conn.cursor()
                    if sign == "force_by_dbname":
                        cur.execute(sql_dict.get(sign), (datname,))
                    else:
                        cur.execute(sql_dict.get(sign))
                    querys = cur.fetchall()
                    if querys:
                        for query in querys:
                            if query[1] == sql_dict.get(sign):
                                continue
                            else:
                                cur.execute("select pg_terminate_backend (%s);", (query[0],))
                else:
                    cur = conn.cursor()
                    cur.execute(sql_dict.get(sign), (pid,))
            except Exception as e:
                conn.rollback()


def get_query(depart, sql_id):
    """
    get query details
    :param depart:display data
    :param sql_id: data sql id
    :return:the result after processing
    """
    for item in depart:
        if str(item.get("unique_sql_id")) == sql_id:
            return item.get("query").replace("\n", " ")


def sort_query(order_column, order_type, order_data, page):
    """
    sort by the specified column
    :param order_column:column names are used when sorting
    :param order_type:the type when sorting
    :param order_data:sort usage data
    :param page:execution page name
    :return: returns sorted data
    """
    num_type = {"session": ["pid", 'database'],
                "table": ["tabsize", "idxsize", "n_live_tup", "totalsize", "n_dead_tup", "seq_scan", "seq_tup_read",
                          "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
                          "heap_blks_read", "heap_blks_hit", "idx_blks_hit", "idx_blks_read", "toast_blks_read",
                          "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit", "phywrts", "phyblkrd", "phyblkwrt",
                          "readtim", "writetim", "xact_start_time", "state_change_time"],
                "lock": ["pid", "page", "tuple", "transactionid", "objid"],
                "lock_chain": ["blocker_holder"],
                "wait_event": ["wait", "failed_wait", "total_wait_time", "history_max_wait_time",
                               "history_min_wait_time"],
                "sharemem": ["level", "totalsize", "freesize", "usedsize"],
                "top_mem_used_session": ["pid", "init_mem", "used_mem", "peak_mem"]}
    float_type = {"session": ["db_time", "cpu_time", "excete_time", "data_io_time"],
                  "table": ["tabsize", "idxsize", "totalsize", ],
                  "wait_event": ["avg_wait_time"],
                  "dynamicsql": ["avg_db_time", "data_io_time", "db_time", "cpu_time", "execution_time", "parse_time",
                                 "plan_time", "total_elapse_time", "rewrite_time", "pl_execution_time",
                                 "pl_compilation_time", "sort_time", "hash_time"],
                  "sharemem": ["usedsize/totalsize"]}

    if num_type.get(page) and order_column in num_type.get(page):
        order_query = sorted(order_data, key=lambda x: int(x.get(order_column)), reverse=order_type)
    elif float_type.get(page) and order_column in float_type.get(page):
        order_query = sorted(order_data, key=lambda x: float(x.get(order_column)), reverse=order_type)
    else:
        order_query = sorted(order_data, key=lambda x: x.get(order_column), reverse=order_type)
    return order_query


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


def get_localtime():
    """
    gets the server time currently in use
    :return: return a datetime
    """
    localtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return localtime


def get_host():
    """
    obtain the ip address of the database server
    :return:the obtain server ip
    """
    # 获取主机名
    hostname = socket.gethostname()
    return hostname
