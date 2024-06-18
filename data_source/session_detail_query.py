# -*- coding: utf-8 -*-
""" session_detail data storage calculation """
import json
import pandas as pd
import datetime
import copy


class SessionDetailMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization session_detail data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        date = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(date, str):
            session_detail_df = date
        else:
            if date == []:
                session_detail_df = "The query data is empty!"
            else:
                if len(date[0]) == 51:
                    session_detail_df = pd.DataFrame(date)
                    session_detail_df.columns = ['query_id', "block_sessionid", "backend_start", "query_start", "datid",
                                                 "datname", "usesysid", "usename", "application_name", "client_addr",
                                                 "client_hostname", "client_port", "xact_start_time",
                                                 "state_change_time",
                                                 "waiting", "total_cpu_time", "db_time", "cpu_time", "execution_time",
                                                 "parse_time", "plan_time", "rewrite_time", "pl_execution_time",
                                                 "pl_compilation_time", "net_send_time", "data_io_time", "init_mem",
                                                 "used_mem", "peak_mem", "wait_status", "locktag", "lockmode", "query",
                                                 "pid", "n_commit", "n_rollback", "n_sql", "n_table_scan",
                                                 "n_blocks_fetched", "n_physical_read_operation",
                                                 "n_shared_blocks_dirtied",
                                                 "n_local_blocks_dirtied", "n_shared_blocks_read",
                                                 "n_local_blocks_read",
                                                 "n_blocks_read_time", "n_blocks_write_time", "n_sort_in_memory",
                                                 "n_sort_in_disk", "n_cu_mem_hit", "n_cu_hdd_sync_read",
                                                 "n_cu_hdd_asyn_read"]
                else:
                    session_detail_df = pd.DataFrame(date)
                    session_detail_df.columns = ["query_id", "block_sessionid", "backend_start", "query_start", "datid",
                                                 "datname", "usesysid", "usename", "application_name", "client_addr",
                                                 "client_hostname", "client_port", "xact_start_time",
                                                 "state_change_time",
                                                 "waiting", "total_cpu_time", "db_time", "cpu_time", "execution_time",
                                                 "parse_time", "plan_time", "rewrite_time", "pl_execution_time",
                                                 "pl_compilation_time", "net_send_time", "data_io_time", "wait_status",
                                                 "locktag", "lockmode", "query", "pid", "n_rollback", "n_sql",
                                                 "n_table_scan", "n_blocks_fetched", "n_physical_read_operation",
                                                 "n_shared_blocks_dirtied", "n_local_blocks_dirtied",
                                                 "n_shared_blocks_read", "n_local_blocks_read", "n_blocks_read_time",
                                                 "n_blocks_write_time", "n_sort_in_memory", "n_sort_in_disk",
                                                 "n_cu_mem_hit", "n_cu_hdd_sync_read", "n_cu_hdd_asyn_read"]
                    session_detail_df.insert(loc=26, column='init_mem', value='-')
                    session_detail_df.insert(loc=27, column='used_mem', value='-')
                    session_detail_df.insert(loc=28, column='peak_mem', value='-')
        self.de_part = session_detail_df

    @staticmethod
    def intergrated_data(old_session_detail_data, new_session_detail_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_session_detail_data: last refresh data
        :param new_session_detail_data: refresh data this time
        :return: return the computer result
        """
        session_detail = pd.merge(old_session_detail_data, new_session_detail_data, on=['pid'], how='left')
        session_detail["total_cpu_time"] = str(
            round((int(session_detail["total_cpu_time_x"]) - int(session_detail["total_cpu_time_y"])) / 1000, 4))
        session_detail["db_time"] = str(
            round((int(session_detail["db_time_x"]) - int(session_detail["db_time_y"])) / 1000, 4))
        session_detail["cpu_time"] = str(
            round((int(session_detail["cpu_time_x"]) - int(session_detail["cpu_time_y"])) / 1000, 4))
        session_detail["execution_time"] = str(
            round((int(session_detail["execution_time_x"]) - int(session_detail["execution_time_y"])) / 1000, 4))
        session_detail["data_io_time"] = str(
            round((int(session_detail["data_io_time_x"]) - int(session_detail["data_io_time_y"])) / 1000, 4))
        session_detail["parse_time"] = str(
            round((int(session_detail["parse_time_x"]) - int(session_detail["parse_time_y"])) / 1000, 4))
        session_detail["plan_time"] = str(
            round((int(session_detail["plan_time_x"]) - int(session_detail["plan_time_y"])) / 1000.4))
        session_detail["rewrite_time"] = str(
            round((int(session_detail["rewrite_time_x"]) - int(session_detail["rewrite_time_y"])) / 1000, 4))
        session_detail["pl_execution_time"] = str(
            round((int(session_detail["pl_execution_time_x"]) - int(session_detail["pl_execution_time_y"])) / 1000, 4))
        session_detail["pl_compilation_time"] = str(
            round((int(session_detail["pl_compilation_time_x"]) - int(session_detail["pl_compilation_time_y"])) / 1000,
                  4))
        session_detail["net_send_time"] = str(
            round((int(session_detail["net_send_time_x"]) - int(session_detail["net_send_time_y"])) / 1000, 4))

        session_detail["n_commit"] = str((int(session_detail["n_commit_x"]) - int(session_detail["n_commit_y"])))
        session_detail["n_rollback"] = str((int(session_detail["n_rollback_x"]) - int(session_detail["n_rollback_y"])))
        session_detail["n_sql"] = str((int(session_detail["n_sql_x"]) - int(session_detail["n_sql_y"])))
        session_detail["n_table_scan"] = str(
            (int(session_detail["n_table_scan_x"]) - int(session_detail["n_table_scan_y"])))
        session_detail["n_blocks_fetched"] = str(
            (int(session_detail["n_blocks_fetched_x"]) - int(session_detail["n_blocks_fetched_y"])))
        session_detail["n_physical_read_operation"] = str(
            (int(session_detail["n_physical_read_operation_x"]) - int(session_detail["n_physical_read_operation_y"])))
        session_detail["n_shared_blocks_dirtied"] = str(
            (int(session_detail["n_shared_blocks_dirtied_x"]) - int(session_detail["n_shared_blocks_dirtied_y"])))
        session_detail["n_local_blocks_dirtied"] = str(
            (int(session_detail["n_local_blocks_dirtied_x"]) - int(session_detail["n_local_blocks_dirtied_y"])))
        session_detail["n_shared_blocks_read"] = str(
            (int(session_detail["n_shared_blocks_read_x"]) - int(session_detail["n_shared_blocks_read_y"])))
        session_detail["n_local_blocks_read"] = str(
            (int(session_detail["n_local_blocks_read_x"]) - int(session_detail["n_local_blocks_read_y"])))
        session_detail["n_blocks_read_time"] = str(
            (int(session_detail["n_blocks_read_time_x"]) - int(session_detail["n_blocks_read_time_y"])))
        session_detail["n_blocks_write_time"] = str(
            (int(session_detail["n_blocks_write_time_x"]) - int(session_detail["n_blocks_write_time_y"])))
        session_detail["n_sort_in_memory"] = str(
            (int(session_detail["n_sort_in_memory_x"]) - int(session_detail["n_sort_in_memory_y"])))
        session_detail["n_sort_in_disk"] = str(
            (int(session_detail["n_sort_in_disk_x"]) - int(session_detail["n_sort_in_disk_y"])))
        session_detail["n_cu_mem_hit"] = str(
            (int(session_detail["n_cu_mem_hit_x"]) - int(session_detail["n_cu_mem_hit_y"])))
        session_detail["n_cu_hdd_sync_read"] = str(
            (int(session_detail["n_cu_hdd_sync_read_x"]) - int(session_detail["n_cu_hdd_sync_read_y"])))
        session_detail["n_cu_hdd_asyn_read"] = str(
            (int(session_detail["n_cu_hdd_asyn_read_x"]) - int(session_detail["n_cu_hdd_asyn_read_y"])))

        session_detail["xact_start_time_x"] = session_detail['xact_start_time_x'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))
        session_detail["backend_start_x"] = session_detail['backend_start_x'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))
        session_detail["state_change_time_x"] = session_detail['state_change_time_x'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))
        session_detail_data = session_detail[
            ["query_id_y", "block_sessionid_y", "backend_start_x", "query_start_y", "datid_y", "datname_y",
             "usesysid_y", "usename_y", "application_name_y", "client_addr_y", "client_hostname_y", "client_port_y",
             "xact_start_time_x", "state_change_time_x", "waiting_y", "total_cpu_time", "db_time", "cpu_time",
             "execution_time", "parse_time", "plan_time", "rewrite_time", "pl_execution_time", "pl_compilation_time",
             "net_send_time", "data_io_time", "init_mem_y", "used_mem_y", "peak_mem_y", "wait_status_y", "locktag_y",
             "lockmode_y", "query_y", "n_commit", "n_rollback", "n_sql", "n_table_scan", "n_blocks_fetched",
             "n_physical_read_operation", "n_shared_blocks_dirtied", "n_local_blocks_dirtied", "n_shared_blocks_read",
             "n_local_blocks_read", "n_blocks_read_time", "n_blocks_write_time", "n_sort_in_memory", "n_sort_in_disk",
             "n_cu_mem_hit", "n_cu_hdd_sync_read", "n_cu_hdd_asyn_read"]].reset_index()
        session_detail_data.rename(
            columns={"query_id_y": "query_id",
                     "backend_start_x": "backend_start",
                     "query_start_y": "query_start", "datid_y": "datid", "datname_y": "datname",
                     "usesysid_y": "usesysid", "usename_y": "usename", "application_name_y": "application_name",
                     "client_addr_y": "client_addr", "client_hostname_y": "client_hostname",
                     "client_port_y": "client_port", "xact_start_time_x": "xact_start_time",
                     "state_change_time_x": "state_change_time", "waiting_y": "waiting", "init_mem_y": "init_mem",
                     "used_mem_y": "used_mem", "peak_mem_y": "peak_mem", "wait_status_y": "wait_status",
                     "locktag_y": "locktag", "lockmode_y": "lockmode", "block_sessionid_y": "block_sessionid",
                     "query_y": "query"}, inplace=True)
        session_detail_data.pop('index')

        session_detail_data.replace(to_replace='None', value='-', inplace=True)
        session_detail_data.fillna('-', inplace=True)
        session_detail.astype(str)
        date = session_detail_data.to_json(orient='records')
        session_depart = (date[1:-1]).replace("},", "}!")
        session_depart.split("!")
        session_depart = json.loads(date)
        return session_depart

    @staticmethod
    def split_date(old_session_detail):
        old_depart = copy.deepcopy(old_session_detail)
        old_depart["total_cpu_time"] = "0"
        old_depart["db_time"] = "0"
        old_depart["cpu_time"] = "0"
        old_depart["execution_time"] = "0"
        old_depart["data_io_time"] = "0"
        old_depart["parse_time"] = "0"
        old_depart["plan_time"] = "0"
        old_depart["rewrite_time"] = "0"
        old_depart["pl_execution_time"] = "0"
        old_depart["pl_compilation_time"] = "0"
        old_depart["net_send_time"] = "0"
        old_depart["n_commit"] = "0"
        old_depart["n_rollback"] = "0"
        old_depart["n_sql"] = "0"
        old_depart["n_table_scan"] = "0"
        old_depart["n_blocks_fetched"] = "0"
        old_depart["n_physical_read_operation"] = "0"
        old_depart["n_shared_blocks_dirtied"] = "0"
        old_depart["n_local_blocks_dirtied"] = "0"
        old_depart["n_shared_blocks_read"] = "0"
        old_depart["n_local_blocks_read"] = "0"
        old_depart["n_blocks_read_time"] = "0"
        old_depart["n_blocks_write_time"] = "0"
        old_depart["n_sort_in_memory"] = "0"
        old_depart["n_sort_in_disk"] = "0"
        old_depart["n_cu_mem_hit"] = "0"
        old_depart["n_cu_hdd_sync_read"] = "0"
        old_depart["n_cu_hdd_asyn_read"] = "0"
        old_depart["xact_start_time"] = old_depart['xact_start_time'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))
        old_depart["backend_start"] = old_depart['backend_start'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))
        old_depart["state_change_time"] = old_depart['state_change_time'].apply(
            lambda x: '-' if x == None else datetime.datetime.fromtimestamp(x.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"))

        old_depart.replace(to_replace='None', value='-', inplace=True)
        old_depart.fillna('-', inplace=True)
        old_depart.astype(str)
        date = old_depart.to_json(orient='records')
        body_data = (date[1:-1]).replace("},", "}!")
        body_data.split("!")
        body_data = json.loads(date)
        return body_data


class SessionDetailStackMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
       initialization session data
        :param OpenGaussOption: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            session_stack = data
        else:
            if data == []:
                session_stack = "The query data is empty!"
            else:
                session_stack = pd.DataFrame(data)
                session_stack.columns = ['gs_stack']
        self.de_part = session_stack
