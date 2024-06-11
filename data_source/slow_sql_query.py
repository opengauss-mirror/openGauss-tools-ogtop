# -*- coding: utf-8 -*-
""" slow_sql data storage calculation """
import json

import pandas as pd


class SlowSqlMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
       initialization slow_sql data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            slow_sql_df = data
        else:
            if data == []:
                slow_sql_df = "The query data is empty!"
            else:

                slow_sql_query_df = pd.DataFrame(data)
                slow_sql_query_df.columns = ['db_name', 'user_name', 'application_name', 'client_addr',
                                             'unique_query_id', 'debug_query_id', 'query', 'duration_ms', 'session_id',
                                             'n_soft_parse', 'n_hard_parse', 'query_plan', 'n_returned_rows',
                                             'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted',
                                             'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                             'db_time', 'cpu_time', 'execution_time', 'parse_time', 'plan_time',
                                             'rewrite_time', 'pl_execution_time', 'pl_compilation_time', 'data_io_time',
                                             'net_send_info', 'net_recv_info', 'net_stream_send_info',
                                             'net_stream_recv_info', 'lock_count', 'lock_time', 'lock_wait_count',
                                             'lock_wait_time', 'lock_max_count', 'lwlock_count', 'lwlock_wait_count',
                                             'lwlock_time', 'lwlock_wait_time', 'detail']
                values = {"n_soft_parse": 0, "n_hard_parse": 0, "n_returned_rows": 0, "n_tuples_fetched": 0,
                          "n_tuples_returned": 0, "n_tuples_inserted": 0, "n_tuples_updated": 0, "n_tuples_deleted": 0,
                          "n_blocks_fetched": 0, "n_blocks_hit": 0, "db_time": 0, "cpu_time": 0, "execution_time": 0,
                          "parse_time": 0, "plan_time": 0, "rewrite_time": 0, "pl_execution_time": 0,
                          "pl_compilation_time": 0, "data_io_time": 0, "lock_count": 0, "lock_time": 0,
                          "lock_wait_count": 0, "lock_wait_time": 0, "lock_max_count": 0, "lwlock_count": 0,
                          "lwlock_wait_count": 0, "lwlock_time": 0, "lwlock_wait_time": 0
                          }
                slow_sql_query_df.fillna(value=values, inplace=True)
                # session_df.replace(to_replace='None', value='-', inplace=True)
                # session_df.replace(to_replace='', value='-', inplace=True)
                slow_sql_query_df.fillna('-', inplace=True)
                slow_sql_df = slow_sql_query_df.loc[slow_sql_query_df['query'] != '-']
                slow_sql_df = slow_sql_df.reset_index()
                slow_sql_df.pop('index')
        self.de_part = slow_sql_df

    @staticmethod
    def intergrated_data(depart,database):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param slow_sql_data: refresh data this time
        :return: return the computer result
        """
        depart['db_time'] = round((depart['db_time']) / 1000000, 4)
        depart['cpu_time'] = round((depart['cpu_time']) / 1000000, 4)
        depart['execution_time'] = round((depart['execution_time']) / 1000000, 4)
        depart['data_io_time'] = round((depart['data_io_time']) / 1000000, 4)
        depart['net_send_info_time'] = round(
            ((depart['net_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        depart['net_send_info_n_calls'] = (
            depart['net_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        depart['net_send_info_size'] = (
            depart['net_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        depart['net_recv_info_time'] = round(
            ((depart['net_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        depart['net_recv_info_n_calls'] = (
            depart['net_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        depart['net_recv_info_size'] = (
            depart['net_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        depart['net_stream_send_info_time'] = round(
            ((depart['net_stream_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000,
            4)
        depart['net_stream_send_info_n_calls'] = (
            depart['net_stream_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        depart['net_stream_send_info_size'] = (
            depart['net_stream_send_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        depart['net_stream_recv_info_time'] = round(
            ((depart['net_stream_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000,
            4)
        depart['net_stream_recv_info_n_calls'] = (
            depart['net_stream_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        depart['net_stream_recv_info_size'] = (
            depart['net_stream_recv_info'].apply(lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        slow_sql_date = depart[
            ['session_id', 'query', 'unique_query_id', 'debug_query_id', 'db_name', 'user_name', 'application_name',
             'client_addr', 'duration_ms', 'n_soft_parse', 'n_hard_parse', 'query_plan', 'n_returned_rows',
             'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated', 'n_tuples_deleted',
             'n_blocks_fetched', 'n_blocks_hit', 'db_time', 'cpu_time', 'execution_time', 'parse_time', 'plan_time',
             'rewrite_time', 'pl_execution_time', 'pl_compilation_time', 'data_io_time', 'net_send_info_time',
             'net_send_info_n_calls', 'net_send_info_size', 'net_recv_info_time', 'net_recv_info_n_calls',
             'net_recv_info_size', 'net_stream_send_info_time', 'net_stream_send_info_n_calls',
             'net_stream_send_info_size', 'net_stream_recv_info_time', 'net_stream_recv_info_n_calls',
             'net_stream_recv_info_size', 'lock_count', 'lock_time', 'lock_wait_count', 'lock_wait_time',
             'lock_max_count', 'lwlock_count', 'lwlock_wait_count', 'lwlock_time', 'lwlock_wait_time',
             'detail']].reset_index()

        slow_sql_date.pop('index')
        # res_slow_sql_date = slow_sql_date.rename(
        #     columns={'db_name_x': 'db_name', 'user_name_x': 'user_name', 'application_name_x': 'application_name',
        #              'client_addr_x': 'client_addr', 'unique_query_id_x': 'unique_query_id', 'query_x': 'query',
        #              'duration_ms_x': 'duration_ms','n_soft_parse_x': 'n_soft_parse',
        #              'n_hard_parse_x': 'n_hard_parse', 'query_plan_x': 'query_plan', 'detail_x': 'detail'})
        slow_sql_sql_data = slow_sql_date.loc[ (slow_sql_date['db_name'] == database)]
        slow_sql_query_data_df = slow_sql_sql_data[["query"]]
        slow_sql_query_data_df.replace(to_replace=0, value='-', inplace=True)
        mask = slow_sql_query_data_df["query"].str.match('MERGE INTO', case=False)
        slow_sql_query_data = slow_sql_query_data_df[~mask]
        slow_sql_query_data.insert(loc=1, column='count', value=1)

        res_slow_sql_date = slow_sql_date.to_json(orient='records')
        data_depart = (res_slow_sql_date[1:-1]).replace("},", "}!")
        data_depart.split("!")
        data_depart = json.loads(res_slow_sql_date)
        return data_depart, slow_sql_query_data

    @staticmethod
    def related_data(depart, debug_query_id):
        try:
            depart_df = pd.DataFrame(depart,
                                     columns=['session_id', 'query', 'unique_query_id', 'debug_query_id', 'db_name',
                                              'user_name', 'application_name', 'client_addr', 'duration_ms',
                                              'n_soft_parse', 'n_hard_parse', 'query_plan', 'n_returned_rows',
                                              'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted',
                                              'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched',
                                              'n_blocks_hit', 'db_time', 'cpu_time', 'execution_time', 'parse_time',
                                              'plan_time', 'rewrite_time', 'pl_execution_time',
                                              'pl_compilation_time', 'data_io_time', 'net_send_info_time',
                                              'net_send_info_n_calls', 'net_send_info_size', 'net_recv_info_time',
                                              'net_recv_info_n_calls', 'net_recv_info_size',
                                              'net_stream_send_info_time', 'net_stream_send_info_n_calls',
                                              'net_stream_send_info_size', 'net_stream_recv_info_time',
                                              'net_stream_recv_info_n_calls', 'net_stream_recv_info_size',
                                              'lock_count', 'lock_time', 'lock_wait_count', 'lock_wait_time',
                                              'lock_max_count', 'lwlock_count', 'lwlock_wait_count', 'lwlock_time',
                                              'lwlock_wait_time', 'detail'])
            related_df = depart_df.loc[depart_df['debug_query_id'] == int(debug_query_id)]
            related_data_df = related_df[["query", "query_plan", "detail"]]
            data = related_data_df.iloc[0]
            query = data["query"]
            query_plan = data["query_plan"]
            detail = data["detail"]
        except Exception as e:
            query = ""
            query_plan = ""
            detail = ""
        return query, query_plan, detail
