#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
import json
import pandas as pd


class SnapSummaryMoreStatementMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization snap_summary_more_statement data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            snap_summary_more_statement_df = data
        else:
            if data == []:
                snap_summary_more_statement_df = "The query data is empty!"
            else:
                snap_summary_more_statement_df = pd.DataFrame(data,
                                                              columns=['snapshot_id', 'snap_user_name',
                                                                       'snap_unique_sql_id', 'snap_node_name',
                                                                       'snap_node_id', 'snap_user_id', 'snap_query',
                                                                       'n_calls', 'total_elapse_time',
                                                                       'n_returned_rows', 'n_tuples_fetched',
                                                                       'n_tuples_returned', 'n_tuples_inserted',
                                                                       'n_tuples_updated', 'n_tuples_deleted',
                                                                       'n_blocks_fetched', 'n_blocks_hit',
                                                                       'n_soft_parse', 'n_hard_parse', 'db_time',
                                                                       'cpu_time', 'execution_time', 'parse_time',
                                                                       'plan_time', 'rewrite_time', 'pl_execution_time',
                                                                       'pl_compilation_time', 'snap_net_send_info_time',
                                                                       'snap_net_send_info_n_calls',
                                                                       'snap_net_send_info_size',
                                                                       'snap_net_recv_info_time',
                                                                       'snap_net_recv_info_n_calls',
                                                                       'snap_net_recv_info_size',
                                                                       'snap_net_stream_send_info_time',
                                                                       'snap_net_stream_send_info_n_calls',
                                                                       'snap_net_stream_send_info_size',
                                                                       'snap_net_stream_recv_info_time',
                                                                       'snap_net_stream_recv_info_info_n_calls',
                                                                       'snap_net_stream_recv_info_size',
                                                                       'snap_last_updated', 'sort_count', 'sort_time',
                                                                       'sort_mem_used', 'sort_spill_count',
                                                                       'sort_spill_size', 'hash_count', 'hash_time',
                                                                       'hash_mem_used', 'hash_spill_count',
                                                                       'hash_spill_size'])
        self.de_part = snap_summary_more_statement_df

    @staticmethod
    def intergrated_data(snap_summary_more_statement_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param redo_time_count_data: refresh data this time
        :return: return the computer result
        """
        snap_summary_more_statement_df['snap_net_send_info_time'] = round(
            (snap_summary_more_statement_df['snap_net_send_info_time']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_send_info_n_calls'] = round(
            (snap_summary_more_statement_df['snap_net_send_info_n_calls']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_send_info_size'] = round(
            (snap_summary_more_statement_df['snap_net_send_info_size']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_recv_info_time'] = round(
            (snap_summary_more_statement_df['snap_net_recv_info_time']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_recv_info_n_calls'] = round(
            (snap_summary_more_statement_df['snap_net_recv_info_n_calls']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_recv_info_size'] = round(
            (snap_summary_more_statement_df['snap_net_recv_info_size']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_send_info_time'] = round(
            (snap_summary_more_statement_df['snap_net_stream_send_info_time']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_send_info_n_calls'] = round(
            (snap_summary_more_statement_df['snap_net_stream_send_info_n_calls']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_send_info_size'] = round(
            (snap_summary_more_statement_df['snap_net_stream_send_info_size']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_recv_info_time'] = round(
            (snap_summary_more_statement_df['snap_net_stream_recv_info_time']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_recv_info_info_n_calls'] = round(
            (snap_summary_more_statement_df['snap_net_stream_recv_info_info_n_calls']) / 1000000, 4)
        snap_summary_more_statement_df['snap_net_stream_recv_info_size'] = round(
            (snap_summary_more_statement_df['snap_net_stream_recv_info_size']) / 1000000, 4)
        snap_summary_more_statement_df['db_time'] = round(snap_summary_more_statement_df['db_time'] / 1000000, 4)
        snap_summary_more_statement_df['cpu_time'] = round(snap_summary_more_statement_df['cpu_time'] / 1000000, 4)
        snap_summary_more_statement_df['execution_time'] = round(
            snap_summary_more_statement_df['execution_time'] / 1000000, 4)
        snap_summary_more_statement_df['parse_time'] = round(snap_summary_more_statement_df['parse_time'] / 1000000, 4)
        snap_summary_more_statement_df['plan_time'] = round(snap_summary_more_statement_df['plan_time'] / 1000000, 4)
        snap_summary_more_statement_df['total_elapse_time'] = round(
            snap_summary_more_statement_df['total_elapse_time'] / 1000000, 4)
        snap_summary_more_statement_df['rewrite_time'] = round(snap_summary_more_statement_df['rewrite_time'] / 1000000,
                                                               4)
        snap_summary_more_statement_df['pl_execution_time'] = round(
            snap_summary_more_statement_df['pl_execution_time'] / 1000000, 4)
        snap_summary_more_statement_df['pl_compilation_time'] = round(
            snap_summary_more_statement_df['pl_compilation_time'] / 1000000, 4)
        snap_summary_more_statement_df['sort_time'] = round(snap_summary_more_statement_df['sort_time'] / 1000000, 4)
        snap_summary_more_statement_df['hash_time'] = round(snap_summary_more_statement_df['hash_time'] / 1000000, 4)
        snap_summary_more_statement_df = snap_summary_more_statement_df.astype(str)
        snap_unique_sql_id_set = set()
        for i in range(snap_summary_more_statement_df.shape[0]):
            snap_unique_sql_id = snap_summary_more_statement_df.iloc[i]["snap_unique_sql_id"]
            snap_unique_sql_id_set.add(snap_unique_sql_id)
        data_list = snap_summary_more_statement_df.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)

        return snap_unique_sql_id_set, data
