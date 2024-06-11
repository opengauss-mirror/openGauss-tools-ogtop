#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pandas as pd

__author__ = 'hf'


class SnapSummaryStatementMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization snap_summary_statement data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            snap_summary_statement_df = data
        else:
            if data == []:
                snap_summary_statement_df = "The query data is empty!"
            else:
                snap_summary_statement_df = pd.DataFrame(data,
                                                         columns=['snapshot_id', 'snap_node_name', 'snap_node_id',
                                                                  'snap_user_name', 'snap_user_id',
                                                                  'snap_unique_sql_id', 'snap_n_calls',
                                                                  'snap_min_elapse_time', 'snap_max_elapse_time',
                                                                  'snap_total_elapse_time', 'snap_n_returned_rows',
                                                                  'snap_n_tuples_fetched', 'snap_n_tuples_returned',
                                                                  'snap_n_tuples_inserted', 'snap_n_tuples_updated',
                                                                  'snap_n_tuples_deleted', 'snap_n_blocks_fetched',
                                                                  'snap_n_blocks_hit', 'snap_n_soft_parse',
                                                                  'snap_n_hard_parse', 'snap_db_time', 'snap_cpu_time',
                                                                  'snap_execution_time', 'snap_parse_time',
                                                                  'snap_plan_time', 'snap_rewrite_time',
                                                                  'snap_pl_execution_time', 'snap_pl_compilation_time',
                                                                  'snap_data_io_time',
                                                                  "snap_net_send_info_time",
                                                                  "snap_net_send_info_n_calls",
                                                                  "snap_net_send_info_size", "snap_net_recv_info_time",
                                                                  "snap_net_recv_info_n_calls",
                                                                  "snap_net_recv_info_size",
                                                                  "snap_net_stream_send_info_time",
                                                                  "snap_net_stream_send_info_n_calls",
                                                                  "snap_net_stream_send_info_size",
                                                                  "snap_net_stream_recv_info_time",
                                                                  "snap_net_stream_recv_info_info_n_calls",
                                                                  "snap_net_stream_recv_info_size",
                                                                  'snap_last_updated',
                                                                  'snap_sort_count', 'snap_sort_time',
                                                                  'snap_sort_mem_used', 'snap_sort_spill_count',
                                                                  'snap_sort_spill_size', 'snap_hash_count',
                                                                  'snap_hash_time', 'snap_hash_mem_used',
                                                                  'snap_hash_spill_count', 'snap_hash_spill_size',
                                                                  'snap_query'])
        self.de_part = snap_summary_statement_df

    @staticmethod
    def intergrated_data(snap_summary_statement_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param snap_summary_statement_df: refresh data this time
        :return: return the computer result
        """
        snap_summary_statement_df['snap_net_send_info_time'] = round(
            (snap_summary_statement_df['snap_net_send_info_time']) / 1000000, 4)
        snap_summary_statement_df['snap_net_send_info_n_calls'] = round(
            (snap_summary_statement_df['snap_net_send_info_n_calls']) / 1000000, 4)
        snap_summary_statement_df['snap_net_send_info_size'] = round(
            (snap_summary_statement_df['snap_net_send_info_size']) / 1000000, 4)
        snap_summary_statement_df['snap_net_recv_info_time'] = round(
            (snap_summary_statement_df['snap_net_recv_info_time']) / 1000000, 4)
        snap_summary_statement_df['snap_net_recv_info_n_calls'] = round(
            (snap_summary_statement_df['snap_net_recv_info_n_calls']) / 1000000, 4)
        snap_summary_statement_df['snap_net_recv_info_size'] = round(
            (snap_summary_statement_df['snap_net_recv_info_size']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_send_info_time'] = round(
            (snap_summary_statement_df['snap_net_stream_send_info_time']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_send_info_n_calls'] = round(
            (snap_summary_statement_df['snap_net_stream_send_info_n_calls']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_send_info_size'] = round(
            (snap_summary_statement_df['snap_net_stream_send_info_size']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_recv_info_time'] = round(
            (snap_summary_statement_df['snap_net_stream_recv_info_time']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_recv_info_info_n_calls'] = round(
            (snap_summary_statement_df['snap_net_stream_recv_info_info_n_calls']) / 1000000, 4)
        snap_summary_statement_df['snap_net_stream_recv_info_size'] = round(
            (snap_summary_statement_df['snap_net_stream_recv_info_size']) / 1000000, 4)
        snap_summary_statement_df['snap_db_time'] = round(snap_summary_statement_df['snap_db_time'] / 1000000, 4)
        snap_summary_statement_df['snap_cpu_time'] = round(snap_summary_statement_df['snap_cpu_time'] / 1000000, 4)
        snap_summary_statement_df['snap_execution_time'] = round(
            snap_summary_statement_df['snap_execution_time'] / 1000000,
            4)
        snap_summary_statement_df['snap_parse_time'] = round(snap_summary_statement_df['snap_parse_time'] / 1000000, 4)
        snap_summary_statement_df['snap_plan_time'] = round(snap_summary_statement_df['snap_plan_time'] / 1000000, 4)
        snap_summary_statement_df['snap_total_elapse_time'] = round(
            snap_summary_statement_df['snap_total_elapse_time'] / 1000000,
            4)
        snap_summary_statement_df['snap_rewrite_time'] = round(snap_summary_statement_df['snap_rewrite_time'] / 1000000,
                                                               4)
        snap_summary_statement_df['snap_pl_execution_time'] = round(
            snap_summary_statement_df['snap_pl_execution_time'] / 1000000,
            4)
        snap_summary_statement_df['snap_pl_compilation_time'] = round(
            snap_summary_statement_df['snap_pl_compilation_time'] / 1000000, 4)
        snap_summary_statement_df['snap_sort_time'] = round(snap_summary_statement_df['snap_sort_time'] / 1000000, 4)
        snap_summary_statement_df['snap_hash_time'] = round(snap_summary_statement_df['snap_hash_time'] / 1000000, 4)
        snap_summary_statement_df = snap_summary_statement_df.astype(str)
        data_list = snap_summary_statement_df.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)
        return data
