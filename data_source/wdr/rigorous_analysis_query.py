#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

__author__ = 'hf'


class RigorousAnalysisMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization sql_rigorous_analysis data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            rigorous_analysis_df = data
        else:
            if data == []:
                rigorous_analysis_df = "The query data is empty!"
            else:
                rigorous_analysis_df = pd.DataFrame(data,
                                                    columns=['snapshot_id', 'snap_user_name', 'snap_unique_sql_id',
                                                             'snap_node_name', 'snap_node_id', 'snap_user_id',
                                                             'snap_query', 'n_calls', 'total_elapse_time',
                                                             'n_returned_rows', 'n_tuples_fetched', 'n_tuples_returned',
                                                             'n_tuples_inserted', 'n_tuples_updated',
                                                             'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                                             'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
                                                             'execution_time', 'parse_time', 'plan_time',
                                                             'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
                                                             'snap_net_send_info_time', 'snap_net_send_info_n_calls',
                                                             'snap_net_send_info_size', 'snap_net_recv_info_time',
                                                             'snap_net_recv_info_n_calls', 'snap_net_recv_info_size',
                                                             'snap_net_stream_send_info_time',
                                                             'snap_net_stream_send_info_n_calls',
                                                             'snap_net_stream_send_info_size',
                                                             'snap_net_stream_recv_info_time',
                                                             'snap_net_stream_recv_info_info_n_calls',
                                                             'snap_net_stream_recv_info_size', 'snap_last_updated',
                                                             'sort_count', 'sort_time', 'sort_mem_used',
                                                             'sort_spill_count', 'sort_spill_size', 'hash_count',
                                                             'hash_time', 'hash_mem_used', 'hash_spill_count',
                                                             'hash_spill_size'])
        self.de_part = rigorous_analysis_df

    @staticmethod
    def intergrated_data(rigorous_analysis_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param rigorous_analysis_df: refresh data this time
        :return: return the computer result
        """
        mean_df = rigorous_analysis_df.mean()
        std_df = rigorous_analysis_df.std()
        rigorous_analysis_df['n_calls_avg'] = mean_df['n_calls']
        rigorous_analysis_df['n_calls_std'] = std_df['n_calls']
        rigorous_analysis_df['total_elapse_time_avg'] = mean_df['total_elapse_time']
        rigorous_analysis_df['total_elapse_time_std'] = std_df['total_elapse_time']
        rigorous_analysis_df['n_returned_rows_avg'] = mean_df['n_returned_rows']
        rigorous_analysis_df['n_returned_rows_std'] = std_df['n_returned_rows']
        rigorous_analysis_df['n_tuples_fetched_avg'] = mean_df['n_tuples_fetched']
        rigorous_analysis_df['n_tuples_fetched_std'] = std_df['n_tuples_fetched']
        rigorous_analysis_df['n_tuples_returned_avg'] = mean_df['n_tuples_returned']
        rigorous_analysis_df['n_tuples_returned_std'] = std_df['n_tuples_returned']
        rigorous_analysis_df['n_tuples_inserted_avg'] = mean_df['n_tuples_inserted']
        rigorous_analysis_df['n_tuples_inserted_std'] = std_df['n_tuples_inserted']
        rigorous_analysis_df['n_tuples_updated_avg'] = mean_df['n_tuples_updated']
        rigorous_analysis_df['n_tuples_updated_std'] = std_df['n_tuples_updated']
        rigorous_analysis_df['n_tuples_deleted_avg'] = mean_df['n_tuples_deleted']
        rigorous_analysis_df['n_tuples_deleted_std'] = std_df['n_tuples_deleted']
        rigorous_analysis_df['n_blocks_fetched_avg'] = mean_df['n_blocks_fetched']
        rigorous_analysis_df['n_blocks_fetched_std'] = std_df['n_blocks_fetched']
        rigorous_analysis_df['n_blocks_hit_avg'] = mean_df['n_blocks_hit']
        rigorous_analysis_df['n_blocks_hit_std'] = std_df['n_blocks_hit']
        rigorous_analysis_df['n_soft_parse_avg'] = mean_df['n_soft_parse']
        rigorous_analysis_df['n_soft_parse_std'] = std_df['n_soft_parse']
        rigorous_analysis_df['n_hard_parse_avg'] = mean_df['n_hard_parse']
        rigorous_analysis_df['n_hard_parse_std'] = std_df['n_hard_parse']
        rigorous_analysis_df['db_time_avg'] = mean_df['db_time']
        rigorous_analysis_df['db_time_std'] = std_df['db_time']
        rigorous_analysis_df['cpu_time_avg'] = mean_df['cpu_time']
        rigorous_analysis_df['cpu_time_std'] = std_df['cpu_time']
        rigorous_analysis_df['execution_time_avg'] = mean_df['execution_time']
        rigorous_analysis_df['execution_time_std'] = std_df['execution_time']
        rigorous_analysis_df['parse_time_avg'] = mean_df['parse_time']
        rigorous_analysis_df['parse_time_std'] = std_df['parse_time']
        rigorous_analysis_df['plan_time_avg'] = mean_df['plan_time']
        rigorous_analysis_df['plan_time_std'] = std_df['plan_time']
        rigorous_analysis_df['rewrite_time_avg'] = mean_df['rewrite_time']
        rigorous_analysis_df['rewrite_time_std'] = std_df['rewrite_time']
        rigorous_analysis_df['pl_execution_time_avg'] = mean_df['pl_execution_time']
        rigorous_analysis_df['pl_execution_time_std'] = std_df['pl_execution_time']
        rigorous_analysis_df['pl_compilation_time_avg'] = mean_df['pl_compilation_time']
        rigorous_analysis_df['pl_compilation_time_std'] = std_df['pl_compilation_time']
        rigorous_analysis_df['snap_net_send_info_time_avg'] = mean_df['snap_net_send_info_time']
        rigorous_analysis_df['snap_net_send_info_time_std'] = std_df['snap_net_send_info_time']
        rigorous_analysis_df['snap_net_send_info_n_calls_avg'] = mean_df['snap_net_send_info_n_calls']
        rigorous_analysis_df['snap_net_send_info_n_calls_std'] = std_df['snap_net_send_info_n_calls']
        rigorous_analysis_df['snap_net_send_info_size_avg'] = mean_df['snap_net_send_info_size']
        rigorous_analysis_df['snap_net_send_info_size_std'] = std_df['snap_net_send_info_size']
        rigorous_analysis_df['snap_net_recv_info_time_avg'] = mean_df['snap_net_recv_info_time']
        rigorous_analysis_df['snap_net_recv_info_time_std'] = std_df['snap_net_recv_info_time']
        rigorous_analysis_df['snap_net_recv_info_n_calls_avg'] = mean_df['snap_net_recv_info_n_calls']
        rigorous_analysis_df['snap_net_recv_info_n_calls_std'] = std_df['snap_net_recv_info_n_calls']
        rigorous_analysis_df['snap_net_recv_info_size_avg'] = mean_df['snap_net_recv_info_size']
        rigorous_analysis_df['snap_net_recv_info_size_std'] = std_df['snap_net_recv_info_size']
        rigorous_analysis_df['snap_net_stream_send_info_time_avg'] = mean_df['snap_net_stream_send_info_time']
        rigorous_analysis_df['snap_net_stream_send_info_time_std'] = std_df['snap_net_stream_send_info_time']
        rigorous_analysis_df['snap_net_stream_send_info_n_calls_avg'] = mean_df['snap_net_stream_send_info_n_calls']
        rigorous_analysis_df['snap_net_stream_send_info_n_calls_std'] = std_df['snap_net_stream_send_info_n_calls']
        rigorous_analysis_df['snap_net_stream_send_info_size_avg'] = mean_df['snap_net_stream_send_info_size']
        rigorous_analysis_df['snap_net_stream_send_info_size_std'] = std_df['snap_net_stream_send_info_size']
        rigorous_analysis_df['snap_net_stream_recv_info_time_avg'] = mean_df['snap_net_stream_recv_info_time']
        rigorous_analysis_df['snap_net_stream_recv_info_time_std'] = std_df['snap_net_stream_recv_info_time']
        rigorous_analysis_df['snap_net_stream_recv_info_info_n_calls_avg'] = mean_df[
            'snap_net_stream_recv_info_info_n_calls']
        rigorous_analysis_df['snap_net_stream_recv_info_info_n_calls_std'] = std_df[
            'snap_net_stream_recv_info_info_n_calls']
        rigorous_analysis_df['snap_net_stream_recv_info_size_avg'] = mean_df['snap_net_stream_recv_info_size']
        rigorous_analysis_df['snap_net_stream_recv_info_size_std'] = std_df['snap_net_stream_recv_info_size']
        rigorous_analysis_df['sort_count_avg'] = mean_df['sort_count']
        rigorous_analysis_df['sort_count_std'] = std_df['sort_count']
        rigorous_analysis_df['sort_time_avg'] = mean_df['sort_time']
        rigorous_analysis_df['sort_time_std'] = std_df['sort_time']
        rigorous_analysis_df['sort_mem_used_avg'] = mean_df['sort_mem_used']
        rigorous_analysis_df['sort_mem_used_std'] = std_df['sort_mem_used']
        rigorous_analysis_df['sort_spill_count_avg'] = mean_df['sort_spill_count']
        rigorous_analysis_df['sort_spill_count_std'] = std_df['sort_spill_count']
        rigorous_analysis_df['sort_spill_size_avg'] = mean_df['sort_spill_size']
        rigorous_analysis_df['sort_spill_size_std'] = std_df['sort_spill_size']
        rigorous_analysis_df['hash_count_avg'] = mean_df['hash_count']
        rigorous_analysis_df['hash_count_std'] = std_df['hash_count']
        rigorous_analysis_df['hash_time_avg'] = mean_df['hash_time']
        rigorous_analysis_df['hash_time_std'] = std_df['hash_time']
        rigorous_analysis_df['hash_mem_used_avg'] = mean_df['hash_mem_used']
        rigorous_analysis_df['hash_mem_used_std'] = std_df['hash_mem_used']
        rigorous_analysis_df['hash_spill_count_avg'] = mean_df['hash_spill_count']
        rigorous_analysis_df['hash_spill_count_std'] = std_df['hash_spill_count']
        rigorous_analysis_df['hash_spill_size_avg'] = mean_df['hash_spill_size']
        rigorous_analysis_df['hash_spill_size_std'] = std_df['hash_spill_size']
        rigorous_analysis_df.fillna(0, inplace=True)
        rigorous_analysis_df['n_calls_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_calls'] - x['n_calls_avg']) > x['n_calls_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_calls_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_calls'] - x['n_calls_avg']) * 10000 if x['n_calls_std'] == 0 else abs(
                x['n_calls'] - x['n_calls_avg']) / x['n_calls_std'], axis=1)
        rigorous_analysis_df['total_elapse_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['total_elapse_time'] - x['total_elapse_time_avg']) > x[
                'total_elapse_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['total_elapse_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['total_elapse_time'] - x['total_elapse_time_avg']) * 10000 if x[
                                                                                              'total_elapse_time_std'] == 0 else abs(
                x['total_elapse_time'] - x['total_elapse_time_avg']) / x['total_elapse_time_std'], axis=1)
        rigorous_analysis_df['n_returned_rows_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_returned_rows'] - x['n_returned_rows_avg']) > x['n_returned_rows_std'] * 3 else 0,
            axis=1)
        rigorous_analysis_df['n_returned_rows_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_returned_rows'] - x['n_returned_rows_avg']) * 10000 if x[
                                                                                          'n_returned_rows_std'] == 0 else abs(
                x['n_returned_rows'] - x['n_returned_rows_avg']) / x['n_returned_rows_std'], axis=1)
        rigorous_analysis_df['n_tuples_fetched_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_tuples_fetched'] - x['n_tuples_fetched_avg']) > x[
                'n_tuples_fetched_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_tuples_fetched_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_tuples_fetched'] - x['n_tuples_fetched_avg']) * 10000 if x[
                                                                                            'n_tuples_fetched_std'] == 0 else abs(
                x['n_tuples_fetched'] - x['n_tuples_fetched_avg']) / x['n_tuples_fetched_std'], axis=1)
        rigorous_analysis_df['n_tuples_returned_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_tuples_returned'] - x['n_tuples_returned_avg']) > x[
                'n_tuples_returned_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_tuples_returned_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_tuples_returned'] - x['n_tuples_returned_avg']) * 10000 if x[
                                                                                              'n_tuples_returned_std'] == 0 else abs(
                x['n_tuples_returned'] - x['n_tuples_returned_avg']) / x['n_tuples_returned_std'], axis=1)
        rigorous_analysis_df['n_tuples_inserted_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_tuples_inserted'] - x['n_tuples_inserted_avg']) > x[
                'n_tuples_inserted_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_tuples_inserted_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_tuples_inserted'] - x['n_tuples_inserted_avg']) * 10000 if x[
                                                                                              'n_tuples_inserted_std'] == 0 else abs(
                x['n_tuples_inserted'] - x['n_tuples_inserted_avg']) / x['n_tuples_inserted_std'], axis=1)
        rigorous_analysis_df['n_tuples_updated_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_tuples_updated'] - x['n_tuples_updated_avg']) > x[
                'n_tuples_updated_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_tuples_updated_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_tuples_updated'] - x['n_tuples_updated_avg']) * 10000 if x[
                                                                                            'n_tuples_updated_std'] == 0 else abs(
                x['n_tuples_updated'] - x['n_tuples_updated_avg']) / x['n_tuples_updated_std'], axis=1)
        rigorous_analysis_df['n_tuples_deleted_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_tuples_deleted'] - x['n_tuples_deleted_avg']) > x[
                'n_tuples_deleted_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_tuples_deleted_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_tuples_deleted'] - x['n_tuples_deleted_avg']) * 10000 if x[
                                                                                            'n_tuples_deleted_std'] == 0 else abs(
                x['n_tuples_deleted'] - x['n_tuples_deleted_avg']) / x['n_tuples_deleted_std'], axis=1)
        rigorous_analysis_df['n_blocks_fetched_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_blocks_fetched'] - x['n_blocks_fetched_avg']) > x[
                'n_blocks_fetched_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_blocks_fetched_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_blocks_fetched'] - x['n_blocks_fetched_avg']) * 10000 if x[
                                                                                            'n_blocks_fetched_std'] == 0 else abs(
                x['n_blocks_fetched'] - x['n_blocks_fetched_avg']) / x['n_blocks_fetched_std'], axis=1)
        rigorous_analysis_df['n_blocks_hit_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_blocks_hit'] - x['n_blocks_hit_avg']) > x['n_blocks_hit_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_blocks_hit_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_blocks_hit'] - x['n_blocks_hit_avg']) * 10000 if x['n_blocks_hit_std'] == 0 else abs(
                x['n_blocks_hit'] - x['n_blocks_hit_avg']) / x['n_blocks_hit_std'], axis=1)
        rigorous_analysis_df['n_soft_parse_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_soft_parse'] - x['n_soft_parse_avg']) > x['n_soft_parse_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_soft_parse_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_soft_parse'] - x['n_soft_parse_avg']) * 10000 if x['n_soft_parse_std'] == 0 else abs(
                x['n_soft_parse'] - x['n_soft_parse_avg']) / x['n_soft_parse_std'], axis=1)
        rigorous_analysis_df['n_hard_parse_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['n_hard_parse'] - x['n_hard_parse_avg']) > x['n_hard_parse_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['n_hard_parse_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['n_hard_parse'] - x['n_hard_parse_avg']) * 10000 if x['n_hard_parse_std'] == 0 else abs(
                x['n_hard_parse'] - x['n_hard_parse_avg']) / x['n_hard_parse_std'], axis=1)
        rigorous_analysis_df['db_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['db_time'] - x['db_time_avg']) > x['db_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['db_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['db_time'] - x['db_time_avg']) * 10000 if x['db_time_std'] == 0 else abs(
                x['db_time'] - x['db_time_avg']) / x['db_time_std'], axis=1)
        rigorous_analysis_df['cpu_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['cpu_time'] - x['cpu_time_avg']) > x['cpu_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['cpu_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['cpu_time'] - x['cpu_time_avg']) * 10000 if x['cpu_time_std'] == 0 else abs(
                x['cpu_time'] - x['cpu_time_avg']) / x['cpu_time_std'], axis=1)
        rigorous_analysis_df['execution_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['execution_time'] - x['execution_time_avg']) > x['execution_time_std'] * 3 else 0,
            axis=1)
        rigorous_analysis_df['execution_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['execution_time'] - x['execution_time_avg']) * 10000 if x[
                                                                                        'execution_time_std'] == 0 else abs(
                x['execution_time'] - x['execution_time_avg']) / x['execution_time_std'], axis=1)
        rigorous_analysis_df['parse_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['parse_time'] - x['parse_time_avg']) > x['parse_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['parse_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['parse_time'] - x['parse_time_avg']) * 10000 if x['parse_time_std'] == 0 else abs(
                x['parse_time'] - x['parse_time_avg']) / x['parse_time_std'], axis=1)
        rigorous_analysis_df['plan_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['plan_time'] - x['plan_time_avg']) > x['plan_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['plan_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['plan_time'] - x['plan_time_avg']) * 10000 if x['plan_time_std'] == 0 else abs(
                x['plan_time'] - x['plan_time_avg']) / x['plan_time_std'], axis=1)
        rigorous_analysis_df['rewrite_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['rewrite_time'] - x['rewrite_time_avg']) > x['rewrite_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['rewrite_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['rewrite_time'] - x['rewrite_time_avg']) * 10000 if x['rewrite_time_std'] == 0 else abs(
                x['rewrite_time'] - x['rewrite_time_avg']) / x['rewrite_time_std'], axis=1)
        rigorous_analysis_df['pl_execution_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['pl_execution_time'] - x['pl_execution_time_avg']) > x[
                'pl_execution_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['pl_execution_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['pl_execution_time'] - x['pl_execution_time_avg']) * 10000 if x[
                                                                                              'pl_execution_time_std'] == 0 else abs(
                x['pl_execution_time_time'] - x['pl_execution_time_avg']) / x['pl_execution_time_std'], axis=1)
        rigorous_analysis_df['pl_compilation_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['pl_compilation_time'] - x['pl_compilation_time_avg']) > x[
                'pl_compilation_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['pl_compilation_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['pl_compilation_time'] - x['pl_compilation_time_avg']) * 10000 if x[
                                                                                                  'pl_compilation_time_std'] == 0 else abs(
                x['pl_compilation_time'] - x['pl_compilation_time_avg']) / x['pl_compilation_time_std'], axis=1)
        rigorous_analysis_df['snap_net_send_info_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['pl_compilation_time'] - x['pl_compilation_time_avg']) > x[
                'pl_compilation_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_send_info_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['pl_compilation_time'] - x['pl_compilation_time_avg']) * 10000 if x[
                                                                                                  'pl_compilation_time_std'] == 0 else abs(
                x['pl_compilation_time'] - x['pl_compilation_time_avg']) / x['pl_compilation_time_std'], axis=1)
        rigorous_analysis_df['snap_net_send_info_n_calls_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_send_info_n_calls'] - x['snap_net_send_info_n_calls_avg']) > x[
                'snap_net_send_info_n_calls_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_send_info_n_calls_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_send_info_n_calls'] - x['snap_net_send_info_n_calls_avg']) * 10000 if x[
                                                                                                                'snap_net_send_info_n_calls_std'] == 0 else abs(
                x['snap_net_send_info_n_calls'] - x['snap_net_send_info_n_calls_avg']) / x[
                                                                                                                                                                'snap_net_send_info_n_calls_std'],
            axis=1)
        rigorous_analysis_df['snap_net_send_info_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_send_info_size'] - x['snap_net_send_info_size_avg']) > x[
                'snap_net_send_info_size_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_send_info_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_send_info_size'] - x['snap_net_send_info_size_avg']) * 10000 if x[
                                                                                                          'snap_net_send_info_size_std'] == 0 else abs(
                x['snap_net_send_info_size'] - x['snap_net_send_info_size_avg']) / x['snap_net_send_info_size_std'],
            axis=1)
        rigorous_analysis_df['snap_net_recv_info_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_recv_info_time'] - x['snap_net_recv_info_time_avg']) > x[
                'snap_net_recv_info_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_recv_info_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_recv_info_time'] - x['snap_net_recv_info_time_avg']) * 10000 if x[
                                                                                                          'snap_net_recv_info_time_std'] == 0 else abs(
                x['snap_net_recv_info_time'] - x['snap_net_recv_info_time_avg']) / x['snap_net_recv_info_time_std'],
            axis=1)
        rigorous_analysis_df['snap_net_recv_info_n_calls_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_recv_info_n_calls'] - x['snap_net_recv_info_n_calls_avg']) > x[
                'snap_net_recv_info_n_calls_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_recv_info_n_calls_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_recv_info_n_calls'] - x['snap_net_recv_info_n_calls_avg']) * 10000 if x[
                                                                                                                'snap_net_recv_info_n_calls_std'] == 0 else abs(
                x['snap_net_recv_info_n_calls'] - x['snap_net_recv_info_n_calls_avg']) / x[
                                                                                                                                                                'snap_net_recv_info_n_calls_std'],
            axis=1)
        rigorous_analysis_df['snap_net_recv_info_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_recv_info_size'] - x['snap_net_recv_info_size_avg']) > x[
                'snap_net_recv_info_size_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_recv_info_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_recv_info_size'] - x['snap_net_recv_info_size_avg']) * 10000 if x[
                                                                                                          'snap_net_recv_info_size_std'] == 0 else abs(
                x['snap_net_recv_info_size'] - x['snap_net_recv_info_size_avg']) / x['snap_net_recv_info_size_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_stream_send_info_time'] - x['snap_net_stream_send_info_time_avg']) > x[
                'snap_net_stream_send_info_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_stream_send_info_time'] - x['snap_net_stream_send_info_time_avg']) * 10000 if x[
                                                                                                                        'snap_net_stream_send_info_time_std'] == 0 else abs(
                x['snap_net_stream_send_info_time'] - x['snap_net_stream_send_info_time_avg']) / x[
                                                                                                                                                                            'snap_net_stream_send_info_time_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_n_calls_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_stream_send_info_n_calls'] - x['snap_net_stream_send_info_n_calls_avg']) > x[
                'snap_net_stream_send_info_n_calls_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_n_calls_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(
                x['snap_net_stream_send_info_n_calls'] - x['snap_net_stream_send_info_n_calls_avg']) * 10000 if x[
                                                                                                                    'snap_net_stream_send_info_n_calls_std'] == 0 else abs(
                x['snap_net_stream_send_info_n_calls'] - x['snap_net_stream_send_info_n_calls_avg']) / x[
                                                                                                                                                                           'snap_net_stream_send_info_n_calls_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_stream_send_info_size'] - x['snap_net_stream_send_info_size_avg']) > x[
                'snap_net_stream_send_info_size_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_send_info_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_stream_send_info_size'] - x['snap_net_stream_send_info_size_avg']) * 10000 if x[
                                                                                                                        'snap_net_stream_send_info_size_std'] == 0 else abs(
                x['snap_net_stream_send_info_size'] - x['snap_net_stream_send_info_size_avg']) / x[
                                                                                                                                                                            'snap_net_stream_send_info_size_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_stream_recv_info_time'] - x['snap_net_stream_recv_info_time_avg']) > x[
                'snap_net_stream_recv_info_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_stream_recv_info_time'] - x['snap_net_stream_recv_info_time_avg']) * 10000 if x[
                                                                                                                        'snap_net_stream_recv_info_time_std'] == 0 else abs(
                x['snap_net_stream_recv_info_time'] - x['snap_net_stream_recv_info_time_avg']) / x[
                                                                                                                                                                            'snap_net_stream_recv_info_time_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_info_n_calls_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(
                x['snap_net_stream_recv_info_info_n_calls'] - x['snap_net_stream_recv_info_info_n_calls_avg']) > x[
                               'snap_net_stream_recv_info_info_n_calls_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_info_n_calls_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_stream_recv_info_info_n_calls'] - x[
                'snap_net_stream_recv_info_info_n_calls_avg']) * 10000 if x[
                                                                              'snap_net_stream_recv_info_info_n_calls_std'] == 0 else abs(
                x['snap_net_stream_recv_info_info_n_calls'] - x['snap_net_stream_recv_info_info_n_calls_avg']) / x[
                                                                                                                                          'snap_net_stream_recv_info_info_n_calls_std'],
            axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['snap_net_stream_recv_info_size'] - x['snap_net_stream_recv_info_size_avg']) > x[
                'snap_net_stream_recv_info_size_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['snap_net_stream_recv_info_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['snap_net_stream_recv_info_size'] - x['snap_net_stream_recv_info_size_avg']) * 10000 if x[
                                                                                                                        'snap_net_stream_recv_info_size_std'] == 0 else abs(
                x['snap_net_stream_recv_info_size'] - x['snap_net_stream_recv_info_size_avg']) / x[
                                                                                                                                                                            'snap_net_stream_recv_info_size_std'],
            axis=1)
        rigorous_analysis_df['sort_count_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['sort_count'] - x['sort_count_avg']) > x['sort_count_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['sort_count_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['sort_count'] - x['sort_count_avg']) * 10000 if x['sort_count_std'] == 0 else abs(
                x['sort_count'] - x['sort_count_avg']) / x['sort_count_std'], axis=1)
        rigorous_analysis_df['sort_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['sort_time'] - x['sort_time_avg']) > x['sort_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['sort_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['sort_time'] - x['sort_time_avg']) * 10000 if x['sort_time_std'] == 0 else abs(
                x['sort_time'] - x['sort_time_avg']) / x['sort_time_std'], axis=1)
        rigorous_analysis_df['sort_mem_used_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['sort_mem_used'] - x['sort_mem_used_avg']) > x['sort_mem_used_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['sort_mem_used_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['sort_mem_used'] - x['sort_mem_used_avg']) * 10000 if x['sort_mem_used_std'] == 0 else abs(
                x['sort_mem_used'] - x['sort_mem_used_avg']) / x['sort_mem_used_std'], axis=1)
        rigorous_analysis_df['sort_spill_count_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['sort_mem_used'] - x['sort_mem_used_avg']) > x['sort_mem_used_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['sort_spill_count_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['sort_mem_used'] - x['sort_mem_used_avg']) * 10000 if x['sort_mem_used_std'] == 0 else abs(
                x['sort_mem_used'] - x['sort_mem_used_avg']) / x['sort_mem_used_std'], axis=1)
        rigorous_analysis_df['sort_spill_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['sort_spill_size'] - x['sort_spill_size_avg']) > x['sort_spill_size_std'] * 3 else 0,
            axis=1)
        rigorous_analysis_df['sort_spill_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['sort_spill_size'] - x['sort_spill_size_avg']) * 10000 if x[
                                                                                          'sort_spill_size_std'] == 0 else abs(
                x['sort_spill_size'] - x['sort_spill_size_avg']) / x['sort_spill_size_std'], axis=1)
        rigorous_analysis_df['hash_count_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['hash_count'] - x['hash_count_avg']) > x['hash_count_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['hash_count_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['hash_count'] - x['hash_count_avg']) * 10000 if x['hash_count_std'] == 0 else abs(
                x['hash_count'] - x['hash_count_avg']) / x['hash_count_std'], axis=1)
        rigorous_analysis_df['hash_time_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['hash_time'] - x['hash_time_avg']) > x['hash_time_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['hash_time_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['hash_time'] - x['hash_time_avg']) * 10000 if x['hash_time_std'] == 0 else abs(
                x['hash_time'] - x['hash_time_avg']) / x['hash_time_std'], axis=1)
        rigorous_analysis_df['hash_mem_used_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['hash_mem_used'] - x['hash_mem_used_avg']) > x['hash_mem_used_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['hash_mem_used_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['hash_mem_used'] - x['hash_mem_used_avg']) * 10000 if x['hash_mem_used_std'] == 0 else abs(
                x['hash_mem_used'] - x['hash_mem_used_avg']) / x['hash_mem_used_std'], axis=1)
        rigorous_analysis_df['hash_spill_count_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['hash_spill_count'] - x['hash_spill_count_avg']) > x[
                'hash_spill_count_std'] * 3 else 0, axis=1)
        rigorous_analysis_df['hash_spill_count_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['hash_spill_count'] - x['hash_spill_count_avg']) * 10000 if x[
                                                                                            'hash_spill_count_std'] == 0 else abs(
                x['hash_spill_count'] - x['hash_spill_count_avg']) / x['hash_spill_count_std'], axis=1)
        rigorous_analysis_df['hash_spill_size_pre'] = rigorous_analysis_df.apply(
            lambda x: 1 if abs(x['hash_spill_size'] - x['hash_spill_size_avg']) > x['hash_spill_size_std'] * 3 else 0,
            axis=1)
        rigorous_analysis_df['hash_spill_size_rate'] = rigorous_analysis_df.apply(
            lambda x: abs(x['hash_spill_size'] - x['hash_spill_size_avg']) * 10000 if x[
                                                                                          'hash_spill_size_std'] == 0 else abs(
                x['hash_spill_size'] - x['hash_spill_size_avg']) / x['hash_spill_size_std'], axis=1)
        inspection_list = []
        for target in ['n_calls', 'total_elapse_time', 'n_returned_rows', 'n_tuples_fetched', 'n_tuples_returned',
                       'n_tuples_inserted', 'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                       'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time', 'execution_time', 'parse_time',
                       'plan_time', 'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
                       'snap_net_send_info_time', 'snap_net_send_info_n_calls', 'snap_net_send_info_size',
                       'snap_net_recv_info_time', 'snap_net_recv_info_n_calls', 'snap_net_recv_info_size',
                       'snap_net_stream_send_info_time', 'snap_net_stream_send_info_n_calls',
                       'snap_net_stream_send_info_size', 'snap_net_stream_recv_info_time',
                       'snap_net_stream_recv_info_info_n_calls', 'snap_net_stream_recv_info_size', 'sort_count',
                       'sort_time', 'sort_mem_used', 'sort_spill_count', 'sort_spill_size', 'hash_count', 'hash_time',
                       'hash_mem_used', 'hash_spill_count', 'hash_spill_size']:
            inspection_list.append(target + "_pre")
        inspection_data = rigorous_analysis_df[inspection_list]
        data_t = inspection_data.T
        valid_data = data_t.loc[(data_t != 0).any(axis=1)].T
        res_valid_data = valid_data.to_dict(orient='records')
        query_info = []
        rate_info = []
        for i in range(len(res_valid_data)):
            for k in res_valid_data[i].keys():
                query_info.append(k.split('_pre')[0])
                rate_info.append(k.split('_pre')[0] + "_rate")
        query_data = rigorous_analysis_df[query_info].to_dict(orient='records')
        rate_data = rigorous_analysis_df[rate_info].to_dict(orient='records')
        snapshot_date = {"snapshot_id": rigorous_analysis_df.loc[0]['snapshot_id'],
                         "snap_unique_sql_id": rigorous_analysis_df.loc[0]['snap_unique_sql_id']}
        if rate_data and query_data:
            res_data = {**query_data[0], **rate_data[0]}
            return snapshot_date, res_data
        else:
            return snapshot_date, {}
