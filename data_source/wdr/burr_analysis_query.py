#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
import json
import pandas as pd
import warnings

warnings.filterwarnings('ignore')


class BurrAnalysisMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization sql_burr_detection data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            burr_analysis_df = data
        else:
            if data == []:
                burr_analysis_df = "The query data is empty!"
            else:
                burr_analysis_df = pd.DataFrame(data,
                                                columns=['snapshot_id', 'snap_user_name', 'snap_unique_sql_id',
                                                         'snap_node_name', 'snap_node_id', 'snap_user_id', 'snap_query',
                                                         'n_calls', 'total_elapse_time', 'n_returned_rows',
                                                         'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted',
                                                         'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched',
                                                         'n_blocks_hit', 'n_soft_parse', 'n_hard_parse', 'db_time',
                                                         'cpu_time', 'execution_time', 'parse_time', 'plan_time',
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
                                                         'sort_count', 'sort_time', 'sort_mem_used', 'sort_spill_count',
                                                         'sort_spill_size', 'hash_count', 'hash_time', 'hash_mem_used',
                                                         'hash_spill_count', 'hash_spill_size'])
        self.de_part = burr_analysis_df

    @staticmethod
    def intergrated_data(burr_analysis_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param burr_analysis_df: refresh data this time
        :return: return the computer result
        """

        data_info = []
        burr_analysis_df['avg_execution_time'] = burr_analysis_df['db_time'] / burr_analysis_df['n_calls']
        for p, i in burr_analysis_df.groupby(['snap_user_name', 'snap_unique_sql_id']):
            i['max_execution_time'] = i['db_time'].max()
            i['std_avg_execution_time'] = i['avg_execution_time'].std()
            i['sum_n_calls'] = i['n_calls'].sum()
            i['max_avg_execution_time'] = i['avg_execution_time'].max()
            i['readline'] = i['avg_execution_time'].mean() * 3 + i['std_avg_execution_time'] * 3
            i.fillna(0, inplace=True)
            i = i[(i['sum_n_calls'] > 1000000) & (i['max_avg_execution_time'] > 100000) & (
                    i['max_avg_execution_time'] > i['readline'])]
            i = i[i['max_avg_execution_time'] == i['avg_execution_time']]
            i = i.astype(str)
            data_list = i.to_json(orient='records')
            data = (data_list[1:-1]).replace("},", "}!")
            data.split("!")
            data = json.loads(data_list)
            for i in data:
                data_info.append(i)
        if data_info:
            df = pd.DataFrame(data_info)
            df[['max_execution_time', 'sum_n_calls', 'readline']] = df[
                ['max_execution_time', 'sum_n_calls', 'readline']].astype(float)
            res = df.to_json(orient='records')
            res_depart = (res[1:-1]).replace("},", "}!")
            res_depart.split("!")
            res_depart = json.loads(res)
        else:
            res_depart = {}
        return res_depart
