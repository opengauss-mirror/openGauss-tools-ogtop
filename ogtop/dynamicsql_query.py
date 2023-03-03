# -*- coding: utf-8 -*-
""" dynamicsql data storage calculation """
import json
import pandas as pd


class DynamicsqlMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization dynamicsql data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        try:
            dynamicsql_df = pd.DataFrame(data, columns=['target_ip', 'snaptime', 'user_name', 'unique_sql_id', 'query',
                                                        'n_calls', 'total_elapse_time', 'n_returned_rows',
                                                        'n_tuples_fetched',
                                                        'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated',
                                                        'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                                        'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
                                                        'execution_time', 'parse_time', 'plan_time', 'rewrite_time',
                                                        'pl_execution_time', 'pl_compilation_time', 'data_io_time',
                                                        'net_send_info', 'net_recv_info', 'net_stream_send_info',
                                                        'net_stream_recv_info', 'sort_count', 'sort_time',
                                                        'sort_mem_used',
                                                        'sort_spill_count', 'sort_spill_size', 'hash_count',
                                                        'hash_time', 'hash_mem_used', 'hash_spill_count',
                                                        'hash_spill_size'])
        except Exception  as e:
            dynamicsql_df = data
        self.de_part = dynamicsql_df

    @staticmethod
    def intergrated_data(dynamicsql_data):
        """
        consolidate the data and convert it to json format
        :param dynamicsql_data: data to be converted
        :return:
        """
        data_list = dynamicsql_data.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data_dynamicsql = json.loads(data_list)

        return data_dynamicsql

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        new_dynamicsql = pd.DataFrame(depart)
        new_dynamicsql.columns = ['target_ip', 'snaptime', 'user_name', 'unique_sql_id', 'query',
                                  'n_calls1', 'total_elapse_time', 'n_returned_rows', 'n_tuples_fetched',
                                  'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated',
                                  'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                  'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
                                  'execution_time', 'parse_time', 'plan_time', 'rewrite_time',
                                  'pl_execution_time', 'pl_compilation_time', 'data_io_time',
                                  'net_send_info', 'net_recv_info', 'net_stream_send_info',
                                  'net_stream_recv_info', 'sort_count', 'sort_time', 'sort_mem_used',
                                  'sort_spill_count', 'sort_spill_size', 'hash_count',
                                  'hash_time', 'hash_mem_used', 'hash_spill_count', 'hash_spill_size']
        old_dynamicsql = pd.DataFrame(old_depart)
        old_dynamicsql.columns = ['target_ip', 'snaptime', 'user_name', 'unique_sql_id', 'query',
                                  'n_calls2', 'total_elapse_time', 'n_returned_rows', 'n_tuples_fetched',
                                  'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated',
                                  'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                  'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
                                  'execution_time', 'parse_time', 'plan_time', 'rewrite_time',
                                  'pl_execution_time', 'pl_compilation_time', 'data_io_time',
                                  'net_send_info', 'net_recv_info', 'net_stream_send_info',
                                  'net_stream_recv_info', 'sort_count', 'sort_time', 'sort_mem_used',
                                  'sort_spill_count', 'sort_spill_size', 'hash_count',
                                  'hash_time', 'hash_mem_used', 'hash_spill_count', 'hash_spill_size']
        old_dynamicsql = old_dynamicsql[['unique_sql_id', 'n_calls2']]

        dynamicsql = pd.merge(new_dynamicsql, old_dynamicsql, on=['unique_sql_id'], how='inner')
        field_values = {'db_time': 0, 'n_calls1': 0, 'n_calls2': 0}
        dynamicsql.fillna(value=field_values, inplace=True)
        dynamicsql['n_calls'] = dynamicsql['n_calls1'] - dynamicsql['n_calls2']
        dynamicsql_date = dynamicsql[['target_ip', 'snaptime', 'user_name', 'unique_sql_id', 'query',
                                      'n_calls', 'total_elapse_time', 'n_returned_rows', 'n_tuples_fetched',
                                      'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated',
                                      'n_tuples_deleted', 'n_blocks_fetched', 'n_blocks_hit',
                                      'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
                                      'execution_time', 'parse_time', 'plan_time', 'rewrite_time',
                                      'pl_execution_time', 'pl_compilation_time', 'data_io_time',
                                      'net_send_info', 'net_recv_info', 'net_stream_send_info',
                                      'net_stream_recv_info', 'sort_count', 'sort_time', 'sort_mem_used',
                                      'sort_spill_count', 'sort_spill_size', 'hash_count',
                                      'hash_time', 'hash_mem_used', 'hash_spill_count',
                                      'hash_spill_size']].reset_index()
        dynamicsql_date.pop('index')
        res_dynamicsql_date = dynamicsql_date[dynamicsql_date['n_calls'] > 0]
        res_dynamicsql_date['avg_db_time'] = round(round(res_dynamicsql_date['db_time'] / res_dynamicsql_date['n_calls'], 3) / 1000000, 4)
        res_dynamicsql_date['data_io_time'] = round(res_dynamicsql_date["data_io_time"] / 1000000, 4)
        res_dynamicsql_date['db_time'] = round(res_dynamicsql_date["db_time"] / 1000000, 4)
        res_dynamicsql_date['cpu_time'] = round(res_dynamicsql_date["cpu_time"] / 1000000, 4)
        res_dynamicsql_date['execution_time'] = round(res_dynamicsql_date["execution_time"] / 1000000, 4)
        res_dynamicsql_date['parse_time'] = round(res_dynamicsql_date["parse_time"] / 1000000, 4)
        res_dynamicsql_date['plan_time'] = round(res_dynamicsql_date["plan_time"] / 1000000, 4)
        res_dynamicsql_date['total_elapse_time'] = round(res_dynamicsql_date["total_elapse_time"] / 1000000, 4)
        res_dynamicsql_date['rewrite_time'] = round(res_dynamicsql_date["rewrite_time"] / 1000000, 4)
        res_dynamicsql_date['pl_execution_time'] = round(res_dynamicsql_date["pl_execution_time"] / 1000000, 4)
        res_dynamicsql_date['pl_compilation_time'] = round(res_dynamicsql_date["pl_compilation_time"] / 1000000, 4)
        res_dynamicsql_date['sort_time'] = round(res_dynamicsql_date["sort_time"] / 1000000, 4)
        res_dynamicsql_date['hash_time'] = round(res_dynamicsql_date["hash_time"] / 1000000, 4)

        res_dynamicsql_date.replace(to_replace='None', value='-', inplace=True)
        res_dynamicsql_date.fillna('-', inplace=True)
        header_data = str(res_dynamicsql_date.shape[0])
        date = res_dynamicsql_date.to_json(orient='records')
        depart_depart = (date[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(date)

        return depart_depart, {"data_length": header_data}
