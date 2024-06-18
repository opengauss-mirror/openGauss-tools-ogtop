# -*- coding: utf-8 -*-
""" dynamicsql data storage calculation """
import json
import pandas as pd
import copy


class DynamicsqlMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization dynamicsql data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            dynamicsql_df = data
        else:
            if data == []:
                dynamicsql_df = "The query data is empty!"
            else:
                dynamicsql_df = pd.DataFrame(data, columns=['user_name', 'unique_sql_id', 'query',
                                                            'n_calls', 'total_elapse_time', 'n_returned_rows',
                                                            'n_tuples_fetched', 'n_tuples_returned',
                                                            'n_tuples_inserted',
                                                            'n_tuples_updated', 'n_tuples_deleted', 'n_blocks_fetched',
                                                            'n_blocks_hit', 'n_soft_parse', 'n_hard_parse', 'db_time',
                                                            'cpu_time', 'execution_time', 'parse_time', 'plan_time',
                                                            'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
                                                            'data_io_time', 'net_send_info', 'net_recv_info',
                                                            'net_stream_send_info', 'net_stream_recv_info',
                                                            'sort_count',
                                                            'sort_time', 'sort_mem_used', 'sort_spill_count',
                                                            'sort_spill_size', 'hash_count', 'hash_time',
                                                            'hash_mem_used',
                                                            'hash_spill_count', 'hash_spill_size'])
        self.de_part = dynamicsql_df

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        dynamicsql_info = pd.merge(depart, old_depart, on=['unique_sql_id', 'user_name'], how='left')
        dynamicsql_info.fillna(value=0, inplace=True)
        dynamicsql_info['n_calls'] = dynamicsql_info['n_calls_x'] - dynamicsql_info['n_calls_y']
        dynamicsql = dynamicsql_info.loc[dynamicsql_info['n_calls'] != 0]
        dynamicsql = dynamicsql.reset_index()
        dynamicsql.pop('index')
        dynamicsql['total_elapse_time'] = dynamicsql['total_elapse_time_x'] - dynamicsql['total_elapse_time_y']
        dynamicsql['n_returned_rows'] = dynamicsql['n_returned_rows_x'] - dynamicsql['n_returned_rows_y']
        dynamicsql['n_tuples_fetched'] = dynamicsql['n_tuples_fetched_x'] - dynamicsql['n_tuples_fetched_y']
        dynamicsql['n_tuples_returned'] = dynamicsql['n_tuples_returned_x'] - dynamicsql['n_tuples_returned_y']
        dynamicsql['n_tuples_inserted'] = dynamicsql['n_tuples_inserted_x'] - dynamicsql['n_tuples_inserted_y']
        dynamicsql['n_tuples_updated'] = dynamicsql['n_tuples_updated_x'] - dynamicsql['n_tuples_updated_y']
        dynamicsql['n_tuples_deleted'] = dynamicsql['n_tuples_deleted_x'] - dynamicsql['n_tuples_deleted_y']
        dynamicsql['n_blocks_fetched'] = dynamicsql['n_blocks_fetched_x'] - dynamicsql['n_blocks_fetched_y']
        dynamicsql['n_blocks_hit'] = dynamicsql['n_blocks_hit_x'] - dynamicsql['n_blocks_hit_y']
        dynamicsql['n_soft_parse'] = dynamicsql['n_soft_parse_x'] - dynamicsql['n_soft_parse_y']
        dynamicsql['n_hard_parse'] = dynamicsql['n_hard_parse_x'] - dynamicsql['n_hard_parse_y']
        dynamicsql['db_time'] = dynamicsql['db_time_x'] - dynamicsql['db_time_y']
        dynamicsql['cpu_time'] = dynamicsql['cpu_time_x'] - dynamicsql['cpu_time_y']
        dynamicsql['execution_time'] = dynamicsql['execution_time_x'] - dynamicsql['execution_time_y']
        dynamicsql['parse_time'] = dynamicsql['parse_time_x'] - dynamicsql['parse_time_y']
        dynamicsql['plan_time'] = dynamicsql['plan_time_x'] - dynamicsql['plan_time_y']
        dynamicsql['rewrite_time'] = dynamicsql['rewrite_time_x'] - dynamicsql['rewrite_time_y']
        dynamicsql['pl_execution_time'] = dynamicsql['pl_execution_time_x'] - dynamicsql['pl_execution_time_y']
        dynamicsql['pl_compilation_time'] = dynamicsql['pl_compilation_time_x'] - dynamicsql['pl_compilation_time_y']
        dynamicsql['data_io_time'] = dynamicsql['data_io_time_x'] - dynamicsql['data_io_time_y']
        dynamicsql['sort_count'] = dynamicsql['sort_count_x'] - dynamicsql['sort_count_y']
        dynamicsql['sort_time'] = dynamicsql['sort_time_x'] - dynamicsql['sort_time_y']
        dynamicsql['sort_mem_used'] = dynamicsql['sort_mem_used_x'] - dynamicsql['sort_mem_used_y']
        dynamicsql['sort_spill_count'] = dynamicsql['sort_spill_count_x'] - dynamicsql['sort_spill_count_y']
        dynamicsql['sort_spill_size'] = dynamicsql['sort_spill_size_x'] - dynamicsql['sort_spill_size_y']
        dynamicsql['hash_count'] = dynamicsql['hash_count_x'] - dynamicsql['hash_count_y']
        dynamicsql['hash_time'] = dynamicsql['hash_time_x'] - dynamicsql['hash_time_y']
        dynamicsql['hash_mem_used'] = dynamicsql['hash_mem_used_x'] - dynamicsql['hash_mem_used_y']
        dynamicsql['hash_spill_count'] = dynamicsql['hash_spill_count_x'] - dynamicsql['hash_spill_count_y']
        dynamicsql['hash_spill_size'] = dynamicsql['hash_spill_size_x'] - dynamicsql['hash_spill_size_y']
        dynamicsql['net_send_info_time'] = round(((dynamicsql['net_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time')))) - (dynamicsql['net_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        dynamicsql['net_send_info_n_calls'] = (dynamicsql['net_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls')))) - (dynamicsql['net_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        dynamicsql['net_send_info_size'] = (dynamicsql['net_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size')))) - (dynamicsql['net_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        dynamicsql['net_recv_info_time'] = round(((dynamicsql['net_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time')))) - (dynamicsql['net_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        dynamicsql['net_recv_info_n_calls'] = (dynamicsql['net_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls')))) - (dynamicsql['net_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        dynamicsql['net_recv_info_size'] = (dynamicsql['net_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size')))) - (dynamicsql['net_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        dynamicsql['net_stream_send_info_time'] = round(((dynamicsql['net_stream_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time')))) - (dynamicsql['net_stream_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        dynamicsql['net_stream_send_info_n_calls'] = (dynamicsql['net_stream_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls')))) - (dynamicsql['net_stream_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        dynamicsql['net_stream_send_info_size'] = (dynamicsql['net_stream_send_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size')))) - (dynamicsql['net_stream_send_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size'))))
        dynamicsql['net_stream_recv_info_time'] = round(((dynamicsql['net_stream_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time')))) - (dynamicsql['net_stream_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('time'))))) / 1000000, 4)
        dynamicsql['net_stream_recv_info_n_calls'] = (dynamicsql['net_stream_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls')))) - (dynamicsql['net_stream_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('n_calls'))))
        dynamicsql['net_stream_recv_info_size'] = (dynamicsql['net_stream_recv_info_x'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size')))) - (dynamicsql['net_stream_recv_info_y'].apply(
            lambda x: 0 if x == 0 else float(eval(x).get('size'))))

        dynamicsql_date = dynamicsql[
            ['user_name', 'unique_sql_id', 'query_x', 'n_calls', 'total_elapse_time', 'n_returned_rows',
             'n_tuples_fetched', 'n_tuples_returned', 'n_tuples_inserted', 'n_tuples_updated', 'n_tuples_deleted',
             'n_blocks_fetched', 'n_blocks_hit', 'n_soft_parse', 'n_hard_parse', 'db_time', 'cpu_time',
             'execution_time', 'parse_time', 'plan_time', 'rewrite_time', 'pl_execution_time', 'pl_compilation_time',
             'data_io_time', 'net_send_info_time', 'net_send_info_n_calls', 'net_send_info_size', 'net_recv_info_time',
             'net_recv_info_n_calls', 'net_recv_info_size', 'net_stream_send_info_time', 'net_stream_send_info_n_calls',
             'net_stream_send_info_size', 'net_stream_recv_info_time', 'net_stream_recv_info_n_calls',
             'net_stream_recv_info_size', 'sort_count', 'sort_time', 'sort_mem_used', 'sort_spill_count',
             'sort_spill_size', 'hash_count', 'hash_time', 'hash_mem_used', 'hash_spill_count',
             'hash_spill_size']].reset_index()

        dynamicsql_date.pop('index')
        res_dynamicsql_date = dynamicsql_date.rename(columns={'query_x': 'query'})
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

    @staticmethod
    def split_date(old_dynamicsql):
        old_dynamicsql_ob = copy.deepcopy(old_dynamicsql)
        dynamicsql = old_dynamicsql_ob.loc[old_dynamicsql_ob['n_calls'] != 0]
        dynamicsql = dynamicsql.reset_index()
        dynamicsql.pop('index')
        dynamicsql['total_elapse_time'] = 0
        dynamicsql['n_returned_rows'] = 0
        dynamicsql['n_tuples_fetched'] = 0
        dynamicsql['n_tuples_returned'] = 0
        dynamicsql['n_tuples_inserted'] = 0
        dynamicsql['n_tuples_updated'] = 0
        dynamicsql['n_tuples_deleted'] = 0
        dynamicsql['n_blocks_fetched'] = 0
        dynamicsql['n_blocks_hit'] = 0
        dynamicsql['n_soft_parse'] = 0
        dynamicsql['n_hard_parse'] = 0
        dynamicsql['db_time'] = 0
        dynamicsql['cpu_time'] = 0
        dynamicsql['execution_time'] = 0
        dynamicsql['parse_time'] = 0
        dynamicsql['plan_time'] = 0
        dynamicsql['rewrite_time'] = 0
        dynamicsql['pl_execution_time'] = 0
        dynamicsql['pl_compilation_time'] = 0
        dynamicsql['data_io_time'] = 0
        dynamicsql['sort_count'] = 0
        dynamicsql['sort_time'] = 0
        dynamicsql['sort_mem_used'] = 0
        dynamicsql['sort_spill_count'] = 0
        dynamicsql['sort_spill_size'] = 0
        dynamicsql['hash_count'] = 0
        dynamicsql['hash_time'] = 0
        dynamicsql['hash_mem_used'] = 0
        dynamicsql['hash_spill_count'] = 0
        dynamicsql['hash_spill_size'] = 0
        dynamicsql['net_send_info_time'] = 0
        dynamicsql['net_send_info_n_calls'] = 0
        dynamicsql['net_send_info_size'] = 0
        dynamicsql['net_recv_info_time'] = 0
        dynamicsql['net_recv_info_n_calls'] = 0
        dynamicsql['net_recv_info_size'] = 0
        dynamicsql['net_stream_send_info_time'] = 0
        dynamicsql['net_stream_send_info_n_calls'] = 0
        dynamicsql['net_stream_send_info_size'] = 0
        dynamicsql['net_stream_recv_info_time'] = 0
        dynamicsql['net_stream_recv_info_n_calls'] = 0
        dynamicsql['net_stream_recv_info_size'] = 0
        dynamicsql['avg_db_time'] = 0
        dynamicsql['data_io_time'] = 0
        dynamicsql['db_time'] = 0
        dynamicsql['cpu_time'] = 0
        dynamicsql['execution_time'] = 0
        dynamicsql['parse_time'] = 0
        dynamicsql['plan_time'] = 0
        dynamicsql['total_elapse_time'] = 0
        dynamicsql['rewrite_time'] = 0
        dynamicsql['pl_execution_time'] = 0
        dynamicsql['pl_compilation_time'] = 0
        dynamicsql['sort_time'] = 0
        dynamicsql['hash_time'] = 0
        dynamicsql.replace(to_replace='None', value='-', inplace=True)
        dynamicsql.fillna('-', inplace=True)
        header_data = str(dynamicsql.shape[0])
        date = dynamicsql.to_json(orient='records')
        depart_depart = (date[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(date)
        return depart_depart, {"data_length": header_data}


class IndexRecommendationMain(object):

    def __init__(self, OpenGaussOption, sqls):
        """
        initialization dynamicsql data
        :param OpenGaussOption: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sqls)
        index_recommendation_df = pd.DataFrame(data, columns=['query', 'n_calls'])
        self.de_part = index_recommendation_df
