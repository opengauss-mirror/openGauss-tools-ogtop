# -*- coding: utf-8 -*-
""" table_index data storage calculation """
import json
import pandas as pd
import copy


class TableDetailMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization table_index data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        data = OpenGaussOption.params_query_sql(sql, (params,))
        if isinstance(data, str):
            table_index_df = data
        else:
            if data == []:
                table_index_df = "The query data is empty!"
            else:
                table_index_df = pd.DataFrame(data)
                table_index_df.columns = ["datname", "snaptime", "schemaname", "relname", "indexrelid", "indexrelname",
                                          "idx_scan", "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds",
                                          "phywrts",
                                          "phyblkrd", "phyblkwrt", "readtim", "writetim"]
                table_index_df.replace(to_replace='None', value='-', inplace=True)
                table_index_df.fillna('-', inplace=True)

        self.de_part = table_index_df

    @staticmethod
    def intergrated_data(new_table_index_data, old_table_index_data):
        """
         calculate the increment，save the result to dictionary，transfer and display to the interface
        :param new_table_index_data: refresh data this time
        :param old_table_index_data: refresh data this time
        :return: return the computer result
        """
        dynamicsql_info = pd.merge(new_table_index_data, old_table_index_data, on=['indexrelid'], how='left')
        dynamicsql_info['idx_scan'] = dynamicsql_info['idx_scan_x'] - dynamicsql_info['idx_scan_y']
        dynamicsql_info['idx_tup_read'] = dynamicsql_info['idx_tup_read_x'] - dynamicsql_info['idx_tup_read_y']
        dynamicsql_info['idx_tup_fetch'] = dynamicsql_info['idx_tup_fetch_x'] - dynamicsql_info['idx_tup_fetch_y']
        dynamicsql_info['idx_blks_hit'] = dynamicsql_info['idx_blks_hit_x'] - dynamicsql_info['idx_blks_hit_y']
        dynamicsql_info['phyrds'] = dynamicsql_info['phyrds_x'] - dynamicsql_info['phyrds_y']
        dynamicsql_info['phywrts'] = dynamicsql_info['phywrts_x'] - dynamicsql_info['phywrts_y']
        dynamicsql_info['phyblkrd'] = dynamicsql_info['phyblkrd_x'] - dynamicsql_info['phyblkrd_y']
        dynamicsql_info['phyblkwrt'] = dynamicsql_info['phyblkwrt_x'] - dynamicsql_info['phyblkwrt_y']
        dynamicsql_info['readtim'] = dynamicsql_info['readtim_x'] - dynamicsql_info['readtim_y']
        dynamicsql_info['writetim'] = dynamicsql_info['writetim_x'] - dynamicsql_info['writetim_y']
        dynamicsql_info.rename(
            columns={'datname_x': 'datname', 'snaptime_x': 'snaptime', 'schemaname_x': 'schemaname',
                     'relname_x': 'relname', 'indexrelname_x': 'indexrelname'}, inplace=True)
        data_list = dynamicsql_info[
            ["datname", 'indexrelid', "snaptime", "schemaname", "relname", "indexrelname", "idx_scan",
             "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt",
             "readtim", "writetim"]].to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table

    @staticmethod
    def split_date(old_table_index):
        old_table_index_data = copy.deepcopy(old_table_index)
        old_table_index_data['idx_scan'] = old_table_index_data['idx_scan'].apply(lambda x: 0)
        old_table_index_data['idx_tup_read'] = old_table_index_data['idx_tup_read'].apply(lambda x: 0)
        old_table_index_data['idx_tup_fetch'] = old_table_index_data['idx_tup_fetch'].apply(lambda x: 0)
        old_table_index_data['idx_blks_hit'] = old_table_index_data['idx_blks_hit'].apply(lambda x: 0)
        old_table_index_data['phyrds'] = old_table_index_data['phyrds'].apply(lambda x: 0)
        old_table_index_data['phywrts'] = old_table_index_data['phywrts'].apply(lambda x: 0)
        old_table_index_data['phyblkrd'] = old_table_index_data['phyblkrd'].apply(lambda x: 0)
        old_table_index_data['phyblkwrt'] = old_table_index_data['phyblkwrt'].apply(lambda x: 0)
        old_table_index_data['readtim'] = old_table_index_data['readtim'].apply(lambda x: 0)
        old_table_index_data['writetim'] = old_table_index_data['writetim'].apply(lambda x: 0)
        data_list = old_table_index_data[
            ["datname", 'indexrelid', "snaptime", "schemaname", "relname", "indexrelname", "idx_scan",
             "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt",
             "readtim", "writetim"]].to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table
