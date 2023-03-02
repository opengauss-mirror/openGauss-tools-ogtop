# -*- coding: utf-8 -*-
""" table_detail data storage calculation """
import json
import pandas as pd


class TableDetailMain(object):

    def __init__(self, openGauss_option, sqls, params):
        """
        initialization table_detail data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        data = openGauss_option.params_query_sql(sqls, (params,))
        if data:
            table_detail_df = pd.DataFrame(data)
            table_detail_df.columns = ["target_ip", "datname", "snaptime", "schemaname", "relname", "indexrelname",
                                       "idx_scan", "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts",
                                       "phyblkrd", "phyblkwrt", "readtim", "writetim"]
            table_detail_df.replace(to_replace='None', value='-', inplace=True)
            table_detail_df.fillna('-', inplace=True)
            table_detail_df[
                ["snaptime", "idx_scan", "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts",
                 "phyblkrd", "phyblkwrt", "readtim", "writetim"]] = table_detail_df[
                ["snaptime", "idx_scan", "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts",
                 "phyblkrd", "phyblkwrt", "readtim", "writetim"]].astype(
                int).astype(str)
            self.de_part = table_detail_df

        else:
            self.de_part = pd.DataFrame(data)

    @staticmethod
    def intergrated_data(table_detail_data):
        """
         calculate the increment，save the result to dictionary，transfer and display to the interface
        :param table_detail_data: refresh data this time
        :return: return the computer result
        """
        data_list = table_detail_data[
            ["target_ip", "datname", "snaptime", "schemaname", "relname", "indexrelname", "idx_scan",
             "idx_tup_read", "idx_tup_fetch", "idx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt",
             "readtim", "writetim"]].to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table
