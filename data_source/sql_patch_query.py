# -*- coding: utf-8 -*-
""" sql_patch data storage calculation """
import json
import pandas as pd


class SqlPatchMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization table_index data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            sql_patch_df = data
        else:
            if data == []:
                sql_patch_df = data
            else:
                sql_patch_df = pd.DataFrame(data)
                sql_patch_df.columns = ["patch_name", "unique_sql_id", "username", "enable", "status", "abort",
                                        "hint_string", "hint_node", "original_query", "original_query_tree",
                                        "patched_query", "patched_query_tree", "description"]
                sql_patch_df.replace(to_replace='None', value='-', inplace=True)
                sql_patch_df.fillna('-', inplace=True)

        self.de_part = sql_patch_df

    @staticmethod
    def intergrated_data(sql_patch_query):
        """
         calculate the increment，save the result to dictionary，transfer and display to the interface
        :param sql_patch_query: refresh data this time
        :return: return the computer result
        """
        if isinstance(sql_patch_query,str) or isinstance(sql_patch_query,list):
            data_table = sql_patch_query
        else:
            data_list = sql_patch_query.to_json(orient='records')
            data_table = (data_list[1:-1]).replace("},", "}!")
            data_table.split("!")
            data_table = json.loads(data_list)
        return data_table
