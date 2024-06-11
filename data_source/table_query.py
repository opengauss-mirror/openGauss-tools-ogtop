# -*- coding: utf-8 -*-
""" table data storage calculation """

import json
import pandas as pd


class TableMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization table data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            table_df = data
        else:
            if data == []:
                table_df = "The query data is empty!"
            else:
                table_df = pd.DataFrame(data)
                table_df.columns = ['relname', 'schemaname', 'datname', "relid", "n_live_tup",
                                    "n_dead_tup", 'last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
                                    'last_data_changed', 'tabsize', 'idxsize', 'totalsize']
                values = {'last_vacuum': '-', 'last_autovacuum': '-', 'last_analyze': '-', 'last_autoanalyze': '-',
                          'last_data_changed': '-'}
                table_df.fillna(value=values, inplace=True)
                table_df = table_df.reset_index()
                table_df.pop('index')
                table_df[['last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
                          'last_data_changed']] = table_df[
                    ['last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
                     'last_data_changed']].astype(str)

        self.de_part = table_df

    @staticmethod
    def intergrated_data(table_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param table_data: refresh data this time
        :return: return the computer result
        """
        table_data['tabsize'] = round(table_data['tabsize'] / 1028 / 1028, 3)
        table_data['idxsize'] = round(table_data['idxsize'] / 1028 / 1028, 3)
        table_data['totalsize'] = round(table_data['totalsize'] / 1028 / 1028, 3)
        data_list = table_data[
            ['datname', 'schemaname', 'relname', 'relid', 'tabsize', 'idxsize', 'totalsize', "n_live_tup",
             "n_dead_tup", 'last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
             'last_data_changed']].to_json(
            orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table
