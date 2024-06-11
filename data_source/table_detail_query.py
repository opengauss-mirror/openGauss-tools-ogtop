# -*- coding: utf-8 -*-
""" table_index data storage calculation """
import json
import pandas as pd


class TableDetailMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization table_index data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        data = OpenGaussOption.params_query_sql(sql, params)
        if isinstance(data, str):
            table_detail_df = data
        else:
            if data == []:
                table_detail_df = "The query data is empty!"
            else:
                table_detail_df = pd.DataFrame(data)
                table_detail_df.columns = ["datname", "relid", "schemaname", "relname", "table_detail"]
                table_detail_df.replace(to_replace='None', value='-', inplace=True)
                table_detail_df.fillna('-', inplace=True)

        self.de_part = table_detail_df

    @staticmethod
    def intergrated_data(table_detail_query):
        """
         calculate the increment，save the result to dictionary，transfer and display to the interface
        :param table_detail_query: refresh data this time
        :return: return the computer result
        """
        data_list = table_detail_query.to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table
