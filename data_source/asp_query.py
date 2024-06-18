# -*- coding: utf-8 -*-
""" asp data storage calculation """
import json
import pandas as pd


class SessionHistoryMain(object):

    def __init__(self, OpenGaussOption, sqls, params):
        """
        initialization asp data
        :param OpenGaussOption: openGauss database operation object
        :param sqls: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        data = OpenGaussOption.params_query_sql(sqls, params)
        if isinstance(data, str):
            self.de_part = data
        else:
            if data == []:
                asp_df = []
            else:
                try:
                    asp_df = pd.DataFrame(data)
                    asp_list = ["datname", "username", "sessionid", "client_addr", "duration_ms",
                                "event", "unique_query", "sample_time", "unique_query_id"]
                    asp_df.columns = asp_list
                    asp_df.fillna('-', inplace=True)
                    asp_df = asp_df.reset_index()
                    asp_df.pop('index')
                except Exception as e:
                    asp_df = []
            self.de_part = asp_df

    @staticmethod
    def intergrated_data(asp_data, database):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param asp_data: refresh data this time
        :return: return the computer result
        """
        asp_sql_data = asp_data.loc[asp_data['datname'] == database]

        active_data = asp_sql_data[["unique_query"]]
        active_data.rename(columns={'unique_query': 'query'}, inplace=True)
        # mask = active_data_df["query"].str.match('MERGE INTO', case=False)
        # active_data_list = active_data_df[~mask]
        active_data.insert(loc=1, column='count', value=1)
        data_list = asp_data[
            ["datname", "username", "sessionid", "client_addr", "duration_ms", "event", "unique_query", "sample_time",
             "unique_query_id"]].to_json(orient='records')
        data_asp = (data_list[1:-1]).replace("},", "}!")
        data_asp.split("!")
        data_asp = json.loads(data_list)
        return data_asp, active_data
