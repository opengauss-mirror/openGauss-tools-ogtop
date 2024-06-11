# -*- coding: utf-8 -*-
""" session_history data storage calculation """
import json
import pandas as pd


class SessionHistoryMain(object):

    def __init__(self, openGauss_option, sqls, params):
        """
        initialization session_history data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        data = openGauss_option.params_query_sql(sqls, params)
        if isinstance(data, str):
            session_history_df = data
        else:
            if data == []:
                session_history_df = "The query data is empty!"
            else:
                session_history_df = pd.DataFrame(data)
                session_history_list = ["datname", "username", "sessionid", "client_addr", "sample_time", "duration_ms",
                                        "event", "unique_query_id", "unique_query"]
                session_history_df.columns = session_history_list
                session_history_df.fillna('-', inplace=True)
                session_history_df = session_history_df.reset_index()
                session_history_df.pop('index')
        self.de_part = session_history_df

    @staticmethod
    def intergrated_data(session_history_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param session_history_data: refresh data this time
        :return: return the computer result
        """
        data_list = session_history_data[
            ["datname", "username", "sessionid", "client_addr", "sample_time", "duration_ms",
             "event", "unique_query_id", "unique_query"]].to_json(
            orient='records')
        data_session_history = (data_list[1:-1]).replace("},", "}!")
        data_session_history.split("!")
        data_session_history = json.loads(data_list)
        return data_session_history
