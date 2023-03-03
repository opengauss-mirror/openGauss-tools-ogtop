# -*- coding: utf-8 -*-
""" top_mem_used_session data storage calculation """

import json
import pandas as pd


class TopMemUsedSessionMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization top_mem_used_session data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        top_mem_used_session_df = pd.DataFrame(data,
                                               columns=['sessid', 'init_mem', 'used_mem', 'peak_mem', 'pid', 'query',
                                                        'usename', 'client_addr'])
        self.de_part = top_mem_used_session_df[
            ['pid', 'init_mem', 'used_mem', 'peak_mem', 'query', 'usename', 'client_addr']]

    @staticmethod
    def intergrated_data(top_mem_used_session_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param top_mem_used_session_data: refresh data this time
        :return: return the computer result
        """
        values = {'init_mem': str(0), 'used_mem': str(0), 'peak_mem': str(0), 'client_addr': '-'}
        top_mem_used_session_data.fillna(value=values, inplace=True)
        top_mem_used_session_data.replace(to_replace='None', value='-', inplace=True)
        top_mem_used_session_data.replace(to_replace='', value='-', inplace=True)
        top_mem_used_session_data.fillna('-', inplace=True)
        top_mem_used_session_data[['init_mem', 'used_mem', 'peak_mem']] = top_mem_used_session_data[
            ['init_mem', 'used_mem', 'peak_mem']].astype(
            'int').astype('str')
        header_data = str(top_mem_used_session_data.shape[0])
        data_list = top_mem_used_session_data[
            ['pid', 'init_mem', 'used_mem', 'peak_mem', 'query', 'usename', 'client_addr']].to_json(orient='records')
        data_top_mem_used_session = (data_list[1:-1]).replace("},", "}!")
        data_top_mem_used_session.split("!")
        data_top_mem_used_session = json.loads(data_list)
        return data_top_mem_used_session, {"data_length": header_data}
