#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
import json
import pandas as pd


class XlogRedoStaticsMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization xlog_redo_statics data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            xlog_redo_statics_df = data
        else:
            if data == []:
                xlog_redo_statics_df = "The query data is empty!"
            else:
                xlog_redo_statics_df = pd.DataFrame(data,
                                                    columns=['xlog_type', 'rmid', 'info', 'num', 'extra'])
        self.de_part = xlog_redo_statics_df

    @staticmethod
    def intergrated_data(new_xlog_redo_statics_data, old_xlog_redo_statics_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param new_xlog_redo_statics_data: refresh data this time
        :param old_xlog_redo_statics_data: last refresh data
        :return: return the computer result
        """
        xlog_redo_statics_data_info = pd.merge(new_xlog_redo_statics_data, old_xlog_redo_statics_data, on=['xlog_type'],
                                               how='left')
        xlog_redo_statics_data_info['num'] = xlog_redo_statics_data_info['num_x'] - \
                                                     xlog_redo_statics_data_info['num_y']
        xlog_redo_statics_data_info.rename(columns={'rmid_x': 'rmid', 'info_x': 'info', 'extra_x': 'extra',},
                                           inplace=True)
        data_list = xlog_redo_statics_data_info.to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table

    @staticmethod
    def split_date(old_xlog_redo_statics_data):
        old_xlog_redo_statics_data['num'] = 0
        data_list = old_xlog_redo_statics_data.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)
        return data