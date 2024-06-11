#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
""" redo_time_count data storage calculation """

import json
import pandas as pd


class RedoTimeCountMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization redo_time_count data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            redo_time_count_df = data
        else:
            if data == []:
                redo_time_count_df = "The query data is empty!"
            else:
                redo_time_count_df = pd.DataFrame(data,
                                                  columns=['thread_name', 'step1_total', 'step1_count', 'step2_total',
                                                           'step2_count', 'step3_total', 'step3_count', 'step4_total',
                                                           'step4_count', 'step5_total', 'step5_count', 'step6_total',
                                                           'step6_count', 'step7_total', 'step7_count', 'step8_total',
                                                           'step8_count', 'step9_total', 'step9_count'
                                                           ])
        self.de_part = redo_time_count_df

    @staticmethod
    def intergrated_data(new_redo_time_count_data, old_redo_time_count_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param redo_time_count_data: refresh data this time
        :return: return the computer result
        """
        redo_time_count_data_info = pd.merge(new_redo_time_count_data, old_redo_time_count_data, on=['thread_name'],
                                             how='left')
        redo_time_count_data_info['step1_total'] = redo_time_count_data_info['step1_total_x'] - \
                                                   redo_time_count_data_info['step1_total_y']
        redo_time_count_data_info['step1_count'] = redo_time_count_data_info['step1_count_x'] - \
                                                   redo_time_count_data_info['step1_count_y']
        redo_time_count_data_info['step2_total'] = redo_time_count_data_info['step2_total_x'] - \
                                                   redo_time_count_data_info['step2_total_y']
        redo_time_count_data_info['step2_count'] = redo_time_count_data_info['step2_count_x'] - \
                                                   redo_time_count_data_info['step2_count_y']
        redo_time_count_data_info['step3_total'] = redo_time_count_data_info['step3_total_x'] - \
                                                   redo_time_count_data_info['step3_total_y']
        redo_time_count_data_info['step3_count'] = redo_time_count_data_info['step3_count_x'] - \
                                                   redo_time_count_data_info['step3_count_y']
        redo_time_count_data_info['step4_total'] = redo_time_count_data_info['step4_total_x'] - \
                                                   redo_time_count_data_info['step4_total_y']
        redo_time_count_data_info['step4_count'] = redo_time_count_data_info['step4_count_x'] - \
                                                   redo_time_count_data_info['step4_count_y']
        redo_time_count_data_info['step5_total'] = redo_time_count_data_info['step5_total_x'] - \
                                                   redo_time_count_data_info['step5_total_y']
        redo_time_count_data_info['step5_count'] = redo_time_count_data_info['step5_count_x'] - \
                                                   redo_time_count_data_info['step5_count_y']
        redo_time_count_data_info['step6_total'] = redo_time_count_data_info['step6_total_x'] - \
                                                   redo_time_count_data_info['step6_total_y']
        redo_time_count_data_info['step6_count'] = redo_time_count_data_info['step6_count_x'] - \
                                                   redo_time_count_data_info['step6_count_y']
        redo_time_count_data_info['step7_total'] = redo_time_count_data_info['step7_total_x'] - \
                                                   redo_time_count_data_info['step7_total_y']
        redo_time_count_data_info['step7_count'] = redo_time_count_data_info['step7_count_x'] - \
                                                   redo_time_count_data_info['step7_count_y']
        redo_time_count_data_info['step8_total'] = redo_time_count_data_info['step8_total_x'] - \
                                                   redo_time_count_data_info['step8_total_y']
        redo_time_count_data_info['step8_count'] = redo_time_count_data_info['step8_count_x'] - \
                                                   redo_time_count_data_info['step8_count_y']
        redo_time_count_data_info['step9_total'] = redo_time_count_data_info['step9_total_x'] - \
                                                   redo_time_count_data_info['step9_total_y']
        redo_time_count_data_info['step9_count'] = redo_time_count_data_info['step9_count_x'] - \
                                                   redo_time_count_data_info['step9_count_y']
        data_list = redo_time_count_data_info.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)
        return data

    @staticmethod
    def split_date(old_redo_time_count_data):
        old_redo_time_count_data['step1_total']=0
        old_redo_time_count_data['step1_count']=0
        old_redo_time_count_data['step2_total']=0
        old_redo_time_count_data['step2_count']=0
        old_redo_time_count_data['step3_total']=0
        old_redo_time_count_data['step3_count']=0
        old_redo_time_count_data['step4_total']=0
        old_redo_time_count_data['step4_count']=0
        old_redo_time_count_data['step5_total']=0
        old_redo_time_count_data['step5_count']=0
        old_redo_time_count_data['step6_total']=0
        old_redo_time_count_data['step6_count']=0
        old_redo_time_count_data['step7_total']=0
        old_redo_time_count_data['step7_count']=0
        old_redo_time_count_data['step8_total']=0
        old_redo_time_count_data['step8_count']=0
        old_redo_time_count_data['step9_total']=0
        old_redo_time_count_data['step9_count']=0
        data_list = old_redo_time_count_data.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)
        return data