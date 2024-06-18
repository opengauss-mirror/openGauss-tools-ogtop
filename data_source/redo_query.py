#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
import json
import pandas as pd


class RedoMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization redo data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            redo_df = data
        else:
            if data == []:
                redo_df = "The query data is empty!"
            else:
                redo_df = pd.DataFrame(data,
                                       columns=['node_name', 'redo_start_ptr', 'redo_start_time', 'redo_done_time',
                                                'curr_time', 'min_recovery_point', 'read_ptr', 'last_replayed_read_ptr',
                                                'recovery_done_ptr', 'read_xlog_io_counter', 'read_xlog_io_total_dur',
                                                'read_data_io_counter', 'read_data_io_total_dur',
                                                'write_data_io_counter', 'write_data_io_total_dur',
                                                'process_pending_counter', 'process_pending_total_dur', 'apply_counter',
                                                'apply_total_dur', 'speed', 'local_max_ptr', 'primary_flush_ptr',
                                                'worker_info'])
        self.de_part = redo_df

    @staticmethod
    def intergrated_data(redo_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param redo_time_count_data: refresh data this time
        :return: return the computer result
        """
        data_list = redo_df.to_json(orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table
