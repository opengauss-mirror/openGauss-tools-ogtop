#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'hf'
import json
import pandas as pd


class WrdMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization wdr data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            wrd_df = data
        else:
            if data == []:
                wrd_df = "The query data is empty!"
            else:
                wrd_df = pd.DataFrame(data,columns=["snapshot_id", "start_ts", "end_ts"])
        self.de_part = wrd_df

    @staticmethod
    def intergrated_data(wrd_df):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param wrd_df: refresh data this time
        :return: return the computer result
        """
        wrd_df = wrd_df.astype(str)
        data_list = wrd_df.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data = json.loads(data_list)
        return data
