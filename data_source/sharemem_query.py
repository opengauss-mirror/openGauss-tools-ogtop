# -*- coding: utf-8 -*-
""" sharemem data storage calculation  """

import json
import pandas as pd


class SharememMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization sharemem data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            sharemem_df = data
        else:
            if data == []:
                sharemem_df = "The query data is empty!"
            else:
                sharemem_df = pd.DataFrame(data,
                                           columns=['target_ip', 'parent', 'contextname', 'level', 'totalsize',
                                                    'freesize',
                                                    'usedsize'])
        self.de_part = sharemem_df

    @staticmethod
    def intergrated_data(sharemem_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param sharemem_data: refresh data this time
        :return: return the computer result
        """

        def fuc1(x):
            if x.totalsize != 0:
                return round((x.usedsize / x.totalsize) * 100, 2)
            else:
                return 0.00

        sharemem_data['usedsize/totalsize'] = sharemem_data.apply(fuc1, axis=1)
        values = {'level': str(0), 'totalsize': str(0), 'freesize': str(0), 'usedsize': str(0)}
        sharemem_data.fillna(value=values, inplace=True)
        sharemem_data.replace(to_replace='None', value='-', inplace=True)
        sharemem_data.fillna('-', inplace=True)
        sharemem_data[['totalsize', 'freesize', 'usedsize']] = sharemem_data[
            ['totalsize', 'freesize', 'usedsize']].astype('int').astype('str')
        header_data = str(sharemem_data.shape[0])
        data_list = sharemem_data[
            ['parent', 'contextname', 'level', 'totalsize', 'freesize', 'usedsize', 'usedsize/totalsize']].to_json(
            orient='records')
        data_sharemem = (data_list[1:-1]).replace("},", "}!")
        data_sharemem.split("!")
        data_sharemem = json.loads(data_list)

        return data_sharemem, {"data_length": header_data}
