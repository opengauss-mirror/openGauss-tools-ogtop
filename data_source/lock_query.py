# -*- coding: utf-8 -*-
""" lock data storage calculation """
import json
import pandas as pd


class LockMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization wait_event data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            lock_df = data
        else:
            if data == []:
                lock_df = "The query data is empty!"
            else:
                lock_df = pd.DataFrame(data, columns=['relname', 'locktype', 'database', 'relation', 'page', 'tuple',
                                                      'transactionid', 'virtualxid', 'objid', 'virtualtransaction',
                                                      'pid', 'mode', 'granted', 'fastpath', 'locktag'])
        self.de_part = lock_df

    @staticmethod
    def intergrated_data(lock_data):
        """
         calculate the increment，save the result to dictionary，transfer and display to the interface
        :param lock_data: refresh data this time
        :return: return the computer result
        """
        values = {'database': '-', 'relation': str(0), 'page': str(0), 'tuple': str(0), 'transactionid': str(0),
                  'virtualxid': str(0), 'objid': str(0)}
        lock_data.fillna(value=values, inplace=True)
        lock_data[['relation', 'objid']] = lock_data[['relation', 'objid']].astype('int').astype('str')

        lock_data.replace(to_replace='None', value='-', inplace=True)
        lock_data.fillna('-', inplace=True)
        data_list = lock_data[["relname", 'pid', 'locktype', 'database', 'relation', 'page', 'tuple',
                               'transactionid', 'virtualxid', 'objid', 'virtualtransaction',
                               'mode', 'granted', 'fastpath', 'locktag']].to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data_lock = json.loads(data_list)
        header_data = {"data_length": str(len(lock_data))}

        return data_lock, header_data
