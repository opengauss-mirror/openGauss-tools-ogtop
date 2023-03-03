# -*- coding: utf-8 -*-
""" lock data storage calculation """
import json
import pandas as pd


class LockMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization wait_event data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        try:
            self.de_part = pd.DataFrame(data, columns=['relname', 'locktype', 'database', 'relation', 'page', 'tuple',
                                                       'transactionid', 'virtualxid', 'objid', 'virtualtransaction',
                                                       'pid', 'mode', 'granted', 'fastpath', 'locktag'])
        except Exception as e:
            self.de_part = data

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
