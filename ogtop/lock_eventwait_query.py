# -*- coding: utf-8 -*-
""" wait_event data storage calculation """
import json
import pandas as pd


class WaitEventMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization wait_event data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        try:
            wait_event_df = pd.DataFrame(data, columns=['nodename', 'type', 'event', 'wait', 'failed_wait',
                                                        'total_wait_time', 'avg_wait_time', 'max_wait_time',
                                                        'min_wait_time',
                                                        'last_updated']).reset_index()
            wait_event_df.pop('index')
            wait_event_df['last_updated'] = wait_event_df['last_updated'].astype('str')
        except Exception as e:
            wait_event_df = data
        self.de_part = wait_event_df

    @staticmethod
    def intergrated_data(lock_event_data):
        """
        consolidate the data and convert it to json format
        :param lock_event_data: data to be converted
        :return:
        """
        data_list = lock_event_data.to_json(orient='records')
        data = (data_list[1:-1]).replace("},", "}!")
        data.split("!")
        data_wait_event = json.loads(data_list)

        return data_wait_event

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        new_wait_event = pd.DataFrame(depart)
        new_wait_event.columns = ['nodename', 'type', 'event', 'wait1', 'failed_wait',
                                  'total_wait_time1', 'avg_wait_time1', 'history_max_wait_time',
                                  'history_min_wait_time', 'last_updated']
        old_wait_event = pd.DataFrame(old_depart)
        old_wait_event.columns = ['nodename', 'type', 'event', 'wait2', 'failed_wait',
                                  'total_wait_time2', 'avg_wait_time', 'max_wait_time',
                                  'min_wait_time', 'last_updated']
        old_wait_event = old_wait_event[['event', 'wait2', 'total_wait_time2']]
        wait_event = pd.merge(new_wait_event, old_wait_event, on=['event'], how='inner')
        wait_event['wait'] = wait_event['wait1'] - wait_event['wait2']

        wait_event['total_wait_time'] = ((wait_event['total_wait_time1'] - wait_event['total_wait_time2']) / 1000)
        wait_event['avg_wait_time'] = wait_event['total_wait_time'] / wait_event['wait']
        wait_event['history_max_wait_time'] = (wait_event['history_max_wait_time'] / 1000)
        wait_event['history_min_wait_time'] = (wait_event['history_min_wait_time'] / 1000)

        wait_event_date = wait_event[['nodename', 'type', 'event', 'wait', 'failed_wait',
                                      'total_wait_time', 'avg_wait_time', 'history_max_wait_time',
                                      'history_min_wait_time', 'last_updated']].sort_values(
            by='total_wait_time').reset_index()
        wait_event_date.pop('index')
        res_wait_event_date = wait_event_date[wait_event_date['wait'] != 0]
        res_wait_event_date.replace(to_replace='None', value='-', inplace=True)
        res_wait_event_date.fillna('-', inplace=True)
        heard_depart = str(wait_event_date.shape[0])
        depart_depart1 = res_wait_event_date.to_json(orient='records')
        depart_depart = (depart_depart1[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(depart_depart1)

        return depart_depart, {"data_length": heard_depart}
