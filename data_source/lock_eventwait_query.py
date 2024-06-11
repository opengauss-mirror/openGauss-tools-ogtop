# -*- coding: utf-8 -*-
""" wait_event data storage calculation """
import json
import pandas as pd
import copy


class WaitEventMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization wait_event data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            wait_event_df = data
        else:
            if data == []:
                wait_event_df = "The query data is empty!"
            else:
                wait_event_df = pd.DataFrame(data, columns=['nodename', 'type', 'event', 'wait', 'failed_wait',
                                                            'total_wait_time', 'avg_wait_time', 'max_wait_time',
                                                            'min_wait_time',
                                                            'last_updated']).reset_index()
                wait_event_df.drop(wait_event_df[wait_event_df['event'] == 'wait cmd'].index, inplace=True)

                wait_event_df.pop('index')
                wait_event_df['last_updated'] = wait_event_df['last_updated'].astype('str')
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
        new_wait_event.columns = ['nodename', 'type', 'event', 'wait1', 'failed_wait1',
                                  'total_wait_time1', 'avg_wait_time1', 'history_max_wait_time',
                                  'history_min_wait_time', 'last_updated']
        old_wait_event = pd.DataFrame(old_depart)
        old_wait_event.columns = ['nodename', 'type', 'event', 'wait2', 'failed_wait2',
                                  'total_wait_time2', 'avg_wait_time', 'max_wait_time',
                                  'min_wait_time', 'last_updated']
        old_wait_event = old_wait_event[['event', 'wait2', 'total_wait_time2', 'failed_wait2']]
        wait_event = pd.merge(new_wait_event, old_wait_event, on=['event'], how='inner')
        wait_event['wait'] = wait_event['wait1'] - wait_event['wait2']
        wait_event['failed_wait'] = wait_event['failed_wait1'] - wait_event['failed_wait2']
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

    @staticmethod
    def split_date(old_lock_eventwait):
        old_depart = copy.deepcopy(old_lock_eventwait)
        old_wait_event = pd.DataFrame(old_depart)
        old_wait_event.columns = ['nodename', 'type', 'event', 'wait2', 'failed_wait2',
                                  'total_wait_time2', 'avg_wait_time', 'max_wait_time',
                                  'min_wait_time', 'last_updated']
        old_wait_event['wait'] = 0
        old_wait_event['failed_wait'] = 0
        old_wait_event['total_wait_time'] = 0
        old_wait_event['avg_wait_time'] = 0
        old_wait_event['history_max_wait_time'] = 0
        old_wait_event['history_min_wait_time'] = 0
        wait_event_date = old_wait_event[['nodename', 'type', 'event', 'wait', 'failed_wait',
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
