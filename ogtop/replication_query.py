# -*- coding: utf-8 -*-
""" replication data storage calculation """

import json
import pandas as pd


class ReplicationMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization replication data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        try:
            columns_list = ['target_ip', 'snaptime', 'local_role', 'peer_role', 'peer_state',
                            'sync_percent', 'sync_state', 'sender_sent_location',
                            'sender_write_location', 'sender_flush_location',
                            'sender_replay_location', 'receiver_received_location',
                            'receiver_write_location', 'receiver_flush_location',
                            'receiver_replay_location', 'receive_gap', 'replay_gap']
            if len(data[0]) == 17:
                replication_df = pd.DataFrame(data, columns=columns_list)
            else:
                replication_df_prepare = pd.DataFrame(data, columns=['target_ip', 'snaptime', 'local_role', 'peer_role',
                                                                     'peer_state',
                                                                     'sync_percent', 'sender_sent_location',
                                                                     'sender_write_location',
                                                                     'sender_flush_location', 'sender_replay_location',
                                                                     'receiver_received_location',
                                                                     'receiver_write_location',
                                                                     'receiver_flush_location',
                                                                     'receiver_replay_location',
                                                                     'receive_gap', 'replay_gap'])
                replication_df_prepare['sync_state'] = '-'
                replication_df = replication_df_prepare[columns_list]
            self.de_part = replication_df
        except:
            self.de_part = data

    @staticmethod
    def intergrated_data(replication_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param replication_data: refresh data this time
        :return: return the computer result
        """
        data_list = replication_data.to_json(orient='records')
        data_replication = (data_list[1:-1]).replace("},", "}!")
        data_replication.split("!")
        data_replication = json.loads(data_list)

        return data_replication

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        column_list = ['target_ip', 'snaptime', 'local_role', 'peer_role', 'peer_state', 'sync_percent', 'sync_state',
                       'sender_sent_location', 'sender_write_location', 'sender_flush_location',
                       'sender_replay_location', 'receiver_received_location', 'receiver_write_location',
                       'receiver_flush_location', 'receiver_replay_location', 'receive_gap', 'replay_gap']
        new_replication_df = pd.DataFrame(depart)
        new_replication_df.columns = column_list
        new_replication_df.rename(columns={'sender_sent_location': 'sender_sent_location1',
                                           'sender_write_location': 'sender_write_location1',
                                           'sender_flush_location': 'sender_flush_location1',
                                           'sender_replay_location': 'sender_replay_location1',
                                           'receiver_received_location': 'receiver_received_location1',
                                           'receiver_write_location': 'receiver_write_location1',
                                           'receiver_flush_location': 'receiver_flush_location1',
                                           'receiver_replay_location': 'receiver_replay_location1'}, inplace=True)

        old_replication_df = pd.DataFrame(old_depart)
        old_replication_df.columns = column_list
        old_replication_df = old_replication_df[['target_ip',
                                                 'sender_sent_location', 'sender_write_location',
                                                 'sender_flush_location', 'sender_replay_location',
                                                 'receiver_received_location', 'receiver_write_location',
                                                 'receiver_flush_location', 'receiver_replay_location']]
        old_replication_df.rename(columns={'sender_sent_location': 'sender_sent_location2',
                                           'sender_write_location': 'sender_write_location2',
                                           'sender_flush_location': 'sender_flush_location2',
                                           'sender_replay_location': 'sender_replay_location2',
                                           'receiver_received_location': 'receiver_received_location2',
                                           'receiver_write_location': 'receiver_write_location2',
                                           'receiver_flush_location': 'receiver_flush_location2',
                                           'receiver_replay_location': 'receiver_replay_location2'}, inplace=True)
        res_replication_date = pd.merge(new_replication_df, old_replication_df, on=['target_ip'], how='inner')
        res_replication_date['sender_sent_location'] = res_replication_date['sender_sent_location1'] - \
                                                       res_replication_date['sender_sent_location2']
        res_replication_date['sender_write_location'] = res_replication_date['sender_write_location1'] - \
                                                        res_replication_date['sender_write_location2']
        res_replication_date['sender_flush_location'] = res_replication_date['sender_flush_location1'] - \
                                                        res_replication_date['sender_flush_location2']
        res_replication_date['sender_replay_location'] = res_replication_date['sender_replay_location1'] - \
                                                         res_replication_date['sender_replay_location2']
        res_replication_date['receiver_received_location'] = res_replication_date['receiver_received_location1'] - \
                                                             res_replication_date['receiver_received_location2']
        res_replication_date['receiver_write_location'] = res_replication_date['receiver_write_location1'] - \
                                                          res_replication_date['receiver_write_location2']
        res_replication_date['receiver_flush_location'] = res_replication_date['receiver_flush_location1'] - \
                                                          res_replication_date['receiver_flush_location2']
        res_replication_date['receiver_replay_location'] = res_replication_date['receiver_replay_location1'] - \
                                                           res_replication_date['receiver_replay_location2']

        replication_date = res_replication_date[['target_ip', 'snaptime', 'local_role', 'peer_role', 'peer_state',
                                                 'sync_percent', 'sync_state', 'sender_sent_location',
                                                 'sender_write_location',
                                                 'sender_flush_location', 'sender_replay_location',
                                                 'receiver_received_location', 'receiver_write_location',
                                                 'receiver_flush_location', 'receiver_replay_location',
                                                 'receive_gap', 'replay_gap']].reset_index()
        replication_date.pop('index')
        reshape_list = ['sender_sent_location', 'sender_write_location',
                        'sender_flush_location', 'sender_replay_location',
                        'receiver_received_location', 'receiver_write_location',
                        'receiver_flush_location', 'receiver_replay_location',
                        'receive_gap', 'replay_gap']
        for k in reshape_list:
            replication_date[k] = replication_date[k].astype('int').astype('str')
        replication_date.replace(to_replace='None', value='-', inplace=True)
        replication_date.fillna('-', inplace=True)
        depart_date = replication_date.to_json(orient='records')
        depart_depart = (depart_date[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(depart_date)

        return depart_depart
