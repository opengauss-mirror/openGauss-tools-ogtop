# -*- coding: utf-8 -*-
""" replication data storage calculation """

import json

import copy
import pandas as pd


class ReplicationMain(object):

    def __init__(self, OpenGaussOption, sql, replication_slots_sql):
        """
        initialization replication data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        slots_data = OpenGaussOption.all_query_sql(replication_slots_sql)
        if isinstance(data, str):
            replication_df = data
        else:
            if data == []:
                replication_df = "The query data is empty!"
            else:
                columns_list = ['channel', 'snaptime', 'local_role', 'peer_role', 'peer_state',
                                'sync_percent', 'sync_state', 'sender_sent_location',
                                'sender_write_location', 'sender_flush_location',
                                'sender_replay_location', 'receiver_received_location',
                                'receiver_write_location', 'receiver_flush_location',
                                'receiver_replay_location', 'receive_gap', 'replay_gap']
                replication_df = pd.DataFrame(data, columns=columns_list)

        if isinstance(slots_data, str):
            slots_replication_df = slots_data
        else:
            if slots_data == []:
                slots_replication_df = "The query data is empty!"
            else:
                slots_columns_list = ['slot_name', 'plugin', 'slot_type', 'datname', 'active',
                                      'catalog_xmin', 'restart_lsn', 'diff_lsn',
                                      'dummy_standby', 'confirmed_flush']
                slots_replication_df = pd.DataFrame(slots_data, columns=slots_columns_list)
        self.de_part = [replication_df, slots_replication_df]

    @staticmethod
    def compute_delta(depart, old_depart, ):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        replication_info = pd.merge(depart[0], old_depart[0], on=['channel'], how='left')
        replication_info.fillna(value=0, inplace=True)
        time = 1 if int((replication_info['snaptime_x'].apply(lambda x: x.timestamp()) - replication_info[
            'snaptime_y'].apply(lambda x: x.timestamp())).loc[0]) == 0 else int((replication_info['snaptime_x'].apply(
            lambda x: x.timestamp()) - replication_info['snaptime_y'].apply(lambda x: x.timestamp())).loc[0])
        # location/time/1024   kb/s
        replication_info['sender_sent_location'] = (replication_info['sender_sent_location_x'] - replication_info[
            'sender_sent_location_y']) / time / 1024
        replication_info['sender_write_location'] = (replication_info['sender_write_location_x'] - replication_info[
            'sender_write_location_y']) / time / 1024
        replication_info['sender_flush_location'] = (replication_info['sender_flush_location_x'] - replication_info[
            'sender_flush_location_y']) / time / 1024
        replication_info['sender_replay_location'] = (replication_info['sender_replay_location_x'] - replication_info[
            'sender_replay_location_y']) / time / 1024
        replication_info['receiver_received_location'] = (replication_info['receiver_received_location_x'] -
                                                          replication_info['receiver_received_location_y']) / \
                                                         time / 1024
        replication_info['receiver_write_location'] = (replication_info['receiver_write_location_x'] - replication_info[
            'receiver_write_location_y']) / time / 1024
        replication_info['receiver_flush_location'] = (replication_info['receiver_flush_location_x'] - replication_info[
            'receiver_flush_location_y']) / time / 1024
        replication_info['receiver_replay_location'] = (replication_info['receiver_replay_location_x'] -
                                                        replication_info['receiver_replay_location_y']) / \
                                                       time / 1024
        # receive_gap、 replay_gap   /1024  kb
        replication_info['receive_gap'] = replication_info['receive_gap_x'] / 1024
        replication_info['replay_gap'] = replication_info['replay_gap_x'] / 1024

        replication_date = replication_info[['channel', 'local_role_x', 'peer_role_x', 'peer_state_x',
                                             'sync_percent_x', 'sync_state_x', 'sender_sent_location',
                                             'sender_write_location', 'sender_flush_location', 'sender_replay_location',
                                             'receiver_received_location', 'receiver_write_location',
                                             'receiver_flush_location', 'receiver_replay_location', 'receive_gap',
                                             'replay_gap']].reset_index()
        replication_date.rename(
            columns={'channel_x': 'channel', 'local_role_x': 'local_role',
                     'peer_role_x': 'peer_role', 'peer_state_x': 'peer_state',
                     'sync_percent_x': 'sync_percent', 'sync_state_x': 'sync_state'}, inplace=True)
        replication_date.pop('index')
        replication_date.replace(to_replace='None', value='-', inplace=True)
        replication_date.fillna('-', inplace=True)
        depart_date = replication_date.to_json(orient='records')
        up_depart = (depart_date[1:-1]).replace("},", "}!")
        up_depart.split("!")
        up_depart = json.loads(depart_date)
        if isinstance(depart[1], str):
            de_depart = []
        else:
            depart[1].fillna('-', inplace=True)
            depart_date_de = depart[1].to_json(orient='records')
            de_depart = (depart_date_de[1:-1]).replace("},", "}!")
            de_depart.split("!")
            de_depart = json.loads(depart_date_de)
        return up_depart, de_depart

    @staticmethod
    def split_date(old_replication):
        old_replication_ob = copy.deepcopy(old_replication[0])
        old_replication_ob['sender_sent_location'] = 0
        old_replication_ob['sender_write_location'] = 0
        old_replication_ob['sender_flush_location'] = 0
        old_replication_ob['sender_replay_location'] = 0
        old_replication_ob['receiver_received_location'] = 0
        old_replication_ob['receiver_write_location'] = 0
        old_replication_ob['receiver_flush_location'] = 0
        old_replication_ob['receiver_replay_location'] = 0
        old_replication_ob['receive_gap'] = old_replication_ob['receive_gap'] / 1024
        old_replication_ob['replay_gap'] = old_replication_ob['replay_gap'] / 1024
        old_replication_ob.fillna('-', inplace=True)
        depart_date_up = old_replication_ob.to_json(orient='records')
        up_depart = (depart_date_up[1:-1]).replace("},", "}!")
        up_depart.split("!")
        up_depart = json.loads(depart_date_up)
        if isinstance(old_replication[1],str):
            de_depart = []
        else:
            old_replication[1].fillna('-', inplace=True)
            depart_date_de = old_replication[1].to_json(orient='records')
            de_depart = (depart_date_de[1:-1]).replace("},", "}!")
            de_depart.split("!")
            de_depart = json.loads(depart_date_de)
        return up_depart, de_depart
