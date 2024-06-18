# -*- coding: utf-8 -*-
""" table_performance data storage calculation """

import json
import pandas as pd


class TablePerformanceMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization table data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            table_df = data
        else:
            if data == []:
                table_df = "The query data is empty!"
            else:
                table_df = pd.DataFrame(data)
                table_df.columns = ["datname", "schemaname", "relname", "relid",
                                    "seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd",
                                    "n_tup_del", "n_tup_hot_upd", "heap_blks_read", "heap_blks_hit", "idx_blks_read",
                                    "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read",
                                    "tidx_blks_hit","phyrds", "phywrts", "phyblkrd", "phyblkwrt", "readtim", "writetim"]
                # table_df[["n_live_tup", 'n_dead_tup']] = table_df[["n_live_tup", 'n_dead_tup']].astype(int).astype(str)

        self.de_part = table_df

    @staticmethod
    def intergrated_data(table_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param table_data: refresh data this time
        :return: return the computer result
        """
        data_list = table_data[
            ["datname", "schemaname", "relname", "relid",
             "seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd",
             "n_tup_del", "n_tup_hot_upd", "heap_blks_read", "heap_blks_hit", "idx_blks_read",
             "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit",
             "phyrds", "phywrts", "phyblkrd", "phyblkwrt", "readtim", "writetim"]].to_json(
            orient='records')
        data_table = (data_list[1:-1]).replace("},", "}!")
        data_table.split("!")
        data_table = json.loads(data_list)
        return data_table

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        new_table_depart = pd.DataFrame(depart)
        new_table_depart.columns = ["datname", "schemaname", "relname", "relid","seq_scan1", "seq_tup_read1", "idx_scan1", "idx_tup_fetch1",
                                    "n_tup_ins1", "n_tup_upd1", "n_tup_del1", "n_tup_hot_upd1", "heap_blks_read1",
                                    "heap_blks_hit1", "idx_blks_read1", "idx_blks_hit1", "toast_blks_read1",
                                    "toast_blks_hit1", "tidx_blks_read1", "tidx_blks_hit1", "phyrds1", "phywrts1",
                                    "phyblkrd1", "phyblkwrt1", "readtim1", "writetim1"]

        old_table = pd.DataFrame(old_depart)
        old_table.columns = ["datname", "schemaname", "relname", "relid", "seq_scan2", "seq_tup_read2", "idx_scan2", "idx_tup_fetch2", "n_tup_ins2",
                             "n_tup_upd2", "n_tup_del2", "n_tup_hot_upd2", "heap_blks_read2", "heap_blks_hit2",
                             "idx_blks_read2", "idx_blks_hit2", "toast_blks_read2", "toast_blks_hit2",
                             "tidx_blks_read2", "tidx_blks_hit2", "phyrds2", "phywrts2", "phyblkrd2", "phyblkwrt2",
                             "readtim2", "writetim2"]

        old_table_depart = old_table[
            ['seq_scan2', 'seq_tup_read2', 'idx_scan2', 'idx_tup_fetch2', 'n_tup_ins2', "n_tup_upd2", "n_tup_del2",
             "n_tup_hot_upd2", "heap_blks_read2", "heap_blks_hit2", "idx_blks_read2", "idx_blks_hit2",
             "toast_blks_read2", "toast_blks_hit2", "tidx_blks_read2", "tidx_blks_hit2", "readtim2", "phyrds2",
             "phywrts2", "phyblkrd2", "phyblkwrt2", "writetim2"]]

        res_table_depart = pd.concat([new_table_depart, old_table_depart], axis=1)
        res_table_depart['seq_scan'] = round(res_table_depart['seq_scan1'] - res_table_depart['seq_scan2'])
        res_table_depart['seq_tup_read'] = round(res_table_depart['seq_tup_read1'] - res_table_depart['seq_tup_read2'])
        res_table_depart['idx_scan'] = round(res_table_depart['idx_scan1'] - res_table_depart['idx_scan2'])
        res_table_depart['idx_tup_fetch'] = round(
            res_table_depart['idx_tup_fetch1'] - res_table_depart['idx_tup_fetch2'])
        res_table_depart['n_tup_ins'] = round(res_table_depart['n_tup_ins1'] - res_table_depart['n_tup_ins2'])
        res_table_depart['n_tup_upd'] = round(res_table_depart['n_tup_upd1'] - res_table_depart['n_tup_upd2'])
        res_table_depart['n_tup_del'] = round(res_table_depart['n_tup_del1'] - res_table_depart['n_tup_del2'])
        res_table_depart['n_tup_hot_upd'] = round(
            res_table_depart['n_tup_hot_upd1'] - res_table_depart['n_tup_hot_upd2'])
        res_table_depart['heap_blks_read'] = round(
            res_table_depart['heap_blks_read1'] - res_table_depart['heap_blks_read2'])
        res_table_depart['heap_blks_hit'] = round(
            res_table_depart['heap_blks_hit1'] - res_table_depart['heap_blks_hit2'])
        res_table_depart['idx_blks_read'] = round(
            res_table_depart['idx_blks_read1'] - res_table_depart['idx_blks_read2'])
        res_table_depart['idx_blks_hit'] = round(res_table_depart['idx_blks_hit1'] - res_table_depart['idx_blks_hit2'])
        res_table_depart['toast_blks_read'] = round(
            res_table_depart['toast_blks_read1'] - res_table_depart['toast_blks_read2'])
        res_table_depart['toast_blks_hit'] = round(
            res_table_depart['toast_blks_hit1'] - res_table_depart['toast_blks_hit2'])
        res_table_depart['tidx_blks_read'] = round(
            res_table_depart['tidx_blks_read1'] - res_table_depart['tidx_blks_read2'])
        res_table_depart['tidx_blks_hit'] = round(
            res_table_depart['tidx_blks_hit1'] - res_table_depart['tidx_blks_hit2'])
        res_table_depart['phyrds'] = round(res_table_depart['phyrds1'] - res_table_depart['phyrds2'])
        res_table_depart['phywrts'] = round(res_table_depart['phywrts1'] - res_table_depart['phywrts2'])
        res_table_depart['phyblkrd'] = round(res_table_depart['phyblkrd1'] - res_table_depart['phyblkrd2'])
        res_table_depart['phyblkwrt'] = round(res_table_depart['phyblkwrt1'] - res_table_depart['phyblkwrt2'])
        res_table_depart['readtim'] = round(res_table_depart['readtim1'] - res_table_depart['readtim2'])
        res_table_depart['writetim'] = round(res_table_depart['writetim1'] - res_table_depart['writetim2'])
        table_depart = res_table_depart[
            ['datname', 'schemaname', 'relname', "relid", "seq_scan",
             "seq_tup_read", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
             "heap_blks_read", "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit",
             "tidx_blks_read", "tidx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt", "readtim", "writetim"]]
        table_depart[["seq_scan", 'seq_tup_read', 'idx_scan', 'idx_tup_fetch', 'n_tup_ins', 'n_tup_upd', 'n_tup_del',
                      'n_tup_hot_upd', 'heap_blks_read', 'heap_blks_hit', 'idx_blks_read', 'idx_blks_hit',
                      'toast_blks_read', 'toast_blks_hit', 'tidx_blks_read', 'tidx_blks_hit', 'readtim', 'writetim']] = \
            table_depart[
                ["seq_scan", 'seq_tup_read', 'idx_scan', 'idx_tup_fetch', 'n_tup_ins', 'n_tup_upd', 'n_tup_del',
                 'n_tup_hot_upd', 'heap_blks_read', 'heap_blks_hit', 'idx_blks_read', 'idx_blks_hit',
                 'toast_blks_read', 'toast_blks_hit', 'tidx_blks_read', 'tidx_blks_hit', 'readtim', 'writetim']].astype(
                int)
        table_depart.replace(to_replace='None', value='-', inplace=True)
        table_depart.fillna('-', inplace=True)
        depart_table = table_depart.to_json(orient='records')
        depart_depart = (depart_table[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(depart_table)
        return depart_depart

    @staticmethod
    def split_date(old_ob):
        old_ob_df = pd.DataFrame(old_ob)
        old_ob_df.columns = ["datname", "schemaname", "relname", "relid","seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
                             "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "heap_blks_read",
                             "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read",
                             "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit", "phyrds", "phywrts",
                             "phyblkrd", "phyblkwrt", "readtim", "writetim"]
        old_ob_df['seq_scan'] = old_ob_df['seq_scan'].apply(lambda x: 0)
        old_ob_df['seq_tup_read'] = old_ob_df['seq_tup_read'].apply(lambda x: 0)
        old_ob_df['idx_scan'] = old_ob_df['idx_scan'].apply(lambda x: 0)
        old_ob_df['idx_tup_fetch'] = old_ob_df['idx_tup_fetch'].apply(lambda x: 0)
        old_ob_df['n_tup_ins'] = old_ob_df['n_tup_ins'].apply(lambda x: 0)
        old_ob_df['n_tup_upd'] = old_ob_df['n_tup_upd'].apply(lambda x: 0)
        old_ob_df['n_tup_del'] = old_ob_df['n_tup_del'].apply(lambda x: 0)
        old_ob_df['n_tup_hot_upd'] = old_ob_df['n_tup_hot_upd'].apply(lambda x: 0)
        old_ob_df['heap_blks_read'] = old_ob_df['heap_blks_read'].apply(lambda x: 0)
        old_ob_df['heap_blks_hit'] = old_ob_df['heap_blks_hit'].apply(lambda x: 0)
        old_ob_df['idx_blks_read'] = old_ob_df['idx_blks_read'].apply(lambda x: 0)
        old_ob_df['idx_blks_hit'] = old_ob_df['idx_blks_hit'].apply(lambda x: 0)
        old_ob_df['toast_blks_read'] = old_ob_df['toast_blks_read'].apply(lambda x: 0)
        old_ob_df['toast_blks_hit'] = old_ob_df['toast_blks_hit'].apply(lambda x: 0)
        old_ob_df['tidx_blks_read'] = old_ob_df['tidx_blks_read'].apply(lambda x: 0)
        old_ob_df['tidx_blks_hit'] = old_ob_df['tidx_blks_hit'].apply(lambda x: 0)
        old_ob_df['phyrds'] = old_ob_df['phyrds'].apply(lambda x: 0)
        old_ob_df['phywrts'] = old_ob_df['phywrts'].apply(lambda x: 0)
        old_ob_df['phyblkrd'] = old_ob_df['phyblkrd'].apply(lambda x: 0)
        old_ob_df['phyblkwrt'] = old_ob_df['phyblkwrt'].apply(lambda x: 0)
        old_ob_df['readtim'] = old_ob_df['readtim'].apply(lambda x: 0)
        old_ob_df['writetim'] = old_ob_df['writetim'].apply(lambda x: 0)
        old_ob_df.replace(to_replace='None', value='-', inplace=True)
        old_ob_df.fillna('-', inplace=True)
        depart_table = old_ob_df.to_json(orient='records')
        depart_depart = (depart_table[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(depart_table)
        return depart_depart
