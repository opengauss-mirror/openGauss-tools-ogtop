# -*- coding: utf-8 -*-
""" table data storage calculation """

import json
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


class TableMain(object):

    def __init__(self, openGauss_option, sqls):
        """
        initialization table data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        """
        data = openGauss_option.all_query_sql(sqls)
        try:
            table_df = pd.DataFrame(data)
            table_df.columns = ["target_ip", "datname", "snaptime", "schemaname", "relname", "last_vacuum",
                                "last_autovacuum", "last_analyze", "last_autoanalyze", "last_data_changed", "tabsize",
                                "idxsize", "totalsize", "n_live_tup", "n_dead_tup", "seq_scan", "seq_tup_read",
                                "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
                                "heap_blks_read", "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read",
                                "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit", "phyrds", "phywrts", "phyblkrd",
                                "phyblkwrt", "readtim", "writetim"]
            values = {'last_vacuum': '-', 'last_autovacuum': '-', 'last_analyze': '-', 'last_autoanalyze': '-',
                      'last_data_changed': '-'}
            table_df.fillna(value=values, inplace=True)
            table_df = table_df.reset_index()
            table_df.pop('index')
            table_df[["n_live_tup", 'n_dead_tup', 'phywrts', 'phyblkrd', 'phyblkwrt']] = \
                table_df[["n_live_tup", 'n_dead_tup', 'phywrts', 'phyblkrd', 'phyblkwrt']].astype(
                    int).astype(str)
            table_df[["snaptime", 'last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
                      'last_data_changed']] = table_df[
                ["snaptime", 'last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze',
                 'last_data_changed']].astype(str)
        except Exception as e:
            table_df = data
        self.de_part = table_df

    @staticmethod
    def intergrated_data(table_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param table_data: refresh data this time
        :return: return the computer result
        """
        data_list = table_data[
            ["target_ip", "datname", "snaptime", "schemaname", "relname", "last_vacuum", "last_autovacuum",
             "last_analyze", "last_autoanalyze", "last_data_changed", "tabsize", "idxsize", "totalsize",
             "n_live_tup", "n_dead_tup", "seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
             "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "heap_blks_read", "heap_blks_hit",
             "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read",
             "tidx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt", "readtim", "writetim"]].to_json(
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
        new_table_depart.columns = ["target_ip", "datname", "snaptime", "schemaname", "relname", "last_vacuum",
                                    "last_autovacuum", "last_analyze", "last_autoanalyze", "last_data_changed",
                                    "tabsize", "idxsize", "totalsize", "n_live_tup", "n_dead_tup", "seq_scan1",
                                    "seq_tup_read1", "idx_scan1", "idx_tup_fetch1", "n_tup_ins1", "n_tup_upd1",
                                    "n_tup_del1", "n_tup_hot_upd1", "heap_blks_read1", "heap_blks_hit1",
                                    "idx_blks_read1", "idx_blks_hit1", "toast_blks_read1", "toast_blks_hit1",
                                    "tidx_blks_read1", "tidx_blks_hit1", "phyrds", "phywrts", "phyblkrd", "phyblkwrt",
                                    "readtim1", "writetim1"]
        old_table = pd.DataFrame(old_depart)
        old_table.columns = ["target_ip", "datname", "snaptime", "schemaname", "relname", "last_vacuum",
                             "last_autovacuum", "last_analyze", "last_autoanalyze", "last_data_changed", "tabsize",
                             "idxsize", "totalsize", "n_live_tup", "n_dead_tup", "seq_scan2", "seq_tup_read2",
                             "idx_scan2", "idx_tup_fetch2", "n_tup_ins2", "n_tup_upd2", "n_tup_del2", "n_tup_hot_upd2",
                             "heap_blks_read2", "heap_blks_hit2", "idx_blks_read2", "idx_blks_hit2", "toast_blks_read2",
                             "toast_blks_hit2", "tidx_blks_read2", "tidx_blks_hit2", "phyrds", "phywrts", "phyblkrd",
                             "phyblkwrt", "readtim2", "writetim2"]
        old_table_depart = old_table[
            ['seq_scan2', 'seq_tup_read2', 'idx_scan2', 'idx_tup_fetch2', 'n_tup_ins2', "n_tup_upd2", "n_tup_del2",
             "n_tup_hot_upd2", "heap_blks_read2", "heap_blks_hit2", "idx_blks_read2", "idx_blks_hit2",
             "toast_blks_read2", "toast_blks_hit2", "tidx_blks_read2", "tidx_blks_hit2", "readtim2", "writetim2"]]

        res_table_depart = pd.concat([new_table_depart, old_table_depart], axis=1)
        res_table_depart['tabsize'] = round(res_table_depart['tabsize'] / 1028 / 1028, 3)
        res_table_depart['idxsize'] = round(res_table_depart['idxsize'] / 1028 / 1028, 3)
        res_table_depart['totalsize'] = round(res_table_depart['totalsize'] / 1028 / 1028, 3)

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
        res_table_depart['readtim'] = round(res_table_depart['readtim1'] - res_table_depart['readtim2'])
        res_table_depart['writetim'] = round(res_table_depart['writetim1'] - res_table_depart['writetim2'])
        table_depart = res_table_depart[
            ["target_ip", "datname", "snaptime", "schemaname", "relname", "last_vacuum", "last_autovacuum",
             "last_analyze", "last_autoanalyze", "last_data_changed", "tabsize", "idxsize", "totalsize",
             "n_live_tup", "n_dead_tup", "seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
             "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "heap_blks_read", "heap_blks_hit",
             "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read",
             "tidx_blks_hit", "phyrds", "phywrts", "phyblkrd", "phyblkwrt", "readtim", "writetim"]]
        table_depart[["seq_scan", 'seq_tup_read', 'idx_scan', 'idx_tup_fetch', 'n_tup_ins', 'n_tup_upd', 'n_tup_del',
                      'n_tup_hot_upd', 'heap_blks_read', 'heap_blks_hit', 'idx_blks_read', 'idx_blks_hit',
                      'toast_blks_read', 'toast_blks_hit', 'tidx_blks_read', 'tidx_blks_hit', 'readtim', 'writetim']] = \
            table_depart[
                ["seq_scan", 'seq_tup_read', 'idx_scan', 'idx_tup_fetch', 'n_tup_ins', 'n_tup_upd', 'n_tup_del',
                 'n_tup_hot_upd', 'heap_blks_read', 'heap_blks_hit', 'idx_blks_read', 'idx_blks_hit',
                 'toast_blks_read', 'toast_blks_hit', 'tidx_blks_read', 'tidx_blks_hit', 'readtim', 'writetim']].astype(
                int)
        values = {'seq_scan': '0', 'seq_tup_read': '0', 'idx_scan': '0', 'idx_tup_fetch': '0',
                  "n_tup_ins": '0', "n_tup_upd": '0', "n_tup_del": '0', "n_tup_hot_upd": '0',
                  "heap_blks_read": '0', "heap_blks_hit": '0', "idx_blks_read": '0', "idx_blks_hit": '0',
                  "toast_blks_read": '0', "toast_blks_hit": '0', "tidx_blks_read": '0', "tidx_blks_hit": '0'
                  }
        table_depart.fillna(value=values, inplace=True)
        table_depart.replace(to_replace='None', value='-', inplace=True)
        table_depart.fillna('-', inplace=True)
        depart_table = table_depart.to_json(orient='records')
        depart_depart = (depart_table[1:-1]).replace("},", "}!")
        depart_depart.split("!")
        depart_depart = json.loads(depart_table)
        return depart_depart
