# -*- coding: utf-8 -*-
""" memory data storage calculation """
import copy
import json
import pandas as pd


class MemoryMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization memory data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        data = OpenGaussOption.all_query_sql(sql)
        if isinstance(data, str):
            memory_df = data
        else:
            if data == []:
                memory_df = "The query data is empty!"
            else:
                memory_df = pd.DataFrame(data)
                memory_df.columns = ["target_ip", "snaptime", "max_process_memory", "process_used_memory",
                                     "max_dynamic_memory", "dynamic_used_memory", "dynamic_used_shrctx",
                                     "max_backend_memory", "backend_used_memory", "max_shared_memory", "shared_used_memory",
                                     "max_cstore_memory", "cstore_used_memory", "max_sctpcomm_memory",
                                     "sctpcomm_used_memory", "other_used_memory", "gpu_max_dynamic_memory",
                                     "gpu_dynamic_used_memory", "pooler_conn_memory", "pooler_freeconn_memory",
                                     "storage_compress_memory", "udf_reserved_memory"]
                values = {'target_ip': '-'}
                memory_df.fillna(value=values, inplace=True)
                memory_df = memory_df.reset_index()
                memory_df.pop('index')
        self.de_part = memory_df

    @staticmethod
    def intergrated_data(memory_data):
        """
         consolidate the data and convert it to json format
        :param memory_data: data to be converted
        :return:
        """
        data_list = memory_data[
            ["target_ip", "snaptime", "max_process_memory", "process_used_memory", "max_dynamic_memory",
             "dynamic_used_memory", "dynamic_used_shrctx", "max_backend_memory", "backend_used_memory",
             "max_shared_memory", "shared_used_memory", "max_cstore_memory", "cstore_used_memory",
             "max_sctpcomm_memory", "sctpcomm_used_memory", "other_used_memory", "gpu_max_dynamic_memory",
             "gpu_dynamic_used_memory", "pooler_conn_memory", "pooler_freeconn_memory",
             "storage_compress_memory", "udf_reserved_memory"]].to_json(
            orient='records')
        data_memory = (data_list[1:-1]).replace("},", "}!")
        data_memory.split("!")
        data_memory = json.loads(data_list)

        return data_memory

    @staticmethod
    def compute_delta(depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param depart: refresh data this time
        :return: return the computer result
        """
        depart_heard_depart = {}
        depart_depart = copy.deepcopy(depart[0])
        depart_heard_depart["process_memory_used_percent"] = int(depart_depart["process_used_memory"]) / int(
            depart_depart["max_process_memory"]) if depart_depart["max_process_memory"] != 0 else 0
        depart_heard_depart["dynamic_memory_used_percent"] = int(depart_depart["dynamic_used_memory"]) / int(
            depart_depart["max_dynamic_memory"]) if depart_depart["max_dynamic_memory"] != 0 else 0
        depart_heard_depart["backend_memory_used_percent"] = int(depart_depart["backend_used_memory"]) / int(
            depart_depart["max_backend_memory"]) if depart_depart["max_backend_memory"] != 0 else 0
        depart_heard_depart["shared_memory_used_percent"] = int(depart_depart["shared_used_memory"]) / int(
            depart_depart["max_shared_memory"]) if depart_depart["max_shared_memory"] != 0 else 0
        depart_heard_depart["cstore_memory_used_percent"] = int(depart_depart["cstore_used_memory"]) / int(
            depart_depart["max_cstore_memory"]) if depart_depart["max_cstore_memory"] != 0 else 0
        depart_heard_depart["sctpcomm_memory_used_percent"] = int(depart_depart["sctpcomm_used_memory"]) / int(
            depart_depart["max_sctpcomm_memory"]) if depart_depart["max_sctpcomm_memory"] != 0 else 0
        depart_heard_depart["gpu_dynamic_memory_used_percent"] = int(depart_depart["gpu_dynamic_used_memory"]) / int(
            depart_depart["gpu_max_dynamic_memory"]) if depart_depart["gpu_max_dynamic_memory"] != 0 else 0
        return depart_depart, depart_heard_depart
