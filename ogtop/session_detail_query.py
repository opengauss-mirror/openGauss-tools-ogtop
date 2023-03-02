# -*- coding: utf-8 -*-
""" session_detail data storage calculation """
import copy


class SessionDetailMain(object):

    def __init__(self, openGauss_option, sqls, params):
        """
        initialization session_detail data
        :param openGauss_option: openGauss database operation object
        :param sqls: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        self.de_part = openGauss_option.params_query_sql(sqls, params)

    @staticmethod
    def intergrated_data(session_detail_data):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param session_detail_data: refresh data this time
        :return: return the computer result
        """
        if len(session_detail_data[0]) != 29:
            data = {
                "query_id": str(session_detail_data[0][22]),
                "state_sum": str(session_detail_data[0][2]) if session_detail_data[0][2] else "-",
                "backend_start": str(session_detail_data[0][21]).split('+', 1)[0],
                "query_start": str(session_detail_data[0][20]).split('+', 1)[0] if session_detail_data[0][20] else '-',
                "datid": str(session_detail_data[0][15]),
                "datname": str(session_detail_data[0][16]),
                "usesysid": str(session_detail_data[0][9]),
                "usename": str(session_detail_data[0][10]),
                "application_name": str(session_detail_data[0][11]),
                "client_addr": str(session_detail_data[0][12]) if session_detail_data[0][12] else '-',
                "client_hostname": str(session_detail_data[0][13]) if session_detail_data[0][13] else '-',
                "client_port": str(session_detail_data[0][14]) if session_detail_data[0][14] else '-',
                "xact_start_time": str(session_detail_data[0][17]).split('+', 1)[0] if session_detail_data[0][
                    17] else '-',
                "state_change_time": str(session_detail_data[0][18]).split('+', 1)[0],
                "waiting": str(session_detail_data[0][19]),
                "total_cpu_time": str(session_detail_data[0][24]) if session_detail_data[0][24] else '0',

                "db_time": str(session_detail_data[0][3]) if len(session_detail_data) == 9 and session_detail_data[0][
                    3] else "0",
                "cpu_time": str(session_detail_data[1][3]) if len(session_detail_data) == 9 and session_detail_data[1][
                    3] else "0",
                "execute_time": str(session_detail_data[2][3]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[2][3] else "0",
                "parse_time": str(session_detail_data[3][3]) if len(session_detail_data) == 9 and
                                                                session_detail_data[3][3] else "0",
                "plan_time": str(session_detail_data[4][3]) if len(session_detail_data) == 9 and session_detail_data[4][
                    3] else "0",
                "rewrite_time": str(session_detail_data[5][3]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[5][3] else "0",
                "pl_execuyion_time": str(session_detail_data[6][3]) if len(session_detail_data) == 9 and
                                                                       session_detail_data[6][3] else "0",
                "pl_compilation_time": str(session_detail_data[7][3]) if len(session_detail_data) == 9 and
                                                                         session_detail_data[7][
                                                                             3] else "0",
                "net_send_time": str(session_detail_data[8][3]) if len(session_detail_data) == 9 and
                                                                   session_detail_data[8][3] else "0",
                "data_io_time": str(session_detail_data[9][3]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[9][3] else "0",

                "init_mem": "-",
                "used_mem": "-",
                "peak_mem": "-",
                "wait_status": str(session_detail_data[0][6]) if session_detail_data[0][6] and session_detail_data[0][
                    6] != None else '-',
                "locktag": str(session_detail_data[0][5]) if session_detail_data[0][5] else '-',
                "lockmode": str(session_detail_data[0][7]) if session_detail_data[0][7] else '-',
                "block_sessionid": str(session_detail_data[0][8]) if session_detail_data[0][8] else '-',
                "query": str(session_detail_data[0][23])
            }
        else:
            data = {
                "query_id": str(session_detail_data[0][26]),
                "state_sum": str(session_detail_data[0][3]) if session_detail_data[0][3] else '-',
                "backend_start": str(session_detail_data[0][25]).split('+', 1)[0],  ##
                "query_start": str(session_detail_data[0][24]).split('+', 1)[0],
                "datid": str(session_detail_data[0][19]),
                "datname": str(session_detail_data[0][20]),
                "usesysid": str(session_detail_data[0][13]),
                "usename": str(session_detail_data[0][14]),
                "application_name": str(session_detail_data[0][15]),
                "client_addr": str(session_detail_data[0][16]),
                "client_hostname": str(session_detail_data[0][17]) if session_detail_data[0][17] else '-',
                "client_port": str(session_detail_data[0][18]),
                "xact_start_time": str(session_detail_data[0][21]).split('+', 1)[0] if session_detail_data[0][
                    21] else '-',
                ##
                "state_change_time": str(session_detail_data[0][22]).split('+', 1)[0],
                "waiting": str(session_detail_data[0][23]),
                "total_cpu_time": str(session_detail_data[0][28]) if session_detail_data[0][28] else "0",

                "db_time": str(session_detail_data[0][4]) if len(session_detail_data) == 9 and session_detail_data[0][
                    4] else "0",
                "cpu_time": str(session_detail_data[1][4]) if len(session_detail_data) == 9 and session_detail_data[1][
                    4] else "0",
                "execute_time": str(session_detail_data[2][4]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[2][4] else "0",
                "parse_time": str(session_detail_data[3][4]) if len(session_detail_data) == 9 and
                                                                session_detail_data[3][4] else "0",
                "plan_time": str(session_detail_data[4][4]) if len(session_detail_data) == 9 and session_detail_data[4][
                    4] else "0",
                "rewrite_time": str(session_detail_data[5][4]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[5][4] else "0",
                "pl_execuyion_time": str(session_detail_data[6][4]) if len(session_detail_data) == 9 and
                                                                       session_detail_data[6][4] else "0",
                "pl_compilation_time": str(session_detail_data[7][4]) if len(session_detail_data) == 9 and
                                                                         session_detail_data[7][
                                                                             4] else "0",
                "net_send_time": str(session_detail_data[8][4]) if len(session_detail_data) == 9 and
                                                                   session_detail_data[8][4] else "0",
                "data_io_time": str(session_detail_data[9][4]) if len(session_detail_data) == 9 and
                                                                  session_detail_data[9][4] else "0",

                "init_mem": str(session_detail_data[0][6]) if session_detail_data[0][6] else "-",
                "used_mem": str(session_detail_data[0][7]) if session_detail_data[0][7] else "-",
                "peak_mem": str(session_detail_data[0][8]) if session_detail_data[0][8] else "-",
                "wait_status": str(session_detail_data[0][10]) if session_detail_data[0][10] and session_detail_data[0][10] != None else "-",
                "locktag": str(session_detail_data[0][9]) if session_detail_data[0][9] and session_detail_data[0][9] != None else "-",
                "lockmode": str(session_detail_data[0][11]) if session_detail_data[0][11] and session_detail_data[0][11] != None else "-",
                "block_sessionid": str(session_detail_data[0][12]) if session_detail_data[0][11] and session_detail_data[0][11] != None else "-",
                "query": str(session_detail_data[0][27]) if session_detail_data[0][27] else "-"}
        return data

    @staticmethod
    def compute_delta(depart, old_depart):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_depart: last refresh data
        :param depart: refresh data this time
        :return: return the computer result
        """
        computed_depart = copy.deepcopy(depart)
        computed_depart["total_cpu_time"] = str(round((int(depart["total_cpu_time"]) - int(old_depart["total_cpu_time"])) / 1000000, 4))
        computed_depart["db_time"] = str(round((int(depart["db_time"]) - int(old_depart["db_time"])) / 1000000,4))
        computed_depart["cpu_time"] = str(round((int(depart["cpu_time"]) - int(old_depart["cpu_time"])) / 1000000,4))
        computed_depart["execute_time"] = str(round((int(depart["execute_time"]) - int(old_depart["execute_time"])) / 1000000,4))
        computed_depart["data_io_time"] = str(round((int(depart["data_io_time"]) - int(old_depart["data_io_time"])) / 1000000,4))
        computed_depart["parse_time"] = str(round((int(depart["parse_time"]) - int(old_depart["parse_time"])) / 1000000,4))
        computed_depart["plan_time"] = str(round((int(depart["plan_time"]) - int(old_depart["plan_time"])) / 1000000.4))
        computed_depart["rewrite_time"] = str(round((int(depart["rewrite_time"]) - int(old_depart["rewrite_time"])) / 1000000,4))
        computed_depart["pl_execuyion_time"] = str(round((int(depart["pl_execuyion_time"]) - int(old_depart["pl_execuyion_time"])) / 1000000,4))
        computed_depart["pl_compilation_time"] = str(round((int(depart["pl_compilation_time"]) - int(old_depart["pl_compilation_time"])) / 1000000,4))
        computed_depart["net_send_time"] = str(round((int(depart["net_send_time"]) - int(old_depart["net_send_time"])) / 1000000,4))
        return computed_depart
