# -*- coding: utf-8 -*-
"""database data storage calculation"""
import json
import datetime


class DatabaseMain(object):

    def __init__(self, OpenGaussOption, sql, params):
        """
        initialization database data
        :param OpenGaussOption: openGauss database operation object
        :param sqls: sql execute specified sql statement
        :param params: sql parameters inquiry
        """
        database_data = OpenGaussOption.query_sql(sql, (params,))
        if isinstance(database_data, str):
            self.de_part = database_data
        else:
            if database_data == []:
                self.de_part = "The query data is empty!"
            else:
                # load → os_load_processes
                # db_state → state
                # total_writes → double_writes
                # writetim → redo_writetim
                if len(database_data) == 54:
                    self.de_part = {
                        "Start_Date": datetime.datetime.strftime(database_data[0], "%Y-%m-%d"),
                        "Start_Time": datetime.datetime.strftime(database_data[0], "%H:%M:%S"),
                        "state": database_data[14] if database_data[14] else "-",
                        "LocalRoll": database_data[15] if database_data[15] else "single",
                        "ProcessMem": database_data[45],
                        "DynamicMem": database_data[47],
                        "SharedMem": database_data[48],
                        "Sessions": database_data[50] if database_data[50] else 0,

                        "Actsess": database_data[49] if database_data[49] else 0,
                        "Waiting": database_data[52] if database_data[52] else 0,
                        "idleintran": database_data[51] if database_data[51] else 0,
                        "xact_commit": database_data[23] if database_data[23] else 0,
                        "xact_rollback": database_data[24] if database_data[24] else 0,
                        "totalcount": database_data[44] if database_data[44] else 0,
                        "ddl_count": database_data[41] if database_data[41] else 0,
                        "dml_count": database_data[42] if database_data[42] else 0,

                        "dcl_count": database_data[43] if database_data[43] else 0,
                        "select_count": database_data[37] if database_data[37] else 0,
                        "update_count": database_data[38] if database_data[38] else 0,
                        "insert_count": database_data[39] if database_data[39] else 0,
                        "delete_count": database_data[40] if database_data[40] else 0,
                        "blks_read": database_data[25] if database_data[25] else 0,
                        "blks_hit": database_data[26] if database_data[26] else 0,
                        "hitratio": database_data[22] if database_data[22] else 0,

                        "tup_fetched": database_data[27] if database_data[27] else 0,
                        "tup_inserted": database_data[28] if database_data[28] else 0,
                        "tup_updated": database_data[29] if database_data[29] else 0,
                        "tup_deleted": database_data[30] if database_data[30] else 0,
                        "DB_TIME": database_data[17] if database_data[17] else 0,
                        "CPU_TIME": int(database_data[18]) if database_data[18] else 0,
                        "EXECUTION_TIME": int(database_data[19]) if database_data[19] else 0,
                        "DATA_IO_TIME": int(database_data[16]) if database_data[16] else 0,

                        "blk_read_time": database_data[31] if database_data[31] else 0,
                        "blk_write_time": database_data[32] if database_data[32] else 0,
                        "redo_writetim": database_data[53] if database_data[53] else 0,
                        "temp_files": database_data[34] if database_data[34] else 0,
                        "temp_bytes": database_data[35] if database_data[35] else 0,
                        "conflicts": database_data[33] if database_data[33] else 0,
                        "deadlocks": database_data[36] if database_data[36] else 0,
                        "checkpoints": database_data[21] if database_data[21] else 0,

                        "double_writes": database_data[20] if database_data[20] else 0,
                        "max_dynamic_memory": database_data[46] if database_data[46] else "0",
                        "num_cpus": int(database_data[1]) if database_data[1] else 0,
                        "num_cpu_cores": int(database_data[2]) if database_data[2] else 0,
                        "num_cpu_sockets": int(database_data[3]) if database_data[3] else 0,
                        "physical_memory_bytes": int(database_data[4]) if database_data[4] else 0,
                        "idle_time": int(database_data[5]) if database_data[5] else 0,
                        "busy_time": int(database_data[6]) if database_data[6] else 0,

                        "user_time": int(database_data[7]) if database_data[7] else 0,
                        "sys_time": int(database_data[8]) if database_data[8] else 0,
                        "iowait_time": int(database_data[9]) if database_data[9] else 0,
                        "nice_time": int(database_data[10]) if database_data[10] else 0,
                        "vm_page_in_bytes": int(database_data[11]) if database_data[11] else 0,
                        "vm_page_out_bytes": int(database_data[12]) if database_data[12] else 0,
                        "os_load_processes": float(database_data[13]) if database_data[13] else 0,
                    }
                else:
                    self.de_part = {
                        "Start_Date": datetime.datetime.strftime(database_data[0], "%Y-%m-%d"),
                        "Start_Time": datetime.datetime.strftime(database_data[0], "%H:%M:%S"),
                        "state": database_data[14] if database_data[14] else "-",
                        "LocalRoll": database_data[15] if database_data[15] else "single",
                        "ProcessMem": '-',
                        "DynamicMem": '-',
                        "SharedMem": '-',
                        "Sessions": database_data[46] if database_data[46] else 0,

                        "Actsess": database_data[45] if database_data[45] else 0,
                        "Waiting": database_data[48] if database_data[48] else 0,
                        "idleintran": database_data[47] if database_data[47] else 0,
                        "xact_commit": database_data[23] if database_data[23] else 0,
                        "xact_rollback": database_data[24] if database_data[24] else 0,
                        "totalcount": database_data[44] if database_data[44] else 0,
                        "ddl_count": database_data[41] if database_data[41] else 0,
                        "dml_count": database_data[42] if database_data[42] else 0,

                        "dcl_count": database_data[43] if database_data[43] else 0,
                        "select_count": database_data[37] if database_data[37] else 0,
                        "update_count": database_data[38] if database_data[38] else 0,
                        "insert_count": database_data[39] if database_data[39] else 0,
                        "delete_count": database_data[40] if database_data[40] else 0,
                        "blks_read": database_data[25] if database_data[25] else 0,
                        "blks_hit": database_data[26] if database_data[26] else 0,
                        "hitratio": database_data[22] if database_data[22] else 0,

                        "tup_fetched": database_data[27] if database_data[27] else 0,
                        "tup_inserted": database_data[28] if database_data[28] else 0,
                        "tup_updated": database_data[29] if database_data[29] else 0,
                        "tup_deleted": database_data[30] if database_data[30] else 0,
                        "DB_TIME": database_data[17] if database_data[17] else 0,
                        "CPU_TIME": int(database_data[18]) if database_data[18] else 0,
                        "EXECUTION_TIME": int(database_data[19]) if database_data[19] else 0,
                        "DATA_IO_TIME": int(database_data[16]) if database_data[16] else 0,

                        "blk_read_time": int("".join(list(filter(str.isdigit, database_data[31])))) if database_data[
                            31] else 0,
                        "blk_write_time": int("".join(list(filter(str.isdigit, database_data[32])))) if database_data[
                            32] else 0,
                        "redo_writetim": database_data[49] if database_data[49] else 0,
                        "temp_files": database_data[34] if database_data[34] else 0,
                        "temp_bytes": database_data[35] if database_data[35] else 0,
                        "conflicts": int("".join(list(filter(str.isdigit, database_data[33])))) if database_data[
                            33] else 0,
                        "deadlocks": database_data[36] if database_data[36] else 0,
                        "checkpoints": database_data[21] if database_data[21] else 0,

                        "double_writes": int(database_data[20]) if database_data[20] else 0,
                        "max_dynamic_memory": "-",
                        "num_cpus": int(database_data[1]) if database_data[1] else 0,
                        "num_cpu_cores": int(database_data[2]) if database_data[2] else 0,
                        "num_cpu_sockets": int(database_data[3]) if database_data[3] else 0,
                        "physical_memory_bytes": int(database_data[4]) if database_data[4] else 0,
                        "idle_time": int(database_data[5]) if database_data[5] else 0,
                        "busy_time": int(database_data[6]) if database_data[6] else 0,

                        "user_time": int(database_data[7]) if database_data[7] else 0,
                        "sys_time": int(database_data[8]) if database_data[8] else 0,
                        "iowait_time": int(database_data[9]) if database_data[9] else 0,
                        "nice_time": int(database_data[10]) if database_data[10] else 0,
                        "vm_page_in_bytes": int(database_data[11]) if database_data[11] else 0,
                        "vm_page_out_bytes": int(database_data[12]) if database_data[12] else 0,
                        "os_load_processes": float(database_data[13]) if database_data[13] else 0,
                    }

    @staticmethod
    def compute_delta(old_ob, new_ob):
        """
        calculate the increment，save the result to dictionary，transfer and display to the interface
        :param old_ob: last refresh data
        :param new_ob: refresh data this time
        :return: return the computer result
        """
        if not (isinstance(old_ob, DatabaseMain) and isinstance(new_ob, DatabaseMain)):
            raise Exception('The parameter object type passed in is incorrect！')
        delta_de = {
            "start_date": old_ob.de_part['Start_Date'],
            "start_time": old_ob.de_part['Start_Time'],
            "wal_status": new_ob.de_part['state'] if new_ob.de_part['state'] else '-',
            "LocalRole": 'single' if new_ob.de_part['LocalRoll'] == '' else new_ob.de_part['LocalRoll'],
            "ProcesMem": new_ob.de_part['ProcessMem'],
            "Dynamic_Mem": new_ob.de_part['DynamicMem'],
            "Shared_Mem": new_ob.de_part['SharedMem'],

            "Session": new_ob.de_part['Sessions'],
            "Actsess": new_ob.de_part['Actsess'],
            "Waiting": '-' if new_ob.de_part['Waiting'] == 'N' else new_ob.de_part['Waiting'],
            "idleintran": new_ob.de_part['idleintran'],
            "xact_commit": new_ob.de_part['xact_commit'] - old_ob.de_part['xact_commit'],
            "xact_rollback": new_ob.de_part['xact_rollback'] - old_ob.de_part['xact_rollback'],
            "totalcount": new_ob.de_part['totalcount'] - old_ob.de_part['totalcount'],

            "ddl_count": new_ob.de_part['ddl_count'] - old_ob.de_part['ddl_count'],
            "dml_count": new_ob.de_part['dml_count'] - old_ob.de_part['dml_count'],
            "dcl_count": new_ob.de_part['dcl_count'] - old_ob.de_part['dcl_count'],
            "select_count": new_ob.de_part['select_count'] - old_ob.de_part['select_count'],
            "update_count": new_ob.de_part['update_count'] - old_ob.de_part['update_count'],
            "insert_count": new_ob.de_part['insert_count'] - old_ob.de_part['insert_count'],
            "delete_count": new_ob.de_part['delete_count'] - old_ob.de_part['delete_count'],

            "blks_read": new_ob.de_part['blks_read'] - old_ob.de_part['blks_read'],
            "blks_hit": new_ob.de_part['blks_hit'] - old_ob.de_part['blks_hit'],
            "tup_fetched": new_ob.de_part['tup_fetched'] - old_ob.de_part['tup_fetched'],
            "tup_inserted": new_ob.de_part['tup_inserted'] - old_ob.de_part['tup_inserted'],
            "tup_update": new_ob.de_part['tup_updated'] - old_ob.de_part['tup_updated'],
            "tup_delete": new_ob.de_part['tup_deleted'] - old_ob.de_part['tup_deleted'],

            "DB_TIME": (int(new_ob.de_part['DB_TIME']) - int(old_ob.de_part['DB_TIME'])) if (int(
                new_ob.de_part['DB_TIME']) - int(old_ob.de_part['DB_TIME'])) == 0 else round((int(
                new_ob.de_part['DB_TIME']) - int(old_ob.de_part['DB_TIME'])) / 1000000, 4),
            "cpu_time": new_ob.de_part['CPU_TIME'] - old_ob.de_part['CPU_TIME'] if new_ob.de_part['CPU_TIME'] -
                                                                                   old_ob.de_part[
                                                                                       'CPU_TIME'] == 0 else round(
                (new_ob.de_part['CPU_TIME'] - old_ob.de_part['CPU_TIME']) / 1000000, 4),
            "execute_time": new_ob.de_part['EXECUTION_TIME'] - old_ob.de_part['EXECUTION_TIME'] if new_ob.de_part[
                                                                                                       'EXECUTION_TIME'] -
                                                                                                   old_ob.de_part[
                                                                                                       'EXECUTION_TIME'] == 0 else round(
                (new_ob.de_part['EXECUTION_TIME'] - old_ob.de_part['EXECUTION_TIME']) / 1000000, 4),
            "data_io_time": new_ob.de_part['DATA_IO_TIME'] - old_ob.de_part['DATA_IO_TIME'] if new_ob.de_part[
                                                                                                   'DATA_IO_TIME'] -
                                                                                               old_ob.de_part[
                                                                                                   'DATA_IO_TIME'] == 0 else round(
                (new_ob.de_part['DATA_IO_TIME'] - old_ob.de_part['DATA_IO_TIME']) / 1000000, 4),
            "blk_read_time": new_ob.de_part['blk_read_time'] - old_ob.de_part['blk_read_time'] if new_ob.de_part[
                                                                                                      'blk_read_time'] -
                                                                                                  old_ob.de_part[
                                                                                                      'blk_read_time'] == 0 else round(
                (new_ob.de_part['blk_read_time'] - old_ob.de_part['blk_read_time']) / 1000, 4),
            "blk_write_time": new_ob.de_part['blk_write_time'] - old_ob.de_part['blk_write_time'] if new_ob.de_part[
                                                                                                         'blk_write_time'] -
                                                                                                     old_ob.de_part[
                                                                                                         'blk_write_time'] == 0 else round(
                (new_ob.de_part['blk_write_time'] - old_ob.de_part['blk_write_time']) / 1000, 4),
            "redo_writetim": new_ob.de_part['redo_writetim'] - old_ob.de_part['redo_writetim'] if new_ob.de_part[
                                                                                                      'redo_writetim'] -
                                                                                                  old_ob.de_part[
                                                                                                      'redo_writetim'] == 0 else round(
                (new_ob.de_part['redo_writetim'] - old_ob.de_part['redo_writetim']) / 1000, 4),

            "temp_files": new_ob.de_part['temp_files'],
            "temp_bytes": new_ob.de_part['temp_bytes'],
            "conflicts": new_ob.de_part['conflicts'] - old_ob.de_part['conflicts'],
            "deadlocks": new_ob.de_part['deadlocks'] - old_ob.de_part['deadlocks'],
            "checkpoints": new_ob.de_part['checkpoints'] - old_ob.de_part['checkpoints'],
            "double_writes": new_ob.de_part['double_writes'] - old_ob.de_part['double_writes'],

            "max_dynamic_memory": '-' if new_ob.de_part['max_dynamic_memory'] == 'N' else new_ob.de_part[
                'max_dynamic_memory'],
            "num_cpus": new_ob.de_part['num_cpus'],
            "num_cpu_cores": new_ob.de_part['num_cpu_cores'],
            "num_cpu_sockets": new_ob.de_part['num_cpu_sockets'],
            "physical_memory_bytes": new_ob.de_part['physical_memory_bytes'],

            "idle_time": new_ob.de_part['idle_time'] - old_ob.de_part['idle_time'],
            "busy_time": new_ob.de_part['busy_time'] - old_ob.de_part['busy_time'],
            "user_time": new_ob.de_part['user_time'] - old_ob.de_part['user_time'],
            "sys_time": new_ob.de_part['sys_time'] - old_ob.de_part['sys_time'],
            "iowait_time": new_ob.de_part['iowait_time'] - old_ob.de_part['iowait_time'],
            "nice_time": new_ob.de_part['nice_time'] - old_ob.de_part['nice_time'],

            "vm_page_in_bytes": new_ob.de_part['vm_page_in_bytes'] - old_ob.de_part['vm_page_in_bytes'],
            "vm_page_out_bytes": new_ob.de_part['vm_page_out_bytes'] - old_ob.de_part['vm_page_out_bytes'],
            "os_load_processes": new_ob.de_part['os_load_processes'],
        }
        idle_time = 0.0 if delta_de['idle_time'] == 0 else delta_de['idle_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        user_time = 0.0 if delta_de['user_time'] == 0 else delta_de['user_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        sys_time = 0.0 if delta_de['sys_time'] == 0 else delta_de['sys_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        iowait_time = 0.0 if delta_de['iowait_time'] == 0 else delta_de['iowait_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        nice_time = 0.0 if delta_de['nice_time'] == 0 else delta_de['nice_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        busy_time = 0.0 if delta_de['busy_time'] == 0 else delta_de['busy_time'] / (
                delta_de['idle_time'] + delta_de['busy_time']) * 100
        delta_de['idle_time'] = str(round(idle_time, 2)) + '%'
        delta_de['user_time'] = str(round(user_time, 2)) + '%'
        delta_de['sys_time'] = str(round(sys_time, 2)) + '%'
        delta_de['iowait_time'] = str(round(iowait_time, 2)) + '%'
        delta_de['nice_time'] = str(round(nice_time, 2)) + '%'

        delta_de["hitratio"] = 0.0 if delta_de['blks_hit'] == 0 else round(
            delta_de["blks_hit"] / (delta_de["blks_hit"] + delta_de["blks_read"]) * 100, 2)
        delta_up = {
            "active session/session": 0.0 if delta_de['Actsess'] == 0 else delta_de['Actsess'] / delta_de[
                'Session'] * 100,
            "blks_hit/(blks_hit+read)": 0.0 if delta_de['blks_hit'] == 0 else delta_de['blks_hit'] / (
                    delta_de['blks_hit'] + delta_de['blks_read']) * 100,
            "select_count/dml_count": 0.0 if delta_de['select_count'] == 0 else delta_de['select_count'] / delta_de[
                'dml_count'] * 100,
            "DATA_IO_TIME/DB_TIME": 0.0 if delta_de['data_io_time'] == 0 else delta_de['data_io_time'] / delta_de[
                'DB_TIME'] * 100,
            "dynamic_used_memory/max_dynamic_memory": 0.0 if delta_de['max_dynamic_memory'] == '-' else delta_de[
                                                                                                            'Dynamic_Mem'] /
                                                                                                        delta_de[
                                                                                                            'max_dynamic_memory'] * 100,
            "busy_time/all_time": busy_time,
        }
        delta_de = json.dumps(delta_de)
        delta_up = json.dumps(delta_up)
        return delta_up, delta_de

    @staticmethod
    def split_date(old_ob):
        delta_de = {
            "start_date": old_ob.de_part['Start_Date'],
            "start_time": old_ob.de_part['Start_Time'],
            "wal_status": old_ob.de_part['state'] if old_ob.de_part['state'] else '-',
            "LocalRole": 'single' if old_ob.de_part['LocalRoll'] == '' else old_ob.de_part['LocalRoll'],
            "ProcesMem": old_ob.de_part['ProcessMem'],
            "Dynamic_Mem": old_ob.de_part['DynamicMem'],
            "Shared_Mem": old_ob.de_part['SharedMem'],
            "Session": old_ob.de_part['Sessions'],

            "Actsess": old_ob.de_part['Actsess'],
            "Waiting": '-' if old_ob.de_part['Waiting'] == 'N' else old_ob.de_part['Waiting'],
            "idleintran": old_ob.de_part['idleintran'],
            "xact_commit": 0,
            "xact_rollback": 0,
            "totalcount": 0,
            "ddl_count": 0,
            "dml_count": 0,

            "dcl_count": 0,
            "select_count": 0,
            "update_count": 0,
            "insert_count": 0,
            "delete_count": 0,
            "blks_read": 0,
            "blks_hit": 0,
            "hitratio": 0.0,

            "tup_fetched": 0,
            "tup_inserted": 0,
            "tup_update": 0,
            "tup_delete": 0,
            "DB_TIME": 0,
            "cpu_time": 0,
            "execute_time": 0,
            "data_io_time": 0,

            "blk_read_time": 0,
            "blk_write_time": 0,
            "redo_writetim": 0,
            "temp_files": old_ob.de_part['temp_files'],
            "temp_bytes": old_ob.de_part['temp_bytes'],
            "conflicts": 0,
            "deadlocks": 0,
            "checkpoints": 0,

            "double_writes": 0,
            "max_dynamic_memory": '-' if old_ob.de_part['max_dynamic_memory'] == 'N' else old_ob.de_part[
                'max_dynamic_memory'],
            "num_cpus": old_ob.de_part['num_cpus'],
            "num_cpu_cores": old_ob.de_part['num_cpu_cores'],
            "num_cpu_sockets": old_ob.de_part['num_cpu_sockets'],
            "physical_memory_bytes": old_ob.de_part['physical_memory_bytes'],
            'idle_time': 0.0,
            'user_time': 0.0,

            'sys_time': 0.0,
            'iowait_time': 0.0,
            'nice_time': 0.0,
            "vm_page_in_bytes": 0,
            "vm_page_out_bytes": 0,
            "os_load_processes": old_ob.de_part['os_load_processes']

        }
        delta_up = {
            "active session/session": 0.0,
            "blks_hit/(blks_hit+read)": 0.0,
            "select_count/dml_count": 0.0,
            "DATA_IO_TIME/DB_TIME": 0.0,
            "dynamic_used_memory/max_dynamic_memory": 0.0,
            "busy_time/all_time": 0.0,
        }
        delta_de = json.dumps(delta_de)
        delta_up = json.dumps(delta_up)
        return delta_up, delta_de
