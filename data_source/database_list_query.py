# -*- coding: utf-8 -*-
"""database data storage calculation"""

import pandas as pd


class DatabaseListMain(object):

    def __init__(self, OpenGaussOption, sql):
        """
        initialization database data
        :param OpenGaussOption: openGauss database operation object
        :param sql: sql execute specified sql statement
        """
        database_list_data = OpenGaussOption.all_query_sql(sql)
        if isinstance(database_list_data, str):
            self.de_part = database_list_data
        else:
            if database_list_data == []:
                self.de_part = "The query data is empty!"
            else:
                database_list_df = pd.DataFrame(database_list_data)
                database_list = ["datid", "datname", "numbackends", "xact_commit", "xact_rollback", "blks_read",
                                 "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
                                 "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks", "blk_read_time",
                                 "blk_write_time", "stats_reset"]
                database_list_df.columns = database_list
                database_list_df.fillna(value=0, inplace=True)
                data_list = database_list_df.to_dict(orient='records')
                self.de_part = data_list
