# -*- coding: utf-8 -*-
import json
import os
import re
import socket
import sys
import time
import copy
import datetime
import pandas as pd
import argparse

import urwid
import urwid.raw_display
import urwid.web_display
from urwid import Text, Divider
from data_source import database_query, session_query, session_detail_query, asp_query, table_query, \
    table_performance_query, table_index_query, table_detail_query, lock_query, lock_eventwait_query, memory_query, \
    sharemem_query, top_mem_used_session_query, dynamicsql_query, replication_query, database_list_query, \
    sql_patch_query, slow_sql_query, redo_query, redo_time_count_query, xlog_redo_statics_query
from data_source.wdr import wdr_query, snap_summary_more_statement_query, burr_analysis_query, rigorous_analysis_query, \
    snap_summary_statement_query
from db import db_config
from db_mind.api_index_advise import api_index_advise
from db.db_util import conversion_type, fuzzy_match, sort_query, OpenGaussOption, get_query, get_querys, \
    create_abort_sql_patch, drop_related_data
from ui.display import home_body, help_body, database_body, session_body, session_detail_body, sql_explain, \
    asp_body, table_body, table_performance_body, table_index_body, lock_body, lock_chain_body, \
    wait_event_body, memory_body, sharemem_body, top_mem_used_session_body, dynamicsql_body, replication_body, \
    index_recommendation_body, database_list_body, table_detail_body, sql_patch_body, slow_sql_body, redo_body, \
    redo_time_count_body, xlog_redo_statics_body, wdr_body, snap_summary_more_statement_body, burr_analysis_body, \
    rigorous_analysis_body, snap_summary_statement_body
from common.shortcut_key_menu import background_data_field, page_menu, help_tip, version_tip

# global variable
AUTO_REFRESH = False
HELP = False  # help page mark
DELAY = 2  # refresh time
PID = False  # pid mark
ID = ""  # pid value
FIND = False  # search identity
JUMP_FIND = False  # jump lookup identity
FUZZY_KEY = ""  # record lookup field
SORT = True  # sort order
Rank = False  # sort tag
RankSign = False  # sort field identification
IDX = ""  # sort field
PAGE_TABLE = "home"
LAST_PAGE = ""
SNAPSHOOT_USE_PAGE = ""
KILL = False  # kill session mark
FINISH = False  # session page kill session mark
BUTTON_KEY = ""
PART_MAX = 0
PART_MIN = 0
PART_SECTION = []
MAX_NUMBER_OF_BITS = 0
QUERY = ""  # query
DEL = False  # lock kill identity
SHOW = False  # floating window sign
ERROR = False  # sql error identity
REFRESH = False  # prevent forcibly refreshing flags
SNAPSHOOT = {}  # snapshot data
INDEX = False  # dbmind flags
EXPLAIN = False  # explain session_detail
SCHEMA = ''  # schema of database
FIXED_PLATE = False  # fixed screen marker
STA_TIME = ""  # start time
END_TIME = ""  # end time
ASP = False  # query history sql tags
TURN = False  # page refresh mark
DETAIL = False  # view table_detail mark
SCHEMANAME_RELNAME = ''  # jumping table_detail requires data
database_conf = {}
ACTIVE_DATA = pd.DataFrame([])
PATCHNAME = ""  # sql patch name
DROP = False  # drop sql patch name mark
SLOW_FIRST_TIME = ""
SLOW_SECOND_END = ""
SLOW = False
STACK = False
START_SNAPSHOT_ID = ""
END_SNAPSHOT_ID = ""
SNAP_UNIQUE_SQL_ID_SET = ""
SNAPSHOT_ID = ""


class Frame(urwid.WidgetWrap):
    """
    class frame,implement dynamic update of Text parts,build_body()、build_frame() reconstitute acting load body、frame
    """

    def __init__(self):
        self.openGau = OpenGaussOption(conn_config=database_conf)
        self.postgres_conf = copy.deepcopy(database_conf)
        self.postgres_conf['database'] = "postgres"
        self.post_openGau = OpenGaussOption(conn_config=self.postgres_conf)
        self.body = urwid.LineBox(urwid.ListBox(urwid.SimpleListWalker([urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"loading..."), ]))], 1),
            width=os.get_terminal_size().columns, align='center', min_width=2)])))
        self.standby = self.openGau.all_query_sql(sql=db_config.standby_sql)[0][0]
        self.old_database_ob = ''
        self.old_database_list_ob = ''
        self.old_session_ob = ''
        self.old_slow_sql_ob = ''
        self.old_session_detail_ob = ""
        self.asp_ob = ""
        self.old_table_performance_ob = ''
        self.old_table_ob = ''
        self.old_table_index_ob = ''
        self.old_table_detail_ob = ''
        self.old_lock_ob = ''
        self.old_wait_event_ob = ''
        self.old_memory_ob = ''
        self.old_sharemem_ob = ''
        self.old_top_mem_used_session_ob = ''
        self.old_dynamicsql_ob = ''
        self.old_dynamicsql_track = ''
        self.old_replication_ob = []
        self.advised_indexes = ""
        self.old_sql_patch_ob = ""
        self.old_redo_ob = ""
        self.old_redo_time_count_ob = ""
        self.old_xlog_redo_statics_ob = ""
        self.old_wdr_ob = ""
        self.burr_analysis = ""
        self.rigorous_analysis = ""
        self.snap_summary_statement = ""
        self.monitor_authority = False
        self.check_whether_wdr_takes_effect = 'off'
        self.old_snap_summary_more_statement_ob = ""
        self.sess_refresh()
        self._w = self.frame
        self.__super.__init__(self._w)

    def sess_refresh(self):
        """
        page fill data main function
        :return:
        """
        global PAGE_TABLE
        global LAST_PAGE
        global ID
        global FUZZY_KEY
        global KILL
        global Rank
        global IDX
        global SORT
        global HELP
        global FINISH
        global PART_MAX
        global PART_MIN
        global PART_SECTION
        global QUERY
        global DEL
        global SHOW
        global ERROR
        global REFRESH
        global SNAPSHOOT
        global INDEX
        global SCHEMA
        global STA_TIME
        global END_TIME
        global ASP
        global ACTIVE_DATA
        global TURN
        global SCHEMANAME_RELNAME
        global DETAIL
        global PATCHNAME
        global SLOW_FIRST_TIME
        global SLOW_SECOND_END
        global SLOW
        global STACK
        global DELAY
        global START_SNAPSHOT_ID
        global END_SNAPSHOT_ID
        global SNAP_UNIQUE_SQL_ID_SET
        global SNAPSHOT_ID

        self.db_host_time = self.openGau.all_query_sql(db_config.time)
        if PAGE_TABLE == "home":
            self.body = home_body(self.db_host_time)
        elif HELP:
            if help_body(PAGE_TABLE):
                self.body = help_body(PAGE_TABLE)
            else:
                HELP = False
        elif PAGE_TABLE == "snapshoot":
            self.edit = urwid.Edit("", edit_pos=0, edit_text=self.snapshot_body(SNAPSHOOT, LAST_PAGE))
            self.body = urwid.Filler(self.edit)
        elif PAGE_TABLE == "database":
            if self.old_database_ob == "":
                SNAPSHOOT = {}
                self.old_database_ob = database_query.DatabaseMain(self.openGau,
                                                                   sql=db_config.database_spare_sql,
                                                                   params=database_conf.get("database")) if isinstance(
                    database_query.DatabaseMain(self.openGau,
                                                sql=db_config.database_sql,
                                                params=database_conf.get("database")).de_part, str) else \
                    database_query.DatabaseMain(self.openGau,
                                                sql=db_config.database_sql,
                                                params=database_conf.get("database"))
                if isinstance(self.old_database_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "database page temporarily inaccessible：{}".format(self.old_database_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    up_parts, de_parts = database_query.DatabaseMain.split_date(self.old_database_ob)
                    depart = json.loads(de_parts)
                    depart = conversion_type(depart)
                    uppart = json.loads(up_parts)
                    self.body = database_body(uppart, depart)
            else:
                if DELAY == 86400 and SNAPSHOOT:
                    uppart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    self.body = database_body(uppart, depart)
                else:
                    new_ob = database_query.DatabaseMain(self.openGau,
                                                         sql=db_config.database_spare_sql,
                                                         params=database_conf.get("database")) if isinstance(
                        database_query.DatabaseMain(self.openGau,
                                                    sql=db_config.database_sql,
                                                    params=database_conf.get("database")).de_part, str) else \
                        database_query.DatabaseMain(self.openGau,
                                                    sql=db_config.database_sql,
                                                    params=database_conf.get("database"))
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "database page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        up_parts, de_parts = database_query.DatabaseMain.compute_delta(self.old_database_ob, new_ob)
                        self.old_database_ob = new_ob
                        del new_ob
                        depart = json.loads(de_parts)
                        depart = conversion_type(depart)
                        uppart = json.loads(up_parts)
                        SNAPSHOOT = {"heard": uppart, "body": depart}
                        self.body = database_body(uppart, depart)
        elif PAGE_TABLE == "session":
            PART_SECTION = [20, 33]
            if self.old_session_ob == '':
                SNAPSHOOT = {}
                self.old_session_ob = session_query.SessionMain(self.openGau, sql=db_config.session_sql)
                if isinstance(self.old_session_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "session page temporarily inaccessible：{}".format(self.old_session_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    old_depart, old_heard_depart, old_active_data = session_query.SessionMain.intergrated_data(
                        self.old_session_ob.de_part, database_conf['database'])
                    depart, heard_depart = session_query.SessionMain.split_date(old_depart, old_heard_depart)
                    self.body = session_body(heard_depart, depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = session_body(heard_depart, depart, PART_MIN, PART_MAX)
                else:
                    new_ob = session_query.SessionMain(self.openGau, sql=db_config.session_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "session page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        new_depart, new_heard_depart, new_active_data = session_query.SessionMain.intergrated_data(
                            new_ob.de_part, database_conf['database'])
                        old_depart, old_heard_depart, old_active_data = session_query.SessionMain.intergrated_data(
                            self.old_session_ob.de_part, database_conf['database'])
                        self.old_session_ob = new_ob
                        ACTIVE_DATA = new_active_data
                        del new_ob
                        depart, heard_depart = session_query.SessionMain.compute_delta(new_depart, new_heard_depart,
                                                                                       old_depart, old_heard_depart)
                        if FUZZY_KEY:
                            if LAST_PAGE == 'wait_event':
                                depart = fuzzy_match(FUZZY_KEY, depart, ["wait_status", "wait_event"])
                            else:
                                depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = session_body(heard_depart, depart, PART_MIN, PART_MAX)
                if PID:
                    self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                    DELAY = 86400
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["session"][18::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(5):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[15]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
                if FINISH:
                    self.force_all = urwid.Button(u"force_all")
                    urwid.connect_signal(self.force_all, 'click', self.clik_order)
                    self.force_by_dbname = urwid.Button(u"force_by_dbname")
                    urwid.connect_signal(self.force_by_dbname, 'click', self.clik_order)
                    self.force_idle = urwid.Button(u"force_idle")
                    urwid.connect_signal(self.force_idle, 'click', self.clik_order)
                    self.force_by_pid = urwid.Button(u"force_by_pid")
                    urwid.connect_signal(self.force_by_pid, 'click', self.clik_order)

                    self.cover = []
                    self.cover.append(urwid.Padding(urwid.Columns([self.force_all, ], 1), align='left'))
                    self.cover.append(urwid.Padding(urwid.Columns([self.force_by_dbname, ], 1), align='left'))
                    self.cover.append(urwid.Padding(urwid.Columns([self.force_idle, ], 1), align='left'))
                    self.cover.append(urwid.Padding(urwid.Columns([self.force_by_pid, ], 1), align='left'))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(self.cover))
        elif PAGE_TABLE == "session_detail":
            if ID != "":
                if self.old_session_detail_ob == "":
                    SNAPSHOOT = {}
                    self.old_session_detail_ob = session_detail_query.SessionDetailMain(self.openGau,
                                                                                        sql=db_config.session_detail_spare_sql,
                                                                                        params=(
                                                                                            f'%{ID}%', f'%{ID}%',
                                                                                            f'%{ID}%', f'%{ID}%',
                                                                                            f'%{ID}%',
                                                                                        )) if isinstance(
                        session_detail_query.SessionDetailMain(self.openGau, sql=db_config.session_detail_sql,
                                                               params=(
                                                                   f'%{ID}%', f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                   f'%{ID}%', f'%{ID}%')).de_part,
                        str) else session_detail_query.SessionDetailMain(self.openGau,
                                                                         sql=db_config.session_detail_sql,
                                                                         params=(
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%'))
                    if isinstance(self.old_session_detail_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = LAST_PAGE
                        self.text = urwid.Text("session_detail page temporarily inaccessible：{}".format(
                            self.old_session_detail_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                else:
                    new_ob = session_detail_query.SessionDetailMain(self.openGau,
                                                                    sql=db_config.session_detail_spare_sql,
                                                                    params=(
                                                                        f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                        f'%{ID}%', f'%{ID}%')) if isinstance(
                        session_detail_query.SessionDetailMain(self.openGau, sql=db_config.session_detail_sql,
                                                               params=(
                                                                   f'%{ID}%', f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                   f'%{ID}%', f'%{ID}%')).de_part,
                        str) else session_detail_query.SessionDetailMain(self.openGau,
                                                                         sql=db_config.session_detail_sql,
                                                                         params=(
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%'))

                    if isinstance(new_ob.de_part, str) or isinstance(self.old_session_detail_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = LAST_PAGE
                        self.text = urwid.Text("".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        if DELAY == 86400:
                            depart = SNAPSHOOT.get("body")
                        else:
                            depart = session_detail_query.SessionDetailMain.intergrated_data(new_ob.de_part,
                                                                                             self.old_session_detail_ob.de_part)
                            self.old_session_detail_ob = new_ob
                            del new_ob
                            QUERY = depart[0].get("query")
                            SNAPSHOOT = {"body": depart}
                        if STACK:
                            gs_stack = session_detail_query.SessionDetailStackMain(self.openGau,
                                                                                   sql=db_config.session_stack,
                                                                                   params=(f'{ID}',))
                            if isinstance(gs_stack.de_part, str):
                                ERROR = True
                                PAGE_TABLE = "home"
                                self.body = home_body(self.db_host_time)
                                self.text = urwid.Text(
                                    "session page temporarily inaccessible：{}".format(gs_stack.de_part))
                                self.cover = urwid.Filler(self.text, "middle")
                            else:
                                session_stack_data = gs_stack.de_part.iloc[0]
                                session_stack = session_stack_data["gs_stack"]
                                indicator_list = []
                                data_list = []
                                indicator_list.append(urwid.Padding(Text("gs_stack:")))
                                data_list.append(urwid.Padding(Text(session_stack)))
                                data = urwid.Padding(urwid.Columns(data_list, 1),
                                                     width=os.get_terminal_size().columns,
                                                     align='center')
                                indicator_data = urwid.Padding(urwid.Columns(indicator_list, 1),
                                                               width=os.get_terminal_size().columns,
                                                               align='center')
                                self.cover = urwid.ListBox(
                                    urwid.SimpleListWalker([indicator_data, Divider(u'_'), data]), )
                                SHOW = True

                        self.body = session_detail_body(depart)
                        if KILL:
                            self.edit = urwid.Edit("whether to kill{}(y/n):".format(PID), edit_pos=1)
                            self.edit.set_edit_text("")
                            self.body = urwid.LineBox(urwid.ListBox(
                                urwid.SimpleListWalker([urwid.Padding(self.edit, left=2, right=0, min_width=20)])))
                        elif EXPLAIN:
                            self.edit = urwid.Edit("Please input schema_name(end with enter):", edit_pos=0,
                                                   edit_text="")
                            self.cover = urwid.Filler(self.edit, "middle")
            else:
                PAGE_TABLE = LAST_PAGE
        elif PAGE_TABLE == "asp":
            PART_SECTION = [0, 8]
            if STA_TIME and END_TIME:
                if self.asp_ob == "":
                    SNAPSHOOT = {}
                    self.asp_ob = asp_query.SessionHistoryMain(self.post_openGau,
                                                               sqls=db_config.asp_sql,
                                                               params=(
                                                                   f'{STA_TIME}',
                                                                   f'{END_TIME}'))
                    if isinstance(self.asp_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "asp page temporarily inaccessible：{}".format(self.asp_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    elif isinstance(self.asp_ob.de_part, list):
                        SNAPSHOOT['body'] = self.asp_ob.de_part
                        self.body = asp_body(self.asp_ob.de_part, PART_MIN, PART_MAX)
                    else:
                        depart, active_data = asp_query.SessionHistoryMain.intergrated_data(
                            self.asp_ob.de_part, database_conf['database'])
                        DELAY = 86400
                        ACTIVE_DATA = active_data
                        SNAPSHOOT['body'] = depart
                        self.body = asp_body(depart, PART_MIN, PART_MAX)
                else:
                    depart = SNAPSHOOT.get("body")
                    if FUZZY_KEY:
                        depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                    if Rank and IDX:
                        depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        DELAY = 86400
                    if ID:
                        QUERY = get_querys(session_id=ID, page=PAGE_TABLE, depart=depart)
                        if QUERY:
                            SHOW = True
                            self.cover = urwid.Filler(urwid.Text(QUERY), "middle")
                    SNAPSHOOT = {"body": depart}
                    self.body = asp_body(depart, PART_MIN, PART_MAX)
                    if PID:
                        self.edit = urwid.Edit("please input session_id(end with enter):", edit_pos=0, edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
                    elif FIND:
                        self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                        self.cover = urwid.Filler(self.edit, "middle")
                    elif RankSign:
                        field_list = background_data_field["asp"][::]
                        button_list = self.make_field_button(field_list)
                        data_list = []
                        index = 0
                        for _ in range(3):
                            data_list.append(
                                urwid.Padding(urwid.Columns(
                                    [urwid.Padding(button_list[index]),
                                     urwid.Padding(button_list[index + 1]),
                                     urwid.Padding(button_list[index + 2])], 1),
                                    width=100, align='center', min_width=5),
                            )
                            index += 3
                        self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
            else:
                if ASP:
                    self.edit = urwid.Edit(
                        "please enter what to find(year-month-day hour:minute:second):",
                        edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "sql_explain":
            try:
                strinfo = re.compile("\$[1-9]\d*")
                parameter_list = strinfo.findall(QUERY)
                if parameter_list:
                    advise = ["The index recommendation function needs to provide specific variable values！"]
                else:
                    advise = self.openGau.all_query_sql(
                        db_config.session_plan_sql.get("advise").format('{}'.format(QUERY.replace("'", "''"))))
            except Exception as e:
                advise = []
            try:
                sign = time.time()
                explain = self.openGau.explain_query_sql(
                    db_config.session_plan_sql.get("explain").format(str(sign).replace('.', '')),
                    db_config.session_plan_sql.get("prepare").format(str(sign).replace('.', '')),
                    QUERY)
            except Exception as e:
                explain = []
            if advise or explain:
                # if DELAY == 86400:
                #     advise = SNAPSHOOT.get("body").get("advise")
                #     explain = SNAPSHOOT.get("body").get("explain")
                SNAPSHOOT = {"body": {"query": QUERY, "advise": advise, "explain": explain}}
                self.body = sql_explain(QUERY, advise, explain)
            else:
                ERROR = True
                self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=0, edit_text="")
                self.cover = urwid.Filler(self.edit, "middle")
                PAGE_TABLE = LAST_PAGE
        elif PAGE_TABLE == "table":
            PART_SECTION = [3, 11]
            if self.old_table_ob == '':
                SNAPSHOOT = {}
                self.old_table_ob = table_query.TableMain(self.openGau, sql=db_config.table_sql)
                if isinstance(self.old_table_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text("table page temporarily inaccessible：{}".format(self.old_table_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = table_query.TableMain.intergrated_data(self.old_table_ob.de_part)
                    self.body = table_body(depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = table_body(depart, PART_MIN, PART_MAX)
                else:
                    new_table_ob = table_query.TableMain(self.openGau, sql=db_config.table_sql)
                    if isinstance(new_table_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "table page temporarily inaccessible：{}".format(new_table_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = table_query.TableMain.intergrated_data(new_table_ob.de_part)
                        self.old_table_ob = new_table_ob
                        del new_table_ob
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                            DELAY = 86400
                        SNAPSHOOT = {"body": depart}
                        self.body = table_body(depart, PART_MIN, PART_MAX)
                if DETAIL:
                    self.edit = urwid.Edit("please input schemaname.relname(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["table"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(3):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[10]),
                         urwid.Padding(button_list[11]),
                         urwid.Padding(button_list[12])], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(
                        data_list
                    ))
        elif PAGE_TABLE == "table_performance":
            PART_SECTION = [3, 24]
            if self.old_table_performance_ob == '':
                SNAPSHOOT = {}
                self.old_table_performance_ob = table_performance_query.TablePerformanceMain(self.openGau,
                                                                                             sql=db_config.table_performance_sql)
                if isinstance(self.old_table_performance_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "table_performance page temporarily inaccessible：{}".format(
                            self.old_table_performance_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    old_depart = table_performance_query.TablePerformanceMain.intergrated_data(
                        self.old_table_performance_ob.de_part)
                    depart = table_performance_query.TablePerformanceMain.split_date(old_depart)
                    self.body = table_performance_body(depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = table_performance_body(depart, PART_MIN, PART_MAX)
                else:
                    new_ob = table_performance_query.TablePerformanceMain(self.openGau,
                                                                          sql=db_config.table_performance_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "table_performance page temporarily inaccessible：{}".format(
                                new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        new_depart = table_performance_query.TablePerformanceMain.intergrated_data(new_ob.de_part)
                        old_depart = table_performance_query.TablePerformanceMain.intergrated_data(
                            self.old_table_performance_ob.de_part)
                        self.old_table_performance_ob = new_ob
                        del new_ob
                        depart = table_performance_query.TablePerformanceMain.compute_delta(new_depart, old_depart)
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"body": depart}
                        self.body = table_performance_body(depart, PART_MIN, PART_MAX)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif DETAIL:
                    self.edit = urwid.Edit("please input schemaname.relname(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["table_performance"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(8):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    # data_list.append(urwid.Padding(urwid.Columns(
                    #     [urwid.Padding(button_list[24]),
                    #      urwid.Padding(button_list[25]),
                    #      urwid.Padding(urwid.Text([("")]))], 1),
                    #     width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(
                        data_list
                    ))
        elif PAGE_TABLE == "table_index":
            PART_SECTION = [2, 13]
            if self.old_table_index_ob == '':
                SNAPSHOOT = {}
                self.old_table_index_ob = table_index_query.TableDetailMain(self.openGau,
                                                                            sql=db_config.table_index_sql,
                                                                            params=ID)
                if isinstance(self.old_table_index_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = LAST_PAGE
                    self.text = urwid.Text(
                        "table_index page temporarily inaccessible：{}".format(self.old_table_index_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = table_index_query.TableDetailMain.split_date(self.old_table_index_ob.de_part)
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                    self.body = table_index_body(depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    new_depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = table_index_body(new_depart)
                else:
                    new_ob = table_index_query.TableDetailMain(self.openGau, sql=db_config.table_index_sql,
                                                               params=ID)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = LAST_PAGE
                        self.text = urwid.Text(
                            "table_index page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        new_depart = table_index_query.TableDetailMain.intergrated_data(new_ob.de_part,
                                                                                        self.old_table_index_ob.de_part)
                        self.old_table_index_ob = new_ob
                        del new_ob
                        SNAPSHOOT = {"body": new_depart}
                        self.body = table_index_body(new_depart)
        elif PAGE_TABLE == "table_detail":
            if SCHEMANAME_RELNAME:
                try:
                    schemaname = SCHEMANAME_RELNAME.split('.')[0]
                    relname = SCHEMANAME_RELNAME.split('.')[1]
                except Exception as e:
                    schemaname = ''
                    relname = ''
                if schemaname and relname:
                    self.old_table_detail_ob = table_detail_query.TableDetailMain(self.openGau,
                                                                                  sql=db_config.table_detail_sql,
                                                                                  params=(f'{SCHEMANAME_RELNAME}',
                                                                                          schemaname,
                                                                                          relname))
                    if isinstance(self.old_table_detail_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "table_detail page temporarily inaccessible：{}".format(self.old_table_detail_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = table_detail_query.TableDetailMain.intergrated_data(
                            self.old_table_detail_ob.de_part)
                        SNAPSHOOT = {"body": depart}
                        if PID:
                            self.edit = urwid.Edit("please input relid(end with enter):", edit_pos=0, edit_text="")
                            self.cover = urwid.Filler(self.edit, "middle")
                        self.body = table_detail_body(depart)
                else:
                    PAGE_TABLE = LAST_PAGE
        elif PAGE_TABLE == 'lock':
            PART_SECTION = [2, 14]
            if self.old_lock_ob == '':
                self.old_lock_ob = lock_query.LockMain(self.openGau, sql=db_config.lock_sql)
                if isinstance(self.old_lock_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text("lock page temporarily inaccessible：{}".format(self.old_lock_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart, heard_depart = lock_query.LockMain.intergrated_data(self.old_lock_ob.de_part)
                    self.body = lock_body(depart, heard_depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    self.body = lock_body(depart, heard_depart, PART_MIN, PART_MAX)
                    TURN = False
                else:
                    new_ob = lock_query.LockMain(self.openGau, sql=db_config.lock_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text("lock page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart, heard_depart = lock_query.LockMain.intergrated_data(new_ob.de_part)
                        self.old_lock_ob = new_ob
                        del new_ob
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = lock_body(depart, heard_depart, PART_MIN, PART_MAX)
                if PID:
                    self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["lock"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(4):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[12]),
                         urwid.Padding(button_list[13]),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
                elif DEL:
                    self.edit = urwid.Edit("please enter pid to delete(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == 'lock_chain':
            if SNAPSHOOT and (DELAY == 86400 or TURN):
                date_list = SNAPSHOOT.get("body")
                TURN = False
                self.body = lock_chain_body(date_list)
            else:
                new_ob = lock_query.LockMain(self.openGau, sql=db_config.lock_sql)
                if isinstance(new_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text("lock page temporarily inaccessible：{}".format(self.old_lock_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    self.old_lock_chain_ob = new_ob
                    depart, heard_depart = lock_query.LockMain.intergrated_data(new_ob.de_part)
                    df_depart = \
                        pd.DataFrame(depart, columns=['pid', 'locktype', 'database', 'relation', 'page', 'tuple',
                                                      'transactionid', 'virtualxid', 'objid', 'virtualtransaction',
                                                      'mode', 'granted', 'fastpath', 'locktag'])[
                            ['pid', 'granted', 'locktag']].sort_values(by='locktag').reset_index()
                    df_depart.pop('index')
                    degranted_list = list(set(df_depart[df_depart['granted'] == False]['locktag'].tolist()))
                    date_list = []
                    for tags in degranted_list:
                        blocker_holder = \
                            df_depart[(df_depart['locktag'] == tags) & (df_depart['granted'] == True)].reset_index()[
                                'pid'][
                                0]
                        lock_blocked_agent_id = \
                            df_depart[(df_depart['locktag'] == tags) & (df_depart['granted'] == False)].reset_index()[
                                'pid'][0]
                        if blocker_holder and lock_blocked_agent_id:
                            date_list.append(
                                {"blocker_holder": blocker_holder, "lock_blocked_agent_id": lock_blocked_agent_id})
                    if Rank and IDX and date_list:
                        date_list = sort_query(IDX, SORT, date_list, PAGE_TABLE)
                    SNAPSHOOT = {"body": date_list}
                    self.body = lock_chain_body(date_list)
            if PID:
                self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
            if DEL:
                self.edit = urwid.Edit("please enter pid to delete(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == 'wait_event':
            if self.old_wait_event_ob == '':
                SNAPSHOOT = {}
                self.old_wait_event_ob = lock_eventwait_query.WaitEventMain(self.openGau, sql=db_config.even_twait_sql)
                if isinstance(self.old_wait_event_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "wait_event page temporarily inaccessible：{}".format(self.old_wait_event_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    old_depart = lock_eventwait_query.WaitEventMain.intergrated_data(self.old_wait_event_ob.de_part)
                    depart, new_heard_depart = lock_eventwait_query.WaitEventMain.split_date(old_depart)
                    self.body = wait_event_body(depart, new_heard_depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    new_heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = wait_event_body(depart, new_heard_depart)
                else:
                    new_ob = lock_eventwait_query.WaitEventMain(self.openGau, sql=db_config.even_twait_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "wait_event page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        new_depart = lock_eventwait_query.WaitEventMain.intergrated_data(new_ob.de_part)
                        old_depart = lock_eventwait_query.WaitEventMain.intergrated_data(self.old_wait_event_ob.de_part)
                        depart, new_heard_depart = lock_eventwait_query.WaitEventMain.compute_delta(new_depart,
                                                                                                    old_depart)
                        self.old_wait_event_ob = new_ob
                        del new_ob
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": new_heard_depart, "body": depart}
                        self.body = wait_event_body(depart, new_heard_depart)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["wait_event"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(3):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[9]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
                elif JUMP_FIND:
                    self.edit = urwid.Edit(
                        "please input event to sort in session page(end with enter):",
                        edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "memory":
            if self.old_memory_ob == '':
                self.old_memory_ob = memory_query.MemoryMain(self.openGau, sql=db_config.memory_sql)
                if isinstance(self.old_memory_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text("memory page temporarily inaccessible：{}".format(self.old_memory_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart, heard_depart = memory_query.MemoryMain.compute_delta(
                        memory_query.MemoryMain.intergrated_data(self.old_memory_ob.de_part))
                    self.body = memory_body(depart, heard_depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = memory_body(depart, heard_depart)
                else:
                    new_ob = memory_query.MemoryMain(self.openGau, sql=db_config.memory_sql)

                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "memory page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        new_depart = memory_query.MemoryMain.intergrated_data(new_ob.de_part)
                        depart, heard_depart = memory_query.MemoryMain.compute_delta(new_depart)
                        self.old_memory_ob = new_ob
                        del new_ob
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = memory_body(depart, heard_depart)
        elif PAGE_TABLE == 'sharemem':
            if self.old_sharemem_ob == '':
                self.old_sharemem_ob = sharemem_query.SharememMain(self.openGau, sql=db_config.sharemem_sql)
                if isinstance(self.old_sharemem_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "sharemem page temporarily inaccessible：{}".format(self.old_sharemem_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart, heard_depart = sharemem_query.SharememMain.intergrated_data(self.old_sharemem_ob.de_part)
                    self.body = sharemem_body(depart, heard_depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = sharemem_body(depart, heard_depart)
                else:
                    new_ob = sharemem_query.SharememMain(self.openGau, sql=db_config.sharemem_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "sharemem page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        self.old_sharemem_ob = new_ob
                        depart, heard_depart = sharemem_query.SharememMain.intergrated_data(new_ob.de_part)
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = sharemem_body(depart, heard_depart)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                if RankSign:
                    field_list = background_data_field["sharemem"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(2):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[6]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
        elif PAGE_TABLE == 'top_mem_used_session':
            if self.old_top_mem_used_session_ob == '':
                self.old_top_mem_used_session_ob = top_mem_used_session_query.TopMemUsedSessionMain(self.openGau,
                                                                                                    sql=db_config.top_mem_used_session_sql)
                if isinstance(self.old_top_mem_used_session_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "sharemem page temporarily inaccessible：{}".format(self.old_top_mem_used_session_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart, heard_depart = top_mem_used_session_query.TopMemUsedSessionMain.intergrated_data(
                        self.old_top_mem_used_session_ob.de_part)
                    self.body = top_mem_used_session_body(depart, heard_depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = top_mem_used_session_body(depart, heard_depart)
                else:
                    new_ob = top_mem_used_session_query.TopMemUsedSessionMain(self.openGau,
                                                                              sql=db_config.top_mem_used_session_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text("sharemem page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        self.old_top_mem_used_session_ob = new_ob
                        depart, heard_depart = top_mem_used_session_query.TopMemUsedSessionMain.intergrated_data(
                            new_ob.de_part)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = top_mem_used_session_body(depart, heard_depart)
                if PID:
                    self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["top_mem_used_session"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(2):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[6]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
                elif DEL:
                    self.edit = urwid.Edit("please enter pid to delete(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "dynamicsql":
            PART_SECTION = [3, 47]
            if self.old_dynamicsql_ob == '' and self.old_dynamicsql_track == '':
                SNAPSHOOT = {}
                self.old_dynamicsql_ob = dynamicsql_query.DynamicsqlMain(self.openGau, sql=db_config.dynamicsql_sql)
                self.old_dynamicsql_track = self.openGau.all_query_sql(db_config.enable_resource_track)[0][0]
                if isinstance(self.old_dynamicsql_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "dynamicsql page temporarily inaccessible：{}".format(self.old_dynamicsql_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                elif self.old_dynamicsql_track == 'off':
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "dynamicsql page temporarily inaccessible：enable_resource_track is off")
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart, heard_depart = dynamicsql_query.DynamicsqlMain.split_date(self.old_dynamicsql_ob.de_part)
                    self.body = dynamicsql_body(depart, heard_depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = dynamicsql_body(depart, heard_depart, PART_MIN, PART_MAX)
                else:
                    new_ob = dynamicsql_query.DynamicsqlMain(self.openGau, sql=db_config.dynamicsql_sql)
                    new_dynamicsql_track = self.openGau.all_query_sql(db_config.enable_resource_track)[0][0]
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "dynamicsql page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    elif new_dynamicsql_track == 'off':
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "dynamicsql page temporarily inaccessible：enable_resource_track is off")
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart, heard_depart = dynamicsql_query.DynamicsqlMain.compute_delta(new_ob.de_part,
                                                                                             self.old_dynamicsql_ob.de_part)
                        self.old_dynamicsql_ob = new_ob
                        del new_ob
                        if FUZZY_KEY:
                            depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                        if Rank and IDX:
                            depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        SNAPSHOOT = {"heard": heard_depart, "body": depart}
                        self.body = dynamicsql_body(depart, heard_depart, PART_MIN, PART_MAX)
                if ID and SHOW is False:
                    QUERY = get_query(self.openGau, db_config.dynamicsql_query_sql, ID)
                    if QUERY:
                        data_list = []
                        indicator_list = []
                        indicator_list.append(urwid.Padding(Text("query:")))
                        data_list.append(urwid.Padding(Text(QUERY)))
                        data = urwid.Padding(urwid.Columns(data_list, 1),
                                             width=os.get_terminal_size().columns,
                                             align='center')
                        indicator_data = urwid.Padding(urwid.Columns(indicator_list, 1),
                                                       width=os.get_terminal_size().columns,
                                                       align='center')
                        self.cover = urwid.ListBox(urwid.SimpleListWalker([indicator_data, Divider(u'_'), data]), )
                        SHOW = True
                elif PID:
                    self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif INDEX:
                    self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["dynamicsql"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(15):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[45]),
                         urwid.Padding(button_list[46]),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
        elif PAGE_TABLE == "replication":
            PART_SECTION = [2, 15]
            if self.old_replication_ob == []:
                SNAPSHOOT = {}
                self.old_replication_ob = replication_query.ReplicationMain(self.openGau, sql=db_config.replication_sql,
                                                                            replication_slots_sql=db_config.replication_slots_sql)
                if isinstance(self.old_replication_ob.de_part[0], str) and isinstance(
                        self.old_replication_ob.de_part[1], str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "replication page temporarily inaccessible：{} and {}".format(self.old_replication_ob.de_part[0],
                                                                                     self.old_replication_ob.de_part[
                                                                                         1]))
                    self.cover = urwid.Filler(self.text, "middle")
                elif isinstance(self.old_replication_ob.de_part[0], str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "replication page temporarily inaccessible：{}".format(self.old_replication_ob.de_part[0]))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    up_depart, de_depart = replication_query.ReplicationMain.split_date(self.old_replication_ob.de_part)
                    self.body = replication_body(up_depart, de_depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    up_depart = SNAPSHOOT.get("heard")
                    de_depart = SNAPSHOOT.get("body")
                    TURN = False
                else:
                    new_ob = replication_query.ReplicationMain(self.openGau, sql=db_config.replication_sql,
                                                               replication_slots_sql=db_config.replication_slots_sql)
                    if isinstance(new_ob.de_part[0], str) and isinstance(
                            new_ob.de_part[1], str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "replication page temporarily inaccessible：{} and {}".format(
                                new_ob.de_part[0],
                                new_ob.de_part[
                                    1]))
                        self.cover = urwid.Filler(self.text, "middle")
                    elif isinstance(new_ob.de_part[0], str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "replication page temporarily inaccessible：{}".format(new_ob.de_part[0]))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        up_depart, de_depart = replication_query.ReplicationMain.compute_delta(new_ob.de_part,
                                                                                               self.old_replication_ob.de_part)
                        self.old_replication_ob = new_ob
                        del new_ob
                        SNAPSHOOT = {"heard": up_depart, "body": de_depart}
                        if FUZZY_KEY:
                            up_depart = fuzzy_match(FUZZY_KEY, up_depart, background_data_field[PAGE_TABLE])
                        if ID:
                            drop_related_data(self.openGau, db_config.drop_replication_slot_sql, ID)
                            ID = ""
                            SNAPSHOOT = {"heard": up_depart, "body": de_depart}
                        self.body = replication_body(up_depart, de_depart)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif DROP:
                    self.edit = urwid.Edit("please input slot_name (end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "index_recommendation":
            if ID or ACTIVE_DATA.empty == False:
                if ID and ACTIVE_DATA.empty:
                    unique_sql_id = int(ID)
                    data = dynamicsql_query.IndexRecommendationMain(self.openGau,
                                                                    sqls=db_config.index_recommendation_sql.format(
                                                                        unique_sql_id))
                    active_parame = {data.de_part.iloc[0][0]: data.de_part.iloc[0][1]}
                else:
                    active_parame = dict()
                    for i in range(ACTIVE_DATA.shape[0]):
                        active = ACTIVE_DATA.iloc[i]
                        active_parame[active["query"]] = active["count"]
                if self.advised_indexes == "":
                    try:
                        res_data = api_index_advise(sql_pairs=active_parame, connection=self.openGau.conn)
                    except Exception as e:
                        res_data = 'WARNING'
                    if str(res_data).count('WARNING'):
                        PAGE_TABLE = LAST_PAGE
                        ID = ""
                    else:
                        self.advised_indexes = res_data[0]
                        SNAPSHOOT = {"body": {"advised_indexes": self.advised_indexes}}
                        self.body = index_recommendation_body(self.advised_indexes)
            else:
                PAGE_TABLE = LAST_PAGE
                ID = ""
        elif PAGE_TABLE == "database_list":
            PART_SECTION = [1, 17]
            if self.old_database_list_ob == "":
                self.old_database_list_ob = database_list_query.DatabaseListMain(self.openGau,
                                                                                 sql=db_config.database_list_sql)
                if isinstance(self.old_database_list_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "database page temporarily inaccessible：{}".format(self.old_database_list_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
            else:
                if DELAY == 86400:
                    depart = SNAPSHOOT.get("body")
                    self.body = database_list_body(depart, PART_MIN, PART_MAX)
                else:
                    depart = database_list_query.DatabaseListMain(self.openGau,
                                                                  sql=db_config.database_list_sql).de_part
                    if isinstance(depart, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "database page temporarily inaccessible：{}".format(depart))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        SNAPSHOOT = {"body": depart}
                        depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        self.body = database_list_body(depart, PART_MIN, PART_MAX)
        elif PAGE_TABLE == "sql_patch":
            PART_SECTION = [2, 12]
            if self.old_sql_patch_ob == "":
                self.old_sql_patch_ob = sql_patch_query.SqlPatchMain(self.openGau, sql=db_config.sql_patch_sql)
                if isinstance(self.old_sql_patch_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "sql_patch page temporarily inaccessible：{}".format(self.old_sql_patch_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = sql_patch_query.SqlPatchMain.intergrated_data(self.old_sql_patch_ob.de_part)
                    self.body = sql_patch_body(depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                else:
                    new_ob = sql_patch_query.SqlPatchMain(self.openGau, sql=db_config.sql_patch_sql)
                    depart = sql_patch_query.SqlPatchMain.intergrated_data(new_ob.de_part)
                    self.old_sql_patch_ob = new_ob
                    del new_ob
                    SNAPSHOOT = {"body": depart}
                if isinstance(depart, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "sql_patch page temporarily inaccessible：{}".format(self.old_sql_patch_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    if PATCHNAME and ID:
                        create_abort_sql_patch(self.openGau, PATCHNAME, ID)
                        PATCHNAME = ""
                        ID = ""
                    if PATCHNAME:
                        drop_related_data(self.openGau, db_config.drop_sql_patch, PATCHNAME)
                        PATCHNAME = ""
                    if PID:
                        self.edit = urwid.Edit("please input patchname uniquesqlid(end with enter):", edit_pos=0,
                                               edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
                    elif DROP:
                        self.edit = urwid.Edit("please input patchname (end with enter):", edit_pos=0,
                                               edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
                    self.body = sql_patch_body(depart, PART_MIN, PART_MAX)
        elif PAGE_TABLE == "slow_sql":
            PART_SECTION = [2, 50]
            if STA_TIME and END_TIME:
                if self.old_slow_sql_ob == '':
                    SNAPSHOOT = {}
                    self.old_slow_sql_ob = slow_sql_query.SlowSqlMain(self.post_openGau, sql=db_config.slow_sql,
                                                                      params=(
                                                                          f'{STA_TIME}',
                                                                          f'{END_TIME}'))
                    if isinstance(self.old_slow_sql_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "slow_sql page temporarily inaccessible：{} ".format(self.old_slow_sql_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart, active_data = slow_sql_query.SlowSqlMain.intergrated_data(
                            self.old_slow_sql_ob.de_part, database_conf['database'])
                        ACTIVE_DATA = active_data
                        SNAPSHOOT = {"body": depart}
                        self.body = slow_sql_body(depart, PART_MIN, PART_MAX)
                else:
                    depart = SNAPSHOOT.get("body")
                    if FUZZY_KEY:
                        depart = fuzzy_match(FUZZY_KEY, depart, background_data_field[PAGE_TABLE])
                    if Rank and IDX:
                        depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        DELAY = 86400
                    self.body = slow_sql_body(depart, PART_MIN, PART_MAX)
                    if ID and SHOW is False:
                        query, query_plan, detail = slow_sql_query.SlowSqlMain.related_data(SNAPSHOOT.get("body"), ID)
                        if query == '' and query_plan == '' and detail == '':
                            self.edit = urwid.Edit("The query data is empty!", edit_pos=0, edit_text="")
                            self.cover = urwid.Filler(self.edit, "middle")
                            SHOW = True
                        else:
                            indicator_list = []
                            data_list = []
                            indicator_list.append(urwid.Padding(Text("query:")))
                            data_list.append(urwid.Padding(Text(query)))
                            indicator_list.append(urwid.Padding(Text("query_plan:")))
                            data_list.append(urwid.Padding(Text(query_plan)))
                            indicator_list.append(urwid.Padding(Text("detail:")))
                            data_list.append(urwid.Padding(Text(detail)))
                            data = urwid.Padding(urwid.Columns(data_list, 1),
                                                 width=os.get_terminal_size().columns,
                                                 align='center')
                            indicator_data = urwid.Padding(urwid.Columns(indicator_list, 1),
                                                           width=os.get_terminal_size().columns,
                                                           align='center')
                            self.cover = urwid.ListBox(urwid.SimpleListWalker([indicator_data, Divider(u'_'), data]), )
                            SHOW = True
                    if PID:
                        self.edit = urwid.Edit("please input debug_query_id(end with enter):", edit_pos=0, edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
                    elif FIND:
                        self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                        self.cover = urwid.Filler(self.edit, "middle")
                    elif RankSign:
                        field_list = background_data_field["slow_sql"][::]
                        button_list = self.make_field_button(field_list)
                        data_list = []
                        index = 0
                        for _ in range(16):
                            data_list.append(
                                urwid.Padding(urwid.Columns(
                                    [urwid.Padding(button_list[index]),
                                     urwid.Padding(button_list[index + 1]),
                                     urwid.Padding(button_list[index + 2])], 1),
                                    width=100, align='center', min_width=5),
                            )
                            index += 3
                        data_list.append(urwid.Padding(urwid.Columns(
                            [urwid.Padding(button_list[49]),
                             urwid.Padding(button_list[50]),
                             urwid.Padding(urwid.Text([("")]))], 1),
                            width=100, align='center', min_width=5))
                        self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))

            else:
                if SLOW:
                    self.edit = urwid.Edit(
                        "please enter what to find(year-month-day hour:minute:second~year-month-day hour:minute:second):",
                        edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "redo":
            PART_SECTION = [0, 22]
            if self.old_redo_ob == '':
                SNAPSHOOT = {}
                self.old_redo_ob = redo_query.RedoMain(self.openGau, sql=db_config.redo_stat_sql)
                if isinstance(self.old_redo_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "redo page temporarily inaccessible：{} ".format(self.old_redo_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = redo_query.RedoMain.intergrated_data(self.old_redo_ob.de_part)
                    SNAPSHOOT = {"body": depart}
                    self.body = redo_body(depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = redo_body(depart, PART_MIN, PART_MAX)
                else:
                    new_ob = redo_query.RedoMain(self.openGau, sql=db_config.redo_stat_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "redo page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = redo_query.RedoMain.intergrated_data(new_ob.de_part)
                        self.old_redo_ob = new_ob
                        SNAPSHOOT = {"body": depart}
                        depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                        self.body = redo_body(depart, PART_MIN, PART_MAX)
        elif PAGE_TABLE == "redo_time_count":
            PART_SECTION = [1, 18]
            if self.old_redo_time_count_ob == '':
                SNAPSHOOT = {}
                self.old_redo_time_count_ob = redo_time_count_query.RedoTimeCountMain(self.openGau,
                                                                                      sql=db_config.redo_time_count_sql)

                if isinstance(self.old_redo_time_count_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "redo_time_count page temporarily inaccessible：{} ".format(self.old_redo_time_count_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = redo_time_count_query.RedoTimeCountMain.split_date(self.old_redo_time_count_ob.de_part)
                    SNAPSHOOT = {"body": depart}
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                    self.body = redo_time_count_body(depart, PART_MIN, PART_MAX)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = redo_time_count_body(depart, PART_MIN, PART_MAX)
                else:
                    new_ob = redo_time_count_query.RedoTimeCountMain(self.openGau, sql=db_config.redo_time_count_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "redo_time_count page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = redo_time_count_query.RedoTimeCountMain.intergrated_data(new_ob.de_part,
                                                                                          self.old_redo_time_count_ob.de_part)
                        self.old_redo_time_count_ob = new_ob
                        del new_ob
                        SNAPSHOOT = {"body": depart}
                        self.body = redo_time_count_body(depart, PART_MIN, PART_MAX)
        elif PAGE_TABLE == "xlog_redo_statics":
            if self.old_xlog_redo_statics_ob == '':
                SNAPSHOOT = {}
                self.old_xlog_redo_statics_ob = xlog_redo_statics_query.XlogRedoStaticsMain(self.openGau,
                                                                                            sql=db_config.xlog_redo_statics_sql)
                if isinstance(self.old_xlog_redo_statics_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "xlog_redo_statics page temporarily inaccessible：{} ".format(
                            self.old_xlog_redo_statics_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = xlog_redo_statics_query.XlogRedoStaticsMain.split_date(
                        self.old_xlog_redo_statics_ob.de_part)
                    SNAPSHOOT = {"body": depart}
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                    self.body = xlog_redo_statics_body(depart)
            else:
                if SNAPSHOOT and (DELAY == 86400 or TURN):
                    depart = SNAPSHOOT.get("body")
                    TURN = False
                    self.body = xlog_redo_statics_body(depart)
                else:
                    new_ob = xlog_redo_statics_query.XlogRedoStaticsMain(self.openGau,
                                                                         sql=db_config.xlog_redo_statics_sql)
                    if isinstance(new_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "xlog_redo_statics page temporarily inaccessible：{}".format(new_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = xlog_redo_statics_query.XlogRedoStaticsMain.intergrated_data(new_ob.de_part,
                                                                                              self.old_xlog_redo_statics_ob.de_part)
                        self.old_xlog_redo_statics_ob = new_ob
                        del new_ob
                        SNAPSHOOT = {"body": depart}
                        self.body = xlog_redo_statics_body(depart)
        elif PAGE_TABLE == "wdr":
            # if self.old_wdr_ob == '':
            #     SNAPSHOOT = {}
            #     self.old_wdr_ob = wdr_query.WrdMain(self.post_openGau, sql=db_config.wdr_sql)
            #     if isinstance(self.old_wdr_ob.de_part, str):
            #         ERROR = True
            #         PAGE_TABLE = "home"
            #         self.body = home_body(self.db_host_time)
            #         self.text = urwid.Text(
            #             "wdr page temporarily inaccessible：{} ".format(self.old_wdr_ob.de_part))
            #         self.cover = urwid.Filler(self.text, "middle")
            #     else:
            #         depart = wdr_query.WrdMain.intergrated_data(self.old_wdr_ob.de_part)
            #         SNAPSHOOT = {"body": depart}
            #         self.body = wdr_body(depart)
            # else:
            #     if SNAPSHOOT and (DELAY == 86400 or TURN):
            #         depart = SNAPSHOOT.get("body")
            #         TURN = False
            #         self.body = wdr_body(depart)
            #     else:
            #         new_ob = wdr_query.WrdMain(self.post_openGau, sql=db_config.wdr_sql)
            #         if isinstance(new_ob.de_part, str):
            #             ERROR = True
            #             PAGE_TABLE = "home"
            #             self.body = home_body(self.db_host_time)
            #             self.text = urwid.Text(
            #                 "wdr page temporarily inaccessible：{}".format(new_ob.de_part))
            #             self.cover = urwid.Filler(self.text, "middle")
            #         else:
            #             depart = wdr_query.WrdMain.intergrated_data(new_ob.de_part)
            #             self.old_redo_ob = new_ob
            #             SNAPSHOOT = {"body": depart}
            #             self.body = wdr_body(depart)
            #     if PID:
            #         self.edit = urwid.Edit("please input snapshot_id(xx-xx):", edit_pos=0, edit_text="")
            #         self.cover = urwid.Filler(self.edit, "middle")
            #         DELAY = 86400
            #         self.cover = urwid.Filler(self.edit, "middle")

            if self.monitor_authority and self.check_whether_wdr_takes_effect == 'on':
                if self.old_wdr_ob == '':
                    SNAPSHOOT = {}
                    self.old_wdr_ob = wdr_query.WrdMain(self.post_openGau, sql=db_config.wdr_sql)
                    if isinstance(self.old_wdr_ob.de_part, str):
                        ERROR = True
                        PAGE_TABLE = "home"
                        self.body = home_body(self.db_host_time)
                        self.text = urwid.Text(
                            "wdr page temporarily inaccessible：{} ".format(self.old_wdr_ob.de_part))
                        self.cover = urwid.Filler(self.text, "middle")
                    else:
                        depart = wdr_query.WrdMain.intergrated_data(self.old_wdr_ob.de_part)
                        SNAPSHOOT = {"body": depart}
                        self.body = wdr_body(depart)
                else:
                    if SNAPSHOOT and (DELAY == 86400 or TURN):
                        depart = SNAPSHOOT.get("body")
                        TURN = False
                        self.body = wdr_body(depart)
                    else:
                        new_ob = wdr_query.WrdMain(self.post_openGau, sql=db_config.wdr_sql)
                        if isinstance(new_ob.de_part, str):
                            ERROR = True
                            PAGE_TABLE = "home"
                            self.body = home_body(self.db_host_time)
                            self.text = urwid.Text(
                                "wdr page temporarily inaccessible：{}".format(new_ob.de_part))
                            self.cover = urwid.Filler(self.text, "middle")
                        else:
                            depart = wdr_query.WrdMain.intergrated_data(new_ob.de_part)
                            self.old_redo_ob = new_ob
                            SNAPSHOOT = {"body": depart}
                            self.body = wdr_body(depart)
                    if SHOW:
                        self.text = urwid.Text(
                            f"wdr page temporarily inaccessible：{sys.path[0]}/ogtop_WDR_ASH.html")
                        self.cover = urwid.Filler(self.text, "middle")
                        DELAY = 86400
                    if PID:
                        self.edit = urwid.Edit("please input snapshot_id(xx-xx):", edit_pos=0, edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
                        DELAY = 86400
                    elif DETAIL:
                        self.edit = urwid.Edit("please input snapshot_id(xx-xx):", edit_pos=0, edit_text="")
                        self.cover = urwid.Filler(self.edit, "middle")
            else:
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "wdr page temporarily inaccessible：The user has no permission or wrd is disabled!")
                self.cover = urwid.Filler(self.text, "middle")
        elif PAGE_TABLE == "snap_summary_more_statement":
            PART_SECTION = [3, 48]
            if self.old_snap_summary_more_statement_ob == '':
                SNAPSHOOT = {}
                self.old_snap_summary_more_statement_ob = snap_summary_more_statement_query.SnapSummaryMoreStatementMain(
                    self.post_openGau, sql=db_config.snap_summary_more_statement,
                    params=(START_SNAPSHOT_ID, END_SNAPSHOT_ID))
                if isinstance(self.old_snap_summary_more_statement_ob.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "snap_summary_more_statement page temporarily inaccessible：{} ".format(
                            self.old_snap_summary_more_statement_ob.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    SNAP_UNIQUE_SQL_ID_SET, depart = snap_summary_more_statement_query.SnapSummaryMoreStatementMain.intergrated_data(
                        self.old_snap_summary_more_statement_ob.de_part)
                    SNAPSHOOT = {"body": depart}
                    self.body = snap_summary_more_statement_body(depart, PART_MIN, PART_MAX)
            else:
                depart = SNAPSHOOT.get("body")
                TURN = False
                depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                self.body = snap_summary_more_statement_body(depart, PART_MIN, PART_MAX)
                if DETAIL:
                    self.edit = urwid.Edit("please input snapshot_id-snap_unique_sql_id(end with enter):", edit_pos=0,
                                           edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif PID:
                    self.edit = urwid.Edit("please input snap_unique_sql_id(end with enter):", edit_pos=0,
                                           edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = background_data_field["snap_summary_more_statement"][::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(16):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[47]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
        elif PAGE_TABLE == "burr_analysis":
            if self.burr_analysis == '':
                SNAPSHOOT = {}
                self.burr_analysis = burr_analysis_query.BurrAnalysisMain(self.post_openGau,
                                                                          sql=db_config.snap_summary_avg_statement,
                                                                          params=(
                                                                              END_SNAPSHOT_ID, START_SNAPSHOT_ID,
                                                                              tuple(SNAP_UNIQUE_SQL_ID_SET)))
                if isinstance(self.burr_analysis.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "wdr page temporarily inaccessible：{} ".format(self.burr_analysis.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = burr_analysis_query.BurrAnalysisMain.intergrated_data(self.burr_analysis.de_part)
                    SNAPSHOOT = {"body": depart}
                    self.body = burr_analysis_body(depart)
            else:
                depart = SNAPSHOOT.get("body")
                TURN = False
                self.body = burr_analysis_body(depart)
            if PID:
                self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text="")
                self.cover = urwid.Filler(self.edit, "middle")
            elif DETAIL:
                self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text="")
                self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "rigorous_analysis":
            if self.rigorous_analysis == '':
                SNAPSHOOT = {}
                self.rigorous_analysis = rigorous_analysis_query.RigorousAnalysisMain(self.post_openGau,
                                                                                      sql=db_config.snap_summary_avg_statement,
                                                                                      params=(
                                                                                          END_SNAPSHOT_ID,
                                                                                          START_SNAPSHOT_ID,
                                                                                          (ID,)))
                if isinstance(self.rigorous_analysis.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "rigorous_analysis page temporarily inaccessible：{} ".format(self.rigorous_analysis.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    head, depart = rigorous_analysis_query.RigorousAnalysisMain.intergrated_data(
                        self.rigorous_analysis.de_part)
                    SNAPSHOOT = {"body": depart, "head": head}
                    self.body = rigorous_analysis_body(head, depart)
            else:
                head = SNAPSHOOT.get("head")
                depart = SNAPSHOOT.get("body")
                TURN = False
                self.body = rigorous_analysis_body(head, depart)
            if DETAIL:
                self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text=str(ID))
                self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "snap_summary_statement":
            if self.snap_summary_statement == '':
                SNAPSHOOT = {}
                self.snap_summary_statement = snap_summary_statement_query.SnapSummaryStatementMain(self.post_openGau,
                                                                                                    sql=db_config.snap_summary_statement,
                                                                                                    params=(
                                                                                                        SNAPSHOT_ID,
                                                                                                        ID))
                if isinstance(self.snap_summary_statement.de_part, str):
                    ERROR = True
                    PAGE_TABLE = "home"
                    self.body = home_body(self.db_host_time)
                    self.text = urwid.Text(
                        "snap_summary_statement page temporarily inaccessible：{} ".format(
                            self.snap_summary_statement.de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    depart = snap_summary_statement_query.SnapSummaryStatementMain.intergrated_data(
                        self.snap_summary_statement.de_part)
                    SNAPSHOOT = {"body": depart}
                    self.body = snap_summary_statement_body(depart)
            else:
                depart = SNAPSHOOT.get("body")
                TURN = False
                self.body = snap_summary_statement_body(depart)

        self.header = urwid.Columns([
            urwid.Padding(urwid.Text(
                [('time', u"Local Time: "), str(self.db_host_time[0][0]).split('.')[0] + "\n",
                 ('time', u"Query interval: "), str(DELAY) + "s"]),
                left=2, right=0, min_width=20),
            urwid.Padding(urwid.Text(
                [('title', u"{}\n".format(PAGE_TABLE)), "PID:" + ID if ID and PAGE_TABLE == "session_detail" else ""]),
                left=2, right=5, min_width=20),
            urwid.Padding(urwid.Text(
                [('database', u"Port: "), "{}\n".format(database_conf.get("port")), ('database', 'dbname: '),
                 (database_conf.get("database"))]), left=2, right=0, min_width=20)], 20)
        self.footer = urwid.Columns([
            urwid.Padding(urwid.Text(self.make_page_menu(PAGE_TABLE)), left=2, right=0, min_width=20)
        ], 20)
        REFRESH = False
        if self.body:
            self.frame = urwid.Frame(urwid.AttrWrap(self.body, 'body'), header=self.header, footer=self.footer)
        if SNAPSHOOT and (
                FINISH or PID or FIND or JUMP_FIND or RankSign or DEL or SHOW or INDEX or EXPLAIN or DETAIL or DROP):  # 覆盖
            self.frame = urwid.Overlay(urwid.LineBox(self.cover), self.frame, 'center', ("relative", 60), "middle",
                                       ("relative", 60), 20, 9)
        elif ASP or ERROR or SLOW or STACK:  # 覆盖
            self.frame = urwid.Overlay(urwid.LineBox(self.cover), self.frame, 'center', ("relative", 60), "middle",
                                       ("relative", 60), 20, 9)

    def make_page_menu(self, page):
        menu = page_menu.get(page)
        menu_list = []
        if menu:
            for k, v in menu.items():
                menu_style = ('ctrl', u"{}".format("{}：{}".format(k, v)))
                menu_list.append(menu_style)
                menu_list.append('  ')
        else:
            menu_list.append('  ')
        return menu_list

    def make_field_button(self, field_list):
        """
        make sort selection buttons
        :param field_list: page all fields
        :return: return botton made through  urwid
        """
        button_list = []
        for field in field_list:
            button = urwid.Button(field)
            urwid.connect_signal(button, 'click', self.clik_order)
            button_list.append(button)
        return button_list

    def refresh(self):
        """
        refresh data immediately
        """
        self.sess_refresh()
        self._w = self.frame

    def get_text(self):
        """
        get input
        :return: return input data
        """
        text = self.edit.edit_text
        return text

    def clik_order(self, field):
        """
        make click button
        :param field: page all fields
        :return:return botton made through  urwid
        """
        global FINISH
        global DELAY
        global PID
        global BUTTON_KEY
        global IDX
        global RankSign
        global Rank
        global SORT
        global TURN
        button = field.get_label()
        BUTTON_KEY = button
        openGau = OpenGaussOption(conn_config=database_conf)
        if button == "force_all":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_all")
            BUTTON_KEY = ""
            self.openGau.conn.close()
            sess_main()
            raise urwid.ExitMainLoop()
        elif button == "force_by_dbname":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_by_dbname", datname=database_conf.get("database"))
            BUTTON_KEY = ""
            self.openGau.conn.close()
            sess_main()
            raise urwid.ExitMainLoop()
        elif button == "force_idle":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_idle")
            BUTTON_KEY = ""
            self.openGau.conn.close()
            sess_main()
            raise urwid.ExitMainLoop()
        elif button == "force_by_pid":
            FINISH = False
            TURN = True
            PID = True
            DELAY = 2
            self.openGau.conn.close()
            sess_main()
            raise urwid.ExitMainLoop()
        else:
            DELAY = 2
            RankSign = False
            Rank = True
            if IDX == button:
                if SORT:
                    SORT = False
                else:
                    SORT = True
            else:
                SORT = True
                IDX = button
            self.openGau.conn.close()

            sess_main()
            raise urwid.ExitMainLoop()

    def snapshot_body(self, snapshot_data, last_page):
        """
        processing snapshot data
        :param snapshot_data:data recorded by a snapshot
        :param last_page:snapshot page
        :return: snapshot data
        """
        field_list = background_data_field[last_page]
        heard = snapshot_data.get("heard")
        body = snapshot_data.get("body")
        text_data = ""
        body_field, heard_field = self.dist_field(last_page, field_list)
        if heard:
            text_data += "heard："
            text_data += "\n"
            for i in range(len(heard_field)):
                text_data += heard_field[i]
                text_data += ":"
                text_data += str(heard[heard_field[i]])
                text_data += "\n"
            text_data += "\n"
        if body:
            text_data += "body："
            text_data += "\n"
            if isinstance(body, dict):
                for i in range(len(body_field)):
                    text_data += body_field[i]
                    text_data += ":"
                    text_data += str(body[body_field[i]])
                    text_data += "\n"
            if isinstance(body, list):
                for body_data in body:
                    num = 0
                    for _ in range(len(body_field)):
                        text_data += body_field[num]
                        text_data += ":"
                        text_data += str(body_data[body_field[num]])
                        text_data += "\n"
                        num += 1
                    text_data += "\n"
        return text_data

    def dist_field(self, last_page, field_list):
        """
        generate snapshot field
        :param last_page:page
        :param field_list:page field list
        :return:
        """
        if last_page == 'database':
            body_field = field_list[6:-1]
            heard_field = field_list[0:6]
        elif last_page == 'session':
            body_field = field_list[18:-1]
            heard_field = field_list[0:18]
        elif last_page == 'session_detail':
            body_field = []
            heard_field = field_list[0:-1]
        elif last_page == 'sql_explain':
            body_field = field_list[0:-1]
            heard_field = []
        elif last_page == 'asp':
            body_field = field_list[0:-1]
            heard_field = []
        elif last_page == 'table':
            body_field = field_list[0:-1]
            heard_field = []
        elif last_page == 'lock':
            body_field = field_list[2:-1]
            heard_field = field_list[0:2]
        elif last_page == 'wait_event':
            body_field = field_list[1:-1]
            heard_field = field_list[0:1]
        elif last_page == 'memory':
            body_field = field_list[7:-1]
            heard_field = field_list[0:7]
        elif last_page == 'sharemem':
            body_field = field_list[1:-1]
            heard_field = field_list[0:1]
        elif last_page == 'top_mem_used_session':
            body_field = field_list[1:-1]
            heard_field = field_list[0:1]
        elif last_page == 'dynamicsql':
            body_field = field_list[1:-1]
            heard_field = field_list[0:1]
        elif last_page == 'rigorous_analysis':
            body_field = field_list[1:-1]
            heard_field = field_list[0:1]
        else:
            body_field = field_list[0:-1]
            heard_field = []
        return body_field, heard_field


def sess_main():
    """
    program shortcut keys refresh the main entry
    :return:
    """
    palette = [
        ('ctrl', 'light gray', 'dark blue'),
        ('host', 'light gray', 'dark blue'),
        ('version', 'light gray', 'dark blue'),
        ('database', 'dark gray', 'black', ('bold', 'standout')),
        ('time', 'dark gray', 'black', ('bold', 'standout')),
        ('title', 'dark green', 'black'),
        ('field', 'yellow', 'black', 'bold'),
        ('warning', 'black', 'light gray'),
    ]

    if urwid.web_display.is_web_request():
        screen = urwid.web_display.Screen()
    else:
        screen = urwid.raw_display.Screen()

    frame = Frame()

    def unhandled(key):
        """
        shortcut key control
        :param key:shortcut key
        :return:
        """
        global BUTTON_KEY
        global PID
        global ID
        global DELAY
        global FIND
        global FUZZY_KEY
        global Rank
        global SORT
        global PAGE_TABLE
        global KILL
        global IDX
        global RankSign
        global HELP
        global FINISH
        global PART_MAX
        global PART_MIN
        global PART_SECTION
        global MAX_NUMBER_OF_BITS
        global QUERY
        global DEL
        global SHOW
        global ERROR
        global REFRESH
        global JUMP_FIND
        global LAST_PAGE
        global SNAPSHOOT_USE_PAGE
        global ERROR
        global INDEX
        global EXPLAIN
        global SCHEMA
        global FIXED_PLATE
        global STA_TIME
        global END_TIME
        global ASP
        global DETAIL
        global ACTIVE_DATA
        global SNAPSHOOT
        global TURN
        global SCHEMANAME_RELNAME
        global PATCHNAME
        global DROP
        global SLOW_FIRST_TIME
        global SLOW_SECOND_END
        global SLOW
        global STACK
        global START_SNAPSHOT_ID
        global END_SNAPSHOT_ID
        global SNAP_UNIQUE_SQL_ID_SET
        global SNAPSHOT_ID
        LAST_PAGE = PAGE_TABLE
        if key in ["d", "T", "s", "U", "W", "m", "D", "r", "l", "P", "E", "o", "x", "w"]:
            INDEX = False
            PID = False
            FIND = False
            FUZZY_KEY = ""
            Rank = False
            RankSign = False
            SORT = False
            KILL = False
            IDX = ""
            SCHEMANAME_RELNAME = ""
            HELP = False
            FINISH = False
            BUTTON_KEY = ""
            ID = ""
            QUERY = ""
            DEL = False
            IDX = ""
            DEL = False
            SHOW = False
            ERROR = False
            INDEX = False
            EXPLAIN = False
            ASP = False
            DETAIL = False
            PATCHNAME = ""
            DROP = False
            STA_TIME = ''
            END_TIME = ''
            SLOW = False
            STACK = False
            START_SNAPSHOT_ID = ""
            END_SNAPSHOT_ID = ""
            SNAP_UNIQUE_SQL_ID_SET = ""
            frame.advised_indexes = ""
            frame.old_database_ob = ''
            frame.old_session_ob = ''
            frame.old_slow_sql_ob = ""
            frame.old_session_detail_ob = ''
            frame.asp_ob = ""
            frame.old_table_ob = ''
            frame.old_table_performance_ob = ''
            frame.old_table_index_ob = ''
            frame.old_table_detail_ob = ''
            frame.old_lock_ob = ''
            frame.old_wait_event_ob = ''
            frame.old_memory_ob = ''
            frame.old_sharemem_ob = ''
            frame.old_top_mem_used_session_ob = ''
            frame.old_dynamicsql_ob = ''
            frame.old_dynamicsql_track = ''
            frame.old_sql_patch_ob = ''
            frame.old_replication_ob = []
            frame.old_redo_ob = ""
            frame.old_redo_time_count_ob = ""
            frame.old_xlog_redo_statics_ob = ""
            frame.old_wdr_ob = ""
            frame.old_snap_summary_more_statement_ob = ""
            frame.burr_analysis = ""
            frame.rigorous_analysis = ""
            frame.snap_summary_statement = ""
            ACTIVE_DATA = pd.DataFrame([])
            SNAPSHOOT = {}
            if DELAY == 86400:
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
        elif key in ["A", "t", "B"]:
            INDEX = False
            PID = False
            FIND = False
            FUZZY_KEY = ""
            Rank = False
            RankSign = False
            SORT = False
            KILL = False
            IDX = ""
            SCHEMANAME_RELNAME = ""
            HELP = False
            FINISH = False
            BUTTON_KEY = ""
            ID = ""
            QUERY = ""
            DEL = False
            IDX = ""
            DEL = False
            SHOW = False
            ERROR = False
            INDEX = False
            EXPLAIN = False
            ASP = False
            DETAIL = False
            PATCHNAME = ""
            DROP = False
            STA_TIME = ''
            END_TIME = ''
            SLOW = False
            STACK = False
            frame.advised_indexes = ""
            frame.old_database_ob = ''
            frame.old_session_ob = ''
            frame.old_slow_sql_ob = ""
            frame.old_session_detail_ob = ''
            frame.asp_ob = ""
            frame.old_table_ob = ''
            frame.old_table_performance_ob = ''
            frame.old_table_index_ob = ''
            frame.old_table_detail_ob = ''
            frame.old_lock_ob = ''
            frame.old_wait_event_ob = ''
            frame.old_memory_ob = ''
            frame.old_sharemem_ob = ''
            frame.old_top_mem_used_session_ob = ''
            frame.old_dynamicsql_ob = ''
            frame.old_dynamicsql_track = ''
            frame.old_sql_patch_ob = ''
            frame.old_replication_ob = []
            ACTIVE_DATA = pd.DataFrame([])
            SNAPSHOOT = {}
            DELAY = 86400
        if HELP and key not in ['q', 'h', 'esc']:
            frame.refresh()
        elif key == 'q':
            raise urwid.ExitMainLoop()
        elif key == 'h':
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                HELP = True
                DELAY = 86400
                frame.refresh()
        elif key == ' ':
            # 空格键实时刷新
            if REFRESH is False:
                REFRESH = True
                frame.refresh()

        elif key == "F":
            if PAGE_TABLE in ['snapshoot', 'asp', 'table', 'slow_sql', 'table_detail']:
                frame.refresh()

            elif DELAY == 2:
                DELAY = 86400
                # FIXED_PLATE = True
                sloop.set_alarm_at(DELAY, refresh)
            else:
                DELAY = 2
                # FIXED_PLATE = False
                sloop.set_alarm_at(DELAY, refresh)
        elif key == "p":
            if PAGE_TABLE not in ["home", "snapshoot", "rigorous_analysis"]:
                SNAPSHOOT_USE_PAGE = PAGE_TABLE
                PAGE_TABLE = "snapshoot"
                DELAY = 86400
                frame.refresh()
        elif key == "f9" and PAGE_TABLE == "snapshoot":
            data = frame.get_text()
            with open(sys.path[0] + "/ogtop_{}".format(SNAPSHOOT_USE_PAGE) + ".txt", 'w')as f:
                f.write(data)
            PAGE_TABLE = SNAPSHOOT_USE_PAGE
            if PAGE_TABLE in ['asp', 'table', 'table_detail', 'slow_sql', 'sql_explain', 'index_recommendation']:
                frame.refresh()
            else:
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
        elif key == "f10" and PAGE_TABLE == "snapshoot":
            PAGE_TABLE = SNAPSHOOT_USE_PAGE
            if PAGE_TABLE in ['asp', 'table', 'table_detail', 'slow_sql', 'sql_explain', 'index_recommendation']:
                frame.refresh()
            else:
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
        elif key == 'd':
            PAGE_TABLE = "database"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 's':
            Rank = True
            SORT = False
            IDX = "current_state"
            QUERY = ""
            ID = ""
            PAGE_TABLE = "session"
            MAX_NUMBER_OF_BITS = 7
            PART_MAX = 25
            PART_MIN = 20
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'A':
            Rank = True
            SORT = False
            IDX = "datname"
            QUERY = ""
            ID = ""
            ASP = True
            PAGE_TABLE = "asp"
            DELAY = 86400
            SNAPSHOOT = {}
            sloop.set_alarm_at(DELAY, refresh)
        elif key == "t":
            PAGE_TABLE = "table"
            Rank = True
            SORT = True
            IDX = "totalsize"
            MAX_NUMBER_OF_BITS = 5
            PART_MAX = 7
            PART_MIN = 3
            DELAY = 86400
            frame.refresh()
        elif key == "T":
            Rank = True
            SORT = False
            IDX = "relname"
            PAGE_TABLE = "table_performance"
            MAX_NUMBER_OF_BITS = 5
            PART_MAX = 7
            PART_MIN = 3
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'U':
            Rank = True
            SORT = False
            IDX = "pid"
            PAGE_TABLE = "lock"
            MAX_NUMBER_OF_BITS = 7
            PART_MAX = 8
            PART_MIN = 2
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'L' and PAGE_TABLE in ["lock"]:
            PAGE_TABLE = "lock_chain"
            IDX = "blocker_holder"
            Rank = True
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'W':
            Rank = True
            SORT = True
            IDX = "total_wait_time"
            PAGE_TABLE = "wait_event"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "m":
            PAGE_TABLE = "memory"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "D":
            PAGE_TABLE = "dynamicsql"
            Rank = True
            SORT = True
            IDX = "db_time"
            MAX_NUMBER_OF_BITS = 6
            PART_MAX = 8
            PART_MIN = 3
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            # else:
            #     frame.refresh()
        elif key == "r":
            PAGE_TABLE = "replication"
            MAX_NUMBER_OF_BITS = 6
            PART_MAX = 7
            PART_MIN = 2
            Rank = True
            IDX = "channel"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "l":
            PAGE_TABLE = "database_list"
            Rank = True
            IDX = "datname"
            MAX_NUMBER_OF_BITS = 7
            PART_MAX = 8
            PART_MIN = 1
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "P":
            Rank = True
            SORT = True
            IDX = "patch_name"
            PAGE_TABLE = "sql_patch"
            MAX_NUMBER_OF_BITS = 6
            PART_MAX = 7
            PART_MIN = 2
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "B":
            SLOW = True
            Rank = True
            SORT = False
            IDX = "duration_ms"
            QUERY = ""
            ID = ""
            PAGE_TABLE = "slow_sql"
            DELAY = 86400
            sloop.set_alarm_at(DELAY, refresh)
        elif key == 'S' and PAGE_TABLE == "memory":
            PAGE_TABLE = "sharemem"
            Rank = True
            SORT = True
            IDX = "usedsize"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'R' and PAGE_TABLE == "memory":
            PAGE_TABLE = "top_mem_used_session"
            Rank = True
            SORT = True
            IDX = "used_mem"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'j' and PAGE_TABLE == "wait_event":
            PID = False
            ID = ""
            FIND = False
            SORT = True
            Rank = False
            RankSign = False
            STACK = False
            IDX = ""
            DELAY = 86400
            JUMP_FIND = True
            frame.refresh()
        elif key == 'a' and PAGE_TABLE in ["session", "asp", "table_detail", "lock",
                                           "lock_chain", "top_mem_used_session", "dynamicsql", "sql_patch", "slow_sql",
                                           "wdr", "snap_summary_more_statement", "burr_analysis"]:
            FIND = False
            JUMP_FIND = False
            RankSign = False
            PATCHNAME = ""
            DROP = False
            STACK = False
            DELAY = 86400
            PID = True
            frame.refresh()
        elif key == "/" and PAGE_TABLE in ["session", "asp", "table", "table_performance", "lock",
                                           "wait_event", "sharemem", "top_mem_used_session", "replication",
                                           "dynamicsql", "slow_sql", "snap_summary_more_statement"]:
            PID = False
            ID = ""
            JUMP_FIND = False
            RankSign = False
            PATCHNAME = ""
            DROP = False
            STACK = False
            FIND = True
            DELAY = 86400
            frame.refresh()
        elif key == "c" and PAGE_TABLE in ["session_detail"]:
            STACK = True
            frame.refresh()
        elif key == "z" and PAGE_TABLE in ["session", "asp", "table", "table_performance", "lock",
                                           "wait_event", "sharemem",
                                           "top_mem_used_session", "dynamicsql", "slow_sql",
                                           "snap_summary_more_statement"]:
            PID = False
            ID = ""
            FIND = False
            JUMP_FIND = False
            PATCHNAME = ""
            DROP = False
            STACK = False
            DELAY = 86400
            RankSign = True
            frame.refresh()
        elif key == 'v' and PAGE_TABLE in ["table", "table_performance", "snap_summary_more_statement", "burr_analysis",
                                           "rigorous_analysis", "wdr"]:
            FIND = False
            JUMP_FIND = False
            RankSign = False
            DELAY = 86400
            PID = False
            STACK = False
            DETAIL = True
            frame.refresh()
        elif key == "i" and PAGE_TABLE in ["session", "dynamicsql", "asp", "slow_sql"]:
            if PAGE_TABLE in ["asp", "slow_sql"]:
                FIND = False
                JUMP_FIND = False
                RankSign = False
                PID = False
                STACK = False
                PAGE_TABLE = "index_recommendation"
                frame.refresh()
            elif PAGE_TABLE in ["session"]:
                FIND = False
                JUMP_FIND = False
                RankSign = False
                PID = False
                STACK = False
                DELAY = 86400
                PAGE_TABLE = "index_recommendation"
                frame.refresh()
            else:
                FIND = False
                JUMP_FIND = False
                RankSign = False
                DELAY = 86400
                PID = False
                STACK = False
                INDEX = True
                frame.refresh()
        elif key == "e" and PAGE_TABLE in ["session_detail"]:
            EXPLAIN = True
            DELAY = 86400
            frame.refresh()
        elif key == "f" and PAGE_TABLE in ["session", "session_detail", "lock", "lock_chain",
                                           "top_mem_used_session", "sql_patch", "replication"] and HELP == False:
            if PAGE_TABLE == "session_detail":
                DELAY = 86400
                KILL = True
                frame.refresh()
            elif PAGE_TABLE == "session":
                DELAY = 86400
                FINISH = True
                frame.refresh()
            elif PAGE_TABLE in ["sql_patch", "replication"]:
                DELAY = 86400
                DROP = True
                frame.refresh()
            else:
                DELAY = 86400
                DEL = True
                frame.refresh()
        elif key == "b" and PAGE_TABLE in ["sql_explain", "table_index"]:
            if PAGE_TABLE == "sql_explain":
                DELAY = 2
                PAGE_TABLE = "session_detail"
                sloop.set_alarm_in(DELAY, refresh)
            else:
                DELAY = 86400
                PAGE_TABLE = "table_detail"
                frame.refresh()
        elif key == "E":
            Rank = True
            SORT = True
            IDX = "redo_start_ptr"
            PAGE_TABLE = "redo"
            MAX_NUMBER_OF_BITS = 8
            PART_MAX = 7
            PART_MIN = 0
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "o":
            Rank = True
            SORT = True
            IDX = "thread_name"
            PAGE_TABLE = "redo_time_count"
            MAX_NUMBER_OF_BITS = 7
            PART_MAX = 7
            PART_MIN = 1
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "x":
            PAGE_TABLE = "xlog_redo_statics"
            Rank = True
            IDX = "rmid"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'M' and PAGE_TABLE == 'snap_summary_more_statement':
            PAGE_TABLE = "burr_analysis"
            DELAY = 86400
            frame.refresh()
        elif key == "w":
            openGau = OpenGaussOption(conn_config=database_conf)
            frame.monitor_authority = \
                openGau.params_query_sql(db_config.monitor_authority_sql, (database_conf['user'],))[0][0]
            frame.check_whether_wdr_takes_effect = \
                openGau.all_query_sql(db_config.check_whether_wdr_takes_effect_sql)[0][0]
            openGau.conn.close()
            PAGE_TABLE = "wdr"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "enter":
            if PID and PAGE_TABLE in ["session", "asp", "table_detail", "lock", "lock_chain",
                                      "top_mem_used_session", "dynamicsql", "sql_patch", "slow_sql", "wdr",
                                      "snap_summary_more_statement", "burr_analysis"]:
                if BUTTON_KEY == "force_by_pid":
                    try:
                        if int(frame.get_text()):
                            PID = False
                            DELAY = 2
                            ID = frame.get_text()
                            openGau = OpenGaussOption(conn_config=database_conf)
                            openGau.finish_sql("force_by_pid", pid=ID)
                            BUTTON_KEY = ""
                            sloop.set_alarm_in(DELAY, refresh)
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["table_detail"]:
                    Rank = True
                    IDX = "indexrelname"
                    PID = False
                    DELAY = 2
                    MAX_NUMBER_OF_BITS = 6
                    PART_MAX = 7
                    PART_MIN = 2
                    ID = frame.get_text()
                    PAGE_TABLE = "table_index"
                    sloop.set_alarm_in(DELAY, refresh)
                elif PID and PAGE_TABLE in ["dynamicsql"]:
                    try:
                        if int(frame.get_text()):
                            PID = False
                            ID = frame.get_text()
                            frame.refresh()
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["asp"]:
                    try:
                        if int(frame.get_text()):
                            PID = False
                            ID = frame.get_text()
                            frame.refresh()
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["sql_patch"]:
                    try:
                        if frame.get_text():
                            PID = False
                            PATCHNAME = frame.get_text().split(" ")[0]
                            ID = int(frame.get_text().split(" ")[1])
                            DELAY = 2
                            sloop.set_alarm_in(DELAY, refresh)
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["slow_sql"]:
                    try:
                        if frame.get_text():
                            PID = False
                            ID = frame.get_text()
                            frame.refresh()
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["wdr"]:
                    try:
                        if frame.get_text():
                            PID = False
                            IDX = 'total_elapse_time'
                            snapshot_id_list = frame.get_text().split('-')
                            START_SNAPSHOT_ID = int(snapshot_id_list[0])
                            END_SNAPSHOT_ID = int(snapshot_id_list[1])
                            PAGE_TABLE = 'snap_summary_more_statement'
                            MAX_NUMBER_OF_BITS = 5
                            PART_MAX = 8
                            PART_MIN = 4
                            frame.refresh()
                    except Exception as e:
                        '-'
                elif PID and PAGE_TABLE in ["snap_summary_more_statement", "burr_analysis"]:
                    try:
                        if frame.get_text():
                            PID = False
                            ID = frame.get_text()
                            PAGE_TABLE = 'rigorous_analysis'
                            frame.refresh()
                    except Exception as e:
                        '-'
                else:
                    try:
                        if int(frame.get_text()):
                            PID = False
                            DELAY = 2
                            ID = frame.get_text()
                            PAGE_TABLE = "session_detail"
                            sloop.set_alarm_in(DELAY, refresh)
                    except Exception as e:
                        '-'
            elif DETAIL and PAGE_TABLE in ["table", "table_performance", "snap_summary_more_statement", 'burr_analysis',
                                           'rigorous_analysis', 'wdr']:
                if PAGE_TABLE in ["snap_summary_more_statement", 'burr_analysis', 'rigorous_analysis']:
                    try:
                        if frame.get_text():
                            DETAIL = False
                            snapshot_id_list = frame.get_text().split('-')
                            SNAPSHOT_ID = snapshot_id_list[0]
                            ID = snapshot_id_list[1]
                            PAGE_TABLE = 'snap_summary_statement'
                            frame.refresh()
                    except Exception as e:
                        '-'
                elif PAGE_TABLE in ["wdr"]:
                    try:
                        if frame.get_text():
                            DETAIL = False
                            snapshot_id_list = frame.get_text().split('-')
                            start_snapshot_id = snapshot_id_list[0]
                            end_snapshot_id = snapshot_id_list[1]
                            try:
                                if os.path.exists(f'{sys.path[0]}/ogtop_WDR_ASH.html'):
                                    os.system('touch ./ogtop_.html')
                                openGau = OpenGaussOption(conn_config=database_conf)
                                wdr_sql1 = "select create_wdr_snapshot();"
                                wdr_sql2 = f"select generate_wdr_report({start_snapshot_id},{end_snapshot_id},'all','node',pgxc_node_str()::cstring);"
                                openGau.all_query_sql(wdr_sql1)
                                with open(f'{sys.path[0]}/ogtop_WDR_ASH.html', 'w') as f:
                                    f.write(openGau.all_query_sql(wdr_sql2)[0][0])
                                SHOW = True
                            except Exception as e:
                                '-'
                            frame.refresh()
                    except Exception as e:
                        '-'
                else:
                    DETAIL = False
                    DELAY = 86400
                    SCHEMANAME_RELNAME = frame.get_text()
                    PAGE_TABLE = "table_detail"
                    frame.refresh()
            elif INDEX:
                try:
                    if int(frame.get_text()):
                        INDEX = False
                        DELAY = 2
                        ID = frame.get_text()
                        PAGE_TABLE = "index_recommendation"
                        sloop.set_alarm_in(DELAY, refresh)
                except Exception as e:
                    '-'
            elif DEL and PAGE_TABLE in ["lock", "lock_chain", "top_mem_used_session", "sql_patch"]:
                DEL = False
                DELAY = 2
                ID = frame.get_text()
                openGau = OpenGaussOption(conn_config=database_conf)
                sql = "select pg_terminate_backend (%s);"
                openGau.params_query_sql(sql, (ID,))
                sloop.set_alarm_in(DELAY, refresh)
            elif FIND and PAGE_TABLE in ["session", "asp", "table", "table_performance", "lock",
                                         "wait_event", "sharemem", "top_mem_used_session", "replication", "dynamicsql",
                                         "slow_sql"]:
                if PAGE_TABLE in ['asp', 'table', "slow_sql"]:
                    FIND = False
                    FUZZY_KEY = frame.get_text()
                    frame.refresh()
                else:
                    FIND = False
                    FUZZY_KEY = frame.get_text()
                    DELAY = 2
                    sloop.set_alarm_in(DELAY, refresh)
            elif ASP:
                try:
                    if datetime.datetime.strptime(frame.get_text(), "%Y-%m-%d %H:%M:%S"):
                        MAX_NUMBER_OF_BITS = 6
                        PART_MAX = 7
                        PART_MIN = 0
                        STA_TIME = frame.get_text()
                        ASP = False
                        END_TIME = frame.get_text()
                        DELAY = 2
                        sloop.set_alarm_in(DELAY, refresh)
                except Exception  as e:
                    '-'
            elif SLOW:
                try:
                    if datetime.datetime.strptime(frame.get_text().split('~')[0],
                                                  "%Y-%m-%d %H:%M:%S") and datetime.datetime.strptime(
                        frame.get_text().split('~')[1], "%Y-%m-%d %H:%M:%S"):
                        MAX_NUMBER_OF_BITS = 6
                        PART_MAX = 7
                        PART_MIN = 2
                        STA_TIME = frame.get_text().split('~')[0]
                        END_TIME = frame.get_text().split('~')[1]
                        SLOW = False
                        frame.refresh()
                except Exception  as e:
                    '-'
            elif JUMP_FIND and PAGE_TABLE in ["wait_event"]:
                PAGE_TABLE = "session"
                Rank = True
                SORT = False
                IDX = "current_state"
                JUMP_FIND = False
                PART_MAX = 25
                PART_MIN = 20
                FUZZY_KEY = frame.get_text()
                FUZZY_KEY = frame.get_text()
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            elif PAGE_TABLE in ["session_detail"] and KILL:
                if frame.get_text() == 'y':
                    openGau = OpenGaussOption(conn_config=database_conf)
                    sql = "select pg_terminate_backend (%s);"
                    openGau.params_query_sql(sql, (ID,))
                    DELAY = 2
                    ID = ""
                    KILL = False
                    if PAGE_TABLE not in ["lock", "lock_chain"]:
                        PAGE_TABLE = "session"
                    sloop.set_alarm_in(DELAY, refresh)
                elif frame.get_text() == 'n':
                    DELAY = 2
                    KILL = False
                    sloop.set_alarm_in(DELAY, refresh)
            elif EXPLAIN and PAGE_TABLE in ["session_detail"]:
                SCHEMA = frame.get_text()
                openGau = OpenGaussOption(conn_config=database_conf)
                sql = "select nspname from pg_namespace;"
                schema_info = openGau.all_query_sql(sql)
                schema_list = []
                for schema_index in range(len(schema_info)):
                    schema_list.append(schema_info[schema_index][0])
                if SCHEMA in schema_list:
                    sql = "set current_schema =  (%s);"
                    openGau.params_query_sql(sql, (SCHEMA,))
                    PAGE_TABLE = "sql_explain"
                    EXPLAIN = False
                    SCHEMA = ""
                    frame.refresh()
                else:
                    frame.text = urwid.Text("schema is not found!")
                    frame.cover = urwid.Filler(frame.text, "middle")
                    ERROR = True
                    DELAY = 2
                    EXPLAIN = False
                    SCHEMA = ""
                    sloop.set_alarm_in(DELAY, refresh)
            elif DROP and PAGE_TABLE in ["sql_patch", "replication"]:
                if PAGE_TABLE == "sql_patch":
                    try:
                        if frame.get_text():
                            DROP = False
                            PATCHNAME = frame.get_text()
                            DELAY = 2
                            sloop.set_alarm_in(DELAY, refresh)
                    except Exception as e:
                        '-'
                else:
                    try:
                        if frame.get_text():
                            DROP = False
                            ID = frame.get_text()
                            DELAY = 2
                            sloop.set_alarm_in(DELAY, refresh)
                    except Exception as e:
                        '-'
        elif key == "esc" and PAGE_TABLE != "snapshoot":
            ERROR = False
            PID = False
            FIND = False
            JUMP_FIND = False
            RankSign = False
            KILL = False
            HELP = False
            FINISH = False
            BUTTON_KEY = ""
            DEL = False
            SHOW = False
            INDEX = False
            EXPLAIN = False
            SCHEMA = ""
            FUZZY_KEY = ""
            ASP = False
            SLOW = False
            DETAIL = False
            PATCHNAME = ""
            DROP = False
            if PAGE_TABLE in ['asp', "table", "table_detail", "slow_sql"]:
                ID = ""
                QUERY = ""
                DELAY = 86400
                frame.refresh()
            elif PAGE_TABLE in ['sql_explain', 'index_recommendation', 'snap_summary_more_statement', 'burr_analysis',
                                'rigorous_analysis', 'home']:
                DELAY = 86400
                frame.refresh()
            elif PAGE_TABLE in ['session_detail', 'table_index']:
                STACK = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                ID = ""
                QUERY = ""
                if DELAY != 2:
                    DELAY = 2
                    sloop.set_alarm_in(DELAY, refresh)
        elif key == "left" and PAGE_TABLE != "snapshoot" and RankSign + FIND + PID + STACK == False:
            if PAGE_TABLE not in ["home", 'snapshoot', 'database', 'session_detail', 'sql_explain', 'table_detail',
                                  'table_index', 'lock_chain', 'wait_event', 'memory', 'top_mem_used_session',
                                  'replication', 'sharemem', 'xlog_redo_statics', 'wdr', 'burr_analysis',
                                  'rigorous_analysis']:
                if PART_MIN > PART_SECTION[0]:
                    PART_MAX -= 1
                    PART_MIN -= 1
                TURN = True
                frame.refresh()
        elif key == "right" and PAGE_TABLE != "snapshoot" and RankSign + FIND + PID + STACK == False:
            if PAGE_TABLE not in ["home", 'snapshoot', 'database', 'session_detail', 'sql_explain', 'table_detail',
                                  'table_index', 'lock_chain', 'wait_event', 'memory', 'top_mem_used_session',
                                  'replication', 'sharemem', 'xlog_redo_statics', 'wdr', 'burr_analysis',
                                  'rigorous_analysis']:
                if PART_MAX < PART_SECTION[1]:
                    PART_MAX += 1
                    PART_MIN += 1
                TURN = True
                frame.refresh()

    sloop = urwid.MainLoop(frame, handle_mouse=False, palette=palette, screen=screen, unhandled_input=unhandled)

    def refresh(_loop, _data):
        """
        timed  refresh
        :param _loop:
        :param _data:
        :return:
        """
        frame.refresh()
        _loop.set_alarm_in(DELAY, refresh)

    sloop.set_alarm_in(DELAY, refresh)
    sloop.run()


def sutep():
    """
    program validation start
    :return:
    """
    try:
        op = OpenGaussOption(conn_config=database_conf)
        if op.conn:
            op.conn.close()
    except Exception as e:
        return
    sess_main()


if __name__ == '__main__':
    """program main entry"""
    import getpass
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-v', '--version', help="output version information, then exit", action='store_true')
    parser.add_argument('-?', '--help', help=" show this help, then exit", action='store_true')
    parser.add_argument('-h', '--host', help='ip address of target database server')
    parser.add_argument('-p', '--port', help='target database port')
    parser.add_argument('-d', '--database', help='target database')
    parser.add_argument('-u', '--user', help='login user')
    parser.add_argument('-W', '--password', help='the password of specified database user')
    args = parser.parse_args()
    if args.version:
        print(version_tip)
        sys.exit(0)
    elif args.help:
        print(help_tip)
        sys.exit(0)
    if args.host == None:
        args.host = socket.gethostbyname(socket.gethostname())
    if getpass.getuser() == "omm":
        if args.user == None:
            args.user = getpass.getuser()
    if args.password is None and args.user != "omm":
        try:
            password = getpass.getpass("please enter the database user password：")
            args.password = password
        except KeyboardInterrupt as e:
            print("\nthank you for using")
    if args.host and args.port and args.database and args.user and args.password:
        try:
            print("Starting,please wait!")
            database_conf["host"] = args.host
            database_conf["port"] = int(args.port)
            database_conf["database"] = args.database
            database_conf["user"] = args.user
            database_conf["password"] = args.password
            database_conf["application_name"] = 'ogtop'
            sutep()
        except KeyboardInterrupt as e:
            print("\nthank you for using")
    elif args.user == "omm" and socket.gethostbyname(socket.gethostname()) == args.host:
        try:
            print("Starting,please wait!")
            database_conf["host"] = args.host
            database_conf["port"] = int(args.port)
            database_conf["database"] = args.database
            database_conf["user"] = args.user
            database_conf["password"] = args.password
            database_conf["application_name"] = 'ogtop'
            sutep()
        except KeyboardInterrupt as e:
            print("\nthank you for using")
    else:
        print('failed to connect Ogtop')
        sys.exit(0)
