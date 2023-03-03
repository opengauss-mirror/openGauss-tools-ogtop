# -*- coding: utf-8 -*-
import sys
import json
import time
import pandas as pd
import re

import socket
import urwid
import urwid.raw_display
import urwid.web_display
import db_config
import database_query
import session_query
import session_detail_query
import table_query
import table_detail_query
import lock_query
import lock_eventwait_query
import memory_query
import sharemem_query
import top_mem_used_session_query
import dynamicsql_query
import replication_query
from db_util import conversion_type, fuzzy_match, sort_query, openGauss_option, get_localtime, get_host, get_query
from display import database_body, session_body, session_detail_body, home_body, help_body, table_body, \
    table_index_body, lock_body, lock_detail_body, sql_explain, wait_event_body, memory_body, sharemem_body, \
    top_mem_used_session_body, dynamicsql_body, replication_body
from shortcut_key_menu import sort_querys_dict

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
KILL = False  # session_detail page  kill session mark
FINISH = False  # session page kill session mark
BUTTON_KEY = ""
PART = 1
PART_SIZE = 0
QUERY = ""  # session query
DEL = False  # lock kill identity
SHOW = False  # floating window sign
ERROR = False  # sql error identity
REFRESH = False  # prevent forcibly refreshing flags
SNAPSHOOT = ""  # snapshot data
database_conf = {}


class Frame(urwid.WidgetWrap):
    """
    frame类,实现Text部件动态更新,bulid_body()、bulid_frame()重新构建加载body、frame
    """

    def __init__(self):
        self.openGau = openGauss_option(conn_config=database_conf)
        self.old_database_ob = database_query.DatabaseMain(self.openGau,
                                                           sqls=db_config.database_spare_sql,
                                                           params=database_conf.get("database")) if isinstance(
            database_query.DatabaseMain(self.openGau,
                                        sqls=db_config.database_sql,
                                        params=database_conf.get("database")).de_part, str) else \
            database_query.DatabaseMain(self.openGau,
                                        sqls=db_config.database_sql,
                                        params=database_conf.get("database"))
        self.old_session_ob = session_query.SessionMain(self.openGau, sqls=db_config.session_sql)
        self.old_session_detail_ob = ""
        self.old_table_ob = table_query.TableMain(self.openGau, sqls=db_config.table_sql)
        self.old_table_index_ob = ""
        self.old_lock_ob = lock_query.LockMain(self.openGau, sqls=db_config.lock_sql)
        self.old_wait_event_ob = lock_eventwait_query.WaitEventMain(self.openGau, sqls=db_config.even_twait_sql)
        self.old_memory_ob = memory_query.MemoryMain(self.openGau, sqls=db_config.memory_sql)
        self.old_dynamicsql_ob = dynamicsql_query.DynamicsqlMain(self.openGau, sqls=db_config.dynamicsql_sql)
        self.view_role = self.openGau.all_query_sql(db_config.view_role)
        self.old_main_replication_ob = replication_query.ReplicationMain(self.openGau,
                                                                         sqls=db_config.main_replication_sql)
        self.old_prepare_replication_ob = replication_query.ReplicationMain(self.openGau,
                                                                            sqls=db_config.prepare_replication_sql)
        self.sess_refresh()
        self._w = self.frame
        self.__super.__init__(self._w)

    def sess_refresh(self):
        """
        页面填充数据主函数
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
        global PART
        global QUERY
        global DEL
        global PART_SIZE
        global SHOW
        global ERROR
        global REFRESH
        global SNAPSHOOT
        self.db_host_time = self.openGau.all_query_sql(db_config.time)
        if PAGE_TABLE == "home":
            self.body = home_body(self.db_host_time)
        elif HELP:
            PART_SIZE = 1
            if help_body(PAGE_TABLE):
                self.body = help_body(PAGE_TABLE)
            else:
                HELP = False
        elif PAGE_TABLE == "snapshoot":
            self.edit = urwid.Edit("", edit_pos=0, edit_text=self.snapshot_body(SNAPSHOOT, LAST_PAGE))
            self.body = urwid.Filler(self.edit)
        elif PAGE_TABLE == "database":
            if isinstance(self.old_database_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("database page temporarily inaccessible：{}".format(self.old_database_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                new_ob = database_query.DatabaseMain(self.openGau,
                                                     sqls=db_config.database_spare_sql,
                                                     params=database_conf.get("database")) if isinstance(
                    database_query.DatabaseMain(self.openGau,
                                                sqls=db_config.database_sql,
                                                params=database_conf.get("database")).de_part, str) else \
                    database_query.DatabaseMain(self.openGau,
                                                sqls=db_config.database_sql,
                                                params=database_conf.get("database"))
                values = database_query.DatabaseMain.compute_delta(self.old_database_ob, new_ob)
                self.old_database_ob = new_ob
                del new_ob
                up_parts, de_parts = values.split('%')
                depart = json.loads(de_parts)
                depart = conversion_type(depart)
                uppart = json.loads(up_parts)
                if DELAY == 86400:
                    uppart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": uppart, "body": depart}
                self.body = database_body(uppart, depart)
        elif PAGE_TABLE == "session":
            if isinstance(self.old_session_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("session page temporarily inaccessible：{}".format(self.old_session_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 3
                new_ob = session_query.SessionMain(self.openGau, sqls=db_config.session_sql)
                new_depart, new_heard_depart = session_query.SessionMain.intergrated_data(new_ob.de_part)
                old_depart, old_heard_depart = session_query.SessionMain.intergrated_data(
                    self.old_session_ob.de_part)
                self.old_session_ob = new_ob
                del new_ob
                depart, heard_depart = session_query.SessionMain.compute_delta(new_depart, new_heard_depart,
                                                                               old_depart, old_heard_depart)
                if DELAY == 86400:
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": heard_depart, "body": depart}
                if FUZZY_KEY:
                    depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
                if Rank and IDX:
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)

                self.body = session_body(heard_depart, depart, PART)
                if PID:
                    self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = sort_querys_dict["session"][18::]
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
                    self.cover = urwid.LineBox(self.cover)
        elif PAGE_TABLE == "session_detail":
            if ID != "":
                if self.old_session_detail_ob == "":
                    self.old_session_detail_ob = session_detail_query.SessionDetailMain(self.openGau,
                                                                                        sqls=db_config.session_detail_spare_sql,
                                                                                        params=(
                                                                                            f'%{ID}%', f'%{ID}%',
                                                                                            f'%{ID}%', f'%{ID}%',
                                                                                            f'%{ID}%',
                                                                                        )) if isinstance(
                        session_detail_query.SessionDetailMain(self.openGau, sqls=db_config.session_detail_sql,
                                                               params=(
                                                                   f'%{ID}%', f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                   f'%{ID}%',
                                                                   f'%{ID}%')).de_part,
                        str) else session_detail_query.SessionDetailMain(self.openGau,
                                                                         sqls=db_config.session_detail_sql,
                                                                         params=(
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                             f'%{ID}%',
                                                                             f'%{ID}%', f'%{ID}%'))
                if isinstance(self.old_session_detail_ob, str):
                    ERROR = True
                    PAGE_TABLE = LAST_PAGE
                    self.text = urwid.Text("session_detail page temporarily inaccessible：{}".format(
                        session_detail_query.SessionDetailMain(self.openGau,
                                                               sqls=db_config.session_detail_sql,
                                                               params=(
                                                                   f'%{ID}%', f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                   f'%{ID}%',
                                                                   f'%{ID}%')).de_part))
                    self.cover = urwid.Filler(self.text, "middle")
                else:
                    PART_SIZE = 1
                    new_ob = session_detail_query.SessionDetailMain(self.openGau,
                                                                    sqls=db_config.session_detail_spare_sql,
                                                                    params=(
                                                                        f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                        f'%{ID}%',
                                                                        f'%{ID}%')) if isinstance(
                        session_detail_query.SessionDetailMain(self.openGau, sqls=db_config.session_detail_sql,
                                                               params=(
                                                                   f'%{ID}%', f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                   f'%{ID}%',
                                                                   f'%{ID}%')).de_part,
                        str) else session_detail_query.SessionDetailMain(self.openGau,
                                                                         sqls=db_config.session_detail_sql,
                                                                         params=(
                                                                             f'%{ID}%', f'%{ID}%', f'%{ID}%',
                                                                             f'%{ID}%',
                                                                             f'%{ID}%', f'%{ID}%'))
                    if new_ob.de_part and self.old_session_detail_ob.de_part:
                        new_depart = session_detail_query.SessionDetailMain.intergrated_data(new_ob.de_part)
                        old_depart = session_detail_query.SessionDetailMain.intergrated_data(
                            self.old_session_detail_ob.de_part)
                        depart = session_detail_query.SessionDetailMain.compute_delta(new_depart, old_depart)

                        self.old_session_detail_ob = new_ob
                        del new_ob
                        QUERY = depart.get("query")
                        if DELAY == 86400:
                            depart = SNAPSHOOT.get("body")
                        SNAPSHOOT = {"body": depart}
                        self.body = session_detail_body(depart)
                        if KILL:
                            self.edit = urwid.Edit("whether to kill{}(y/n):".format(PID), edit_pos=1)
                            self.edit.set_edit_text("")
                            self.body = urwid.LineBox(urwid.ListBox(
                                urwid.SimpleListWalker([urwid.Padding(self.edit, left=2, right=0, min_width=20)])))
                    else:
                        ERROR = True
                        PAGE_TABLE = LAST_PAGE
                        self.text = urwid.Text("session_detail page temporarily inaccessible：the pid has failed!")
                        self.cover = urwid.Filler(self.text, "middle")
            else:
                PAGE_TABLE = LAST_PAGE
        elif PAGE_TABLE == "sql_explain":
            PART_SIZE = 1
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
                explan = self.openGau.explain_query_sql(
                    db_config.session_plan_sql.get("explan").format(str(sign).replace('.', '')),
                    db_config.session_plan_sql.get("prepare").format(str(sign).replace('.', '')),
                    QUERY)
            except Exception as e:
                explan = []
            if advise or explan:
                if DELAY == 86400:
                    advise = SNAPSHOOT.get("heard")
                    explan = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": advise, "body": explan}
                self.body = sql_explain(advise, explan)
            else:
                PAGE_TABLE = "session_detail"
        elif PAGE_TABLE == "table":
            if isinstance(self.old_table_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("table page temporarily inaccessible：{}".format(self.old_table_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 6
                new_ob = table_query.TableMain(self.openGau, sqls=db_config.table_sql)
                new_depart = table_query.TableMain.intergrated_data(new_ob.de_part)
                old_depart = table_query.TableMain.intergrated_data(self.old_table_ob.de_part)
                self.old_table_ob = new_ob
                del new_ob
                depart = table_query.TableMain.compute_delta(new_depart, old_depart)
                if DELAY == 86400:
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"body": depart}
                if FUZZY_KEY:
                    depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
                if Rank and IDX:
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)

                if PID:
                    self.edit = urwid.Edit("please input relname(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=0)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = sort_querys_dict["table"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(11):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    data_list.append(urwid.Padding(urwid.Columns(
                        [urwid.Padding(button_list[33]),
                         urwid.Padding(urwid.Text([("")])),
                         urwid.Padding(urwid.Text([("")]))], 1),
                        width=100, align='center', min_width=5))

                    self.cover = urwid.ListBox(urwid.SimpleListWalker(
                        data_list
                    ))
                self.body = table_body(depart, PART)
        elif PAGE_TABLE == "table_index":
            if isinstance(table_detail_query.TableDetailMain(self.openGau, sqls=db_config.table_detail_sql,
                                                             params=ID).de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("table_index page temporarily inaccessible：{}".format(
                    table_detail_query.TableDetailMain(self.openGau, sqls=db_config.table_detail_sql,
                                                       params=ID).de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                new_ob = table_detail_query.TableDetailMain(self.openGau, sqls=db_config.table_detail_sql, params=ID)
                if new_ob.de_part.empty == False:
                    PART_SIZE = 2
                    new_depart = table_detail_query.TableDetailMain.intergrated_data(new_ob.de_part)
                    self.old_table_index_ob = new_ob
                    del new_ob
                    if DELAY == 86400:
                        new_depart = SNAPSHOOT.get("body")
                    SNAPSHOOT = {"body": new_depart}
                    self.body = table_index_body(new_depart, PART)
                else:
                    PAGE_TABLE = "table"
                    ID = ""
        elif PAGE_TABLE == 'lock':
            if isinstance(self.old_lock_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("lock page temporarily inaccessible：{}".format(self.old_lock_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 2
                new_ob = lock_query.LockMain(self.openGau, sqls=db_config.lock_sql)
                depart, heard_depart = lock_query.LockMain.intergrated_data(new_ob.de_part)
                self.old_lock_ob = new_ob
                del new_ob
                if DELAY == 86400:
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": heard_depart, "body": depart}
                if FUZZY_KEY:
                    depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
                if Rank and IDX:
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                self.body = lock_body(depart, heard_depart, PART)
                if PID:
                    self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = sort_querys_dict["lock"][1::]
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
            PART_SIZE = 1
            new_ob = lock_query.LockMain(self.openGau, sqls=db_config.lock_sql)
            self.old_lock_chain_ob = new_ob
            depart, heard_depart = lock_query.LockMain.intergrated_data(new_ob.de_part)
            df_depart = pd.DataFrame(depart, columns=['pid', 'locktype', 'database', 'relation', 'page', 'tuple',
                                                      'transactionid', 'virtualxid', 'objid', 'virtualtransaction',
                                                      'mode', 'granted', 'fastpath', 'locktag'])[
                ['pid', 'granted', 'locktag']].sort_values(by='locktag').reset_index()
            df_depart.pop('index')
            degranted_list = list(set(df_depart[df_depart['granted'] == False]['locktag'].tolist()))
            date_list = []
            for tags in degranted_list:
                blocker_holder = \
                    df_depart[(df_depart['locktag'] == tags) & (df_depart['granted'] == True)].reset_index()['pid'][
                        0]
                lock_blocked_agent_id = \
                    df_depart[(df_depart['locktag'] == tags) & (df_depart['granted'] == False)].reset_index()[
                        'pid'][0]
                if blocker_holder and lock_blocked_agent_id:
                    date_list.append(
                        {"blocker_holder": blocker_holder, "lock_blocked_agent_id": lock_blocked_agent_id})
            if DELAY == 86400:
                date_list = SNAPSHOOT.get("body")
            SNAPSHOOT = {"body": date_list}
            if Rank and IDX and date_list:
                date_list = sort_query(IDX, SORT, date_list, PAGE_TABLE)
            self.body = lock_detail_body(date_list)
            if PID:
                self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
            if DEL:
                self.edit = urwid.Edit("please enter pid to delete(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == 'wait_event':
            if isinstance(self.old_wait_event_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "wait_event page temporarily inaccessible：{}".format(self.old_wait_event_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 1
                new_ob = lock_eventwait_query.WaitEventMain(self.openGau, sqls=db_config.even_twait_sql)
                new_depart = lock_eventwait_query.WaitEventMain.intergrated_data(new_ob.de_part)
                old_depart = lock_eventwait_query.WaitEventMain.intergrated_data(self.old_wait_event_ob.de_part)
                depart, new_heard_depart = lock_eventwait_query.WaitEventMain.compute_delta(new_depart, old_depart)
                self.old_wait_event_ob = new_ob
                del new_ob
                if DELAY == 86400:
                    new_heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": new_heard_depart, "body": depart}
                if FUZZY_KEY:
                    depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
                if Rank and IDX:
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                self.body = wait_event_body(depart, new_heard_depart)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = sort_querys_dict["wait_event"][1::]
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
                        "please input wait_event or wait_status to sort in session page(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
        elif PAGE_TABLE == "memory":
            if isinstance(self.old_memory_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text("memory page temporarily inaccessible：{}".format(self.old_memory_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 1
                new_ob = memory_query.MemoryMain(self.openGau, sqls=db_config.memory_sql)
                new_depart = memory_query.MemoryMain.intergrated_data(new_ob.de_part)
                depart, heard_depart = memory_query.MemoryMain.compute_delta(new_depart)
                self.old_memory_ob = new_ob
                del new_ob
                if DELAY == 86400:
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": heard_depart, "body": depart}
                self.body = memory_body(depart, heard_depart)
        elif PAGE_TABLE == 'sharemem':
            PART_SIZE = 1
            new_ob = sharemem_query.SharememMain(self.openGau, sqls=db_config.sharemem_sql)
            self.old_sharemem_ob = new_ob
            depart, heard_depart = sharemem_query.SharememMain.intergrated_data(new_ob.de_part)
            if DELAY == 86400:
                heard_depart = SNAPSHOOT.get("heard")
                depart = SNAPSHOOT.get("body")
            SNAPSHOOT = {"heard": heard_depart, "body": depart}
            if FUZZY_KEY:
                depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
            if Rank and IDX:
                depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
            self.body = sharemem_body(depart, heard_depart)
            if FIND:
                self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
            if RankSign:
                field_list = sort_querys_dict["sharemem"][1::]
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
            PART_SIZE = 1
            new_ob = top_mem_used_session_query.TopMemUsedSessionMain(self.openGau,
                                                                      sqls=db_config.top_mem_used_session_sql)
            self.old_top_mem_used_session_ob = new_ob
            depart, heard_depart = top_mem_used_session_query.TopMemUsedSessionMain.intergrated_data(new_ob.de_part)
            if DELAY == 86400:
                heard_depart = SNAPSHOOT.get("heard")
                depart = SNAPSHOOT.get("body")
            SNAPSHOOT = {"heard": heard_depart, "body": depart}
            if FUZZY_KEY:
                depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
            if Rank and IDX:
                depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
            self.body = top_mem_used_session_body(depart, heard_depart)
            if PID:
                self.edit = urwid.Edit("please input pid(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
            elif FIND:
                self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                self.cover = urwid.Filler(self.edit, "middle")
            elif RankSign:
                field_list = sort_querys_dict["top_mem_used_session"][1::]
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
            if isinstance(self.old_dynamicsql_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "dynamicsql page temporarily inaccessible：{}".format(self.old_dynamicsql_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            elif self.openGau.all_query_sql(db_config.enable_resource_track)[0][0] == 'off':
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "dynamicsql page temporarily inaccessible：enable_resource_track is off")
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 6
                new_ob = dynamicsql_query.DynamicsqlMain(self.openGau, sqls=db_config.dynamicsql_sql)
                new_depart = dynamicsql_query.DynamicsqlMain.intergrated_data(new_ob.de_part)
                old_depart = dynamicsql_query.DynamicsqlMain.intergrated_data(self.old_dynamicsql_ob.de_part)
                depart, heard_depart = dynamicsql_query.DynamicsqlMain.compute_delta(new_depart, old_depart)
                self.old_dynamicsql_ob = new_ob
                del new_ob
                if DELAY == 86400:
                    heard_depart = SNAPSHOOT.get("heard")
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"heard": heard_depart, "body": depart}
                if Rank and IDX:
                    depart = sort_query(IDX, SORT, depart, PAGE_TABLE)
                if ID:
                    QUERY = get_query(depart, ID)
                    if QUERY:
                        SHOW = True
                        self.cover = urwid.Filler(urwid.Text(QUERY), "middle")
                if PID:
                    self.edit = urwid.Edit("please input unique_sql_id(end with enter):", edit_pos=0, edit_text="")
                    self.cover = urwid.Filler(self.edit, "middle")
                elif RankSign:
                    field_list = sort_querys_dict["dynamicsql"][1::]
                    button_list = self.make_field_button(field_list)
                    data_list = []
                    index = 0
                    for _ in range(13):
                        data_list.append(
                            urwid.Padding(urwid.Columns(
                                [urwid.Padding(button_list[index]),
                                 urwid.Padding(button_list[index + 1]),
                                 urwid.Padding(button_list[index + 2])], 1),
                                width=100, align='center', min_width=5),
                        )
                        index += 3
                    self.cover = urwid.ListBox(urwid.SimpleListWalker(data_list))
                self.body = dynamicsql_body(depart, heard_depart, PART)
        elif PAGE_TABLE == "replication":
            if isinstance(self.old_main_replication_ob.de_part, str) and isinstance(
                    self.old_prepare_replication_ob.de_part, str):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "replication page temporarily inaccessible：{}".format(self.old_main_replication_ob.de_part))
                self.cover = urwid.Filler(self.text, "middle")
            elif isinstance(self.old_main_replication_ob.de_part, list) and isinstance(
                    self.old_prepare_replication_ob.de_part, list):
                ERROR = True
                PAGE_TABLE = "home"
                self.body = home_body(self.db_host_time)
                self.text = urwid.Text(
                    "replication page temporarily inaccessible：The database replication is abnormal or is deployed on a single server。")
                self.cover = urwid.Filler(self.text, "middle")
            else:
                PART_SIZE = 2
                if self.view_role[0][0] == "Primary":
                    new_ob = replication_query.ReplicationMain(self.openGau, sqls=db_config.main_replication_sql)
                    new_depart = replication_query.ReplicationMain.intergrated_data(new_ob.de_part)
                    old_depart = replication_query.ReplicationMain.intergrated_data(
                        self.old_main_replication_ob.de_part)
                else:
                    new_ob = replication_query.ReplicationMain(self.openGau, sqls=db_config.prepare_replication_sql)
                    new_depart = replication_query.ReplicationMain.intergrated_data(new_ob.de_part)
                    old_depart = replication_query.ReplicationMain.intergrated_data(
                        self.old_prepare_replication_ob.de_part)
                depart = replication_query.ReplicationMain.compute_delta(new_depart, old_depart)
                self.old_replication_ob = new_ob
                del new_ob
                if DELAY == 86400:
                    depart = SNAPSHOOT.get("body")
                SNAPSHOOT = {"body": depart}
                if FUZZY_KEY:
                    depart = fuzzy_match(FUZZY_KEY, depart, sort_querys_dict[PAGE_TABLE])
                self.body = replication_body(depart, PART)
                if FIND:
                    self.edit = urwid.Edit("please enter what to find(end with enter):", edit_pos=1)
                    self.cover = urwid.Filler(self.edit, "middle")
        self.header = urwid.Columns([
            urwid.Padding(urwid.Text(
                [('time', u"Local Time: "), get_localtime() + "\n", ('time', u"Query interval: "), str(DELAY) + "s"]),
                left=2, right=0, min_width=20),
            urwid.Padding(urwid.Text(
                [('title', u"{}\n".format(PAGE_TABLE)), "PID:" + ID if ID and PAGE_TABLE == "session_detail" else ""]),
                left=2, right=5, min_width=20),
            urwid.Padding(urwid.Text(
                [('database', u"Port: "), "{}\n".format(database_conf.get("port")), ('database', 'dbname: '),
                 (database_conf.get("database"))]), left=2, right=0, min_width=20)], 20)
        self.footer = urwid.Columns([
            urwid.Padding(
                urwid.Text([('ctrl', u"{}".format("Quit: q" if PAGE_TABLE != "snapshoot" else "Save：F9")), "      ",
                            ('ctrl', u"{}".format("Help: h" if PAGE_TABLE != "snapshoot" else "Discard：F10"))]),
                left=2, right=0, min_width=20),
            urwid.Padding(urwid.Text([('host', u"Hostname: "), get_host()]), left=2, right=0, min_width=20),
            urwid.Padding(urwid.Text([('version', u"ogtop: "), "v1.0"]), left=2, right=0, min_width=20),
            urwid.Padding(urwid.Text(
                [('version', u"page: "), "{}/{}".format(PART, PART_SIZE + 1 if PART_SIZE == 0 else PART_SIZE)]), left=2,
                right=0, min_width=20)], 20)
        REFRESH = False
        self.frame = urwid.Frame(urwid.AttrWrap(self.body, 'body'), header=self.header, footer=self.footer)
        if FINISH or PID or FIND or JUMP_FIND or RankSign or DEL or SHOW or ERROR:  # 覆盖
            self.frame = urwid.Overlay(urwid.LineBox(self.cover), self.frame, 'center', ("relative", 60), "middle",
                                       ("relative", 60), 20, 9)

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

        button = field.get_label()
        BUTTON_KEY = button
        openGau = openGauss_option(conn_config=database_conf)
        if button == "force_all":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_all")
            BUTTON_KEY = ""
            sess_main()
            raise urwid.ExitMainLoop()

        elif button == "force_by_dbname":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_by_dbname", datname=database_conf.get("database"))
            BUTTON_KEY = ""
            sess_main()
            raise urwid.ExitMainLoop()

        elif button == "force_idle":
            FINISH = False
            DELAY = 2
            openGau.finish_sql("force_idle")
            BUTTON_KEY = ""
            sess_main()
            raise urwid.ExitMainLoop()

        elif button == "force_by_pid":
            FINISH = False
            PID = True
            DELAY = 86400
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

            sess_main()
            raise urwid.ExitMainLoop()

    def snapshot_body(self, snapshot_data, last_page):
        """
        processing snapshot data
        :param snapshot_data:data recorded by a snapshot
        :param last_page:snapshot page
        :return: snapshot data
        """
        field_list = sort_querys_dict[last_page]

        heard = snapshot_data.get("heard")
        body = snapshot_data.get("body")
        count = len(heard)
        text_data = ""
        if count > 0:
            head_field = field_list[0:count] if count > 1 else [field_list[0]]
            for i in range(len(head_field)):
                text_data += head_field[i]
                text_data += ":"
                text_data += str(heard[head_field[i]])
                text_data += "\n"
            body_field = field_list[count::]
            text_data += "\n"
            if isinstance(body, dict):
                for i in range(len(body_field) - 1):
                    text_data += body_field[i]
                    text_data += ":"
                    text_data += str(body[body_field[i]])
                    text_data += "\n"
            else:
                for body_data in body:
                    num = 0
                    for _ in range(len(body_field) - 1):
                        text_data += body_field[num]
                        text_data += ":"
                        text_data += str(body_data[body_field[num]])
                        text_data += "\n"
                        num += 1
                    text_data += "\n"
        else:
            body_field = field_list
            if isinstance(body, dict):
                for i in range(len(body_field) - 1):
                    text_data += body_field[i]
                    text_data += ":"
                    text_data += str(body[body_field[i]])
                    text_data += "\n"
            else:
                for body_data in body:
                    num = 0
                    for _ in range(len(body_field) - 1):
                        text_data += body_field[num]
                        text_data += ":"
                        text_data += str(body_data[body_field[num]])
                        text_data += "\n"
                        num += 1
                    text_data += "\n"

        return text_data


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
        global PART
        global QUERY
        global DEL
        global SHOW
        global PART_SIZE
        global ERROR
        global REFRESH
        global JUMP_FIND
        global LAST_PAGE
        global SNAPSHOOT_USE_PAGE
        global ERROR
        LAST_PAGE = PAGE_TABLE
        if key in ["p", "d", "t", "s", "U", "W", "m", "D", "r"]:
            PID = False
            FIND = False
            FUZZY_KEY = ""
            Rank = False
            RankSign = False
            SORT = False
            KILL = False
            IDX = ""
            HELP = False
            FINISH = False
            BUTTON_KEY = ""
            ID = ""
            QUERY = ""
            DEL = False
            PART_SIZE = 0
            IDX = ""
            PART = 1
            DEL = False
            SHOW = False
            ERROR = False
            frame.old_session_detail_ob = ""
            frame.old_table_index_ob = ""
            if DELAY == 86400:
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
        if key == 'q':
            raise urwid.ExitMainLoop()
        elif key == 'h':
            if HELP:
                PART = 1
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                PART = 1
                HELP = True
                DELAY = 86400
                frame.refresh()
        elif key == ' ':
            # 空格键实时刷新
            if REFRESH is False:
                REFRESH = True
                frame.refresh()
        elif key == "F":
            G = False
            if DELAY == 2:
                DELAY = 86400
                sloop.set_alarm_at(DELAY, refresh)
            else:
                DELAY = 2
                sloop.set_alarm_at(DELAY, refresh)
        elif key == "p":
            if PAGE_TABLE != "home" or PAGE_TABLE != "snapshoot":
                SNAPSHOOT_USE_PAGE = PAGE_TABLE
                PAGE_TABLE = "snapshoot"
                DELAY = 86400
                frame.refresh()
        elif key == "f9" and PAGE_TABLE == "snapshoot":
            data = frame.get_text()
            with open(sys.path[0] + "/" + str(time.time()) + ".txt", 'w')as f:
                f.write(data)
            PAGE_TABLE = SNAPSHOOT_USE_PAGE
            DELAY = 2
            sloop.set_alarm_in(DELAY, refresh)
        elif key == "f10" and PAGE_TABLE == "snapshoot":
            PAGE_TABLE = SNAPSHOOT_USE_PAGE
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
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "t":
            PAGE_TABLE = "table"
            Rank = True
            SORT = True
            IDX = "totalsize"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'U':
            PAGE_TABLE = "lock"
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
            PART = 1
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
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == "r":
            PAGE_TABLE = "replication"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'S' and PAGE_TABLE == "memory":
            PAGE_TABLE = "sharemem"
            Rank = True
            SORT = True
            IDX = "usedsize/totalsize"
            if HELP:
                HELP = False
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            else:
                frame.refresh()
        elif key == 'R' and PAGE_TABLE == "memory":
            PAGE_TABLE = "top_mem_used_session"
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
            IDX = ""
            DELAY = 86400
            JUMP_FIND = True
            frame.refresh()
        elif key == 'a' and PAGE_TABLE in ["session", "table", "lock", "lock_chain", "top_mem_used_session",
                                           "dynamicsql"]:
            FIND = False
            JUMP_FIND = False
            RankSign = False
            DELAY = 86400
            PID = True
            frame.refresh()
        elif key == "/" and PAGE_TABLE in ["session", "table", "lock", "wait_event", "sharemem",
                                           "top_mem_used_session", "replication"]:
            PID = False
            ID = ""
            JUMP_FIND = False
            RankSign = False
            FIND = True
            DELAY = 86400
            frame.refresh()
        elif key == "z" and PAGE_TABLE in ["session", "table", "lock", "wait_event", "sharemem",
                                           "top_mem_used_session", "dynamicsql"]:
            PID = False
            ID = ""
            FIND = False
            JUMP_FIND = False
            DELAY = 86400
            RankSign = True
            frame.refresh()
        elif key == "e" and PAGE_TABLE in ["session_detail", "dynamicsql"]:
            if PAGE_TABLE == "dynamicsql" and SHOW:
                ID = ""
                PID = False
                SHOW = False
                PAGE_TABLE = "sql_explain"
                if DELAY != 2:
                    DELAY = 2
                    sloop.set_alarm_in(DELAY, refresh)
                else:
                    frame.refresh()
            elif PAGE_TABLE == "session_detail":
                PAGE_TABLE = "sql_explain"
                if DELAY != 2:
                    DELAY = 2
                    sloop.set_alarm_in(DELAY, refresh)
                else:
                    frame.refresh()
        elif key == "f" and PAGE_TABLE in ["session", "session_detail", "lock", "lock_chain",
                                           "top_mem_used_session"] and HELP == False:
            if PAGE_TABLE == "session_detail":
                DELAY = 86400
                KILL = True
                frame.refresh()
            elif PAGE_TABLE == "session":
                DELAY = 86400
                FINISH = True
                frame.refresh()
            else:
                DELAY = 86400
                DEL = True
                frame.refresh()
        elif key == "b" and PAGE_TABLE == "sql_explain":
            DELAY = 2
            PART = 1
            PAGE_TABLE = "session_detail"
            sloop.set_alarm_in(DELAY, refresh)
        elif key == "enter":
            if PID and PAGE_TABLE in ["session", "table", "lock", "lock_chain", "top_mem_used_session", "dynamicsql"]:
                if BUTTON_KEY == "force_by_pid":
                    PID = False
                    DELAY = 2
                    ID = frame.get_text()
                    openGau = openGauss_option(conn_config=database_conf)
                    openGau.finish_sql("force_by_pid", pid=ID)
                    BUTTON_KEY = ""
                    sloop.set_alarm_in(DELAY, refresh)
                elif PID and PAGE_TABLE in ["table"]:
                    PID = False
                    DELAY = 2
                    PART = 1
                    ID = frame.get_text()
                    PAGE_TABLE = "table_index"
                    sloop.set_alarm_in(DELAY, refresh)
                elif PID and PAGE_TABLE in ["dynamicsql"]:
                    PID = False
                    DELAY = 2
                    ID = frame.get_text()
                    sloop.set_alarm_in(DELAY, refresh)
                else:
                    PID = False
                    DELAY = 2
                    PART = 1
                    ID = frame.get_text()
                    PAGE_TABLE = "session_detail"
                    sloop.set_alarm_in(DELAY, refresh)
            elif DEL and PAGE_TABLE in ["lock", "lock_chain", "top_mem_used_session"]:
                DEL = False
                DELAY = 2
                ID = frame.get_text()
                openGau = openGauss_option(conn_config=database_conf)
                sql = "select pg_terminate_backend (%s);"
                openGau.params_query_sql(sql, (ID,))
                sloop.set_alarm_in(DELAY, refresh)
            elif FIND and PAGE_TABLE in ["session", "table", "lock", "wait_event", "sharemem",
                                         "top_mem_used_session", "replication"]:
                FIND = False
                FUZZY_KEY = frame.get_text()
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            elif JUMP_FIND and PAGE_TABLE in ["wait_event"]:
                PAGE_TABLE = "session"
                Rank = True
                SORT = False
                IDX = "current_state"
                JUMP_FIND = False
                FUZZY_KEY = frame.get_text()
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            elif PAGE_TABLE in ["session_detail"] and KILL:
                if frame.get_text() == 'y':
                    openGau = openGauss_option(conn_config=database_conf)
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
            if DELAY != 2:
                DELAY = 2
                sloop.set_alarm_in(DELAY, refresh)
            if PAGE_TABLE == "dynamicsql":
                ID = ""
        elif key == "left" and PAGE_TABLE != "snapshoot":
            if PAGE_TABLE != "home":
                if PART > 1 and RankSign + FIND + PID == False:
                    PART -= 1
                frame.refresh()
        elif key == "right" and PAGE_TABLE != "snapshoot" and RankSign + FIND + PID == False:
            if PAGE_TABLE != "home":
                if PART < PART_SIZE:
                    PART += 1
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
    op = openGauss_option(conn_config=database_conf)
    try:
        if op.conn:
            pass
    except Exception as e:
        return

    urwid.web_display.set_preferences("GStop")
    if urwid.web_display.handle_short_request():
        return
    sess_main()


if __name__ == '__main__':
    """program main entry"""
    import getpass

    Usage = """
To use ogtop\n    Login without password\n        To run ogtop without database password,you need to run ogtop on the database local server and make sure that your system user is omm.\n        Usage:\n            ogtop [port] [database]\n        General options:\n            port -- target database port\n            database -- target database\n    Login with password\n        Usage:\n            ogtop [ip] [port] [database] [user]\n        General options:\n            ip -- ip address of target database server\n            port -- target database port\n            database -- target database\n            user -- login user
    """
    if getpass.getuser() == "omm":
        if len(sys.argv) == 3:
            database_conf["port"] = sys.argv[1]
            database_conf["database"] = sys.argv[2]
            database_conf["ip"] = socket.gethostbyname(socket.gethostname())
            database_conf["user"] = getpass.getuser()
            try:
                sutep()
            except KeyboardInterrupt as e:
                print("\nthank you for using")
        elif len(sys.argv) == 5:
            database_conf["ip"] = sys.argv[1]
            database_conf["port"] = sys.argv[2]
            database_conf["database"] = sys.argv[3]
            database_conf["user"] = sys.argv[4]
            try:
                password = input("please enter the database user password：")
                database_conf["password"] = password
                sutep()
            except KeyboardInterrupt as e:
                print("\nthank you for using")
        else:
            print(Usage)
    else:
        if len(sys.argv) == 5:
            database_conf["ip"] = sys.argv[1]
            database_conf["port"] = sys.argv[2]
            database_conf["database"] = sys.argv[3]
            database_conf["user"] = sys.argv[4]
            try:
                password = input("please enter the database user password：")
                database_conf["password"] = password
                sutep()
            except KeyboardInterrupt as e:
                print("\nthank you for using")
        else:
            print(Usage)
