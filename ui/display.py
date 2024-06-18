import json
import datetime
import time
import urwid
import urwid.raw_display
import urwid.web_display
from urwid import Padding, Text, Divider
from db.db_util import progress_bar, check_alarm_data
from common import shortcut_key_menu
import os
from common.shortcut_key_menu import display_field, background_data_field
import pandas as pd

display_database = display_field["database"]
background_database = background_data_field["database"]
display_session = display_field["session"]
background_session = background_data_field["session"]
display_session_detail = display_field["session_detail"]
background_session_detail = background_data_field["session_detail"]
display_session_plan = display_field["sql_explain"]
display_asp = display_field["asp"]
background_asp = background_data_field["asp"]
display_table = display_field["table"]
background_table = background_data_field["table"]
display_table_detail = display_field["table_detail"]
background_table_detail = background_data_field["table_detail"]
display_table_performance = display_field["table_performance"]
background_table_performance = background_data_field["table_performance"]
display_table_index = display_field["table_index"]
background_table_index = background_data_field["table_index"]
display_lock = display_field["lock"]
background_lock = background_data_field["lock"]
display_lock_chain = display_field['lock_chain']
background_lock_chain = background_data_field['lock_chain']
display_wait_event = display_field["wait_event"]
background_wait_event = background_data_field["wait_event"]
display_memory = display_field["memory"]
background_memory = background_data_field["memory"]
display_sharemem = display_field["sharemem"]
background_sharemem = background_data_field["sharemem"]
display_top_mem_used_session = display_field["top_mem_used_session"]
background_top_mem_used_session = background_data_field["top_mem_used_session"]
display_dynamicsql = display_field["dynamicsql"]
background_dynamicsql = background_data_field["dynamicsql"]
display_replication = display_field["replication"]
background_replication = background_data_field["replication"]
display_index_recommendation = display_field["index_recommendation"]
display_database_list = display_field["database_list"]
background_database_list = background_data_field["database_list"]
display_sql_patch = display_field["sql_patch"]
background_sql_patch = background_data_field["sql_patch"]
display_slow_sql = display_field["slow_sql"]
background_slow_sql = background_data_field["slow_sql"]
display_redo = display_field["redo"]
background_redo = background_data_field["redo"]
display_redo_time_count = display_field["redo_time_count"]
background_redo_time_count = background_data_field["redo_time_count"]
display_xlog_redo_statics = display_field["xlog_redo_statics"]
background_xlog_redo_statics = background_data_field["xlog_redo_statics"]
display_wdr = display_field["wdr"]
background_wdr = background_data_field["wdr"]
display_snap_summary_more_statement = display_field["snap_summary_more_statement"]
background_snap_summary_more_statement = background_data_field["snap_summary_more_statement"]
display_burr_analysis = display_field["burr_analysis"]
background_burr_analysis = background_data_field["burr_analysis"]
display_snap_summary_statement = display_field["snap_summary_statement"]
background_snap_summary_statement = background_data_field["snap_summary_statement"]


def check_data_is_highlighted(data, dis_norm, back_norm, page, display_segment):
    if display_segment == "complete_section":
        if dis_norm == "active session/session" and page == "database":
            display = [('field', u"                            {}: ".format(dis_norm)),
                       progress_bar(data)] if check_alarm_data(
                float(data), page, back_norm) == "safety" else [
                ('field', u"                            {}: ".format(dis_norm)),
                ('warning', u"{}".format(progress_bar(data)))]
        elif dis_norm == "dynamic_used_memory/max_dynamic_memory":
            display = [('field', u"            {}: ".format(dis_norm)),
                       progress_bar(data)] if check_alarm_data(
                float(data), page, back_norm) == "safety" else [
                ('field', u"            {}: ".format(dis_norm)), ('warning', u"{}".format(progress_bar(data)))]
        elif dis_norm == "busy_time/all_time":
            display = [('field', u"                                {}: ".format(dis_norm)),
                       progress_bar(data)] if check_alarm_data(
                float(data), page, back_norm) == "safety" else [
                ('field', u"                                {}: ".format(dis_norm)),
                ('warning', u"{}".format(progress_bar(data)))]
        elif dis_norm == "DATA_IO_TIME/DB_TIME":
            display = [('field', u"                              {}: ".format(dis_norm)),
                       progress_bar(data)] if check_alarm_data(
                float(data), page, back_norm) == "safety" else [
                ('field', u"                              {}: ".format(dis_norm)),
                ('warning', u"{}".format(progress_bar(data)))]
        elif dis_norm == "active session/session" and page == "session":
            display = progress_bar(data) if check_alarm_data(float(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(progress_bar(data)))
    else:
        if dis_norm == "wal_status":
            display = data if check_alarm_data(data, page, back_norm) else (('warning', u"{}".format(data)), 0)
        elif dis_norm == "active session/session" and page == "session":
            display = progress_bar(data) if check_alarm_data(float(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(progress_bar(data)))
        elif dis_norm == "n_rollback" or dis_norm == "n_sql":
            display = data[1] if check_alarm_data(float(data[0]), page, "n_rollback/n_sql") == "safety" else (
                'warning', u"{}".format(data[1]))
        elif dis_norm == "n_shared_blocks_read" or dis_norm == "n_shared_blocks_read" or dis_norm == "n_blocks_read_time":
            display = data[1] if check_alarm_data(float(data[0]), page,
                                                  "n_shared_blocks_read/(n_shared_blocks_read+n_blocks_read_time)") == "safety" else (
                'warning', u"{}".format(data[1]))
        elif dis_norm == "n_dead_tup" or dis_norm == "n_live_tup":
            display = data[1] if check_alarm_data(float(data[0]), page,
                                                  "n_dead_tup/(n_live_tup+n_dead_tup)") == "safety" else (
                'warning', u"{}".format(data[1]))
        elif dis_norm == "block_sessionid":
            display = str(data) if check_alarm_data(str(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(str(data)))
        elif page == "memory":
            display = progress_bar(data * 100) if check_alarm_data(float(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(progress_bar(data * 100)))
        elif dis_norm == "last_autovacuum":
            display = str(data) if check_alarm_data(str(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(str(data)))
        elif dis_norm == "peer_state":
            display = data if check_alarm_data(data, page, back_norm) == "safety" else (
                'warning', u"{}".format(data))
        elif dis_norm == "receive_gap" or dis_norm == "replay_gap" or dis_norm == "diff_lsn":
            display = str(data) if check_alarm_data(float(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(str(data)))
        else:
            display = str(data) if check_alarm_data(float(data), page, back_norm) == "safety" else (
                'warning', u"{}".format(str(data)))
    return display


def home_body(db_host_time):
    """
    project home page
    :param db_host_time: server time currently in use
    :return: return the page generated through urwid
    """
    if isinstance(db_host_time[0], str):
        diff_time = 5
    else:
        data = pd.DataFrame(db_host_time[0], columns=["datetime"])
        data[['datetime']] = data[['datetime']].astype('str')
        data_list = data[["datetime"]].to_json(orient='records')
        data_list = json.loads(data_list)
        diff_time = time.mktime(datetime.datetime.now().timetuple()) - time.mktime(
            datetime.datetime.strptime(data_list[0].get("datetime").split('.', 1)[0], "%Y-%m-%d %H:%M:%S").timetuple())
    blank = urwid.Divider()
    text_list = []
    for k, v in shortcut_key_menu.menu.items():
        text_list.append(urwid.Padding(urwid.Text([('field', "{}: ".format(k)), v])))
    listbox_body = [
        Padding(
            urwid.Text([('field',
                         "   ___   ____ _              \n  / _ \ / ___| |_ ___  _ __  \n | | | | |  _| __/ _ \| '_ \ \n | |_| | |_| | || (_) | |_) |\n  \___/ \____|\__\___/| .__/ \n                      |_|    ")]),
            width=50,
            align='center', min_width=2),
        blank,
    ]
    num = 0
    for _ in range(int(len(text_list) / 2)):
        listbox_body.append(
            urwid.Padding(urwid.Columns([text_list[num], text_list[num + 1], ], 1), width=50, align='center',
                          min_width=20))
        num += 2
    if len(text_list) % 2 == 1:
        listbox_body.append(
            urwid.Padding(urwid.Columns([text_list[int(-1)], ], 1), width=50, align='center',
                          min_width=20), )
    if diff_time > 5:
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(
            urwid.Padding(urwid.Columns([urwid.Padding(
                urwid.Text([('field',
                             "Warning:the time of ogtop local server is different from the time select from target database！！")]))],
                1),
                width=55, align='center', min_width=20), )
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def help_body(page):
    """
    project help page
    :param page: project page name
    :return:return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"pages")]))), ], 0), width=50,
                align='center',
                min_width=2), blank, blank, blank]
    data_list = []
    for k, v in shortcut_key_menu.menu.items():
        data_list.append(urwid.Padding(urwid.Text([('field', "{}: ".format(k)), v])))
    num = 0
    for _ in range(int(len(data_list) / 2)):
        listbox_body.append(
            urwid.Padding(urwid.Columns([data_list[num], data_list[num + 1], ], 1), width=100, align='center',
                          min_width=20))
        num += 2
    if len(data_list) % 2 == 1:
        listbox_body.append(
            urwid.Padding(urwid.Columns([data_list[int(-1)], ], 1), width=100, align='center',
                          min_width=20), )
    shortcut_key = shortcut_key_menu.function_menu.get(page)
    if shortcut_key:
        listbox_body.append(Divider(u'_'))
        listbox_body.append(
            Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"function")]))), ], 0), width=50,
                    align='center',
                    min_width=2))
        listbox_body.append(blank)
        listbox_body.append(blank)
        listbox_body.append(blank)
        text_list = []
        for k, v in shortcut_key.items():
            text_list.append(urwid.Padding(urwid.Text([('field', "{}: ".format(k)), v])))
        line_num = len(text_list) / 2
        init_num = 0
        for _ in range(int(line_num)):
            listbox_body.append(
                urwid.Padding(urwid.Columns([text_list[init_num], text_list[init_num + 1], ], 1), width=100,
                              align='center', min_width=20))
            init_num += 2
        if len(text_list) % 2 == 1:
            listbox_body.append(
                urwid.Padding(urwid.Columns([text_list[-1]], 1), width=100, align='center',
                              min_width=20), )
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"page map")]))), ], 0), width=50,
                align='center',
                min_width=2))
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([('field', u"session"), "-->session_detail-->sql_explain"]))), ], 0),
            width=100, align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([('field', u"session"), "-->index_recommendation"]))), ], 0),
            width=100, align='center',
            min_width=2))
    listbox_body.append(
        Padding(
            urwid.Columns([(urwid.Padding(urwid.Text([('field', u"table"), "-->table_detail-->table_index"]))), ], 0),
            width=100,
            align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([('field', u"table_performance"), "-->table_detail-->table_index"]))), ], 0),
            width=100,
            align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"lock"), "-->session_detail"]))), ], 0), width=100,
                align='center',
                min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([len("lock") * " " + "-->lock_chain-->session_detail"]))), ], 0), width=100,
            align='center', min_width=2))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(
            urwid.Text([('field', u"memory"), "-->sharemem&top_mem_used_session"]))), ], 0), width=100,
            align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([len("memory") * " " + "-->top_mem_used_session-->session_detail"]))), ], 0),
            width=100, align='center', min_width=2))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(
            urwid.Text([('field', u"dynamicsql"), "-->index_recommendation"]))), ], 0), width=100,
            align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(
            urwid.Text([('field', u"wdr"),
                        "-->snap_summary_more_statement-->snap_summary_statement&burr_analysis&rigorous_analysis"]))), ],
            0), width=100,
            align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([len("wdr") * " " + "-->burr_analysis-->snap_summary_statement&rigorous_analysis"]))), ], 0),
            width=100, align='center', min_width=2))
    listbox_body.append(
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([len("wdr") * " " + "-->rigorous_analysis-->snap_summary_statement"]))), ], 0),
            width=100, align='center', min_width=2))
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def database_body(uppart, depart):
    """
    project database page
    :param uppart:the header display the data
    :param depart:the data that the page needs to display
    :return:return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            check_data_is_highlighted(uppart.get(background_database[0], 0), display_database[0],
                                      background_database[0],
                                      "database", "complete_section")))), ], 0), width=140, align='center',
            min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            [('field', u"                          {}: ".format(display_database[1])),
             progress_bar(uppart.get(background_database[1], 0))]))), ], 0), width=140, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            [('field', u"                            {}: ".format(display_database[2])),
             progress_bar(uppart.get(background_database[2], 0))]))), ], 0), width=140, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            check_data_is_highlighted(uppart.get(background_database[3], 0), display_database[3],
                                      background_database[3],
                                      "database", "complete_section")))), ], 0), width=140, align='center',
            min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            check_data_is_highlighted(uppart.get(background_database[4], 0), display_database[4],
                                      background_database[4],
                                      "database", "complete_section")))), ], 0), width=140, align='center',
            min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text(
            check_data_is_highlighted(uppart.get(background_database[5], 0), display_database[5],
                                      background_database[5],
                                      "database", "complete_section")))), ], 0), width=140, align='center',
            min_width=2),
        Divider(u'_'),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(display_database[6]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[7]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[8]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[9]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[10]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[11]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(display_database[12]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[6], " "))),
                                     urwid.Padding(Text(depart.get(background_database[7], " "))),
                                     urwid.Padding(Text(
                                         check_data_is_highlighted(depart.get(background_database[8], 0),
                                                                   display_database[8],
                                                                   background_database[8], "database",
                                                                   "half_section"))),
                                     urwid.Padding(Text(depart.get(background_database[9], " "))),
                                     urwid.Padding(Text(depart.get(background_database[10], 0))),
                                     urwid.Padding(Text(depart.get(background_database[11], 0))),
                                     urwid.Padding(Text(depart.get(background_database[12], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[13]))]))),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[14]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[15]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[16]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[17]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[18]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[19]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[13], 0))),
                                     urwid.Padding(Text(depart.get(background_database[14], 0))),
                                     urwid.Padding(Text(depart.get(background_database[15], 0))),
                                     urwid.Padding(Text(
                                         check_data_is_highlighted(depart.get(background_database[16], 0),
                                                                   display_database[16],
                                                                   background_database[16], "database",
                                                                   "half_section"))),
                                     urwid.Padding(Text(depart.get(background_database[17], 0))),
                                     urwid.Padding(Text(depart.get(background_database[18], 0))),
                                     urwid.Padding(Text(depart.get(background_database[19], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[20]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[21]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[22]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[23]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[24]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[25]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[26]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[20], 0))),
                                     urwid.Padding(Text(depart.get(background_database[21], 0))),
                                     urwid.Padding(Text(depart.get(background_database[22], 0))),
                                     urwid.Padding(Text(depart.get(background_database[23], 0))),
                                     urwid.Padding(Text(depart.get(background_database[24], 0))),
                                     urwid.Padding(Text(depart.get(background_database[25], 0))),
                                     urwid.Padding(Text(depart.get(background_database[26], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[27]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[28]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[29]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[30]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[31]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[32]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[33]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[27], 0))),
                                     urwid.Padding(Text(depart.get(background_database[28], 0))),
                                     urwid.Padding(Text(depart.get(background_database[29], 0))),
                                     urwid.Padding(Text(depart.get(background_database[30], 0))),
                                     urwid.Padding(Text(depart.get(background_database[31], 0))),
                                     urwid.Padding(Text(
                                         check_data_is_highlighted(depart.get(background_database[32], 0),
                                                                   display_database[32],
                                                                   background_database[32], "database",
                                                                   "half_section"))),
                                     urwid.Padding(Text(depart.get(background_database[33], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[34]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[35]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[36]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[37]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[38]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[39]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[40]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[34], 0))),
                                     urwid.Padding(Text(depart.get(background_database[35], 0))),
                                     urwid.Padding(Text(depart.get(background_database[36], 0))),
                                     urwid.Padding(Text(depart.get(background_database[37], 0))),
                                     urwid.Padding(Text(depart.get(background_database[38], 0))),
                                     urwid.Padding(Text(depart.get(background_database[39], 0))),
                                     urwid.Padding(Text(depart.get(background_database[40], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[41]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[42]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[43]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[44]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[45]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[46]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[47]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[41], 0))),
                                     urwid.Padding(Text(depart.get(background_database[42], 0))),
                                     urwid.Padding(Text(depart.get(background_database[43], 0))),
                                     urwid.Padding(Text(depart.get(background_database[44], 0))),
                                     urwid.Padding(Text(depart.get(background_database[45], 0))),
                                     urwid.Padding(Text(depart.get(background_database[46], 0))),
                                     urwid.Padding(Text(
                                         check_data_is_highlighted(depart.get(background_database[47], 0),
                                                                   display_database[47],
                                                                   background_database[47], "database",
                                                                   "half_section"))), ], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[48]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[49]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[50]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[51]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[52]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[53]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[54]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[48], 0))),
                                     urwid.Padding(Text(depart.get(background_database[49], 0))),
                                     urwid.Padding(Text(depart.get(background_database[50], 0))),
                                     urwid.Padding(Text(depart.get(background_database[51], 0))),
                                     urwid.Padding(Text(depart.get(background_database[52], 0))),
                                     urwid.Padding(Text(depart.get(background_database[53], 0))),
                                     urwid.Padding(Text(depart.get(background_database[54], 0)))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[55]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[56]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[57]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(display_database[58]))])),
                                     urwid.Padding(urwid.Text([('field', u' ')])),
                                     urwid.Padding(urwid.Text([('field', u' ')])),
                                     urwid.Padding(urwid.Text([('field', u' ')]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart.get(background_database[55], 0))),
                                     urwid.Padding(Text(depart.get(background_database[56], 0))),
                                     urwid.Padding(Text(depart.get(background_database[57], 0))),
                                     urwid.Padding(Text(depart.get(background_database[58], 0))),
                                     urwid.Padding(Text("")),
                                     urwid.Padding(Text("")),
                                     urwid.Padding(Text(""))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center', min_width=8),
        blank,
        blank,
        blank]
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def session_body(heard_depart, depart, part_min, part_max):
    """
    project session page
    :param heard_depart:the header display the data
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [blank,
                    Padding(urwid.Text([((u"—" * 125)), ]), width=150, align='center', min_width=2),
                    Padding(urwid.Text(
                        [("|"), ('field', u"{}: ".format(display_session[0])),
                         check_data_is_highlighted(heard_depart.get(background_session[0]), display_session[0],
                                                   background_session[0], "session", "half_section"),
                         " " * (125 - len(
                             "|{}: ".format(display_session[0]) + str(check_data_is_highlighted(
                                 heard_depart.get(background_session[0]), display_session[0],
                                 background_session[0], "session", "half_section")))), ("|")]),
                        width=150, align='center', min_width=2),
                    Padding(urwid.Text([((u"—" * 125)), ]), width=150, align='center', min_width=2),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text([('field', u"{}............: ".format(display_session[1])),
                                                  heard_depart.get(background_session[1])])),
                        urwid.Padding(urwid.Text([('field', u"   "), ])),
                        urwid.Padding(urwid.Text([('field', u"{}.........: ".format(display_session[2])),
                                                  heard_depart.get(background_session[2])]), align='right')], 1),
                        width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20, ),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text([('field', u"{}..........: ".format(display_session[3])),
                                                  heard_depart.get(background_session[3]) if int(
                                                      heard_depart.get(background_session[6])) == 0
                                                  else
                                                  check_data_is_highlighted(
                                                      [int(heard_depart.get(background_session[3])) / int(
                                                          heard_depart.get(background_session[6])),
                                                       (heard_depart.get(background_session[3]))],
                                                      display_session[3],
                                                      background_session[3], "session",
                                                      "half_section"), ])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}: ".format(display_session[4])), heard_depart.get(background_session[4])])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}: ".format(display_session[5])), heard_depart.get(background_session[5])]))],
                        1),
                        width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text([('field', u"{}...............: ".format(display_session[6])),
                                                  heard_depart.get(background_session[6]) if int(
                                                      heard_depart.get(background_session[6])) == 0
                                                  else
                                                  check_data_is_highlighted(
                                                      [int(heard_depart.get(background_session[3])) / int(
                                                          heard_depart.get(background_session[6])),
                                                       (heard_depart.get(background_session[6]))],
                                                      display_session[3],
                                                      background_session[3], "session",
                                                      "half_section")])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}.....: ".format(display_session[7])),
                             heard_depart.get(background_session[7])])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}..: ".format(display_session[8])),
                             heard_depart.get(background_session[8])]))],
                        1), width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text([('field', u"{}........: ".format(display_session[9])),
                                                  heard_depart.get(background_session[9])])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}.: ".format(display_session[10])),
                             heard_depart.get(background_session[10])])),
                        urwid.Padding(urwid.Text([('field', u"{}...: ".format(display_session[11])),
                                                  heard_depart.get(background_session[11])]))], 1),
                        width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text(
                            [('field', u"{}: ".format(display_session[12])),
                             heard_depart.get(background_session[12])
                             if (int(
                                 heard_depart.get(background_session[12])) + int(
                                 heard_depart.get(background_session[13]))) == 0
                             else
                             check_data_is_highlighted(
                                 [int(heard_depart.get(background_session[12])) / (int(
                                     heard_depart.get(background_session[12])) + int(
                                     heard_depart.get(background_session[13]))),
                                  (heard_depart.get(
                                      background_session[12]))],
                                 display_session[12],
                                 background_session[12], "session",
                                 "half_section")])),
                        urwid.Padding(urwid.Text(
                            [('field', u"{}.: ".format(display_session[13])),
                             heard_depart.get(background_session[13]) if
                             (int(
                                 heard_depart.get(background_session[12])) + int(
                                 heard_depart.get(background_session[13]))) == 0
                             else
                             check_data_is_highlighted(
                                 [int(heard_depart.get(background_session[12])) / (int(
                                     heard_depart.get(background_session[12])) + int(
                                     heard_depart.get(background_session[13]))),
                                  (heard_depart.get(
                                      background_session[13]))],
                                 display_session[13],
                                 background_session[13], "session",
                                 "half_section")])),
                        urwid.Padding(urwid.Text([('field', u"{}......: ".format(display_session[14])),
                                                  heard_depart.get(background_session[14])]))], 1),
                        width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20),
                    urwid.Padding(urwid.Columns([
                        urwid.Padding(urwid.Text([('field', u"{}....: ".format(display_session[15])),
                                                  heard_depart.get(background_session[15])])),
                        urwid.Padding(urwid.Text([('field', u"{}.......: ".format(display_session[16])),
                                                  heard_depart.get(background_session[16])])),
                        urwid.Padding(urwid.Text([('field', u"{}.......: ".format(display_session[17])),
                                                  heard_depart.get(background_session[17])]))], 1),
                        width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                        align='center', min_width=20), blank, Divider(u'_'), ]
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_session[18]))]),
                           urwid.Text([('field', u'{}'.format(display_session[19]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_session[part_min + 5]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_session[18])))))
        display_content.append(
            urwid.Padding(Text(str(depart[num].get(background_session[19]))[:60]), width=80, min_width=1))

        for i in background_session[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num].get(i)))))

        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def session_detail_body(depart):
    """
    project session_detail page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text(
                [('field', u"{}:".format(display_session_detail[0])), str(depart[0][background_session_detail[0]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[1])),
                     check_data_is_highlighted(depart[0].get(background_session_detail[1]), display_session_detail[1],
                                               background_session_detail[1], "session_detail", "half_section")
                     ])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[2])),
                     str(depart[0][background_session_detail[2]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[3])),
                     str(depart[0][background_session_detail[3]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[4])),
                     str(depart[0][background_session_detail[4]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[5])),
                     str(depart[0][background_session_detail[5]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[6])),
                     str(depart[0][background_session_detail[6]])])),
                urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[7])),
                     str(depart[0][background_session_detail[7]])]))],
            1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text(
                    [('field', u"{}:".format(display_session_detail[8])),
                     str(depart[0][background_session_detail[8]])])),
                    urwid.Padding(urwid.Text(
                        [('field', u"{}:".format(display_session_detail[9])),
                         str(depart[0][background_session_detail[9]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[10])),
                                    str(depart[0][background_session_detail[10]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[11])),
                                    str(depart[0][background_session_detail[11]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[12])),
                                    str(depart[0][background_session_detail[12]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[13])),
                                    str(depart[0][background_session_detail[13]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[14])),
                                    str(depart[0][background_session_detail[14]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}(s):".format(display_session_detail[15])),
                                    str(depart[0][background_session_detail[15]])]))],
                1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}:".format(display_session_detail[16])),
                                str(depart[0][background_session_detail[16]])
                                # (str(depart[0][background_session_detail[16]]) if
                                #  int(float(depart[0][background_session_detail[
                                #      19]])) == 0 else str(
                                #     depart[0][background_session_detail[16]]) if check_alarm_data(
                                #     (float(depart[0][background_session_detail[19]]) / float(
                                #         depart[0][background_session_detail[16]])),
                                #     "session_detail",
                                #     "io_db") == "safety" else (
                                #     'warning', u"{}".format(str(depart[0][background_session_detail[16]]))))
                                ])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[17])),
                                    str(depart[0][background_session_detail[17]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[18])),
                                    str(depart[0][background_session_detail[19]])
                                    # (str(depart[0][background_session_detail[19]]) if
                                    #  int(float(depart[0][background_session_detail[
                                    #      19]])) == 0 else str(
                                    #     depart[0][background_session_detail[16]]) if check_alarm_data(
                                    #     (float(depart[0][background_session_detail[19]]) / float(
                                    #         depart[0][background_session_detail[16]])),
                                    #     "session_detail",
                                    #     "io_db") == "safety" else (
                                    #     'warning', u"{}".format(str(depart[0][background_session_detail[19]]))))
                                    ])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[19])),
                                    str(depart[0][background_session_detail[19]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[20])),
                                    str(depart[0][background_session_detail[20]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[21])),
                                    str(depart[0][background_session_detail[21]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[22])),
                                    str(depart[0][background_session_detail[22]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[23])),
                                    str(depart[0][background_session_detail[23]])]))],
                1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}:".format(display_session_detail[24])),
                                str(depart[0][background_session_detail[24]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[25])),
                                    str(depart[0][background_session_detail[25]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[26])),
                                    str(depart[0][background_session_detail[26]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[27])),
                                    str(depart[0][background_session_detail[27]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[28])),
                                    str(depart[0][background_session_detail[28]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[29])),
                                    str(depart[0][background_session_detail[29]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[30])),
                                    str(depart[0][background_session_detail[30]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(display_session_detail[31])),
                                    str(depart[0][background_session_detail[31]])]))],
                1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[33])),
                                           str(depart[0][background_session_detail[33]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[34])),
                                           str(depart[0][background_session_detail[34]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[35])),
                                           str(depart[0][background_session_detail[35]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[36])),
                                           str(depart[0][background_session_detail[36]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[37])),
                                           str(depart[0][background_session_detail[37]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[38])),
                                           str(depart[0][background_session_detail[38]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[39])),
                                           str(depart[0][background_session_detail[39]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[40])),
                                           str(depart[0][background_session_detail[40]])]))],
                1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[41])),
                                           str(depart[0][background_session_detail[41]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[42])),
                                           str(depart[0][background_session_detail[42]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[43])),
                                           str(depart[0][background_session_detail[43]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[44])),
                                           str(depart[0][background_session_detail[44]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[45])),
                                           str(depart[0][background_session_detail[45]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[46])),
                                           str(depart[0][background_session_detail[46]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[47])),
                                           str(depart[0][background_session_detail[47]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(display_session_detail[48])),
                                           str(depart[0][background_session_detail[48]])]))],
                1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}: ".format(display_session_detail[49])),
                                str(depart[0][background_session_detail[49]])])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])), ], 1), width=os.get_terminal_size().columns,
            align='center', min_width=100),
        Divider(u'_'),
        urwid.Padding(
            urwid.Text(
                [('field', u"{}: ".format(display_session_detail[32])), str(depart[0][background_session_detail[32]])])
        ),
    ]
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def asp_body(depart, part_min, part_max):
    """
    project asp page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_asp[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 6]))]),
                           urwid.Text([('field', u'{}'.format(display_asp[part_min + 7]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = []
        for i in background_asp[part_min: part_max + 1]:
            if background_asp.index(i) == 6:
                display_content.append(
                    urwid.Padding(Text(str(depart[num].get(background_asp[6]))[:60]), width=80,
                                  min_width=1))
            elif background_asp.index(i) == 4:
                display_content.append(urwid.Padding(Text([
                    check_data_is_highlighted(depart[num].get(i), i,
                                              i, "asp", "half_section")])))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def sql_explain(query, advise, explain):
    """
    project session explain&advise page
    :param query:sql information
    :param advise:advise of session sql
    :param explain:explain of session sql
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(display_session_plan[0])), ]))],
                          1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(Divider(u'_' * os.get_terminal_size().columns))
    listbox_body.append(urwid.Padding(urwid.Text([query])))
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(display_session_plan[1])), ]))],
                          1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(Divider(u'_' * os.get_terminal_size().columns))

    if advise:
        if isinstance(advise, str):
            listbox_body.append(urwid.Padding(urwid.Text([(str(advise))])))
        else:
            for item in advise:
                listbox_body.append(urwid.Padding(urwid.Text([(str(item))])))
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(display_session_plan[2])), ]))],
                          1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(Divider(u'_' * os.get_terminal_size().columns))
    if explain:
        if isinstance(explain, str):
            listbox_body.append(urwid.Padding(urwid.Text([(str(explain))])))
        else:
            for item in explain:
                listbox_body.append(urwid.Padding(urwid.Text([(str(item))])))
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_body(depart, part_min, part_max):
    """
    project table page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_table[0]))]),
                           urwid.Text([('field', u'{}'.format(display_table[1]))]),
                           urwid.Text([('field', u'{}'.format(display_table[2]))]),
                           urwid.Text([('field', u'{}'.format(display_table[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_table[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_table[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_table[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_table[part_min + 4]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table[0])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table[1])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table[2])))))
        for i in background_table[part_min: part_max + 1]:
            if i == 'last_autovacuum':
                display_content.append(urwid.Padding(Text(str(
                    check_data_is_highlighted(depart[num].get('last_autovacuum'), i,
                                              i, "table", "half_section")))))
            elif i == "n_dead_tup":
                display_content.append(urwid.Padding(Text(
                    str(depart[num].get(i) if (int(depart[num].get("n_dead_tup")) + int(
                        depart[num].get("n_dead_tup"))) == 0
                        else check_data_is_highlighted(
                        [int(depart[num].get("n_dead_tup")) / (
                                int(depart[num].get("n_live_tup")) + int(depart[
                                                                             num].get("n_dead_tup"))),
                         depart[num][i]], i, i, "table", "half_section")))))
            elif i == "n_live_tup":
                display_content.append(urwid.Padding(Text(
                    str(depart[num].get(i) if (int(depart[num].get("n_live_tup")) + int(
                        depart[num].get("n_live_tup"))) == 0
                        else check_data_is_highlighted(
                        [int(depart[num].get("n_dead_tup")) / (
                                int(depart[num].get("n_live_tup")) + int(depart[
                                                                             num].get("n_dead_tup"))),
                         depart[num][i]], i, i, "table", "half_section")))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_detail_body(depart):
    """

    :param depart:
    :return:
    """
    listbox_body = [urwid.Padding(urwid.Columns([
        urwid.Padding(urwid.Text(
            [('field', u"{}:".format(display_table_detail[0])), str(depart[0][background_table_detail[0]])])),
        urwid.Padding(urwid.Text(
            [('field', u"{}:".format(display_table_detail[1])), str(depart[0][background_table_detail[1]])])),
        urwid.Padding(urwid.Text(
            [('field', u"{}:".format(display_table_detail[2])), str(depart[0][background_table_detail[2]])])),
        urwid.Padding(urwid.Text(
            [('field', u"{}:".format(display_table_detail[3])), str(depart[0][background_table_detail[3]])]))],
        1),
        width=os.get_terminal_size().columns,
        align='center',
        min_width=100),
        Divider(u'_'),
    ]
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}:".format(display_table_detail[4])), ]))], 1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(urwid.Padding(urwid.Text([depart[0].get(background_table_detail[4])])))
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_performance_body(depart, part_min, part_max):
    """
    project table_performance page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_table_performance[0]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[1]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[2]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_table_performance[part_min + 4]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_performance[0])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_performance[1])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_performance[2])))))
        for i in background_table_performance[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_index_body(depart):
    """
    project table_index page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_table_index[0]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[1]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[2]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[3]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[4]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[5]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[6]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[7]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[0])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[1])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[2])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[3])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[4])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[5])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[6])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_table_index[7])))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_table_index[0]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[1]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[8]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[9]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[10]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[11]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[12]))]),
                           urwid.Text([('field', u'{}'.format(display_table_index[13]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content_ectype = list()
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[0])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[1])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[8])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[9])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[10])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[11])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[12])))))
        display_content_ectype.append(urwid.Padding(Text(str(depart[num].get(background_table_index[13])))))
        data = urwid.Padding(
            urwid.Columns(display_content_ectype, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def lock_body(depart, heard_depart, part_min, part_max):
    """
    project lock page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Columns([(urwid.Padding(
        urwid.Text([('field', u" {}: ".format(display_lock[0])), heard_depart[background_lock[0]]]))), ], 0), width=50,
        align='center', min_width=2))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_lock[1]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_lock[part_min + 6]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_lock[1])))))
        for i in background_lock[part_min: part_max + 1]:
            if i == "granted":
                display_content.append(urwid.Padding(Text(
                    check_data_is_highlighted(
                        depart[num][i], i, i, "lock", "half_section"))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def lock_chain_body(depart):
    """
    project lock_detail page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(
                urwid.Text([('field', u"     {}  >>>  {}".format(display_lock_chain[0], display_lock_chain[1]))]))],
            1), width=300, align='left', min_width=20),
        Divider(u'_')
    ]
    if depart:
        for date in depart:
            data = urwid.Padding(
                urwid.Text([('     '), str(date.get(background_lock_chain[0])), ("  >>>  "),
                            str(date.get(background_lock_chain[1]))]), width=60, align='left', min_width=2)
            listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox1 = urwid.LineBox(listbox)
    return linebox1


def wait_event_body(depart, heard_depart, part_min=1, part_max=10):
    """
    project wait_event page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Columns([(urwid.Padding(
        urwid.Text([('field', u" {}: ".format(display_wait_event[0])), heard_depart[display_wait_event[0]]])))], 0),
        width=50, align='center', min_width=2))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_wait_event[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 6]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 7]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 8]))]),
                           urwid.Text([('field', u'{}'.format(display_wait_event[part_min + 9]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = []
        for i in background_wait_event[part_min: part_max + 1]:
            if i == "avg_wait_time":
                display_content.append(urwid.Padding(Text(
                    check_data_is_highlighted(
                        depart[num][i], i, i, "wait_event", "half_section"))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def memory_body(depart, heard_depart):
    """
    project memory page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}     : ".format(display_memory[0])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[0], 0),
                                                              display_memory[0],
                                                              background_memory[0],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}     : ".format(display_memory[1])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[1], 0),
                                                              display_memory[1],
                                                              background_memory[1],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}     : ".format(display_memory[2])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[2], 0),
                                                              display_memory[2],
                                                              background_memory[2],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}      : ".format(display_memory[3])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[3], 0),
                                                              display_memory[3],
                                                              background_memory[3],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}      : ".format(display_memory[4])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[4], 0),
                                                              display_memory[4],
                                                              background_memory[4],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{}    : ".format(display_memory[5])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[5], 0),
                                                              display_memory[5],
                                                              background_memory[5],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"{} : ".format(display_memory[6])),
                                                          check_data_is_highlighted(
                                                              heard_depart.get(background_memory[6], 0),
                                                              display_memory[6],
                                                              background_memory[6],
                                                              "memory", "half_section")
                                                          ]))), ],
                              0), width=150, align='center', min_width=2),
        blank,
        Divider(u'_'),
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[7])), (depart[background_memory[7]])])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[8])), (str(depart[background_memory[8]]))])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[9])), (str(depart[background_memory[9]]))])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[10])), (str(depart[background_memory[10]]))])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[11])), (str(depart[background_memory[11]]))])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[12])), (str(depart[background_memory[12]]))])),
             urwid.Padding(
                 urwid.Text([('field', u"{}: ".format(display_memory[13])), (str(depart[background_memory[13]]))]))],
            1), width=os.get_terminal_size().columns, align='center', min_width=100),
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(
            urwid.Text([('field', u"{}: ".format(display_memory[14])), (str(depart[background_memory[14]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[15])),
                                      (str(depart[background_memory[15]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[16])), (str(depart[background_memory[16]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[17])),
                                      (str(depart[background_memory[17]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[18])), (str(depart[background_memory[18]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[19])),
                                      (str(depart[background_memory[19]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[20])), (str(depart[background_memory[20]]))]))], 1),
            width=os.get_terminal_size().columns, align='center', min_width=100),
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(
            urwid.Text([('field', u"{}: ".format(display_memory[21])), (str(depart[background_memory[21]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[22])),
                                      (str(depart[background_memory[22]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[23])), (str(depart[background_memory[23]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[24])),
                                      (str(depart[background_memory[24]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[25])), (str(depart[background_memory[25]]))])),
            urwid.Padding(urwid.Text([('field', u"{}: ".format(display_memory[26])),
                                      (str(depart[background_memory[26]]))])), urwid.Padding(
                urwid.Text([('field', u"{}: ".format(display_memory[27])), (str(depart[background_memory[27]]))]))], 1),
            width=os.get_terminal_size().columns, align='center', min_width=100),
    ]
    listbox1 = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox1 = urwid.LineBox(listbox1)
    return linebox1


def sharemem_body(depart, heard_depart, part_min=1, part_max=7):
    """
    project sharemem page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Columns([(urwid.Padding(
        urwid.Text([('field', u" {}: ".format(display_sharemem[0])), heard_depart[background_sharemem[0]]]))), ], 0),
        width=50, align='center', min_width=2))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_sharemem[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_sharemem[part_min + 6]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = []
        for i in background_sharemem[part_min: part_max + 1]:
            if i == "usedsize/totalsize":
                display_content.append(urwid.Padding(Text(check_data_is_highlighted(
                    depart[num][i], i, i, "sharemem", "half_section"))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def top_mem_used_session_body(depart, heard_depart, part_min=1, part_max=7):
    """
     project top_mem_used_session page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Columns([(urwid.Padding(urwid.Text(
        [('field', u" {}: ".format(display_top_mem_used_session[0])),
         heard_depart[background_top_mem_used_session[0]]]))), ], 0), width=50, align='center', min_width=2))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_top_mem_used_session[part_min + 6]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = []
        for i in background_top_mem_used_session[part_min: part_max + 1]:
            if background_top_mem_used_session.index(i) == 5:
                display_content.append(urwid.Padding(Text(str(depart[num][i][:32]))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def dynamicsql_body(depart, heard_depart, part_min, part_max):
    """
    project dynamicsql page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Columns([(urwid.Padding(
        urwid.Text([('field', u" {}: ".format(display_dynamicsql[0])), heard_depart[background_dynamicsql[0]]]))), ],
        0), width=50, align='center', min_width=2))
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_dynamicsql[1]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[2]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_dynamicsql[part_min + 5]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_dynamicsql[1])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_dynamicsql[2])))))

        for i in background_dynamicsql[part_min: part_max + 1]:
            if i == "avg_db_time":
                display_content.append(urwid.Padding(Text(
                    check_data_is_highlighted(
                        depart[num][i], i, i, "dynamicsql", "half_section"))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def replication_body(up_depart, de_depart):
    """
    project replication page
    :param up_depart:
    :param de_depart:
    :return:
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_replication[0]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[1]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[2]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[3]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[4]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[5]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[6]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[7]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[8]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(up_depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[0])))))
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[1])))))
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[2])))))
        display_content.append(urwid.Padding(Text(
            check_data_is_highlighted(up_depart[num]['peer_state'], 'peer_state', "peer_state", "replication",
                                      "half_section"))))
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[4])))))
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[5])))))
        display_content.append(urwid.Padding(Text(
            check_data_is_highlighted((up_depart[num]['receive_gap']), 'receive_gap', "receive_gap", "replication",
                                      "half_section"))))
        display_content.append(urwid.Padding(Text(
            check_data_is_highlighted((up_depart[num]['replay_gap']), 'replay_gap', "replay_gap", "replication",
                                      "half_section"))))
        display_content.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[8])))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_replication[0]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[1]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[9]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[10]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[11]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[12]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[13]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[14]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[15]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(up_depart)):
        display_content_ectype = list()
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[0])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[1])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[9])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[10])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[11])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[12])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[13])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[14])))))
        display_content_ectype.append(urwid.Padding(Text(str(up_depart[num].get(background_replication[15])))))
        data = urwid.Padding(
            urwid.Columns(display_content_ectype, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)

    listbox_body.append(blank)
    listbox_body.append(blank)
    listbox_body.append(urwid.Padding(urwid.Text([('title', u"slots")]),
                                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                                      left=os.get_terminal_size().columns,
                                      align='left', min_width=20))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_replication[16]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[17]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[18]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[19]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[20]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[21]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[22]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[23]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[24]))]),
                           urwid.Text([('field', u'{}'.format(display_replication[25]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    for num in range(len(de_depart)):
        display_content_ectype = list()
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[16])))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[17])))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[18])))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[19])))))
        display_content_ectype.append(urwid.Padding(Text(
            check_data_is_highlighted((de_depart[num]['active']), 'active', "active", "replication", "half_section"))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[21])))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[22])))))
        display_content_ectype.append(urwid.Padding(Text(
            check_data_is_highlighted((de_depart[num]['diff_lsn']), 'diff_lsn', "diff_lsn", "replication",
                                      "half_section"))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[24])))))
        display_content_ectype.append(urwid.Padding(Text(str(de_depart[num].get(background_replication[25])))))
        data = urwid.Padding(
            urwid.Columns(display_content_ectype, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def index_recommendation_body(advised_indexes):
    """
    project session explain&advise page
    :param advised_indexes:db_mind of session sql
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(display_index_recommendation[0])), ]))],
                          1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(Divider(u'_' * os.get_terminal_size().columns))
    advised_indexes_num = 0
    if advised_indexes:
        for item in advised_indexes:
            listbox_body.append(urwid.Padding(urwid.Text([("advised_indexe" + str(advised_indexes_num) + "：")])))
            listbox_body.append(urwid.Padding(urwid.Text([("    indexdef：{}".format(item.get("indexdef")))])))
            listbox_body.append(
                urwid.Padding(urwid.Text([("    workload_benefit：{}".format(str(item.get("workload_benefit"))))])))
            listbox_body.append(urwid.Padding(
                urwid.Text([("    workload_improved_rate：{}".format(str(item.get("workload_improved_rate"))))])))
            listbox_body.append(urwid.Padding(urwid.Text([("    useless_indexes：")])))
            for i in range(len(item.get("useless_indexes"))):
                listbox_body.append(
                    urwid.Padding(urwid.Text([("                    {}".format(item.get("useless_indexes")[i]))])))
            listbox_body.append(urwid.Padding(urwid.Text([("    existing_indexes：")])))
            for i in range(len(item.get("existing_indexes"))):
                listbox_body.append(
                    urwid.Padding(urwid.Text([("                     {}".format(item.get("existing_indexes")[i]))])))
            listbox_body.append(urwid.Padding(urwid.Text([("    improved_queriies：")])))
            for i in range(len(item.get("improved_queriies"))):
                listbox_body.append(urwid.Padding(
                    urwid.Text([("      query：{}".format(item.get("improved_queriies")[i].get("query")))])))
                listbox_body.append(urwid.Padding(urwid.Text(
                    [("      query_benefit：{}".format(str(item.get("improved_queriies")[i].get("query_benefit"))))])))
                listbox_body.append(urwid.Padding(urwid.Text([("      query_improved_rate：{}".format(
                    str(item.get("improved_queriies")[i].get("query_improved_rate"))))])))
                listbox_body.append(urwid.Padding(urwid.Text(
                    [("      query_count：{}".format(str(item.get("improved_queriies")[i].get("query_count"))))])))
                listbox_body.append(urwid.Padding(urwid.Text([("      origin_plan：")])))
                for o in range(len(item.get("improved_queriies")[i].get("origin_plan"))):
                    listbox_body.append(urwid.Padding(urwid.Text(
                        [("                  {}".format((item.get("improved_queriies")[i].get("origin_plan"))[o]))])))
                listbox_body.append(urwid.Padding(urwid.Text([("      current_plan：")])))
                for o in range(len(item.get("improved_queriies")[i].get("current_plan"))):
                    listbox_body.append(urwid.Padding(urwid.Text(
                        [("                   {}".format((item.get("improved_queriies")[i].get("current_plan"))[o]))])))
                listbox_body.append(urwid.Padding(urwid.Text([("      other_related_indexes：")])))
                for o in range(len(item.get("improved_queriies")[i].get("other_related_indexes"))):
                    listbox_body.append(urwid.Padding(urwid.Text([("                            {}".format(
                        (item.get("improved_queriies")[i].get("other_related_indexes"))[o]))])))
            advised_indexes_num += 1
            listbox_body.append(blank)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def database_list_body(depart, part_min, part_max):
    """
    project database_list page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_database_list[0]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 6]))]),
                           urwid.Text([('field', u'{}'.format(display_database_list[part_min + 7]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = []
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_database_list[0])))))
        for i in background_database_list[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(
            urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def sql_patch_body(depart, part_min, part_max):
    """
    project sql_patch page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = list()
    listbox_body.append(blank)
    listbox_body.append(Divider(u'_'))
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_sql_patch[0]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[1]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_sql_patch[part_min + 5]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_sql_patch[0])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_sql_patch[1])))))
        for i in background_sql_patch[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def slow_sql_body(depart, part_min, part_max):
    """
    project slow_sql page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_slow_sql[0]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[1]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_slow_sql[part_min + 5]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_slow_sql[0])))))
        display_content.append(
            urwid.Padding(Text(str(depart[num].get(background_slow_sql[1]))[:60]), width=80, min_width=1))
        for i in background_slow_sql[part_min: part_max + 1]:
            if i == "duration_ms":
                display_content.append(urwid.Padding(Text(
                    check_data_is_highlighted(depart[num][i], i, i, "slow_sql", "half_section"))))
            elif i == "detail":
                display_content.append(urwid.Padding(Text(str(depart[num].get(i))[:30])))
            elif i == "query_plan":
                display_content.append(urwid.Padding(Text(str(depart[num].get(i))[:30])))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns,
                             align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def redo_body(depart, part_min, part_max):
    """
    project redo page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_redo[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 6]))]),
                           urwid.Text([('field', u'{}'.format(display_redo[part_min + 7]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        for i in background_redo[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns,
                             align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def redo_time_count_body(depart, part_min, part_max):
    """
    project redo_time_count page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_redo_time_count[0]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 4]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 5]))]),
                           urwid.Text([('field', u'{}'.format(display_redo_time_count[part_min + 6]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_redo_time_count[0])))))
        for i in background_redo_time_count[part_min: part_max + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns,
                             align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def xlog_redo_statics_body(depart):
    """
    project xlog_redo_statics page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_xlog_redo_statics[0]))]),
                           urwid.Text([('field', u'{}'.format(display_xlog_redo_statics[1]))]),
                           urwid.Text([('field', u'{}'.format(display_xlog_redo_statics[2]))]),
                           urwid.Text([('field', u'{}'.format(display_xlog_redo_statics[3]))]),
                           urwid.Text([('field', u'{}'.format(display_xlog_redo_statics[4]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        for i in background_xlog_redo_statics[0: 4 + 1]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns,
                             align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def wdr_body(depart):
    """
    project redo page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_wdr[0]))]),
                           urwid.Text([('field', u'{}'.format(display_wdr[1]))]),
                           urwid.Text([('field', u'{}'.format(display_wdr[2]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        for i in background_wdr[0: 3]:
            display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns,
                             align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def snap_summary_more_statement_body(depart, part_min, part_max):
    """
    project snap_summary_more_statement page
    :param depart:the data that the page needs to display
    :param part_min:page min number
    :param part_max:page max number
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[0]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[1]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[2]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[part_min]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[part_min + 1]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[part_min + 2]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[part_min + 3]))]),
                           urwid.Text([('field', u'{}'.format(display_snap_summary_more_statement[part_min + 4]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_snap_summary_more_statement[0])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_snap_summary_more_statement[1])))))
        display_content.append(urwid.Padding(Text(str(depart[num].get(background_snap_summary_more_statement[2])))))
        for i in background_snap_summary_more_statement[part_min: part_max + 1]:
            if i == 'snap_query':
                display_content.append(urwid.Padding(Text(str(depart[num][i][:30]))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def burr_analysis_body(depart):
    """
    project burr_analysis page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    listbox_body = list()
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Text([('field', u'{}'.format(display_burr_analysis[0]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[1]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[2]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[3]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[4]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[5]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[6]))]),
                           urwid.Text([('field', u'{}'.format(display_burr_analysis[7]))]),
                           ], 1), width=os.get_terminal_size().columns, align='center'))
    listbox_body.append(Divider(u'_'))
    for num in range(len(depart)):
        display_content = list()
        for i in background_burr_analysis:
            if i == 'snap_query':
                display_content.append(urwid.Padding(Text(str(depart[num][i][:30]))))
            else:
                display_content.append(urwid.Padding(Text(str(depart[num][i]))))
        data = urwid.Padding(urwid.Columns(display_content, 1), width=os.get_terminal_size().columns, align='center')
        listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def rigorous_analysis_body(head, depart):
    """
    project rigorous_analysis page
    :param head:the header display the data
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    listbox_body = [
        urwid.Padding(urwid.Columns([(urwid.Padding(
            urwid.Text(
                [('field', u"snapshot_id: "), str(head.get('snapshot_id'))]))), ],
            0), width=50, align='center', min_width=2),
        urwid.Padding(urwid.Columns([(urwid.Padding(
            urwid.Text(
                [('field', u"snap_unique_sql_id: "), str(head.get('snap_unique_sql_id'))]))), ],
            0), width=50, align='center', min_width=2), Divider(u'_')
    ]
    if depart:
        for i in range(int(len(depart) / 2)):
            listbox_body.append(Padding(urwid.Columns([(urwid.Padding(urwid.Text(
                [('field', u"{}: ".format(list(depart.keys())[i])), str(depart.get(list(depart.keys())[i]))
                 ]))), ], 0), width=os.get_terminal_size().columns, align='center', min_width=2))
            listbox_body.append(Padding(urwid.Columns([(urwid.Padding(urwid.Text(
                [('field', u"{}: ".format(list(depart.keys())[i] + '_rate')),
                 str(depart.get(list(depart.keys())[i] + '_rate'))
                 ]))), ], 0), width=os.get_terminal_size().columns, align='center', min_width=2))
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def snap_summary_statement_body(depart):
    """
    project snap_summary_statement page
    :param depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[0])),
                                       str(depart[0][background_snap_summary_statement[0]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[1])),
                                       str(depart[0][background_snap_summary_statement[1]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[2])),
                                       str(depart[0][background_snap_summary_statement[2]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[3])),
                                       str(depart[0][background_snap_summary_statement[3]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[4])),
                                       str(depart[0][background_snap_summary_statement[4]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[5])),
                                       str(depart[0][background_snap_summary_statement[5]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[6])),
                                       str(depart[0][background_snap_summary_statement[6]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[7])),
                                       str(depart[0][background_snap_summary_statement[7]])]))],
            1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[8])),
                                       str(depart[0][background_snap_summary_statement[8]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[9])),
                                       str(depart[0][background_snap_summary_statement[9]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[10])),
                                       str(depart[0][background_snap_summary_statement[10]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[11])),
                                       str(depart[0][background_snap_summary_statement[11]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[12])),
                                       str(depart[0][background_snap_summary_statement[12]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[13])),
                                       str(depart[0][background_snap_summary_statement[13]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[14])),
                                       str(depart[0][background_snap_summary_statement[14]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[15])),
                                       str(depart[0][background_snap_summary_statement[15]])]))],
            1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[17])),
                                          str(depart[0][background_snap_summary_statement[17]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[18])),
                                          str(depart[0][background_snap_summary_statement[18]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[19])),
                                          str(depart[0][background_snap_summary_statement[19]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[20])),
                                          str(depart[0][background_snap_summary_statement[20]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[21])),
                                          str(depart[0][background_snap_summary_statement[21]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[22])),
                                          str(depart[0][background_snap_summary_statement[22]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[23])),
                                          str(depart[0][background_snap_summary_statement[23]])])),
                urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[24])),
                                          str(depart[0][background_snap_summary_statement[24]])]))],
            1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[25])),
                                       str(depart[0][background_snap_summary_statement[25]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[26])),
                                       str(depart[0][background_snap_summary_statement[26]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[27])),
                                       str(depart[0][background_snap_summary_statement[27]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[28])),
                                       str(depart[0][background_snap_summary_statement[28]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[29])),
                                       str(depart[0][background_snap_summary_statement[29]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[30])),
                                       str(depart[0][background_snap_summary_statement[30]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[31])),
                                       str(depart[0][background_snap_summary_statement[31]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[32])),
                                       str(depart[0][background_snap_summary_statement[32]])]))],
            1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[33])),
                                       str(depart[0][background_snap_summary_statement[33]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[34])),
                                       str(depart[0][background_snap_summary_statement[34]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[35])),
                                       str(depart[0][background_snap_summary_statement[35]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[36])),
                                       str(depart[0][background_snap_summary_statement[36]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[37])),
                                       str(depart[0][background_snap_summary_statement[37]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[38])),
                                       str(depart[0][background_snap_summary_statement[38]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[39])),
                                       str(depart[0][background_snap_summary_statement[39]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[40])),
                                       str(depart[0][background_snap_summary_statement[40]])]))],
            1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[41])),
                                       str(depart[0][background_snap_summary_statement[41]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[42])),
                                       str(depart[0][background_snap_summary_statement[42]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[43])),
                                       str(depart[0][background_snap_summary_statement[43]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[44])),
                                       str(depart[0][background_snap_summary_statement[44]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[45])),
                                       str(depart[0][background_snap_summary_statement[45]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[46])),
                                       str(depart[0][background_snap_summary_statement[46]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[47])),
                                       str(depart[0][background_snap_summary_statement[47]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[48])),
                                       str(depart[0][background_snap_summary_statement[48]])]))],
            1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[49])),
                                       str(depart[0][background_snap_summary_statement[49]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[50])),
                                       str(depart[0][background_snap_summary_statement[50]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(display_snap_summary_statement[51])),
                                       str(depart[0][background_snap_summary_statement[51]])])),
             urwid.Padding(urwid.Text([('field', u"      ")])),
             urwid.Padding(urwid.Text([('field', u"      ")])),
             urwid.Padding(urwid.Text([('field', u"      ")])),
             urwid.Padding(urwid.Text([('field', u"      ")])),
             urwid.Padding(urwid.Text([('field', u"      ")])), ], 1), width=os.get_terminal_size().columns,
            align='center', min_width=100),
        Divider(u'_'),
        urwid.Padding(
            urwid.Text([('field', u"{}: ".format(display_snap_summary_statement[52])),
                        str(depart[0][background_snap_summary_statement[52]])])
        ),
    ]
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox
