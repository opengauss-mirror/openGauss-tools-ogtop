import json

import datetime
import time
import urwid
import urwid.raw_display
import urwid.web_display
from urwid import Padding, Text, Divider
from db_util import progress_bar
import shortcut_key_menu
import os
from session_query import computation_time
from shortcut_key_menu import sort_querys_dict
import pandas as pd

database = sort_querys_dict["database"]
session = sort_querys_dict["session"]
session_detail = sort_querys_dict["session_detail"]
session_plan = sort_querys_dict["session_plan"]
table = sort_querys_dict["table"]
table_index = sort_querys_dict["table_index"]
lock = sort_querys_dict["lock"]
lock_detail = sort_querys_dict['lock_detail']
wait_event = sort_querys_dict["wait_event"]
memory = sort_querys_dict["memory"]
sharemem = sort_querys_dict["sharemem"]
top_mem_used_session = sort_querys_dict["top_mem_used_session"]
dynamicsql = sort_querys_dict["dynamicsql"]
replication = sort_querys_dict["replication"]


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
            [(urwid.Padding(urwid.Text([('field', u"session"), "-->session_detail-->session_detail_plan"]))), ], 0),
            width=100, align='center',
            min_width=2))
    listbox_body.append(
        Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u"table"), "-->table_index"]))), ], 0), width=100,
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
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text(
                [('field',
                  u"                            {}: ".format(database[0])), progress_bar(uppart[database[0]])]))),
            ], 0), width=140,
            align='center',
            min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text(
                [('field',
                  u"                          {}: ".format(database[1])), progress_bar(uppart[database[1]])]))),
            ], 0), width=140,
            align='center',
            min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text(
                [('field', u"                            {}: ".format(database[2])), progress_bar(
                    uppart[database[2]])]))),
            ], 0), width=140,
            align='center',
            min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"                              {}: ".format(database[3])), progress_bar(
                    uppart[database[3]])]))),
            ], 0), width=140,
            align='center',
            min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(urwid.Text([('field', u"            {}: ".format(database[4])),
                                        progress_bar(uppart[database[4]]) if uppart[database[4]] != "-" else uppart[
                                            database[4]]]))),
             ], 0), width=140,
            align='center',
            min_width=2),
        Divider(u'_'),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(database[5]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[6]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[7]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[8]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[9]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[10]))])),
                                     urwid.Padding(urwid.Text([('field', u"{}".format(database[11]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[5]])),
                                     urwid.Padding(Text(depart[database[6]])),
                                     urwid.Padding(
                                         urwid.Text([depart[database[7]] if depart[database[7]] == "Streaming" or
                                                                            depart[database[7]] == "-" or depart[
                                                                                database[7]] == "Normal" else (
                                             'warning', u"{}".format(depart[database[7]]))])),
                                     urwid.Padding(Text(depart[database[8]])),
                                     urwid.Padding(Text(depart[database[9]])),
                                     urwid.Padding(Text(depart[database[10]])),
                                     urwid.Padding(Text(depart[database[11]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([(urwid.Padding(urwid.Text([('field', u'{}'.format(database[12]))]))),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[13]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[14]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[15]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[16]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[17]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[18]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[12]])),
                                     urwid.Padding(Text(depart[database[13]])),
                                     urwid.Padding(Text(depart[database[14]])),
                                     urwid.Padding(Text(depart[database[15]])),
                                     urwid.Padding(Text(depart[database[16]])),
                                     urwid.Padding(Text(depart[database[17]] if int(depart[database[17]]) == 0 else (
                                         'warning', u"{}".format(depart[database[17]])))),
                                     urwid.Padding(Text(depart[database[18]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,

        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[19]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[20]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[21]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[22]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[23]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[24]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[25]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[19]])),
                                     urwid.Padding(Text(depart[database[20]])),
                                     urwid.Padding(Text(depart[database[21]])),
                                     urwid.Padding(Text(depart[database[22]])),
                                     urwid.Padding(Text(depart[database[23]])),
                                     urwid.Padding(Text(depart[database[24]])),
                                     urwid.Padding(Text(depart[database[25]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[26]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[27]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[28]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[29]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[30]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[31]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[32]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[26]])),
                                     urwid.Padding(Text(depart[database[27]])),
                                     urwid.Padding(Text(depart[database[28]])),
                                     urwid.Padding(Text(depart[database[29]])),
                                     urwid.Padding(Text(depart[database[30]])),
                                     urwid.Padding(Text(depart[database[31]])),
                                     urwid.Padding(Text(depart[database[32]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[33]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[34]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[35]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}/s'.format(database[36]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}/s'.format(database[37]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}/s'.format(database[38]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}/s'.format(database[39]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[33]])),
                                     urwid.Padding(Text(depart[database[34]])),
                                     urwid.Padding(Text(depart[database[35]])),
                                     urwid.Padding(Text(depart[database[36]])),
                                     urwid.Padding(Text(depart[database[37]])),
                                     urwid.Padding(Text(depart[database[38]])),
                                     urwid.Padding(Text(depart[database[39]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[40]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[41]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[42]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[43]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[44]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[45]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[46]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[40]])),
                                     urwid.Padding(Text(depart[database[41]])),
                                     urwid.Padding(Text(depart[database[42]])),
                                     urwid.Padding(Text(depart[database[43]])),
                                     urwid.Padding(Text(depart[database[44]])),
                                     urwid.Padding(Text(depart[database[45]])),
                                     urwid.Padding(Text(depart[database[46]] if int(
                                         depart[database[46]]) == 0 else (
                                         'warning', u"{}".format(depart[database[46]])))),
                                     ], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[47]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[48]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[49]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[50]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[51]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[52]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[53]))]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[47]])),
                                     urwid.Padding(Text(depart[database[48]])),
                                     urwid.Padding(Text(depart[database[49]])),
                                     urwid.Padding(Text(depart[database[50]])),
                                     urwid.Padding(Text(depart[database[51]])),
                                     urwid.Padding(Text(depart[database[52]])),
                                     urwid.Padding(Text(depart[database[53]]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u'{}'.format(database[54]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[55]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[56]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[57]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[58]))])),
                                     urwid.Padding(urwid.Text([('field', u'{}'.format(database[59]))])),
                                     urwid.Padding(urwid.Text([('field', u' ')]))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        urwid.Padding(urwid.Columns([urwid.Padding(Text(depart[database[54]])),
                                     urwid.Padding(Text(depart[database[55]])),
                                     urwid.Padding(Text(depart[database[56]])),
                                     urwid.Padding(Text(depart[database[57]])),
                                     urwid.Padding(Text(depart[database[58]])),
                                     urwid.Padding(Text(depart[database[59]])),
                                     urwid.Padding(Text(""))], 1),
                      width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
                      align='center',
                      min_width=8),
        blank,
        blank,
        blank, ]
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def session_body(heard_depart, depart, part):
    """
    project session page
    :param heard_depart:the header display the data
    :param depart:the data that the page needs to display
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        Padding(urwid.Text([((u"—" * 125)), ]), width=150, align='center', min_width=2),
        Padding(
            urwid.Text([("|"), ('field', u"{}: ".format(session[0])), heard_depart[session[0]],
                        " " * (125 - len("|{}: ".format(session[0]) + heard_depart[session[0]])), ("|")])
            , width=150, align='center', min_width=2
        ),
        Padding(urwid.Text([((u"—" * 125)), ]), width=150, align='center', min_width=2),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}............: ".format(session[1])), heard_depart[session[1]]])),
                    urwid.Padding(urwid.Text([('field', u"   "), ])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}.........: ".format(session[2])), heard_depart[session[2]]]),
                        align='right')],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20, ),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}..........: ".format(session[3])), heard_depart[session[3]]])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(session[4])), heard_depart[session[4]]])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(session[5])), heard_depart[session[5]]]))],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}...............: ".format(session[6])), heard_depart[session[6]]])),
                    urwid.Padding(urwid.Text([('field', u"{}.....: ".format(session[7])), heard_depart[session[7]]])),
                    urwid.Padding(urwid.Text([('field', u"{}..: ".format(session[8])), heard_depart[session[8]]]))],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}........: ".format(session[9])), heard_depart[session[9]]])),
                 urwid.Padding(urwid.Text([('field', u"{}.: ".format(session[10])), heard_depart[session[10]]])),
                 urwid.Padding(urwid.Text([('field', u"{}...: ".format(session[11])), heard_depart[session[11]]]))],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}: ".format(session[12])), heard_depart[session[12]]])),
                 urwid.Padding(urwid.Text([('field', u"{}.: ".format(session[13])), heard_depart[session[13]]])),
                 urwid.Padding(urwid.Text([('field', u"{}......: ".format(session[14])), heard_depart[session[14]]]))],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}....: ".format(session[15])), heard_depart[session[15]]])),
                 urwid.Padding(urwid.Text([('field', u"{}.......: ".format(session[16])), heard_depart[session[16]]])),
                 urwid.Padding(urwid.Text([('field', u"{}.......: ".format(session[17])), heard_depart[session[17]]]))],
                1),
            width=150 if os.get_terminal_size().columns > 150 else os.get_terminal_size().columns,
            align='center',
            min_width=20),
        blank,
        Divider(u'_'),
    ]
    if part == 1:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(session[18]))]),
                               urwid.Text([('field', u'{}'.format(session[19]))]),
                               urwid.Text([('field', u'{}'.format(session[20]))]),
                               urwid.Text([('field', u'{}'.format(session[21]))]),
                               urwid.Text([('field', u'{}'.format(session[22]))]),
                               urwid.Text([('field', u'{}'.format(session[23]))]),
                               urwid.Text([('field', u'{}'.format(session[24]))]),
                               urwid.Text([('field', u'{}'.format(session[25]))]),
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][session[18]]))),
                               urwid.Padding(Text(str(depart[num].get(session[19])))),
                               urwid.Padding(Text(str(depart[num].get(session[20])))),
                               urwid.Padding(Text(str(depart[num].get(session[21])))),
                               urwid.Padding(Text(str(depart[num].get(session[22])))),
                               urwid.Padding(Text(str(depart[num].get(session[23])))),
                               urwid.Padding(Text(computation_time((depart[num].get(session[24]))))),
                               urwid.Padding(Text(computation_time((depart[num].get(session[25]))))),
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    elif part == 2:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([
                    urwid.Text([('field', u'{}'.format(session[18]))]),
                    urwid.Text([('field', u'{}'.format(session[26]))]),
                    urwid.Text([('field', u'{}'.format(session[27]))]),
                    urwid.Text([('field', u'{}'.format(session[28]))]),
                    urwid.Text([('field', u'{}/s'.format(session[29]))]),
                    urwid.Text([('field', u'{}/s'.format(session[30]))]),
                    urwid.Text([('field', u'{}/s'.format(session[31]))]),
                ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([
                    urwid.Padding(Text(str(depart[num].get(session[18])))),
                    urwid.Padding(Text(str(depart[num].get(session[26])))),
                    urwid.Padding(Text(str(depart[num].get(session[27])))),
                    urwid.Padding(Text(str(depart[num].get(session[28])))),
                    urwid.Padding(Text(str(depart[num].get(session[29])))),
                    urwid.Padding(Text(str(depart[num].get(session[30])))),
                    urwid.Padding(Text(str(depart[num].get(session[31])))),
                ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    else:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(session[18]))]),
                               urwid.Text([('field', u'{}/s'.format(session[32]))]),
                               urwid.Text([('field', u'{}'.format(session[33]))]),
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(session[18])))),
                               urwid.Padding(Text(str(depart[num].get(session[32])))),
                               urwid.Padding(Text(str(depart[num].get(session[33]))[:33]))
                               ], 1), width=os.get_terminal_size().columns, align='center')
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
            [urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[0])), (depart[session_detail[0]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[1])), (depart[session_detail[1]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[2])), (depart[session_detail[2]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[3])), (depart[session_detail[3]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[4])), (depart[session_detail[4]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[5])), (depart[session_detail[5]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[6])), (depart[session_detail[6]])])),
             urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[7])), (depart[session_detail[7]])]))], 1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[8])), (depart[session_detail[8]])])),
                 urwid.Padding(urwid.Text([('field', u"{}:".format(session_detail[9])), (depart[session_detail[9]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}:".format(session_detail[10])), (depart[session_detail[10]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}:".format(session_detail[11])), (depart[session_detail[11]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}:".format(session_detail[12])), (depart[session_detail[12]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}:".format(session_detail[13])), (depart[session_detail[13]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}:".format(session_detail[14])), (depart[session_detail[14]])])),
                 urwid.Padding(
                     urwid.Text([('field', u"{}/s:".format(session_detail[15])), (depart[session_detail[15]])]))],
                1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}/s:".format(session_detail[16])), (depart[session_detail[16]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[17])), (depart[session_detail[17]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[18])), (depart[session_detail[18]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[19])), (depart[session_detail[19]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[20])), (depart[session_detail[20]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[21])), (depart[session_detail[21]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[22])), (depart[session_detail[22]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[23])), (depart[session_detail[23]])]))],
                1),
            width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}:".format(session_detail[24])), (depart[session_detail[24]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[25])), (depart[session_detail[25]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[26])), (depart[session_detail[26]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[27])), (depart[session_detail[27]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[28])), (depart[session_detail[28]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[29])), (depart[session_detail[29]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}:".format(session_detail[30])), (depart[session_detail[30]])])),
                    urwid.Padding(
                        urwid.Text([('field', u"{}/s:".format(session_detail[31])), (depart[session_detail[31]])]))],
                1), width=os.get_terminal_size().columns,
            align='center',
            min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(
                    urwid.Text([('field', u"{}/s: ".format(session_detail[32])), (depart[session_detail[32]])])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])),
                    urwid.Padding(urwid.Text([('field', u"      ")])), ], 1), width=os.get_terminal_size().columns,
            align='center', min_width=100),
        Divider(u'_'),
        urwid.Padding(urwid.Text([('field', u"{}: ".format(session_detail[33])), depart[session_detail[33]]])
                      ),
    ]
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def sql_explain(advise, explan):
    """
    project session explan&advise page
    :param advise:advise of session sql
    :param explan:explan of session sql
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [blank]
    listbox_body.append(
        urwid.Padding(
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(session_plan[0])), ]))],
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
            urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(session_plan[1])), ]))],
                          1),
            width=os.get_terminal_size().columns, align='center', min_width=20))
    listbox_body.append(Divider(u'_' * os.get_terminal_size().columns))
    if explan:
        if isinstance(explan, str):
            listbox_body.append(urwid.Padding(urwid.Text([(str(explan))])))
        else:
            for item in explan:
                listbox_body.append(urwid.Padding(urwid.Text([(str(item))])))
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_body(depart, part):
    """
    project table page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
    ]
    if part == 1:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[2]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[3]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[4]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[5]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[6]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[7]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[0]]))),
                               urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[2]]))),
                               urwid.Padding(Text(str(depart[num][table[3]]))),
                               urwid.Padding(Text(str(depart[num][table[4]]))),
                               urwid.Padding(Text(str(depart[num][table[5]]))),
                               urwid.Padding(Text(str(depart[num][table[6]]))),
                               urwid.Padding(Text(str(depart[num][table[7]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    elif part == 2:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[8]))])),
                               urwid.Padding(urwid.Text([('field', u"{}/MB".format(table[9]))])),
                               urwid.Padding(urwid.Text([('field', u"{}/MB".format(table[10]))])),
                               urwid.Padding(urwid.Text([('field', u"{}/MB".format(table[11]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[12]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[13]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[0]]))),
                               urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[8]]))),
                               urwid.Padding(Text(str(depart[num][table[9]]))),
                               urwid.Padding(Text(str(depart[num][table[10]]))),
                               urwid.Padding(Text(str(depart[num][table[11]]))),
                               urwid.Padding(Text(str(depart[num][table[12]]))),
                               urwid.Padding(Text(str(depart[num][table[13]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    elif part == 3:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[14]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[15]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[16]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[17]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[18]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[19]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[0]]))),
                               urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[14]]))),
                               urwid.Padding(Text(str(depart[num][table[15]]))),
                               urwid.Padding(Text(str(depart[num][table[16]]))),
                               urwid.Padding(Text(str(depart[num][table[17]]))),
                               urwid.Padding(Text(str(depart[num][table[18]]))),
                               urwid.Padding(Text(str(depart[num][table[19]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    elif part == 4:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[20]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[21]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[22]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[23]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[24]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[25]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[0]]))),
                               urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[20]]))),
                               urwid.Padding(Text(str(depart[num][table[21]]))),
                               urwid.Padding(Text(str(depart[num][table[22]]))),
                               urwid.Padding(Text(str(depart[num][table[23]]))),
                               urwid.Padding(Text(str(depart[num][table[24]]))),
                               urwid.Padding(Text(str(depart[num][table[25]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    elif part == 5:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[26]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[27]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[28]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[19]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[30]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[31]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[0]]))),
                               urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[26]]))),
                               urwid.Padding(Text(str(depart[num][table[27]]))),
                               urwid.Padding(Text(str(depart[num][table[28]]))),
                               urwid.Padding(Text(str(depart[num][table[29]]))),
                               urwid.Padding(Text(str(depart[num][table[30]]))),
                               urwid.Padding(Text(str(depart[num][table[31]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    else:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table[0]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[1]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[32]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[33]))])),
                               urwid.Padding(urwid.Text([('field', u"{}".format(table[34]))])),
                               ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num][table[1]]))),
                               urwid.Padding(Text(str(depart[num][table[2]]))),
                               urwid.Padding(Text(str(depart[num][table[32]]))),
                               urwid.Padding(Text(str(depart[num][table[33]]))),
                               urwid.Padding(Text(str(depart[num][table[34]]))),
                               ], 1), align='center')
            listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def table_index_body(depart, part):
    """
    project table_index page
    :param depart:the data that the page needs to display
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        blank,
        Divider(u'_'),
    ]
    if part == 1:
        listbox_body.append(
            urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table_index[0]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[1]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[2]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[3]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[4]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[5]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[6]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[7]))])),
                                         ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][table_index[0]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[1]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[2]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[3]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[4]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[5]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[6]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[7]]))),
                                                ], 1), align='center')
            listbox_body.append(data)
    else:
        listbox_body.append(
            urwid.Padding(urwid.Columns([urwid.Padding(urwid.Text([('field', u"{}".format(table_index[0]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[1]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[8]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[9]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[10]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[11]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[12]))])),
                                         urwid.Padding(urwid.Text([('field', u"{}".format(table_index[13]))])),
                                         ], 1), align='center'), )
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][table_index[0]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[1]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[8]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[9]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[10]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[11]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[12]]))),
                                                urwid.Padding(Text(str(depart[num][table_index[13]]))),
                                                ], 1), align='center')
            listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def lock_body(depart, heard_depart, part):
    """
    project lock page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [blank,
                    urwid.Padding(
                        urwid.Columns(
                            [(urwid.Padding(urwid.Text([('field', u" {}: ".format(lock[0])), heard_depart[lock[0]]]))),
                             ], 0), width=50, align='center', min_width=2),
                    Divider(u'_'),
                    ]
    if part == 1:
        listbox_body.append(urwid.Padding(urwid.Columns([urwid.Text([('field', u'{}'.format(lock[1]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[2]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[3]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[4]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[5]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[6]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[7]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[8]))]),
                                                         ], 1), width=300, align='center', min_width=20), )

        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][lock[1]]))),
                                                urwid.Padding(Text(str(depart[num].get(lock[2])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[3])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[4])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[5])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[6])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[7])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[8])))),
                                                ], 1), width=300, align='center', min_width=20)
            listbox_body.append(data)
    else:
        listbox_body.append(urwid.Padding(urwid.Columns([urwid.Text([('field', u'{}'.format(lock[1]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[9]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[10]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[11]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[12]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[13]))]),
                                                         urwid.Text([('field', u'{}'.format(lock[14]))]),
                                                         ], 1), width=300, align='center', min_width=20), )

        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][lock[1]]))),
                                                urwid.Padding(Text(str(depart[num].get(lock[9])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[10])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[11])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[12])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[13])))),
                                                urwid.Padding(Text(str(depart[num].get(lock[14])))),
                                                ], 1), width=300, align='center', min_width=20)
            listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def lock_detail_body(df_depart):
    """
    project lock_detail page
    :param df_depart:the data that the page needs to display
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        urwid.Padding(urwid.Columns(
            [urwid.Padding(urwid.Text([('field', u"     {}  >>>  {}".format(lock_detail[0], lock_detail[1]))]))], 1),
            width=300, align='left', min_width=20),
        Divider(u'_')
    ]
    if df_depart:
        for date in df_depart:
            data = urwid.Padding(
                urwid.Text([('     '), str(date.get(lock_detail[0])), ("  >>>  "), str(date.get(lock_detail[1]))])
                , width=60, align='left', min_width=2
            )
            listbox_body.append(data)

    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox1 = urwid.LineBox(listbox)
    return linebox1


def wait_event_body(depart, heard_depart):
    """
    project wait_event page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [blank,
                    urwid.Padding(
                        urwid.Columns([(urwid.Padding(
                            urwid.Text([('field', u" {}: ".format(wait_event[0])), heard_depart[wait_event[0]]])))],
                            0), width=50, align='center', min_width=2),
                    Divider(u'_'),
                    urwid.Padding(urwid.Columns([urwid.Text([('field', u'{}'.format(wait_event[1]))]),
                                                 urwid.Text([('field', u'{}'.format(wait_event[2]))]),
                                                 urwid.Text([('field', u'{}'.format(wait_event[3]))]),
                                                 urwid.Text([('field', u'{}'.format(wait_event[4]))]),
                                                 urwid.Text([('field', u'{}'.format(wait_event[5]))]),
                                                 urwid.Text([('field', u'{}/ms'.format(wait_event[6]))]),
                                                 urwid.Text([('field', u'{}/ms'.format(wait_event[7]))]),
                                                 urwid.Text([('field', u'{}/ms'.format(wait_event[8]))]),
                                                 urwid.Text([('field', u'{}/ms'.format(wait_event[9]))]),
                                                 urwid.Text([('field', u'{}'.format(wait_event[10]))]),
                                                 ], 1), width=300, align='center', min_width=20),
                    Divider(u'_')]
    for num in range(len(depart)):
        data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][wait_event[1]]))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[2])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[3])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[4])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[5])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[6])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[7])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[8])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[9])))),
                                            urwid.Padding(Text(str(depart[num].get(wait_event[10])))),
                                            ], 1), width=300, align='center', min_width=20)
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
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}     : ".format(memory[0])), progress_bar(heard_depart[memory[0]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}     : ".format(memory[1])), progress_bar(heard_depart[memory[1]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}     : ".format(memory[2])), progress_bar(heard_depart[memory[2]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}      : ".format(memory[3])), progress_bar(heard_depart[memory[3]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}      : ".format(memory[4])), progress_bar(heard_depart[memory[4]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{}    : ".format(memory[5])), progress_bar(heard_depart[memory[5]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        Padding(urwid.Columns(
            [(urwid.Padding(
                urwid.Text([('field', u"{} : ".format(memory[6])), progress_bar(heard_depart[memory[6]] * 100)]))),
            ], 0), width=150, align='center', min_width=2),
        blank,
        Divider(u'_'),
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[7])), (depart[memory[7]])])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[8])), (str(depart[memory[8]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[9])), (str(depart[memory[9]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[10])), (str(depart[memory[10]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[11])), (str(depart[memory[11]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[12])), (str(depart[memory[12]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[13])), (str(depart[memory[13]]))]))
                 ], 1), width=os.get_terminal_size().columns, align='center', min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[14])), (str(depart[memory[14]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[15])), (str(depart[memory[15]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[16])), (str(depart[memory[16]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[17])), (str(depart[memory[17]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[18])), (str(depart[memory[18]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[19])), (str(depart[memory[19]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[20])), (str(depart[memory[20]]))]))
                 ], 1), width=os.get_terminal_size().columns, align='center', min_width=100),
        blank,
        urwid.Padding(
            urwid.Columns(
                [urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[21])), (str(depart[memory[21]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[22])), (str(depart[memory[22]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[23])), (str(depart[memory[23]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[24])), (str(depart[memory[24]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[25])), (str(depart[memory[25]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[26])), (str(depart[memory[26]]))])),
                 urwid.Padding(urwid.Text([('field', u"{}: ".format(memory[27])), (str(depart[memory[27]]))]))
                 ], 1), width=os.get_terminal_size().columns, align='center', min_width=100),
    ]
    listbox1 = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox1 = urwid.LineBox(listbox1)

    return linebox1


def sharemem_body(depart, heard_depart):
    """
    project sharemem page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :return: return the page generated through urwid
    """

    blank = urwid.Divider()
    listbox_body = [blank,
                    urwid.Padding(urwid.Columns(
                        [(urwid.Padding(
                            urwid.Text([('field', u" {}: ".format(sharemem[0])), heard_depart[sharemem[0]]]))),
                        ], 0), width=50, align='center', min_width=2),
                    Divider(u'_'),
                    urwid.Padding(urwid.Columns([urwid.Text([('field', u'{}'.format(sharemem[1]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[2]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[3]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[4]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[5]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[6]))]),
                                                 urwid.Text([('field', u'{}'.format(sharemem[7]))]),
                                                 ], 1), width=300, align='center', min_width=20),
                    Divider(u'_')]
    for num in range(len(depart)):
        data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num].get(sharemem[1])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[2])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[3])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[4])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[5])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[6])))),
                                            urwid.Padding(Text(str(depart[num].get(sharemem[7])) + "%")),
                                            ], 1), width=300, align='center', min_width=20)
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def top_mem_used_session_body(depart, heard_depart):
    """
     project top_mem_used_session page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [blank,
                    urwid.Padding(
                        urwid.Columns(
                            [(urwid.Padding(
                                urwid.Text([('field', u" {}: ".format(top_mem_used_session[0])),
                                            heard_depart[top_mem_used_session[0]]]))),
                            ], 0), width=50, align='center', min_width=2),
                    Divider(u'_'),
                    urwid.Padding(urwid.Columns([urwid.Text([('field', u'{}'.format(top_mem_used_session[1]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[2]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[3]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[4]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[5]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[6]))]),
                                                 urwid.Text([('field', u'{}'.format(top_mem_used_session[7]))]),
                                                 ], 1), width=300, align='center', min_width=20),
                    Divider(u'_')]
    for num in range(len(depart)):
        data = urwid.Padding(urwid.Columns([urwid.Padding(Text(str(depart[num][top_mem_used_session[1]]))),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[2])))),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[3])))),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[4])))),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[5]))[:32])),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[6])))),
                                            urwid.Padding(Text(str(depart[num].get(top_mem_used_session[7])))),
                                            ], 1), width=300, align='center', min_width=20)
        listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def dynamicsql_body(depart, heard_depart, part):
    """
    project dynamicsql page
    :param depart:the data that the page needs to display
    :param heard_depart:the header display the data
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    listbox_body = [
        blank,
        urwid.Padding(
            urwid.Columns(
                [(urwid.Padding(urwid.Text([('field', u" {}: ".format(dynamicsql[0])), heard_depart[dynamicsql[0]]]))),
                 ], 0), width=50, align='center', min_width=2),
        blank,
        Divider(u'_'),
    ]
    if part == 1:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[4]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[12]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[3]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[2]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[5]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[6]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[1]))]),
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[4])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[12])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[3])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[2])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[5])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[6])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[1])[:30]))),
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    elif part == 2:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[7]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[8]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[9]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[10]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[11]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[13]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[14]))])
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[7])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[8])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[9])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[10])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[11])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[13])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[14]))))
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    elif part == 3:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[15]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[16]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[17]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[18]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[19]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[20]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[21]))])
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[15])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[16])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[17])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[18])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[19])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[20])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[21]))))
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    elif part == 4:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[22]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[23]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[24]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[25]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[26]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[27]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[28]))])
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[22])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[23])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[24])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[25])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[26])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[27])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[28])))),
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    elif part == 5:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[29]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[30]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[31]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[32]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[33]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[34]))]),
                               urwid.Text([('field', u'{}/s'.format(dynamicsql[35]))])
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[29])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[30])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[31])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[32])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[33])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[34])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[35]))))
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    else:
        listbox_body.append(
            urwid.Padding(
                urwid.Columns([urwid.Text([('field', u'{}'.format(dynamicsql[39]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[36]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[37]))]),
                               urwid.Text([('field', u'{}'.format(dynamicsql[38]))])
                               ], 1), width=os.get_terminal_size().columns, align='center'))
        listbox_body.append(Divider(u'_'))
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(dynamicsql[39])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[36])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[37])))),
                               urwid.Padding(Text(str(depart[num].get(dynamicsql[38]))))
                               ], 1), width=os.get_terminal_size().columns, align='center')
            listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox


def replication_body(depart, part):
    """
    project replication page
    :param depart:the data that the page needs to display
    :param part:page number
    :return: return the page generated through urwid
    """
    blank = urwid.Divider()
    if part == 1:
        listbox_body = [blank,
                        urwid.Padding(
                            urwid.Columns([urwid.Text([('field', u'{}'.format(replication[0]))]),
                                           urwid.Text([('field', u'{}'.format(replication[1]))]),
                                           urwid.Text([('field', u'{}'.format(replication[2]))]),
                                           urwid.Text([('field', u'{}'.format(replication[3]))]),
                                           urwid.Text([('field', u'{}'.format(replication[4]))]),
                                           urwid.Text([('field', u'{}'.format(replication[5]))]),
                                           urwid.Text([('field', u'{}'.format(replication[6]))]),
                                           urwid.Text([('field', u'{}'.format(replication[7]))]),
                                           urwid.Text([('field', u'{}'.format(replication[8]))]),
                                           ], 1), width=300, align='center', min_width=20),
                        Divider(u'_')]
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(replication[0])))),
                               urwid.Padding(Text(str(depart[num].get(replication[1])))),
                               urwid.Padding(Text(str(depart[num].get(replication[2])))),
                               urwid.Padding(Text(str(depart[num].get(replication[3])))),
                               urwid.Padding(Text(str(depart[num].get(replication[4])))),
                               urwid.Padding(Text(str(depart[num].get(replication[5])))),
                               urwid.Padding(Text(str(depart[num].get(replication[6])))),
                               urwid.Padding(Text(str(depart[num].get(replication[7])))),
                               urwid.Padding(Text(str(depart[num].get(replication[8])))),
                               ], 1), width=300, align='center', min_width=20)
            listbox_body.append(data)
    else:
        listbox_body = [blank,
                        urwid.Padding(
                            urwid.Columns([urwid.Text([('field', u'{}'.format(replication[0]))]),
                                           urwid.Text([('field', u'{}'.format(replication[9]))]),
                                           urwid.Text([('field', u'{}'.format(replication[10]))]),
                                           urwid.Text([('field', u'{}'.format(replication[11]))]),
                                           urwid.Text([('field', u'{}'.format(replication[12]))]),
                                           urwid.Text([('field', u'{}'.format(replication[13]))]),
                                           urwid.Text([('field', u'{}'.format(replication[14]))]),
                                           ], 1), width=300, align='center', min_width=20),
                        Divider(u'_')]
        for num in range(len(depart)):
            data = urwid.Padding(
                urwid.Columns([urwid.Padding(Text(str(depart[num].get(replication[0])))),
                               urwid.Padding(Text(str(depart[num].get(replication[9])))),
                               urwid.Padding(Text(str(depart[num].get(replication[10])))),
                               urwid.Padding(Text(str(depart[num].get(replication[11])))),
                               urwid.Padding(Text(str(depart[num].get(replication[12])))),
                               urwid.Padding(Text(str(depart[num].get(replication[13])))),
                               urwid.Padding(Text(str(depart[num].get(replication[14])))),
                               ], 1), width=300, align='center', min_width=20)
            listbox_body.append(data)
    listbox = urwid.ListBox(urwid.SimpleListWalker(listbox_body))
    linebox = urwid.LineBox(listbox)
    return linebox
