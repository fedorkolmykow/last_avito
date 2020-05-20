import pytest
import catch_me_if_u_can
import pandas
from datetime import datetime


PATH = '/home/fedor/PycharmProjects/avito_last/test_data'

exp = """            site01              time01               site02  \\
0           vk.com 2013-11-15 09:28:17           oracle.com   
1       oracle.com 2013-11-15 09:52:48                 None   
2  geo.mozilla.org 2013-11-15 11:37:26           oracle.com   
3       google.com 2013-11-15 11:40:34  accounts.google.com   
4  mail.google.com 2013-11-15 11:40:37      apis.google.com   
5  plus.google.com 2013-11-15 11:41:35                 None   

               time02           site03              time03  \\
0 2013-11-15 09:33:04       oracle.com 2013-11-15 09:52:48   
1                 NaT             None                 NaT   
2 2013-11-15 11:40:32       google.com 2013-11-15 11:40:34   
3 2013-11-15 11:40:35  mail.google.com 2013-11-15 11:40:37   
4 2013-11-15 11:40:40  plus.google.com 2013-11-15 11:41:35   
5                 NaT             None                 NaT   

                site04              time04  user_id  
0                 None                 NaT        1  
1                 None                 NaT        1  
2  accounts.google.com 2013-11-15 11:40:35        1  
3      apis.google.com 2013-11-15 11:40:40        1  
4                 None                 NaT        1  
5                 None                 NaT        1  """


def test_prepare_train_set():
    res = catch_me_if_u_can.prepare_train_set(PATH, 4, 2, 30)
    with pandas.option_context('display.max_rows', None, 'display.max_columns', 9):
        assert str(res) == exp


session_queue_input = [
        {
            'site': 'oracle.com',
            'timestamp': datetime(2013, 11, 15, 11, 40, 37)
        },
        {
            'site': 'vk.com',
            'timestamp': datetime(2013, 11, 15, 11, 40, 37)
        },
        {
            'site': 'mail.google.com',
            'timestamp': datetime(2013, 11, 15, 11, 40, 37)
        },
        {
            'site': 'plus.google.com',
            'timestamp': datetime(2013, 11, 15, 12, 40, 37)
        },
    ]


def test_make_session():
    default = catch_me_if_u_can.make_dict(10)
    session_queue = session_queue_input.copy()
    window_queue = []
    ses = catch_me_if_u_can.make_session(default, session_queue, window_queue, 30, 1, 2)
    assert ses['site01'] == session_queue_input[0]['site']
    assert ses['site02'] == session_queue_input[1]['site']
    assert ses['site03'] == session_queue_input[2]['site']
    assert window_queue[0]['site'] == session_queue_input[2]['site']
    assert session_queue[0]['site'] == session_queue_input[3]['site']


def test_fill_session():
    default = catch_me_if_u_can.make_dict(10)
    to_pop = catch_me_if_u_can.fill_session(session_queue_input, 30, default, 0)
    assert to_pop == 3
    assert default['site01'] == session_queue_input[0]['site']
    assert default['site02'] == session_queue_input[1]['site']
    assert default['site03'] == session_queue_input[2]['site']
    assert len(session_queue_input) == 4


def test_make_session_with_empty_session_queue():
    default = catch_me_if_u_can.make_dict(10)
    session_queue = []
    window_queue = session_queue_input.copy()
    ses = catch_me_if_u_can.make_session(default, session_queue, window_queue, 30, 1, 2)
    assert ses['site01'] == session_queue_input[0]['site']
    assert ses['site02'] == session_queue_input[1]['site']
    assert ses['site03'] == session_queue_input[2]['site']
    assert window_queue[0]['site'] == session_queue_input[3]['site']
    assert len(session_queue) == 0