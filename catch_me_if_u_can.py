import os
import sys
import pandas
import csv
from datetime import datetime
from collections import OrderedDict


def make_dict(session_len):
    """
    Генерирует шаблон словаря для сессии
    :param session_len: количество сайтов в сессии
    :return: возвращает шаблон словаря, который отражает сессию
    """
    d = OrderedDict()
    for i in range(session_len):
        d.update({
            'site{:02}'.format(i+1): None,
            'time{:02}'.format(i+1): None,
        })
    d.update({'user_id': None})
    return d


def fill_session(queue, max_duration, session, site_number):
    """
    Заполняет сессию сайтами
    :param queue: список из котрого сайты переносятся в сессию
    :param max_duration: максимальная разница в минутах между посещениями сайтов в одной сессии
    :param session: сессия
    :param site_number: номер сайта в сессии
    :return: количество сайтов, которое было внесенов сессию
    """
    to_pop = 0
    for i in range(len(queue)):
        if site_number:
            delta = queue[i]['timestamp'] - session['time{:02}'.format(site_number)]
            if delta.seconds / 60 > max_duration:
                break
        session['site{:02}'.format(site_number+1)] = queue[i]['site']
        session['time{:02}'.format(site_number+1)] = queue[i]['timestamp']
        to_pop += 1
        site_number += 1
        if site_number == len(session)//2:
            break
    return to_pop


def make_session(default, session_queue, window_queue, max_duration, user_id, tail):
    """
    Возвращает готовую сессию
    :param default: словарь, в копии которого будет хранится сессия
    :param session_queue: список сайтов и времени их посещения, которые могут быть занесены в данную сессию
    :param max_duration: максимальная разница в минутах между посещениями сайтов в одной сессии
    :param window_queue: список сайтов, которые были в прошлой сессии и должны быть в текущей
    :param user_id: уникальый идентификатор пользователя
    :param tail: разница между длинной сессии и размером окна session_length - window_size
    :return: возвращает словарь сессии
    """
    session = default.copy()
    to_pop = 0
    if window_queue:
        to_pop = fill_session(window_queue, max_duration, session, to_pop)
        if to_pop != len(window_queue):
            for _ in range(to_pop):
                window_queue.pop(0)
            return session
        window_queue.clear()
    w_to_pop = to_pop
    to_pop = fill_session(session_queue, max_duration, session, to_pop)
    session['user_id'] = user_id
    for i in range(to_pop):
        if i + w_to_pop < tail:
            session_queue.pop(0)
        else:
            window_queue.append(session_queue.pop(0))
    return session


def prepare_train_set(logs_path: str, session_length: int, window_size: int, max_duration: int):
    default = make_dict(session_length)
    tail = session_length - window_size
    df_data = []
    for paths, _, files in os.walk(logs_path):
        for f in files:
            with open(os.path.join(paths, f), "r") as f_obj:
                user_id = int(f[4:8])
                session_queue = []
                window_queue = []
                reader = csv.DictReader(f_obj, delimiter=',')
                for r in reader:
                    r['timestamp'] = datetime.fromisoformat(r['timestamp'])
                    session_queue.append(r)
                    while len(session_queue) == session_length:
                        session = make_session(default, session_queue, window_queue, max_duration, user_id, tail)
                        df_data.append(session)
                while session_queue or window_queue:
                    session = make_session(default, session_queue, window_queue, max_duration, user_id, tail)
                    df_data.append(session)
    return pandas.DataFrame(df_data)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        PATH = sys.argv[1]
    else:
        PATH = './test_data'
    c = prepare_train_set(PATH, 4, 2, 30)

