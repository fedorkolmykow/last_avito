catch_me_if_u_can.py - выполненное домашнее задание
test_catch.py - тесты на pytest

Домашнее задание от Авито.

1. Создание вирутального окружения
```
python -m venv venv
```
2. Установка пакетов из файла requirements.txt
```
pip install -r requirements.txt
```
3. Зайти в виртуальное окружение:
```
source venv/bin/activate
```
4. Запустить тесты
```
python3 -m pytest
```
5. Запустить cProfile
```
python -m cProfile -o program.prof catch_me_if_u_can.py
```
6. Запустить snakeviz
```
snakeviz program.prof
```
