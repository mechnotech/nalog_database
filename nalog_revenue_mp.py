import multiprocessing
import numpy as np
import xmltodict
import os
import datetime as dt
from progress.bar import IncrementalBar
import psycopg2
from psycopg2 import sql


CPU_UNITS = 4  # multiprocessing.cpu_count()  # Определяем число ядер процесора в системе
directory = r'E:\Garrett\Downloads\baserev'  # Директоря где лежат разархивироанные файлы XML
files = os.listdir(directory)  # Получаем список файлов
#files = files[0:100]
my_dict = np.array_split(files, CPU_UNITS)  # Разделяем список на число ядер
conn = psycopg2.connect(dbname='main_org_db', user='base_user',
                                password='base_user', host='192.168.10.173')
cursor = conn.cursor()

# Функция для независимого потока, send_end - объект для возврата результата от каждого потока
def worker(procnum, send_end):

    #  Если это поток №1, запустим индикатор прогресса.
    if procnum == 0:
        bar = IncrementalBar('Обработка: используем ' + str(CPU_UNITS) + ' ядер',
                             max=len(my_dict[0]), suffix='%(percent).1f%% - %(eta)ds')

    # Функция записи в базу данных revenue
    def insert_db_revenue(nalog_values):
        conn = psycopg2.connect(dbname='main_org_db', user='base_user',
                                password='base_user', host='192.168.10.173')
        with conn.cursor() as cursor:
            conn.autocommit = True
            insert = sql.SQL(
                'INSERT INTO revenue (inn, year, income, expenses) VALUES {}').format(
                sql.SQL(',').join(map(sql.Literal, nalog_values))
            )
            cursor.execute(insert)

    # Проверка, есть ли ИНН в основной базе
    def is_in_msp(inn_org):
        cursor.execute(f'SELECT inn FROM main WHERE inn={inn_org}')
        if cursor.fetchone() is None:

            return False
        else:

            return True

    def insert_not_cmp(values):
        conn = psycopg2.connect(dbname='main_org_db', user='base_user',
                                password='base_user', host='192.168.10.173')
        with conn.cursor() as cursor:
            conn.autocommit = True
            insert = sql.SQL(
                'INSERT INTO not_cmp (inn, comment) VALUES {}').format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(insert)

    # Функция ИНН, доходы и расходы по годовой отчетности из gitданного набора из файла XML
    def ip_vs_org(edxml):
        #  0 - основная система, 1 - УСН + ЕНВД, 2 - УСН, 3 - Енвд, 4 - СРП, 5 - ЕСХН
        year = income = expenses = 0
        inn_org = ''  # OOO, ЗАО, АО
        comment = edxml['Файл']['@ИдФайл']
        nalog_values = []
        sum_income = sum_expenses = 0
        not_in_mcp = []

        if edxml['Файл']['@КолДок'] == '1':
            y = edxml['Файл']['Документ']['@ДатаСост']
            year = dt.datetime.strptime(y, "%d.%m.%Y").date()
            inn_org = edxml['Файл']['Документ']['СведНП']['@ИННЮЛ']
            income = float(edxml['Файл']['Документ']['СведДохРасх']['@СумДоход'])
            expenses = float(edxml['Файл']['Документ']['СведДохРасх']['@СумРасход'])
            sum_income += income
            sum_expenses += expenses
            vs = inn_org, year, income, expenses
            if is_in_msp(inn_org) is True:  # Если ИНН есть в основной таблице, добавляем в список
                nalog_values.append(vs)
            else:
                vsn = int(inn_org), '1 in source'
                not_in_mcp.append(vsn)

        else:
            for i in edxml['Файл']['Документ']:

                y = i['@ДатаСост']
                year = dt.datetime.strptime(y, "%d.%m.%Y").date()
                inn_org = i['СведНП']['@ИННЮЛ']
                income = float(i['СведДохРасх']['@СумДоход'])
                expenses = float(i['СведДохРасх']['@СумРасход'])
                #print(year, inn_org, income, expenses)
                # Накапливаем данные для базы данных
                sum_income += income
                sum_expenses += expenses
                vs = inn_org, year, income, expenses

                if is_in_msp(inn_org) is True:  # Если ИНН есть в основной таблице, добавляем в список
                    nalog_values.append(vs)
                else:
                    vsn = int(inn_org), None
                    not_in_mcp.append(vsn)



        # Вызываем функцию для записи блок в базу данных revenue

        try:
            insert_db_revenue(nalog_values)
        except Exception:
            print('\n', comment)
            pass

        # Вызвываем функцию записи в базу ИНН не входящих в МСП
        try:
            insert_not_cmp(not_in_mcp)
        except Exception:
            pass

        nalog = sum_income, sum_expenses

        # print(nalog, res)
        return nalog

    income = 0
    expenses = 0
    summa = 0


    # Переберем все файлы в подсписке
    for f in my_dict[procnum - 1]:
        fin = open(directory + '\\' + f, 'r', encoding='utf8')
        xml = fin.read()
        fin.close()
        parsedxml = xmltodict.parse(xml)
        #try:
        nalog = ip_vs_org(parsedxml)
        #except Exception:
        #    print(f)
        income += nalog[0]
        expenses += nalog[1]
        summa += len(parsedxml['Файл']['Документ'])
        #not_in_mcp += nalog[2]

        if procnum == 0:  # Если это поток №1 увеличиваем прогресс на один шаг
            bar.next()
    sep = income, expenses, summa

    if procnum == 0:  # Если это поток №1 закрываем индикатор прогресса в конце работы потока
        bar.finish()
    send_end.send(sep)


def main():
    # inp = input('Сколько задействовать CPU ядер (1-' + str(CPU_UNITS) + ')? Enter - испльзовать все :')
    # try:
    #     inp = int(inp)
    # except Exception:
    #     inp = 8
    # if 1 <= inp <= CPU_UNITS:
    #     CPU_UNITS = inp
    tm = dt.datetime.now()
    print('Процесс начат в', tm.strftime('%H:%M:%S'))
    jobs = []
    pipe_list = []

    for i in range(CPU_UNITS):
        recv_end, send_end = multiprocessing.Pipe(False)
        p = multiprocessing.Process(target=worker, args=(i, send_end))
        jobs.append(p)
        pipe_list.append(recv_end)
        p.start()

    for proc in jobs:
        proc.join()
    result_list = [x.recv() for x in pipe_list]
    income = expenses = summa = 0

    for i in result_list:
        income += i[0]
        expenses += i[1]
        summa += i[2]
        #not_in_mcp += i[3]

    period = dt.datetime.now() - tm
    hours = period.seconds // 3600
    minuts = (period.seconds // 60) % 60
    seconds = period.seconds - minuts * 60
    print("Процесс занял {} часов  {} минут   {} секунд".format(hours, minuts,seconds))
    print('Суммарные доходы / расходы по организациям: {:,} млн.р / {:,} млн.р'.format(income//1000000, expenses//1000000))
    print('Суммарный финансовый результат по МСП {:,} млн.р '.format((income - expenses)//1000000))
    print('Документов в файле:', summa)
    #print(not_in_mcp)
    #print(len(not_in_mcp))
    #  input('Press any key')


if __name__ == '__main__':
    main()
