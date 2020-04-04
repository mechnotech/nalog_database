import multiprocessing
import numpy as np
import xmltodict
import os
import datetime
from progress.bar import IncrementalBar
import psycopg2
from psycopg2 import sql


CPU_UNITS = 4  # multiprocessing.cpu_count()  # Определяем число ядер процесора в системе
directory = r'E:\Garrett\Downloads\basesr'  # Директоря где лежат разархивироанные файлы XML
files = os.listdir(directory)  # Получаем список файлов
#files = files[0:1]
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

    # Функция записи в базу данных nalog_sys
    def insert_db_nalog_sys(nalog_values):
        with conn.cursor() as cursor:
            conn.autocommit = True
            insert = sql.SQL(
                'INSERT INTO nalog_sys (inn, usn, envd, crp, esxn) VALUES {}').format(
                sql.SQL(',').join(map(sql.Literal, nalog_values))
            )
            cursor.execute(insert)

    # Функция вычисляет число ИП и Организаций для данного набора из файла XML
    def ip_vs_org(edxml):
        #  0 - основная система, 1 - УСН + ЕНВД, 2 - УСН, 3 - Енвд, 4 - СРП, 5 - ЕСХН
        osno = usn = envd = crp = esxn = usnenvd = err = 0
        inn_org = ''  # OOO, ЗАО, АО
        comment = edxml['Файл']['@ИдФайл']
        nalog_values = []
        for i in edxml['Файл']['Документ']:
            usn_d = False
            envd_d = False
            crp_d = False
            esxn_d = False
            if 'СведСНР' in i:
                if isinstance(i, dict):
                    inn = i['СведНП']['@ИННЮЛ']

                    d = dict(i['СведСНР'])
                    if d['@ПризнЕНВД'] == '0' and d['@ПризнУСН'] == '1':  # УСН
                        usn += 1
                        usn_d = True
                    if d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '1':  # УСН + ЕНВД
                        usnenvd += 1
                        usn_d = True
                        envd_d = True
                    if d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '0':  # EНВД
                        envd += 1
                        envd_d = True
                    if d['@ПризнЕСХН'] == '1':  # ЕСХН
                        esxn += 1
                        esxn_d = True
                    if d['@ПризнСРП'] == '1':  # СРП
                        crp += 1
                        crp_d = True
                else:   # Если файл сосотоял всего из одного документа, нужна особая обработка
                    try:
                        d = edxml['Файл']['Документ']['СведСНР']
                        inn = edxml['Файл']['Документ']['СведНП']['@ИННЮЛ']
                        if d['@ПризнЕНВД'] == '0' and d['@ПризнУСН'] == '1':  # УСН
                            usn += 1
                            usn_d = True
                        if d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '1':  # УСН + ЕНВД
                            usnenvd += 1
                            usn_d = True
                            envd_d = True
                        if d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '0':  # EНВД
                            envd += 1
                            envd_d = True
                        if d['@ПризнЕСХН'] == '1':  # ЕСХН
                            esxn += 1
                            esxn_d = True
                        if d['@ПризнСРП'] == '1':  # СРП
                            crp += 1
                            crp_d = True

                    except Exception:
                        print(edxml, comment)
                        pass
                # Накапливаем данные для базы данных

                vs = inn, usn_d, envd_d, crp_d, esxn_d
                nalog_values.append(vs)

        # Вызываем функцию для записи блок в базу данных nalog_sys
        try:
            insert_db_nalog_sys(nalog_values)
        except Exception:
            pass

        nalog = osno, usn, envd, crp, esxn, usnenvd

        # print(nalog, res)
        return nalog, inn_org

    osno = usn = envd = crp = esxn = usnenvd = 0
    vsego = 0
    summa = 0

    # Переберем все файлы в подсписке и вычислим суммарное число ИП и Организаций
    for f in my_dict[procnum - 1]:
        fin = open(directory + '\\' + f, 'r', encoding='utf8')
        xml = fin.read()
        fin.close()
        parsedxml = xmltodict.parse(xml)
        #try:
        nalog, inn_org = ip_vs_org(parsedxml)
        #except Exception:
        #    print(f)
        osno += nalog[0]
        usn += nalog[1]
        envd += nalog[2]
        crp += nalog[3]
        esxn += nalog[4]
        usnenvd += nalog[5]
        vsego += sum(nalog)
        # org += inn_org
        summa += len(parsedxml['Файл']['Документ'])

        if procnum == 0:  # Если это поток №1 увеличиваем прогресс на один шаг
            bar.next()
    sep = osno, usn, envd, crp, esxn, usnenvd, vsego, summa

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
    tm = datetime.datetime.now()
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
    osno = usn = envd = crp = esxn = usnenvd = vsego = summa = 0
    for i in result_list:
        osno += i[0]
        usn += i[1]
        envd += i[2]
        crp += i[3]
        esxn += i[4]
        usnenvd += i[5]
        vsego += i[6]
        summa += i[7]
    period = datetime.datetime.now() - tm
    hours = period.seconds // 3600
    minuts = (period.seconds // 60) % 60
    seconds = period.seconds - minuts * 60
    print("Процесс занял {} часов  {} минут   {} секунд".format(hours, minuts,seconds))
    print('УСН:', usn, 'ЕНВД:', envd, 'СРП:', crp, 'ЕСХН:', esxn, 'УСН+ЕНВД', usnenvd)
    print('Документов в файле:', summa)
    #  input('Press any key')


if __name__ == '__main__':
    main()
