import multiprocessing
import numpy as np
import xmltodict
import os
import datetime
from progress.bar import IncrementalBar
import psycopg2
from psycopg2 import sql



CPU_UNITS = 5 # multiprocessing.cpu_count()  # Определяем число ядер процесора в системе
directory = r'E:\Garrett\Downloads\baseul'  # Директория где лежат разархивироанные файлы XML
files = os.listdir(directory)
#files = files[0:10]  # Получаем срез из списка файлов
my_dict = np.array_split(files, CPU_UNITS)  # Разделяем список на число ядер

conn = psycopg2.connect(dbname='main_org_db', user='base_user',
                        password='base_user', host='192.168.10.173')
cursor = conn.cursor()

# Функция для независимого потока, send_end - объект для возврата результата от каждого потока
def worker(procnum, send_end):

    def insert_db_main(values):
        #print('в main', values)
        with conn.cursor() as cursor:
            conn.autocommit = True
            insert = sql.SQL(
                'INSERT INTO main (inn, form_prop, org_name, short_name, is_ip, fm, nm, ot, categoria, date_ins, okved_osn, okved_dop, sscr, comment) VALUES {}').format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(insert)


    def insert_db_region(reg_values):
        with conn.cursor() as cursor:
            conn.autocommit = True
            insert = sql.SQL(
                'INSERT INTO region (inn, region, reg_type, reg_name, ra_type, ra_name, nas_type, nas_name) VALUES {}').format(
                sql.SQL(',').join(map(sql.Literal, reg_values))
            )
            cursor.execute(insert)


    #  Если это поток №0, запустим индикатор прогресса.
    if procnum == 0:
        bar = IncrementalBar('Обработка: используем ' + str(CPU_UNITS) + ' ядер',
                             max=len(my_dict[0]), suffix='%(percent).1f%% - %(eta)ds')

    # Функция вычисляет число ИП и Организаций для данного набора из файла XML
    def ip_vs_org(edxml):
        inn_ip = inn_org = miss = 0
        values = []
        reg_values = []
        for i in edxml['Файл']['Документ']:

            # Дата включения в список МСП
            date_ins = i['@ДатаВклМСП']

            # Дополнительные признаки МСП
            # is_ip  Истина если является ИП
            is_ip = i['@ВидСубМСП']
            if is_ip == '2':
                is_ip = True
            else:
                is_ip = False
            # Категория субьекта МСП 1 - микро 2 - малое 3 - среднее
            categoria = i['@КатСубМСП']

            # Среднесписочная численность
            try:
                sscr = i['@ССЧР']
            except Exception:
                sscr = None

                # Данные об ОВЭД - основном и дополнительных
            try:
                okved_osn = i['СвОКВЭД']['СвОКВЭДОсн']['@КодОКВЭД']
                odop = list(i['СвОКВЭД'])
            except Exception:
                okved_osn = None
                # print('Problem  - ', i['СвОКВЭД'], inn)

            okved_dop = []
            try:
                odop = list(i['СвОКВЭД']['СвОКВЭДДоп'])
                if isinstance(odop, list):
                    for y in odop:
                        okved_dop.append(y['@КодОКВЭД'])
            except Exception:
                okved_dop = None

            # Данные о регионе
            d = dict(i['СведМН'])
            region = d['@КодРегион']
            try:
                rg_dict = d['Регион']
                reg_type = rg_dict['@Тип']
                reg_name = rg_dict['@Наим']
            except Exception:
                reg_type = reg_name = ''
            try:
                rg_dict = d['Район']
                ra_type = rg_dict['@Тип']
                ra_name = rg_dict['@Наим']
            except Exception:
                ra_type = ra_name = ''
            try:
                rg_dict = d['НаселПункт']
                nas_type = rg_dict['@Тип']
                nas_name = rg_dict['@Наим']
            except Exception:
                try:
                    rg_dict = d['Город']
                    nas_type = rg_dict['@Тип']
                    nas_name = rg_dict['@Наим']
                except Exception:
                    nas_type = nas_name = ''

            # ___________________ Данные о юрлицах __________________
            if 'ОргВклМСП' in i:
                d = dict(i['ОргВклМСП'])
                inn = d['@ИННЮЛ']
                name = d['@НаимОрг']
                try:
                    short_name = d['@НаимОргСокр']
                except Exception:
                    short_name = ''
                n_ch = name.upper()
                comment = edxml['Файл']['@ИдФайл']

                # Блок определения формы собственности

                if 'ОБЩЕСТВА' in n_ch and 'ОГРАНИЧЕННОЙ'in n_ch and 'ОТВЕТСТВЕННОСТЬЮ' in n_ch:
                    forma = "ООО"
                elif 'ОБЩЕСТВО' in n_ch and 'ОГРАНИЧЕННОЙ'in n_ch and 'ОТВЕТСТВЕННОСТЬЮ' in n_ch:
                    forma = "ООО"
                elif 'ФЕРМЕРСКОЕ' in n_ch and 'ХОЗЯЙСТВО' in n_ch:
                    forma = "КФХ"
                elif 'КРЕСТЬЯНСКОЕ' in n_ch and 'ХОЗЯЙСТВО' in n_ch:
                    forma = "КФХ"
                elif 'ПОТРЕБИТЕЛЬСКОЕ' in n_ch and 'ОБЩЕСТВО' in n_ch:
                    forma = "ПО"
                elif 'ДОПОЛНИТЕЛЬНОЙ ОТВЕТСТВЕННОСТЬЮ' in n_ch and 'ОБЩЕСТВО' in n_ch:
                    forma = "ОДО"
                elif 'ТОВАРИЩЕСТВО' in n_ch and 'ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ' in n_ch:
                    forma = "ТОО"
                elif 'НЕКОММЕРЧЕСКОЕ' in n_ch and 'ОБЩЕСТВО' in n_ch:
                    forma = "НКО"
                elif 'АКЦИОНЕРНОЕ' in n_ch and 'ОБЩЕСТВО' in n_ch:
                    forma = "АО"
                elif 'ГОСУДАРСТВЕННОЕ' in n_ch and 'ПРЕДПРИЯТИЕ' in n_ch:
                    forma = "ГП"
                elif 'ЗАКРЫТОЕ' in n_ch and 'АКЦИОНЕРНОЕ' in n_ch:
                    forma = "ЗАО"
                elif 'ПУБЛИЧНОЕ' in n_ch and 'АКЦИОНЕРНОЕ' in n_ch:
                    forma = "ПАО"
                elif 'КОЛХОЗ' in n_ch:
                    forma = "КОЛХОЗ"
                elif 'АРТЕЛЬ' in n_ch:
                    forma = "АРТЕЛЬ"
                elif 'ТОВАРИЩЕСТВО' in n_ch:
                    forma = "ТОВАРИЩЕСТВО"
                elif 'КООПЕРАТИВ' in n_ch:
                    forma = "КООПЕРАТИВ"
                elif 'КФХ' in n_ch:
                    forma = 'КФХ'
                elif 'ОБЩЕСТВО' in n_ch:
                    forma = "ОБЩЕСТВО"
                else:
                    forma = 'Unknown'
                    miss += 1  # Число аномалий - ошибок в названии организаций
                fm = nm = ot = None  # У юрлиц нет ФИО
                # Данные в таблицу main для юрлиц
                #  inn, form_prop, org_name, short_name, is_ip, fm, nm, ot, categoria, date_ins, okved_osn, okved_dop, sscr, comment
                sv = inn, forma, name, short_name, is_ip, fm, nm, ot, categoria, date_ins, okved_osn, okved_dop, sscr, comment
                values.append(sv)
                # Данные в таблицу region
                svr = inn, region, reg_type, reg_name, ra_type, ra_name, nas_type, nas_name
                reg_values.append(svr)
                inn_org += 1

            # ___________________  Данные о ИП ______________________
            if 'ИПВклМСП' in i:
                d = dict(i['ИПВклМСП'])
                inn = d['@ИННФЛ']
                forma = 'ИП'
                n = dict(d['ФИОИП'])
                comment = edxml['Файл']['@ИдФайл']
                short_name = None
                name = None
                try:
                    fm = n['@Фамилия']
                    fm = fm.upper()
                except Exception:
                    miss += 1
                    fm = None
                try:
                    nm = n['@Имя']
                    nm = nm.upper()
                except Exception:
                    miss += 1
                    nm = None
                try:
                    ot = n['@Отчество']
                    ot = ot.upper()
                except Exception:
                    miss += 1
                    ot = None

                #print('\n', comment, n)
                if isinstance(name, str):
                    name = name.upper()

                # Данные в таблицу main
                sv = inn, forma, name, short_name, is_ip, fm, nm, ot, categoria, date_ins, okved_osn, okved_dop, sscr, comment
                values.append(sv)

                # Данные в таблицу region
                svr = inn, region, reg_type, reg_name, ra_type, ra_name, nas_type, nas_name
                reg_values.append(svr)
                inn_ip += 1




        # Вызываем функции записи в БД результатов обработки одного файла xml
        insert_db_main(values)
        insert_db_region(reg_values)

        return inn_ip, inn_org, miss

    ip = org = summa = miss = 0

    # Переберем все файлы в подсписке и вычислим суммарное число ИП и Организаций
    for f in my_dict[procnum - 1]:
        fin = open(directory + '\\' + f, 'r', encoding='utf8')
        xml = fin.read()
        fin.close()
        parsedxml = xmltodict.parse(xml)

        inn_ip, inn_org, miss_org = ip_vs_org(parsedxml)

        ip += inn_ip
        org += inn_org
        miss += miss_org
        summa += len(parsedxml['Файл']['Документ'])
        if procnum == 0:  # Если это поток №0 увеличиваем прогресс на один шаг
            bar.next()
    sep = ip, org, summa, miss

    if procnum == 0:  # Если это поток №0 закрываем индикатор прогресса в конце работы потока
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
    print('Процесс начат в', tm.strftime('%H.%M.%S'))
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
    ip = org = summa = miss = 0
    for i in result_list:
        ip += i[0]
        org += i[1]
        summa += i[2]
        miss += i[3]
    period = datetime.datetime.now() - tm
    hours = period.seconds // 3600
    minuts = (period.seconds // 60) % 60
    seconds = period.seconds - minuts * 60
    print("Процесс занял {} часов  {} минут   {} секунд".format(hours, minuts, seconds))
    print('Количество ИП РФ:', ip, 'Организаций:', org)
    print('Всего записей:', summa, 'из них обработано:', org + ip)
    print('Аномалий', miss)
    #input('Press any key')


if __name__ == '__main__':
    main()
