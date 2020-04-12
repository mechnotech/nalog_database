#  n10 = ((2*n1 + 4*n2 + 10*n3 + 3*n4 + 5*n5 + 9*n6 + 4*n7 + 6*n8 + 8*n9) mod 11 ) mod 10  - 10 знак ИНН для юрлиц
from nalog_indexes import inn_diap_list  # Импортируем словарь с индексами налоговых и числом организаций в них
import psycopg2
from psycopg2 import sql
import re
from progress.bar import IncrementalBar

conn = psycopg2.connect(dbname='main_org_db', user='base_user',
                        password='base_user', host='192.168.10.173')
cursor = conn.cursor()


# Обработка одного ИНН - убираем лишнее возвращаем строку
def ret_str_inn(dec_inn):
    dec_inn = str(dec_inn)
    mystr = re.sub(r"[,()'Decimal]", "", dec_inn)
    if len(mystr) == 9:
        mystr = '0' + mystr
    return mystr

# Возвращает список всех ИНН в базе (не ИП)
def give_inn_list(start='0100', finish='0109'):
    start_d = int(start) * 1000000
    finish_d = int(finish) * 1000000

    with conn.cursor() as cursor:
        conn.autocommit = True
        give = sql.SQL(f'''select inn from main_res 
        WHERE is_ip is FALSE
        ORDER BY inn ASC''')
        cursor.execute(give)

        list_inn = []

        for i in cursor.fetchall():
            list_inn.append(ret_str_inn(i))

        return list_inn


def insert_db_main(values):
    # print('в main', values)

    with conn.cursor() as cursor:
        conn.autocommit = True
        insert = sql.SQL(
            'INSERT INTO main_res (inn, form_prop, is_ip, categoria, date_ins, comment) VALUES {}').format(
            sql.SQL(',').join(map(sql.Literal, values))
        )
        cursor.execute(insert)


def ret_full_inn(inn_as_sting):  # На вход подается ИНН без последнего знака для юрлиц и последних двух для физлиц
    if len(inn_as_sting) == 9 and inn_as_sting.isdigit():
        n = []
        for i in inn_as_sting:
            n.append(int(i))
        n.append(0)
        formula = 2 * n[0] + 4 * n[1] + 10 * n[2] + 3 * n[3] + 5 * n[4] + 9 * n[5] + 4 * n[6] + 6 * n[7] + 8 * n[8]
        n[9] = (formula % 11) % 10
        inn = []
        for i in n:
            inn.append(str(i))
        inn = ''.join(inn)
        return inn
    else:
        return None


BigINN = []
sum_all = 0
list_new = inn_diap_list
list_old = give_inn_list()
bar = IncrementalBar('Создание большого списка: ', max=len(inn_diap_list), suffix='%(percent).1f%% - %(eta)ds')
for i in range(len(inn_diap_list)):
    bar.next()
    for j in range(list_new[i][1] + 1):
        inn = ret_full_inn(list_new[i][0] + str(j).zfill(5))
        BigINN.append(inn)
bar.finish()

print(len(BigINN), len(list_old))
result = list(set(BigINN) ^ set(list_old))  # Список уникальных ИНН, которых нет в МСП справочнике
print(len(result))
nalog_values = []
bar = IncrementalBar('Запись большого списка в базу: ', max=len(result) // 50000, suffix='%(percent).1f%% - %(eta)ds')
for i in sorted(result):
    inn = int(i)
    form_prop = 'Unknown'
    is_ip = False
    categoria = 0
    date_ins = '01.01.1990'
    comment = 'Не в МСП'
    vs = inn, form_prop, is_ip, categoria, date_ins, comment
    nalog_values.append(vs)
    if len(nalog_values) == 50000:  # Всталяем блок по 5000 записей в базу кандидатов
        insert_db_main(nalog_values)
        nalog_values = []
        bar.next()
bar.finish()
insert_db_main(nalog_values)  # Вставляем хвост от цикла

# result=list(set(BigINN) - set(PresrntINN))
# print(ret_full_inn('780459960'))
# inn_fild = len(nalog_list)*99999
# print('возможных инн:', inn_fild)
# print('время на перебор (1 запрос в секунду) часов или суток:', inn_fild//3600, inn_fild//(3600*24))
