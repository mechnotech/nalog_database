#  n10 = ((2*n1 + 4*n2 + 10*n3 + 3*n4 + 5*n5 + 9*n6 + 4*n7 + 6*n8 + 8*n9) mod 11 ) mod 10  - 10 знак ИНН для юрлиц
from nalog_indexes import inn_diap_dict  # Импортируем словарь с индексами налоговых и числом организаций в них
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

def give_inn_list(start='0101', finish='0102'):
    counter = 0

    start_d = int(start) * 1000000
    finish_d = int(finish) * 1000000
    bar = IncrementalBar('Обработка списка', max=(finish_d-start_d)//1000)
    with conn.cursor() as cursor:
        conn.autocommit = True
        give = sql.SQL(f'''select inn from main 
        WHERE is_ip is FALSE and inn > {start_d} and inn < {finish_d} 
        ORDER BY inn ASC''')
        cursor.execute(give)

        list_inn = []

        #print(ret_str_inn(list[0]), ret_str_inn(list[end-1]))
        for i in cursor.fetchall():
            # list.append(ret_str_inn(i))  # все ИНН в диапазоне
            list_inn.append(ret_str_inn(i))

            # counter +=1
        #     if counter % 1000 == 1:
        #         bar.next()
        # bar.finish()
        # print(f'Для налоговой {start} всего записей {len(list_inn)}')
        # print(f'От {list_inn[0]} до {list_inn[len(list_inn)-1]}')
        return start, len(list_inn), list_inn[len(list_inn)-1]

sum_all = 0
for i in inn_diap_dict.values():
    sum_all += i
print(sum_all)





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


# print(ret_full_inn('780459960'))
# inn_fild = len(nalog_list)*99999
# print('возможных инн:', inn_fild)
# print('время на перебор (1 запрос в секунду) часов или суток:', inn_fild//3600, inn_fild//(3600*24))
