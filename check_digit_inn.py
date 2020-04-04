#  n10 = ((2*n1 + 4*n2 + 10*n3 + 3*n4 + 5*n5 + 9*n6 + 4*n7 + 6*n8 + 8*n9) mod 11 ) mod 10  - 10 знак ИНН для юрлиц


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


print(ret_full_inn('780459960'))
