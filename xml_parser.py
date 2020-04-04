import xmltodict
import os
from progress.bar import IncrementalBar


#print(parsedxml['Файл']['Документ'])


def ip_vs_org(edxml):
    #  0 - основная система, 1 - УСН + ЕНВД, 2 - УСН, 3 - Енвд, 4 - СРП, 5 - ЕСХН
    osno = usn = envd = crp = esxn = usnenvd = 0
    inn_org = ''  # OOO, ЗАО, АО
    for i in edxml['Файл']['Документ']:
        if isinstance(i, dict):
            if 'СведСНР' in i:
                d = dict(i['СведСНР'])

                if d['@ПризнЕНВД'] == '0' and d['@ПризнУСН'] == '1':   # УСН
                    usn += 1
                elif d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '1':   # УСН + ЕНВД
                    usnenvd += 1
                elif d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '0':  # EНВД
                    envd += 1
                elif d['@ПризнЕСХН'] == '1':   # ЕСХН
                    esxn += 1
                elif d['@ПризнСРП'] == '1':  #  СРП
                    crp += 1
                else:
                    osno +=1
        else:
            d = edxml['Файл']['Документ']['СведСНР']
            if d['@ПризнЕНВД'] == '0' and d['@ПризнУСН'] == '1':  # УСН
                usn += 1
            elif d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '1':  # УСН + ЕНВД
                usnenvd += 1
            elif d['@ПризнЕНВД'] == '1' and d['@ПризнУСН'] == '0':  # EНВД
                envd += 1
            elif d['@ПризнЕСХН'] == '1':  # ЕСХН
                esxn += 1
            elif d['@ПризнСРП'] == '1':  # СРП
                crp += 1
            else:
                osno += 1
            nalog = osno, usn, envd, crp, esxn, usnenvd
            return nalog, inn_org

    nalog = osno, usn, envd, crp, esxn, usnenvd
    res = sum(nalog)
    #print(nalog, res)
    return nalog, inn_org

directory = r'E:\Garrett\Downloads\basesr'
files = os.listdir(directory)
osno = usn = envd = crp = esxn = usnenvd = 0
vsego = 0
summa = 0
bar = IncrementalBar('Обработка файлов', max = len(files))

#for f in range(1):  # files[1:100]:
    #fin = open(directory + '\\' + f, 'r', encoding='utf8')
fin = open('E:/Garrett/Downloads/basesr/VO_OTKRDAN1_9965_9965_20190929_e62cc8b8-d925-4fe0-8ef1-ff51f8ac23a7.xml', 'r', encoding='utf8')
#fin = open('E:/Garrett/Downloads/basesr/VO_OTKRDAN1_9965_9965_20190929_b8b82119-39c3-4427-898c-f5e8483c8bcd.xml', 'r', encoding='utf8')
xml = fin.read()
fin.close()
parsedxml = xmltodict.parse(xml)
    #print(parsedxml)
nalog, inn_org = ip_vs_org(parsedxml)
osno += nalog[0]
usn += nalog[1]
envd += nalog[2]
crp += nalog[3]
esxn += nalog[4]
usnenvd += nalog[5]
vsego += sum(nalog)
    #org += inn_org
summa += len(parsedxml['Файл']['Документ'])
bar.next()

bar.finish()
print('ОСНО:', osno, 'УСН:', usn, 'ЕНВД:', envd, 'СРП:', crp, 'ЕСХН:',esxn, 'УСН+ЕНВД', usnenvd)
print('Документов в файле:', summa, 'обработано:', vsego)

#  print(parsedxml['osm']['node'][100]['@id'])

# n = 0
# w = 0
# for node in parsedxml['osm']['node']:
#     if 'tag' in node:
#         tags = node['tag']
#         if isinstance(tags, list):
#             for tag in tags:
#                 if tag['@k'] == 'amenity' and tag['@v'] == 'fuel':
#                     n += 1
#         elif isinstance(tags, dict):
#             if (tags['@v']) == 'fuel':
#                 n += 1
#
# for way in parsedxml['osm']['way']:
#     if 'tag' in way:
#         ways = way['tag']
#         if isinstance(ways, list):
#             for i in ways:
#                 if i['@k'] == 'amenity' and i['@v'] == 'fuel':
#                     w +=1
#         elif isinstance(ways, dict):
#             if (ways['@v']) == 'fuel':
#                 w += 1
#
#
# print(n + w)
