#importador automático de dados do IGPM para bancos de dados

##Desenvolvido por Philipe Couto
##philipephwd@gmail.com



#Abaixo as librarys necessárias:

#execute no terminal > pip3 install pandas
#execute no terminal > pip3 install pyodbc
#execute no terminal > pip3 install glob
#execute no terminal > pip3 install configparser

#Bottle poderá ser utilizado em futuras versões
#from bottle import route, run, static_file, post, get, request
#

from os import lseek
from sqlite3 import SQLITE_CREATE_TABLE
import pandas as pd
import pyodbc
import glob
import configparser
import schedule
import time

from os.path import join

from urllib.request import Request, urlopen


def buscaIgpm():
    url  = 'https://ihack.com.br/dados/igpm.html'

    req = Request(url,  headers={'User-Agent': 'Mozilla/5.0'})
    pagina = urlopen(req).read()
    igpm =  pd.read_html(pagina)


    igpm = igpm[0]
    igpm = igpm[[0,1,2,3]]

    igpm = igpm.iloc[1:]
    
    l = igpm[0]
    #transforma os meses de acordo com o dicionário
    igpm[4] = [i.split('/', 1)[0] for i in l]
    
    dict = {"Jan":"01", "Fev":"02", "Mar":"03", "Abri":"04", "Abr":"04", "Mai":"05", "Jun":"06", "Jul":"07", "Ago":"08", "Set":"09", "Out":"10", "Nov":"11", "Dez":"12"}

    #extrai o ano do objeto 0 do dataframe
    igpm[5] = [i.split('/', 1)[1] for i in l]
    igpm[5] = igpm[5].astype('int')
    #igpm["Data"] = pd.to_datetime(igpm[0], format='%Y%B')
    igpm[1] = igpm[1].astype('float') / 100
    igpm[2] = igpm[2].astype('float') / 10000
    igpm[3] = igpm[3].astype('float') / 10000

    #o método MAP faz a verificação se os valores da tupla em questão existem em um dict
    igpm[6] = igpm[4].map(dict)
    igpm[6] = igpm[6].astype('int')

    #retornando o dataframe igpm
    return igpm


def stringconnSQL(**kw):
    conn = pyodbc.connect(server=kw.get('host'),
                          database=kw.get('database'),
                          user=kw.get('user'),
                          tds_version='7.4',
                          password=kw.get('pass'),
                          port=kw.get('port'),
                          driver= kw.get('driver')) #'/usr/local/lib/libtdsodbc.so')
    cursor = conn.cursor()
    return cursor


def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

def sqlConn():
    #Verifica se existe arquivo de configuração
    if(glob.glob(join('./', "_config.ini"))):
        configFile = glob.glob(join('./', "_config.ini"))
        config = configparser.ConfigParser()
        config.read(configFile)
        configDict = dict(config['DEFAULT'])
        dbcon = stringconnSQL(**configDict)
    else:
        path = input(f'Insira o caminho do arquivo de configuração. \n')
        #leraquivo com dados de acesso ao banco
        configFile = glob.glob(join(path, "*.ini"))
        config = configparser.ConfigParser()
        config.read(configFile)
        configDict = dict(config['DEFAULT'])
        f = open("./_config.ini", "w") #cria o arquivo no mesmo diretório
        f.writelines(config)
        f.close()

        #print(dict(config['DEFAULT']))
        dbcon = stringconnSQL(**configDict)
    
    sequence = 'select NEXT VALUE FOR 4tix_seq_01;'

    if(checkTableExists(dbcon, '4tix_table_igpm') == True):
        ## a tabela existe no banco, então é necessário realizar o insert apenas dos dados mais recentes
        content = buscaIgpm().head(1)
        dbcon_insert_last = stringconnSQL(**configDict)
        
        for one_row in content.itertuples():
           
            dbcon_insert_last.execute('''insert into 4tix_table_igpm(indice_mes, indice_ano, indice_doze, mes, ano, data)''', sequence, one_row[1], one_row[2], one_row[3], one_row[6], one_row[5])
    else:
        ## a tabela não existe é necessário criar e inserir os dados do dataframe
        sql_create_table = '''create table 4tix_table_igpm (
                                        indice_id int primary key,
                                        indice_mes float,
                                        indice_ano float,
                                        indice_doze float,
                                        mes int,
                                        ano int,
                                        data nvarchar(20)
                          )'''
        dbcon.execute(sql_create_table)
        dbcon.commit()
        sql_create_sequencie = '''CREATE SEQUENCE 4tix_seq_01
                                            AS BIGINT
                                            START WITH 1
                                            INCREMENT BY 1
                                            MINVALUE 1
                                            MAXVALUE 99999
                                            NO CYCLE
                                            CACHE 10;'''
        dbcon.execute(sql_create_sequencie)
        dbcon.commit()
        dbcon.close()

        content_ = buscaIgpm().reindex(index=buscaIgpm().index[::-1])
        #abre uma nova conexão com banco para executar o insert
        dbcon_insert = stringconnSQL(**configDict)
        for row in content_.itertuples():  
            dbcon_insert.execute('''insert into 4tix_table_igpm(indice_mes, indice_ano, indice_doze, mes, ano, data)''', sequence, row[1], row[2], row[3], row[6], row[5])
            dbcon_insert.commit()
       
        dbcon_insert.close()

#sqlConn()

def main():
  schedule.every().day.at("00:00").do(sqlConn)

 
