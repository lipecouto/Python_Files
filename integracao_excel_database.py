##Python3.8
##Desenvolvido por Philipe Couto linkedin.com/in/philipecouto

import os
import cx_Oracle
import json
import glob
import pandas as pd
import bottle
from bottle import route, run, static_file, post, get, request, put, response, template


@route('/')
def main():
    conn()
    return static_file('index.html', root='C:/Users/Sankhya/Desktop/SNKPYTHON/')

@route('/css/<filename:path>')
def static(filename):
    return static_file(filename, root='C:/Users/Sankhya/Desktop/SNKPYTHON/css')

@route('/js/<filename:path>')
def static(filename):
    return static_file(filename, root='C:/Users/Sankhya/Desktop/SNKPYTHON/js')

@route('/upload', method='POST')
def do_upload():
    category = request.forms.get('category')
    upload = request.files.get('upload')
    name, ext = os.path.splitext(upload.filename)
    if ext not in ('.xls', '.xlsx'):
        return "File extension not allowed."

    save_path = "C:/Users/Sankhya/Desktop/SNKPYTHON/importacao/{category}".format(category=category)
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    file_path = "{path}/{file}".format(path=save_path, file=upload.filename)
  
    upload.save(file_path, overwrite=True)
    
    return send_data(save_path, upload.filename)

    #return "File successfully saved to '{0}'.".format(save_path)
@route('/sended')
def send_data(path, file_name):
    #print(path)  
    #datafile = glob.glob(os.path.join(path, file_name))

    status = False
    err_msg = ''
    data_sheet = pd.ExcelFile(path+file_name)
    conector = conn()

    #geral
    required_cols_geral = [3, 6, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] #[0, 1, 2, 4, 5, 7, 8, 9]
    ex_geral = data_sheet.parse('Geral', usecols= required_cols_geral)
    df_geral = pd.DataFrame(ex_geral, columns=['Produto', 'Quantidade', 'IMC', 'PROFT', 'CASH COST', 'DISCOUNT', 'COMISSION', 'LICENSES', 'MO', 'ADM', 'SALES', 'RISK', 'ENTREGA'])    
    insertGeral = conector.cursor()
    insertGeral.execute("DELETE AD_IMPORTSHEETGERAL")
    conector.commit()
    seqGeral = 1 
    for geral in df_geral.itertuples():
        try:
            insertGeral.execute("""INSERT INTO AD_IMPORTSHEETGERAL(NROUNICO, CODPROD, QTDNEG, IMC, PROFIT, CASHCOST, DISCOUNT, COMISSION, LICENSES, MANOVER, ADMINISTRATION, SALESMARK, RISK, ENTREGUE) VALUES(:seq, :prod, :qtd, :imc, :proft, :cash, :disc, :comi, :licen, :mo, :adm, :sales, :risk, :entr)""", (seqGeral, geral[1], geral[2], geral[3], geral[4], geral[5], geral[6], geral[7], geral[8], geral[9], geral[10], geral[11], geral[12], geral[13]))
        except cx_Oracle.DatabaseError as e:
            x = e.args[0]
            if hasattr(x, 'code') and hasattr(x, 'message') \
                and x.code == 2091 and 'ORA-02291' in x.message:
                    six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            status = False
        else:
            status = True
            #return status
        seqGeral = seqGeral + 1
        conector.commit()
    conector.close()

    #opfab
    required_cols_opfab = [1, 2, 3] #[0, 1, 2, 4, 5, 7, 8, 9]
    ex_opfab = data_sheet.parse('Operações de Fabricação', usecols=required_cols_opfab)
    df_opFab = pd.DataFrame(ex_opfab, columns=['Produto', 'SETUP', 'RECURSO'])  
    conector = conn()  
    insertOpfab = conector.cursor()
    insertOpfab.execute("DELETE AD_IMPORTSHEETOPFAB")
    conector.commit()    
    seqopfab = 1 
    for opFab in df_opFab.itertuples():
        #return opFab
        try:
            insertOpfab.execute("""INSERT INTO AD_IMPORTSHEETOPFAB(NROUNICO, CODPROD, SETUP, RECURSO) VALUES(:seq, :prod, :setup, :rec)""", (seqopfab, opFab[1], opFab[2], opFab[3]))
        except cx_Oracle.DatabaseError as e:
            x = e.args[0]
            if hasattr(x, 'code') and hasattr(x, 'message') \
                and x.code == 2091 and 'ORA-02291' in x.message:
                    six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            status = False
        else:
            status = True
        seqopfab = seqopfab + 1
        conector.commit()

    conector.close()

    #MatPrima
    required_cols_MatPrima = [1, 2, 4, 5, 6, 7, 8, 9] #[0, 1, 2, 4, 5, 7, 8, 9]
    ex_matpr = data_sheet.parse('Matéria Prima', usecols= required_cols_MatPrima)
    df_matpr = pd.DataFrame(ex_matpr, columns=['Produto', 'Matéria PRIMA', 'DESCRIÇÂO GENERICA', 'UNIDADE', 'GRUPO DE MATERIA PRIMA', 'QUANT', 'CUSTO UNIT.'])    
    conector = conn()
    insertmatpr = conector.cursor()
    insertmatpr.execute("DELETE AD_IMPORTSHEETMATPRIMA")
    conector.commit()    
    seqmatpr = 1 
    df_matpr['Matéria PRIMA'] = df_matpr['Matéria PRIMA'].fillna(0)
    df_matpr['Matéria PRIMA'] = df_matpr['Matéria PRIMA'].replace('.', 0)
    df_matpr['DESCRIÇÂO GENERICA'] = df_matpr['DESCRIÇÂO GENERICA'].fillna('-')
    #df_matpr['QUANT'] = df_matpr['QUANT'].replace(',', '.')
    
    for matPrima in df_matpr.itertuples():
        #print(matPrima[7])
        insertmatpr.execute("""INSERT INTO AD_IMPORTSHEETMATPRIMA(NROUNICO, CODPROD, MATPRIMA, DESCRGENERICA) VALUES(:seq, :prod, :matprima, :descr)""", (seqmatpr, matPrima[1], matPrima[2], matPrima[3]))
        seqmatpr = seqmatpr + 1
        conector.commit()

    conector.close()

    
    if(status == True):
        return('Dados incluidos com sucesso')

def conn():
    try:
        con = cx_Oracle.connect('seu host aqui') 
        return con
    except cx_Oracle.Error():
        return 'Erro de conexao'   
    


class EnableCors(object):
    name = 'enable_cors'
    api = 2

    def apply(self, fn, context):
        def _enable_cors(*args, **kwargs):
            # set CORS headers
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

            if bottle.request.method != 'OPTIONS':
                # actual request; reply with the actual response
                return fn(*args, **kwargs)

        return _enable_cors


app = bottle.app()

@app.route('/cors', method=['OPTIONS', 'GET'])
def lvambience():
    response.headers['Content-type'] = 'application/json'
    return '[1]'

app.install(EnableCors())

if __name__ == '__main__':
   app.run(host='192.168.0.113', port=8001)
