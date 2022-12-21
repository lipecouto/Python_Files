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
    data_sheet = pd.ExcelFile(path+file_name)

    df_sheet = pd.read_excel(data_sheet, sheet_name=['Geral', 'Operações de Fabricação', 'Matéria Prima'])

    #Get DataFrame from Dict
    geral_df = df_sheet.get('Geral')
    oprod_df = df_sheet.get('Operações de Fabricação')
    mprim_df = df_sheet.get('Matéria Prima')

    geral_list = geral_df.to_list()

    #df_geral = pd.DataFrame(geral_df)
    #df_geral = pd.dataframe(geral_df)
    #df_geral = pd.dataframe(geral_df)
    return geral_list
    #for geral in geral_gdf:
     #   return geral
    
    #print(geral_result)
    # Print DataFrame's
    #return (geral_result)
    #return static_file('formulario.html', root='C:/Users/Sankhya/Desktop/SNKPYTHON/')
    #print(schedule_df)



  #return None

def conn():
    try:
        con = cx_Oracle.connect('sankhya/kal123@10.140.11.67:1521/snkdbprd') 
    except cx_Oracle.Error(msg):
        print(msg)    
    


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
