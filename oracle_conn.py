import configparser
from datetime import date, datetime
from time import strftime

import cx_Oracle
import logzero
from logzero import logger as l

config = configparser.ConfigParser()
config.read("config.conf")
_log_path = config.get("logging", "log_path")
logzero.logfile(f"{_log_path}/debug.log", maxBytes=1e6, backupCount=4)


class DbConnect(object):
    def __init__(self):
        config = configparser.ConfigParser()
        if not config.read("config.conf"):
            print("Error! Config file 'config.conf' was not found!")
            exit(1)
        elif (
            config.get("database", "user") == "<username>"
            or config.get("database", "passwd") == "<password>"
            or config.get("database", "host") == "<host_address>"
        ):
            print("Error! Please fix the [database] section of 'config.conf'.")
            exit(1)

        try:
            self.db = cx_Oracle.connect(
                config.get("database", "user"),
                config.get("database", "passwd"),
                config.get("database", "host"),
                encoding="utf8",
            )
        except Exception as e:
            print(f"Error!\tSee traceback below:\n{e}")
            exit(1)

    def run_query(self, query, **conf):
        cursor = self.db.cursor()

        try:
            cursor.execute(query)

            if "update" not in conf:
                cursor.rowfactory = cursor_to_dict(cursor)
                result = make_serializable(cursor.fetchall())
            else:
                self.db.commit()
                result = "success"

            cursor.close()

        except Exception as err:
            result = str(err)

        return result

    def close(self):
        self.db.close()

    # NETWORK FIELDS SECTION
    def get_data(self, table, params):
        query = f"SELECT * FROM {table} WHERE {params[0][0]} = '{params[0][1]}'"

        result = self.run_query(query)

        return result

    def get_isp(self):
        query = (
            "SELECT ("
            " SELECT RAZAOSOCIAL FROM TGFPAR PR "
            " WHERE PR.CODPARC = ANS.CODPARC AND RAZAOSOCIAL IS NOT NULL"
            ") AS OPERADORA FROM AD_TBASN ANS "
            "WHERE EXISTS ("
            " SELECT 1 FROM TGFPAR PR WHERE PR.CODPARC = ANS.CODPARC"
            ")"
        )

        result = dict_to_array(self.run_query(query))

        return result

    def get_services(self):
        query = (
            "SELECT IDENTIFICACAO AS SERVICO FROM TCSPRJ PRJ "
            "WHERE PRJ.CODPROJ "
            "BETWEEN 500000 AND 599999 AND PRJ.ANALITICO = 'S'"
        )

        result = dict_to_array(self.run_query(query))

        return result

    # MEANS OF DELIVERY FIELDS SECTION
    def get_station(self):
        query = "SELECT POP.SIGLA, NUESTACAO FROM AD_TBPOP POP ORDER BY SIGLA"

        result = self.run_query(query)

        return result

    def get_station_host(self):
        query = "SELECT  NUHOST, HOST, NUESTACAO FROM AD_TBHOSTS ORDER BY NUHOST"

        result = self.run_query(query)

        return result

    def get_station_port(self):
        query = (
            "SELECT NROINTERFACE, NUHOST FROM AD_TBHOSTSINTERFACE "
            "ORDER BY NROINTERFACE"
        )

        result = self.run_query(query)

        return result

    # RUN QUERIES FROM QUERIES TABLE
    def get_from_table(self, ref):
        query = f"SELECT SCRIPTA FROM AD_TBSCRIPTS WHERE REF = '{ref}'"

        result = self.run_query(query)

        for item in result:
            query = item["SCRIPTA"]

        result = self.run_query(query)

        return result

    # RUN QUERIES FROM QUERIES TABLE WITH WHERE
    def get_from_table_filtered(self, ref, specifier):
        query = f"SELECT SCRIPTA FROM AD_TBSCRIPTS WHERE REF = '{ref}'"
        result = self.run_query(query)[0]

        l.debug(f"result: <{{{result}}}>")
        l.debug(f"result['SCRIPTA']: <{{{result['SCRIPTA']}}}>")
        l.debug(f"specifier.keys(): <{{{specifier.keys()}}}>")
        l.debug(f"specifier.values(): <{{{specifier.values()}}}>")
        # breakpoint()
        query = result["SCRIPTA"].replace(
            list(specifier)[0], str(specifier[list(specifier)[0]])
        )
        l.debug(f"query: <{{{query}}}>")
        result = self.run_query(query)

        return result

    # UPDATE OS
    def update_os(self, os, data):
        self.run_query("ALTER SESSION SET NLS_DATE_FORMAT ='DD/MM/YYYY'")

        query = (
            f"UPDATE AD_TBHISTCHAMAOS SET "
            f"DHALTERACAO = SYSDATE, "
            f"USUARIOABERT = '', "
            f"DTFECHAMENTO = '{data['dt_end']}', "
            f"EXECUTANTE =  ("
            f" SELECT NOMEUSU FROM TSIUSU WHERE CODUSU = '{data['technician']}'"
            f"), "
            f"DESCRICAO = '{data['obs']}', "
            f"DHPREVISAO = '{data['dt_pred']}', "
            f"STATUS = CASE"
            f" WHEN EXECUTANTE IS NULL"
            f" THEN 1"
            f" WHEN EXECUTANTE IS NOT NULL AND DHPREVISAO IS NOT NULL"
            f" THEN 2"
            f" ELSE 3 "
            f"END "
            f"WHERE  NROUNICO = '{os}' AND NUNICOH = '{data['seq']}'"
        )

        result = self.run_query(query, update="yes")

        return result

    # UPDATE STEP 1
    def update_step_1(self, nro, data):
        query = (
            f"UPDATE AD_TBDADOSERVICO SET "
            f"CODOPERADORA = '{str(data['isp'])}', "
            f"BANDA = '{str(data['speed'])}', "
            f"CLIENTEASN = '{data['asn_client']}', "
            f"CODASN = '{data['asn_code']}', "
            f"IPPEERING = '{data['ip_peering']}', "
            f"SENHABGP = '{data['bgp_pwd']}', "
            f"OBSERVACAO = '{str(data['obs'])}' "
            f"WHERE NUNICO = '{nro}'"
        )

        result = self.run_query(query, update="yes")

        return result

    # UPDATE STEP 2
    def update_step_2(self, nro, data):
        for key in data:
            if data[key] is None:
                data[key] = "null"

        query = (
            f"UPDATE AD_TBDADOSERVICO SET "
            f"ABORDAGEM = '{str(data['medium'])}', "
            f"ENDCAIXAEMENDA = '{str(data['boxAddress'])}', "
            f"DISTANCIACAIXA = {str(data['boxDistance'])}, "
            f"NIVELSPLITTER = {str(data['splitterLevel'])}, "
            f"ESTACAOATIVACAO = {str(data['activStation'])}, "
            f"NUHOSTESTACAOATIVACAO = {str(data['activHost'])}, "
            f"NROINTERFACEATIVA = {str(data['activPort'])}, "
            f"ESTACAOENTREGA = {str(data['deliveryStation'])}, "
            f"NUHOSTESTACAOENTREGA = {str(data['deliveryHost'])}, "
            f"NROINTERFACENTREG = {str(data['deliveryPort'])}, "
            f"CODPROD = {str(data['deliveryGBIC'])}, "
            f"OBSERVACAO2 = '{str(data['obs'])}' "
            f"WHERE NUNICO = '{nro}'"
        )

        result = self.run_query(query, update="yes")

        return result

    # UPDATE STEP 3
    def update_step_3(self, nro, data):
        for key in data:
            if data[key] is None:
                data[key] = "null"

        query = (
            "UPDATE AD_TBDADOSEQUIPSERV SET "
            f"OPERACAO = '{str(data['opMode'])}', "
            f"MARCA = '{str(data['brand'])}', "
            f"IPGERENCIA = '{str(data['mgmtIP'])}', "
            f"PORTGERENC = '{str(data['mgmtPort'])}', "
            f"MODELOODU = '{str(data['odu'])}', "
            f"MODELOIDU = '{str(data['idu'])}', "
            f"TIPOANTENA = '{str(data['antenna'])}', "
            f"DIAMETROANT = '{str(data['diameter'])}', "
            f"FREQUENCIA = '{str(data['freq'])}', "
            f"LARGBANDA = '{str(data['bandwidth'])}', "
            f"SUBBANDAC = '{str(data['subband'])}', "
            f"OBSERVACAO = '{str(data['obs'])}' "
            f"WHERE NUNICO = '{nro}'"
        )

        print(query)
        result = self.run_query(query, update="yes")

        return result

    # UPDATE NETWORK
    def update_network(self, nro, data):

        if data["active"] == "N":
            query = (
                f"UPDATE AD_TBIPCONTROL "
                f"SET ATIVO = 'N', DHDESATIVACAO = SYSDATE "
                f"WHERE NROUNICO = '{nro}' AND NUNICO = '{str(data['id'])}'"
            )
        else:
            query = (
                f"INSERT INTO AD_TBIPCONTROL ("
                f" NUNICO, NROUNICO, BLOCOIPV4,"
                f" IP, MASCARA, GATEWAY,"
                f" ATIVO, VLAN, FUNCAO, DHCADASTRO"
                f") VALUES("
                f" SQC_PRODADOSERV.Nextval, '{nro}', '{data['block']}',"
                f" '{data['ip']}', '{data['mask']}', '{data['gateway']}',"
                f" 'S','{str(data['vlan'])}', '{data['type']}', SYSDATE"
                f")"
            )

        result = self.run_query(query, update="yes")

        return result

    # UPDATE DASHBOARD ITEM
    def update_dash_item(self, os, data):

        if len(data["dt_end"]) > 2:
            query = (
                f"UPDATE AD_TBHISTCHAMAOS SET "
                f"DHALTERACAO = SYSDATE, "
                f"CONGELADO ='{data['freeze']}'," 
                f"DTFECHAMENTO = TO_DATE('{data['dt_end']}', 'dd/mm/yyyy'), "
                f"DESCRICAO = '{data['obs']}', "
                f"TOSITE = 999 "
                f"WHERE NROUNICO = '{str(os)}' AND NUNICOH = '{str(data['seq'])}'"
            )

        else:
            query = (
                f"UPDATE AD_TBHISTCHAMAOS "
                f"SET"
                f" DHALTERACAO = SYSDATE,"
                f" USUARIOABERT = '{data['user']}',"
                f" TOSITE = null ,"
                f" DTFECHAMENTO = null,"
                f" EXECUTANTE = NVL(("
                f"  SELECT NOMEUSU FROM TSIUSU"
                f"  WHERE CODUSU = '{data['technician']}'"
                f"), null),"
                f" DESCRICAO = '{data['obs']}',"
                f" DHPREVISAO = TO_DATE('{data['dt_pred']}', 'dd/mm/yyyy'),"
                f" STATUS = "
                f" CASE"
                f"  WHEN EXECUTANTE IS NULL THEN 1"
                f"  WHEN EXECUTANTE IS NOT NULL OR DHPREVISAO IS NOT NULL"
                f"  THEN 2"
                f"  WHEN DTFECHAMENTO IS NOT NULL"
                f"  THEN 3"
                f"  ELSE 0"
                f" END,"
                f" CONGELADO = '{data['freeze']}' "
                f"WHERE"
                f" NROUNICO = '{str(os)}' AND NUNICOH = '{str(data['seq'])}'"
            )

        try:
            result = self.run_query(query, update="yes")
        except Exception as e:
            l.exception(f"!---Exception raised during query execution! | {e}\n!---Query: {query}")
            query = f"!---EXCEPTION RAISED | {query}"

        # debug { TODO: implement it using logzero
        with open(
            "/var/log/gunicorn/debug_update_dash_item.sql", "a", encoding="utf-8"
        ) as f:
            f.write(f"[{strftime('%Y-%m-%d %H:%M:%S')}] {query}\n")
        # } debug

        return result


# Convert a dictionary result to an array of strings
def dict_to_array(dictionary):
    result_array = dictionary
    result = []

    for item in result_array:
        result.extend(item.values())

    return result


# Convert the cursor result to a dictionary
def cursor_to_dict(cur):
    column_headers = [d[0] for d in cur.description]

    def create_row(*args):
        return dict(zip(column_headers, args))

    return create_row


# Treating non-serializable values
def make_serializable(result):
    for index, row in enumerate(result):
        for header, value in row.items():

            # In case the value is a oracle LOB object
            if isinstance(value, cx_Oracle.LOB):
                result[index][header] = value.read()

            # In case the value is a date/datetime object
            if isinstance(value, (datetime, date)):
                result[index][header] = value.isoformat()

    return result
