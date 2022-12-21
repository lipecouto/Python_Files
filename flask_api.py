import configparser

import logzero

from flask import Flask, request, jsonify, send_from_directory, render_template_string
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager,
    jwt_required,
    create_access_token,
    get_jwt_identity,
)
from oracle_conn import DbConnect
from logzero import logger

config = configparser.ConfigParser()
config.read("config.conf")
_log_path = config.get("logging", "log_path")
logzero.logfile(f"{_log_path}/debug.log", maxBytes=1e6, backupCount=4)

app = Flask(__name__)
CORS(app)  # Comment this line to disable CORS

app.config["JWT_SECRET_KEY"] = config.get("api", "jwt_key")
JWTManager(app)


@app.route("/api/login", methods=["POST"])
def login():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400

    username = request.json.get("username", None)
    password = request.json.get("password", None)
    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    if not password:
        return jsonify({"msg": "Missing password parameter"}), 400

    # TODO hash password and compare with database
    if username != "test" or password != "test":
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token), 200


@app.route('/api/os_report/<string:nro>')
def sel_os(nro):
    conn = DbConnect()
    specifier = {'__SOL': nro}
    client_info = conn.get_from_table_filtered("sel_os_vendor_detail", specifier)
    result = []
    for i in client_info:
        if i['ALTERACAO'] != None:
            if i['DESCRICAO'] != None:
                result = {
                    'msg': 'done',
                    'changing': i['ALTERACAO'],
                    'description': i['DESCRICAO'],
                    'obs': i['OBSERVACOES']
                }

    return jsonify(result), 200

@app.route("/api/client/<string:nro>", methods=["GET"])
def get_client(nro):
    conn = DbConnect()
    specifier = {"__NRO": nro}
    client_info = conn.get_from_table_filtered("sel_services_by_dsgn", specifier)[0]

    delivery = {
        "stations": conn.get_from_table("sel_stations"),
        "hosts": conn.get_from_table("sel_stations_hosts"),
        "ports": conn.get_from_table("sel_hosts_interfaces"),
    }

    logger.debug(f"client_info.NROCONTRATO: <{{{client_info['NROCONTRATO']}}}>")

    specifier2 = {"__CONTRACT": client_info["NROCONTRATO"]}

    if client_info["ABORDAGEM"] and client_info["ABORDAGEM"].lower() == "radio":
        equip_list = {
            "PTP": conn.get_from_table("sel_radio_ptp"),
            "PTMP": conn.get_from_table("sel_radio_ptmp"),
        }
    else:
        equip_list = {
            "PTP": conn.get_from_table("sel_fiber_ptp"),
            "PTMP": conn.get_from_table("sel_fiber_ptmp"),
        }

    equipment = conn.get_from_table_filtered("sel_device_by_dsgn", specifier)
    if len(equipment) == 0:
        equipment = {}
    else:
        equipment = equipment[0]

    result = {
        "isp": conn.get_from_table("sel_operators"),
        "services": dict_to_array(conn.get_from_table("sel_services")),
        "client_info": client_info,
        "network": conn.get_from_table_filtered("sel_ips_by_dsgn", specifier),
        "address": conn.get_from_table_filtered("sel_address_by_dsgn", specifier2)[0],
        "delivery": delivery,
        "equip_list": equip_list,
        "equipment": equipment,
        "gbic_devices": conn.get_from_table("sel_gbic_devices"),
    }
    conn.close()

    return jsonify(result)


@app.route("/api/client_view/<string:nro>", methods=["GET"])
def get_client_view(nro):
    conn = DbConnect()
    specifier = {"__NRO": nro}
    client_info = conn.get_from_table_filtered("sel_services_by_dsgn", specifier)[0]

    specifier2 = {"__CONTRACT": client_info["NROCONTRATO"]}

    equipment = conn.get_from_table_filtered("sel_device_by_dsgn", specifier)
    if len(equipment) == 0:
        equipment = {}
    else:
        equipment = equipment[0]

    card_serv = conn.get_from_table_filtered("sel_card_serv", specifier)[0]
    card_serv.update(conn.get_from_table_filtered("sel_card_fin", specifier)[0])

    result = {
        "card_serv": card_serv,
        "network": conn.get_from_table_filtered("sel_ips_by_dsgn", specifier),
        "address": conn.get_from_table_filtered("sel_address_by_dsgn", specifier2)[0],
        "equipment": equipment,
    }
    conn.close()

    return jsonify(result)


@app.route("/api/delivery", methods=["GET"])
def get_delivery():
    conn = DbConnect()
    result = {
        "station": conn.get_from_table("sel_stations"),
        "host": conn.get_from_table("sel_stations_hosts"),
        "port": conn.get_from_table("sel_hosts_interfaces"),
    }
    conn.close()

    return jsonify(result)


@app.route("/api/clients", methods=["GET"])
def get_clients():
    conn = DbConnect()
    result = conn.get_from_table("sel_clients")
    conn.close()

    return jsonify(result)


@app.route("/api/dashboard", methods=["GET"])
def get_dashboard():
    conn = DbConnect()
    result = {
        "table": conn.get_from_table("sel_tickets"),
        "technicians": conn.get_from_table("sel_technicians"),
        "open_tickets": conn.get_from_table("sel_open_tickets_nbr")[0]["count"],
        "exp_tickets": conn.get_from_table("sel_exp_tickets_nbr")[0]["count"],
        "prcss_tickets": conn.get_from_table("sel_prcss_tickets_nbr")[0]["count"],
        "frzing_tickets": conn.get_from_table("sel_frzing_tickets_nbr")[0]["count"],
    }
    conn.close()

    return jsonify(result)


@app.route("/api/os_details/<string:os>", methods=["GET"])
def get_os_details(os):
    specifier = {"__NRO": os}
    conn = DbConnect()
    result = {
        "details": conn.get_from_table_filtered("sel_details_os", specifier)
    }
    conn.close()

    return jsonify(result)


@app.route("/api/dashboard/<string:os>", methods=["PUT"])
def update_os(os):
    data = request.get_json()

    conn = DbConnect()
    result = conn.update_os(os, data)
    conn.close()

    return jsonify(result)


@app.route("/api/step1/<string:nro>", methods=["PUT"])
def update_step_1(nro):
    data = request.get_json()

    conn = DbConnect()
    result = conn.update_step_1(nro, data)

    if data["network"] and len(data["network"]) > 0:
        for network in data["network"]:
            if result != "success":
                break

            result = conn.update_network(nro, network)

    conn.close()

    return jsonify(result)


@app.route("/api/step2/<string:nro>", methods=["PUT"])
def update_step_2(nro):
    data = request.get_json()

    conn = DbConnect()
    result = conn.update_step_2(nro, data)
    conn.close()

    return jsonify(result)


@app.route("/api/step3/<string:nro>", methods=["PUT"])
def update_step_3(nro):
    data = request.get_json()

    conn = DbConnect()
    result = conn.update_step_3(nro, data)
    conn.close()

    return jsonify(result)


@app.route("/api/dash/<string:os>", methods=["PUT"])
def update_dash_item(os):
    data = request.get_json()

    conn = DbConnect()
    result = conn.update_dash_item(os, data)
    conn.close()

    return jsonify(result)


@app.route("/api/<string:todo_id>", methods=["GET"])
@jwt_required
def generic_get(todo_id):
    params = request.args.items()

    conn = DbConnect()
    result = conn.get_data(todo_id, params)
    conn.close()

    return jsonify(result)


@app.route("/res/css", methods=["GET"])
def get_css():
    return send_from_directory("css", "style.css"), 200


@app.route("/res/js/script", methods=["GET"])
def get_js_script():
    return send_from_directory("js", "script.js"), 200


@app.route("/res/js/script_ro", methods=["GET"])
def get_js_script_ro():
    return send_from_directory("js", "script_ro.js"), 200


# Check query log
@app.route("/log/query", methods=["GET"])
def get_log_query():
    with open(f"{_log_path}/debug_update_dash_item.sql", "r") as f:
        log = f.read()
    log.replace("\n", "<br>")
    log = log.splitlines()
    log.reverse()
    template = (
        "{% for para in text %}"
        "    <p>{{para}}</p>"
        "{% endfor %}"
    )
    return render_template_string(template, text=log), 200


# Convert a dictionary result to an array of strings
def dict_to_array(array_of_dicts):
    result = []

    for item in array_of_dicts:
        result.extend(item.values())

    return result


if config.getboolean("api", "debug"):
    app.run(
        debug=True,
        host=config.get("api", "tcp_ip"),
        port=config.getint("api", "tcp_port"),
    )
