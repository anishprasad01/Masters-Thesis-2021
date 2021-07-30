from flask import Flask, request, jsonify
import json
import importlib
import argparse
import time

frontend_service_functions = None
background_service_functions = None

parser = argparse.ArgumentParser()
parser.add_argument("-mode", help="Specify 1 or 2 to choose server operating mode. Defaults to 1", type=int)
parser.add_argument("-m", help="Specify 1 or 2 to choose server operating mode. Defaults to 1", type=int)
args = parser.parse_args()

if args.mode == 2 or args.m == 2:
    frontend_service_functions = importlib.import_module('frontend_service_functions_mode2', __name__)
    background_service_functions = importlib.import_module('background_service_functions_mode2', __name__)
    print('\nStarting With Serving Mode: 2\n')
else:
    frontend_service_functions = importlib.import_module('frontend_service_functions', __name__)
    background_service_functions = importlib.import_module('background_service_functions', __name__)
    print('\nStarting With Serving Mode: 1\n')

app = Flask(__name__)

@app.route('/')
def index():
    return 'Thesis 2021 - Kubernetes Edge Manager \nAnish Prasad'

@app.route('/services', methods = ['GET'])
def get_all_service_node():
    model_stats = {}

    try:
        file = open('model_stats.json')
        model_stats = json.load(file)
        file.close()
    except:
        print('Unable to Open File')

    request_body = request.json

    if request_body is not None:
        if 'latency' in request_body:
            result = frontend_service_functions.get_best_node_specific_service(request.json, False, model_stats)
            time2 = time.perf_counter()
            with open('model_stats.json', 'w') as save_file:
                json.dump(model_stats, save_file)
            save_file.close()
            return result
        else:
            return frontend_service_functions.get_best_nodes(model_stats)
    else:
        return frontend_service_functions.get_best_nodes(model_stats)

@app.route('/services/<model>/<api_type>/<model_folder>/<model_name_request_type>', methods = ['POST'])
def proxy_request(model, api_type, model_folder, model_name_request_type):
    model_stats = {}
    try:
        file = open('model_stats.json')
        model_stats = json.load(file)
        file.close()
    except:
        print('Unable to Open File')

    request_body = request.json

    result = frontend_service_functions.get_best_node_specific_service({"model" : model, "latency" : request_body['latency']}, False, model_stats)
    with open('model_stats.json', 'w') as save_file:
        json.dump(model_stats, save_file)
    save_file.close()

    print(result)

    endpoint = json.loads(result)

    if endpoint[model] == 'Does not exist, creating new deployment':
        return json.dumps({model : 'Model does not exist, creating new deployment. Try again later'})
    else:
        return "Model not found", 404

@app.route('/dev/model_stats', methods = ['GET', 'POST'])
def get_model_stats():
    model_stats = {}
    try:
        file = open('model_stats.json')
        model_stats = json.load(file)
        file.close()
    except:
        print('Unable to Open File')

    if request.method == 'GET':
        return jsonify(model_stats)
    elif request.method == 'POST':
        request_body = request.json

        if request_body == None:
            new_model_stats = {}
            frontend_service_functions.set_model_stats(new_model_stats)
            return '200 OK'
        else:
            req_dict = request_body
            new_model_stats = req_dict.copy()
            frontend_service_functions.set_model_stats(new_model_stats)
            return '200 OK'
    else:
        return 'Method Not Allowed', 405

@app.route('/dev/request_stats', methods = ['GET'])
def get_request_stats():
    request_stats = {}
    try:
        file = open('request_stats.json')
        request_stats = json.load(file)
        file.close()
        return jsonify(request_stats)
    except:
        print('CANT OPEN FILE GETTING STATS')

@app.route('/dev/server_mem_stats', methods = ['GET'])
def get_server_mem_stats():
    if request.method == 'GET':
        return frontend_service_functions.get_current_server_memory()
    else:
        return 'Method Not Allowed', 405

@app.route('/dev/replicas/<target>/<number>')
def dev_update_replicas(target, number):
    return background_service_functions.update_replicas(target, number)

@app.route('/dev/delete/<target>')
def dev_delete_deployment(target):
    return background_service_functions.delete_deployment(target)

app.run(host='0.0.0.0', port=24432)