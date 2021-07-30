# import background_service_functions
import argparse
import importlib
import random
import json
import requests
from datetime import datetime
from kubernetes import client, config
import time

#set k8s params
namespace = 'deployed-services'
config = config.load_kube_config()
v1 = client.CoreV1Api()
api_instance = client.CoreV1Api()
service = client.V1Service()
apps_api = client.AppsV1Api()
deployment = client.V1Deployment()
custom_obj_api = client.CustomObjectsApi()


background_service_functions = None

parser = argparse.ArgumentParser()
parser.add_argument("-mode", help="Specify 1 or 2 to choose provisioning operating mode. Defaults to 1", type=int)
parser.add_argument("-m", help="Specify 1 or 2 to choose provisioning operating mode. Defaults to 1", type=int)
args = parser.parse_args()

if args.mode == 2 or args.m == 2:
    background_service_functions = importlib.import_module('background_service_functions_mode2', __name__)
    print('\nStarting With Provisioning Mode: 2\n')
else:
    background_service_functions = importlib.import_module('background_service_functions', __name__)
    print('\nStarting With Provisioning Mode: 1\n')

#hours
last_request_threshold = 5
#count
num_requests_threshold = 10

model_stats_endpoint = 'http://localhost:24432/dev/model_stats'

request_stats_endpoint = 'http://localhost:24432/dev/request_stats'

server_mem_endpoint = 'http://localhost:24432/dev/server_mem_stats'

def main():
    if args.mode == 2 or args.m == 2:
        request_stats = get_request_stats()
        node_mem = get_node_mem_metrics()

        solver_results = run_ampl_ipopt_solver(request_stats, node_mem)

        print('\nPERFORMING PROVISIONING...')
        time1 = time.perf_counter()
        print('PROVISIONING SUCCESSFUL: ' + str(background_service_functions.perform_provisioning(solver_results, request_stats)))
        time2 = time.perf_counter()
        print("Mode 2 Provision Time: " + str((time2 - time1) * 1000) + " ms")
    else:
        model_stats = None
    
        try:
            model_stats = load_model_stats()
            print('SUCCESS: MODEL STATS DATA LOADED')
        except:
            print('ERROR: CANNOT GET DATA')
        model_stats = load_model_stats()

        print('Performing Old Services Check with Hour Threshold: ' + str(last_request_threshold))
        remove_old_services(model_stats)
        print('Performing Unused Services Check with Request Count Threshold: ' + str(num_requests_threshold))
        remove_unused_services(model_stats)

        request_stats = get_request_stats()
        node_mem = get_node_mem_metrics()

        solver_results = run_ampl_ipopt_solver(request_stats, node_mem)

        print('\nPERFORMING PROVISIONING...')
        time1 = time.perf_counter()
        print('PROVISIONING SUCCESSFUL: ' + str(background_service_functions.perform_provisioning(solver_results, request_stats)))
        time2 = time.perf_counter()
        print("Mode 1 Provision Time: " + str((time2 - time1) * 1000) + " ms")

def load_model_stats():
    return json.loads(requests.get(model_stats_endpoint).text)

def load_request_stats():
    return json.loads(requests.get(request_stats_endpoint).text)

def load_node_memory():
    return json.loads(requests.get(server_mem_endpoint).text)

def post_model_stats_update(model_stats):
    requests.post(model_stats_endpoint, json=model_stats)

def remove_old_services(model_stats):
    services = v1.list_namespaced_service(namespace, watch=False)
    for currentService in services.items:
        curr_service_name = currentService.metadata.name
        name_array = curr_service_name.split('-')

        if (datetime.now().hour - last_request_threshold) > datetime.strptime(model_stats[name_array[0]]['last_request'], '%m/%d/%y %H:%M:%S').hour:
            print("Removing Old Service: " + currentService.metadata.name)
            background_service_functions.delete_deployment(currentService.metadata.name)
            model_stats.pop(name_array[0])
            post_model_stats_update(model_stats)
        else:
            print("Keeping Service: " + currentService.metadata.name)

def remove_unused_services(model_stats):
    services = v1.list_namespaced_service(namespace, watch=False)
    for currentService in services.items:
        curr_service_name = currentService.metadata.name
        name_array = curr_service_name.split('-')
        
        if model_stats[name_array[0]]['num_requests'] < num_requests_threshold:
            print("Removing Rarely Used Service: " + currentService.metadata.name)
            background_service_functions.delete_deployment(currentService.metadata.name)
            model_stats.pop(name_array[0])
            post_model_stats_update(model_stats)
        else:
            print("Keeping Service: " + currentService.metadata.name)

def get_request_stats():
    request_stats = None
    try:
        request_stats = load_request_stats()
        print('SUCCESS: REQUEST DATA RECIEVED')
    except:
        print('ERROR: CANNOT GET DATA')
    request_stats = load_request_stats()
    return request_stats

def get_node_mem_metrics():
    node_mem = None
    try:
        node_mem = load_node_memory()
        print('SUCCESS: NODE RAM DATA RECIEVED')
    except:
        print('ERROR: CANNOT GET DATA')
    node_mem = load_node_memory()
    return node_mem

def run_ampl_ipopt_solver(request_stats, node_mem):
    #solver file generation
    print("\nMODEL FILE GENERATION SUCCESSFUL: " + str(background_service_functions.build_model_file(len(request_stats)-1)))
    print("DATA FILE GENERATION SUCCESSFUL: " + str(background_service_functions.build_data_file(request_stats, node_mem)))
    print("RUN FILE GENERATION SUCCESSFUL: " + str(background_service_functions.build_run_file(len(request_stats)-1)))

    #run the solver
    print('\nRUNNING SOLVER...')
    print("SOLVER SUCCESSFUL: " + str(background_service_functions.run_solver()))
    solver_results = background_service_functions.get_solver_results(len(request_stats)-1, 5)

    #print the solver results
    print('\nSOLVER RESULTS:')
    for i in range(0, 7):
        print(solver_results[i])

    #return solver results
    return solver_results

if __name__ == "__main__":
    main()