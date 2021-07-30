from kubernetes import client, config
from flask import jsonify
from os import path
from datetime import datetime
import yaml
import json
import time

import background_service_functions

#set k8s params
namespace = 'deployed-services'
config = config.load_kube_config(config_file='/home/anish/Thesis/Kubernetes-Edge-Manager/config')
v1 = client.CoreV1Api()
api_instance = client.CoreV1Api()
service = client.V1Service()
apps_api = client.AppsV1Api()
deployment = client.V1Deployment()
custom_obj_api = client.CustomObjectsApi()

# gets the current cpu usage of a node based on the node's name
def get_node_cpu_usage(node_name, host_ip, min_cpu_usage_node):
    try:
        node = custom_obj_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes/" + node_name)
    except Exception as e: 
        print(e)
        return None
    cpu_usage = node['usage']['cpu']
    if cpu_usage < min_cpu_usage_node['cpu']:
        min_cpu_usage_node['host'] = host_ip
        min_cpu_usage_node['name'] = node_name
        min_cpu_usage_node['cpu'] = cpu_usage
    return cpu_usage


# gets all the pods serving the target image
def findPods(target, podList, min_cpu_usage_node):
    pods = v1.list_namespaced_pod(namespace='deployed-services')
    for currentPod in pods.items:
        if target + '-' in currentPod.metadata.name:
            print('TARGET ' + target)
            print(currentPod.metadata.name)
            print(target in currentPod.metadata.name)
            hostIP = currentPod.status.host_ip
            hostName = currentPod.spec.node_name
            if (get_node_cpu_usage(currentPod.spec.node_name, hostIP, min_cpu_usage_node) is not None) and hostIP not in podList:
                podList.append(hostIP)
            
# gets the NodePort corresponding to the service
def findServicePort(target):
    print('SERVICE PORT TARGET ' + target)
    retService = v1.list_namespaced_service(namespace, watch=False)
    for currentService in retService.items:
        curr_service_name = currentService.metadata.name
        service_name_array = curr_service_name.split('-')

        if target == service_name_array[0]:
            return currentService.spec.ports[0].node_port

# returns the best node for every service
def get_best_nodes(model_stats):
    hostList = {}
    time1 = time.perf_counter()
    services = v1.list_namespaced_service(namespace, watch=False)
    for currentService in services.items:
        #get best nodes specific service expects a dict/json
        curr_service_name = currentService.metadata.name
        name_array = curr_service_name.split('-')
        best_node = get_best_node_specific_service({"model" : name_array[0]}, True, model_stats)
        
        if best_node is not None:
            hostList[name_array[0]] = best_node
        else:
            print("Skipping unavailable service")
    
    if not hostList: # empty
        return {'N/A' : 'No deployed services'}
    else:
        print(hostList)
        time2 = time.perf_counter()
        get_service_time = (time2 - time1) * 1000
        print("\n\nGet ALL Node Time: " + str(get_service_time) + " ms")
        return jsonify(hostList)

# returns the best node for a specific named service
def get_best_node_specific_service(request_body, all_flag, model_stats):
    min_cpu_usage_node = {'host':None, 'cpu':"inf"}
    podList = []
    target = request_body['model']
    print('\nGET BEST NODE SPECIFIC TARGET ' + target)
    
    time1 = time.perf_counter()
    findPods(target, podList, min_cpu_usage_node)

    # all_flag means request is coming from the get_best_nodes function
    if all_flag:
        if min_cpu_usage_node['host'] == None:
            return None
        else:
            if findServicePort(target) is None:
                return 'Deployment in progress'
            else:
                return_obj = min_cpu_usage_node['host'] + ':' + str(findServicePort(target))


                return return_obj
    else:
        if min_cpu_usage_node['host'] == None:
            deployment_result = background_service_functions.create_deployment(target)
            if deployment_result:
                update_model_stats(target, model_stats)
                return json.dumps({target : "Does not exist, creating new deployment"})
            else:
                return json.dumps({target : "Unknown Service"})
        else:
            if findServicePort(target) is None:
                update_model_stats(target, model_stats)
                return json.dumps({target : 'Deployment in progress'})
            else:
                result_json = json.dumps({target : min_cpu_usage_node['host'] + ':' + str(findServicePort(target))})
                time2 = time.perf_counter()
                get_service_time = (time2 - time1) * 1000
                print("\n\nGet SPECIFIC Node Time: " + str(get_service_time) + " ms")

                update_model_stats(target, model_stats)
                print(update_request_stats(target, request_body['latency'], min_cpu_usage_node['host'], min_cpu_usage_node['name']))
                return result_json

def set_model_stats(new_model_stats):
    with open('model_stats.json', 'w') as save_file:
        json.dump(new_model_stats, save_file)
    save_file.close()

def update_model_stats(target, model_stats):
    num_requests = None
    try:
        num_requests = model_stats[target]['num_requests']
    except:
        num_requests = 0
    model_stats[target] = {"last_request" : datetime.strftime(datetime.now(), '%m/%d/%y %H:%M:%S'), "num_requests" : num_requests + 1}
    print(target)
    print(model_stats[target])

def update_request_stats(model_requested, latency_value, server_recommended, server_name):
    request_stats = {}
    file = None

    try:
        file = open('request_stats.json')
        request_stats = json.load(file)
        file.close()
    except:
        print('CANT OPEN FILE REQUEST STATS')
        return False

    request_count = len(request_stats)-1
    if request_count == 0:
        request_stats[request_count] = {"model" : model_requested, "latency" : latency_value, "server" : server_recommended, "server_name" : server_name}
    request_stats[request_count + 1] = {"model" : model_requested, "latency" : latency_value, "server" : server_recommended, "server_name" : server_name}

    with open('request_stats.json', 'w') as save_file:
        json.dump(request_stats, save_file)
    save_file.close()

    return True

def get_current_server_memory():
    server_memory = {}
    try:
        nodes_total = v1.list_node(watch=False)
        nodes_usage = custom_obj_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
        #using a counter avoids nested loop
        count = 0
        for node in nodes_total.items:
            total_mem = node.status.allocatable['memory'][0 : len(node.status.allocatable['memory']) - 2]
            consumed_mem = nodes_usage['items'][count]['usage']['memory'][0 : len(nodes_usage['items'][count]['usage']['memory']) - 2]
            server_memory[node.status.addresses[0].address] = int(total_mem) - int(consumed_mem)
        return jsonify(server_memory)
    except Exception as e: 
        print(e)