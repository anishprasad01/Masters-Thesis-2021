from kubernetes import client, config
from flask import jsonify
from os import path
import os
import yaml

#set k8s params
namespace = 'deployed-services'
config = config.load_kube_config()
v1 = client.CoreV1Api()
api_instance = client.CoreV1Api()
service = client.V1Service()
apps_api = client.AppsV1Api()
deployment = client.V1Deployment()
custom_obj_api = client.CustomObjectsApi()

memory_requirements_table = {"resnet" : 500, "nginx" : 200, "nginxgpu" : 200, "hpt" : 1000}
server_ip_table = {"192.168.1.41" : 0, "192.168.1.23" : 1, "192.168.1.44" : 2, "192.168.1.36" : 3, "192.168.1.53" : 4}
processing_time_table = {"resnet" : {"192.168.1.41" : 0.5, "192.168.1.23" : 1.3, "192.168.1.44" : 0.5, "192.168.1.36" : 0.5, "192.168.1.53" : 0.2}, 
"nginx" : {"192.168.1.41" : 0.1, "192.168.1.23" : 0.15, "192.168.1.44" : 0.1, "192.168.1.36" : 0.1, "192.168.1.53" : 0.07}, 
"nginxgpu" : {"192.168.1.41" : 0.1, "192.168.1.23" : 0.15, "192.168.1.44" : 0.1, "192.168.1.36" : 0.1, "192.168.1.53" : 0.07}, 
"hpt" : {"192.168.1.41" : 1, "192.168.1.23" : 1.5, "192.168.1.44" : 1, "192.168.1.36" : 1, "192.168.1.53" : 0.8}}
server_name_table = {"0" : "jetsonnanoone", "1" : "jetsonnanotwo", "2" : "jetsonnanothree", "3" : "jetsonnanofour", "4" : "jetsonagx"}

# creates a new deployment based on a YAML config file
def create_deployment(target):
    try:
        with open(path.join(path.dirname(__file__), ('deployment_files/' + target + "-deployment.yaml"))) as f:
            dep = yaml.safe_load(f)
            k8s_apps_v1 = client.AppsV1Api()
            resp = k8s_apps_v1.create_namespaced_deployment(
                body=dep, namespace=namespace)
            print("Deployment created. status='%s'" % resp.metadata.name)
            service_result = create_service(target)
            if service_result:
                return True
            else:
                delete_deployment(target)
                return False
    except Exception as e: 
        print(e)
        return False

# creates a new NodePort service based on a YAML config file 
# called by create_deployment
def create_service(target):
    print("Creating Service")
    try:
        with open(path.join(path.dirname(__file__), ('deployment_files/' + target + "-service.yaml"))) as f:
            dep = yaml.safe_load(f)
            k8s_apps_v1 = client.AppsV1Api()
            resp = api_instance.create_namespaced_service(body=dep, namespace=namespace)
            print("Service created. status='%s'" % resp.metadata.name)
        return True
    except Exception as e: 
        print(e)
        return False
    

# updates the number of replicas of the given service
def update_replicas(target, number):
    deployments = apps_api.list_namespaced_deployment(namespace, watch=False)
    for currentDeployment in deployments.items:
        if target in currentDeployment.metadata.name:
            currentDeployment.spec.replicas = int(number)
            apps_api.replace_namespaced_deployment(name=target + '-deployment', namespace=namespace, body=currentDeployment)
            return {target : 'Replica count updated to ' + str(number)}
    return jsonify({target : 'Deployment does not exist. No action taken'})

# deletes the deployment by the given name
def delete_deployment(target):
    deployments = apps_api.list_namespaced_deployment(namespace, watch=False)
    for currentDeployment in deployments.items:
        if target in currentDeployment.metadata.name:
            api_instance.delete_namespaced_service(name=target, namespace=namespace)
            apps_api.delete_namespaced_deployment(name=target + "-deployment", namespace=namespace, body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5))
            return {target : 'Deployment deleted'}    
    return jsonify({target : 'Deployment does not exist. No action taken'})

# gets the current usage of all nodes
def get_all_nodes_usage_metrics():
    return custom_obj_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")    

# gets the current usage of a specific node
def get_node_usage_metrics(node):
    return custom_obj_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes/" + node)    

def build_run_file(num_requests):
    try:
        with open("ampl_files/template.run","r") as template, open("ampl_files/solver_run.run", "w") as run_file:
            for line in template:
                if '<num_req>' in line:
                    run_file.write("print {i in 0.." + str(num_requests) + ", j in 0.." + str(len(server_ip_table)-1) + "}: probability[i,j] >> ampl_files/solver_results.txt;\n")
                else:
                    run_file.write(line)
    except Exception as e:
        print('Run File Creation Failed')
        return False
    return True

def build_model_file(num_models):
    try:
        with open("ampl_files/template.mod","r") as template, open("ampl_files/solver_model.mod", "w") as model_file:
            for line in template:
                if '<num_req>' in line:
                    model_file.write("set request := {0.." + str(num_models) + "};\n")
                elif '<num_models>' in line:
                    model_file.write("set mlmodel := {0.." + str(num_models) + "};\n")
                else:
                    model_file.write(line)
    except Exception as e:
        print('Model File Creation Failed')
        return False
    return True

def build_data_file(request_stats, available_memory):
    #get the request stats
    try:
        with open("ampl_files/template.dat", "r") as template, open("ampl_files/solver_data.dat", "w") as data_file:
            #memory copy
            for line in template:
                if '<start_mem_server>' in line:
                    data_file.write(str(0) + " " + str(available_memory['192.168.1.41']) + "\n")
                    data_file.write(str(1) + " " + str(available_memory['192.168.1.23']) + "\n")
                    data_file.write(str(2) + " " + str(available_memory['192.168.1.44']) + "\n")
                    data_file.write(str(3) + " " + str(available_memory['192.168.1.36']) + "\n")
                    data_file.write(str(4) + " " + str(available_memory['192.168.1.53']) + ";\n")
                #memory requirement
                elif '<start_mem_req>' in line:
                    for x in range(0, len(request_stats)):
                        data_file.write(str(x) + " " + str(memory_requirements_table[request_stats[str(x)]['model']]))
                        if x == len(request_stats)-1:
                            data_file.write(";\n")
                        else:
                            data_file.write("\n")
                #RTT
                elif '<begin_rtt>' in line:
                    for k in range(0, len(request_stats)):
                        curr_request = request_stats[str(k)]
                        if curr_request['server'] == "192.168.1.41":
                            data_file.write(str(k) + " " + str(curr_request['latency']) + " 9999 9999 9999 9999")
                        elif curr_request['server'] == "192.168.1.23":
                            data_file.write(str(k) + " 9999 " + str(curr_request['latency']) + " 9999 9999 9999")
                        elif curr_request['server'] == "192.168.1.44":
                            data_file.write(str(k) + " 9999 9999 " + str(curr_request['latency']) + " 9999 9999")
                        elif curr_request['server'] == "192.168.1.36":
                            data_file.write(str(k) + " 9999 9999 9999 " + str(curr_request['latency']) + " 9999")
                        elif curr_request['server'] == "192.168.1.53":
                            data_file.write(str(k) + " 9999 9999 9999 9999 " + str(curr_request['latency']))

                        if k == len(request_stats)-1:
                            data_file.write(";\n")
                        else:
                            data_file.write("\n")
                #try the processing time of the specific model requested on each server in each row (request)
                #so if request 5 is for resnet, then row 5 will be the exec time of resnet on all 5 servers
                elif '<begin_exec_time>' in line:
                    for y in range(0, len(request_stats)):
                        curr_request = request_stats[str(y)]
                        data_file.write(str(y) + " " + 
                        str(processing_time_table[curr_request['model']]['192.168.1.41']) + " " +
                        str(processing_time_table[curr_request['model']]['192.168.1.23']) + " " +
                        str(processing_time_table[curr_request['model']]['192.168.1.44']) + " " +
                        str(processing_time_table[curr_request['model']]['192.168.1.36']) + " " +
                        str(processing_time_table[curr_request['model']]['192.168.1.53']))

                        if y == len(request_stats)-1:
                            data_file.write(";\n")
                        else:
                            data_file.write("\n")
                else:
                    data_file.write(line)
    except Exception as e:
        print('Data File Creation Failed')
        return False      
    return True

def run_solver():
    #create the output file if it does not exist
    if path.exists('ampl_files/solver_results.txt'):
        pass
    else:
        open('ampl_files/solver_results.txt', 'x').close()

    try:
        #no need for AMPL to print to console, so redirect to /dev/null
        os.system('./ampl_files/ampl ampl_files/solver_run.run > /dev/null')
        return True
    except Exception as e:
        print('Solver Execution Failed')
    return False

def get_solver_results(request_count, server_count):
    solver_results = [[0] * server_count for _ in range(request_count)]  #create empty array
    with open('ampl_files/solver_results.txt', 'r') as file:
        for i in range(0, request_count):
            for j in range(0, server_count):
                curr_result = file.readline()
                curr_num = int(curr_result[0 : 1])
                solver_results[i][j] = curr_num
    file.close()
    #clear the file
    open('ampl_files/solver_results.txt', 'w').close()
    return solver_results

def check_model_available(model, server_name):
    pods = v1.list_namespaced_pod(namespace='deployed-services')
    for currentPod in pods.items:
        if model in currentPod.metadata.name and currentPod.spec.node_name == server_name:
            if currentPod.status.container_statuses is not None:
                return currentPod.status.container_statuses[0].ready
    return False

def generate_deployment_yaml(model, server_name):
    if path.exists('deployment_files/' + model + '-' + server_name + '-deployment.yaml'):
        return model + "-" + server_name
    else:
        try:
            with open('deployment_files/' + model + "-deployment-template.yaml","r") as template, open('deployment_files/' + model + "-" + server_name +"-deployment.yaml", "w") as deployment_file:
                for line in template:
                    if '<node-name>' in line:
                        deployment_file.write("      nodeName: " + server_name + "\n")
                    elif '<app-name>' in line:
                        deployment_file.write("      app: " + model + "-" + server_name + "\n")
                    elif '<app-name-template>' in line:
                        deployment_file.write("        app: " + model + "-" + server_name + "\n")
                    elif '<deployment-name>' in line:
                        deployment_file.write("  name: " + model + "-" + server_name + "-deployment" + "\n")
                    else:
                        deployment_file.write(line)
            deployment_file.close()
            generate_service_yaml(model, server_name)
            return model + "-" + server_name
        except Exception as e:
            print('Deployment File Creation Failed')
            return False

def generate_service_yaml(model, server_name):
    if path.exists('deployment_files/' + model + '-' + server_name + '-service.yaml'):
        return True
    else:
        try:
            with open('deployment_files/' + model + "-service-template.yaml","r") as template, open('deployment_files/' + model + "-" + server_name +"-service.yaml", "w") as service_file:
                for line in template:
                    if '<service-name>' in line:
                        service_file.write("  name: " + model + "-" + server_name + "\n")
                    elif '<deployment-name>' in line:
                        service_file.write("    app: " + model + "-" + server_name + "\n")
                    else:
                        service_file.write(line)
            service_file.close()
        except Exception as e:
            print('Service File Creation Failed')
            return False
    return False

def perform_provisioning(solver_results, request_stats):
    for i in range(0, len(solver_results)-1):
        curr_request = request_stats[str(i)]
        model = curr_request['model']
        for j in range(0, len(curr_request)-1):
            model_available = check_model_available(model, server_name_table[str(j)])
            if solver_results[i][j] == 1 and not model_available:
                print('ATTEMPTING PROVISIONING OF: ' + model)
                #deploy
                try:
                    deployment_filename = generate_deployment_yaml(model, server_name_table[str(j)])
                    create_deployment(deployment_filename)
                    done = False
                    
                    while(not done):
                        done = check_model_available(model, server_name_table[str(j)])

                    #return True
                except Exception as e:
                    print('ERROR WITH PROVISIONING DEPLOYMENT')
                    print(e)
                    raise
    return True

def check_provisioning():
    ready = True
    if not check_model_available("hpt", "jetsonnanothree"):
        ready = False
    if not check_model_available("resnet", "jetsonnanothree"):
        ready = False
    return ready