try:
    import requests
except Exception as e:
    print(e)
    print('requests module is missing')
try:
    import ujson
except Exception as e:
    print(e)
    print('ujson module is missing')
from time import sleep
import subprocess
#############################################################
#                                                           #
#   Modify the variables below                              #
#                                                           #
#############################################################

unravel_base_url = 'http://localhost:3000'
memory_threshold = 80              #%
cpu_threshold = 10                 #%
min_nodes = 4                      # Initial workerNodes
max_nodes = 5                      # Max workernodes Allowed
resource_group = 'UNRAVEL01'
cluster_name = 'estspk2rh74'

#Unravel Log in credentials
login_data = {'user':{'login':'admin','password':'unraveldata'}}


#############################################################
#                                                           #
#   DO NOT Modify the variables below                       #
#                                                           #
#############################################################
try:
    login_uri = unravel_base_url + '/users/sign_in'
    app_search_uri = unravel_base_url + '/api/v1/apps/search'
    total_cores_across_hosts = unravel_base_url + '/api/v1/clusters/resources/cpu/total'
    total_memory_across_hosts = unravel_base_url + '/api/v1/clusters/resources/memory/total'
    allocated_cores_across_hosts = unravel_base_url + '/api/v1/clusters/resources/cpu/allocated'
    allocated_memory_across_hosts = unravel_base_url + '/api/v1/clusters/resources/memory/allocated'
except:
    print("Unravel Url is not exist")
    exit()

threshold_count_limit = 5

s = requests.Session()

def check_login():
    try:
        response = s.post(login_uri,json=login_data)
        # res = s.post(cdh_login, data=cdh_secret)
    except Exception as e:
        print(e)

    if response.status_code == 200:
        return True
    else:
        return False


def check_threshold(threshold_count, resources_usage):
    cpu_usage = resources_usage['cpu_usage']
    memory_usage = resources_usage['memory_usage']
    total_cores = resources_usage['total_cores']
    total_memory = resources_usage['total_memory']
    nodes_count = resources_usage['nodes_count']
    if cpu_usage > cpu_threshold or memory_usage > memory_threshold:
        if threshold_count < threshold_count_limit:
            return ('Up Scale threshold reach')
        # if threshold_count >= threshold_count_limit and (total_cores < max_cpu_allow or total_memory < max_memory_allow):
        if threshold_count >= threshold_count_limit and nodes_count < max_nodes:
            return ('Up Scaling')
    else:
        # if threshold_count > -threshold_count_limit and (total_cores > min_cpu_allow or total_memory > min_memory_allow):
        if threshold_count > -threshold_count_limit and nodes_count > min_nodes:
            return ('Down Scale threshold reach')
        if threshold_count <= -threshold_count_limit :
            return ('Down Scaling')

# Get Cluster Workdernode Count
def get_workdernode():
    json = subprocess.check_output(['azure', 'hdinsight', 'cluster', 'show', '-g', resource_group, '-c', cluster_name, '--json'])
    # json = subprocess.check_output(['azure', 'hdinsight', 'cluster', 'list', '--json'])
    cluster = ujson.loads(json)
    if cluster['name'] == cluster_name:
		if cluster['properties']['computeProfile']['roles'][1]['name'] == 'workernode':
			workerNodes = cluster['properties']['computeProfile']['roles'][1]['targetInstanceCount']
    return workerNodes


# Retrieve Allocated resources
def get_resources():
    res = s.post(login_uri, json=login_data)

    try:
        # Current cpu percentage
        res = s.get(total_cores_across_hosts)
        res1 = s.get(allocated_cores_across_hosts)
        total_cores = float(ujson.loads(res.text)[-1]['avg_totalvc'])
        cores_allocated = float(ujson.loads(res1.text)[-1]['avg_allocatedvcores'])
        cpu_percent_usage = cores_allocated / total_cores  * 100

        # Current memory percentage and total_memory
        res = s.get(total_memory_across_hosts)
        res1 = s.get(allocated_memory_across_hosts)
        total_memory = float(ujson.loads(res.text)[-1]['avg_totalmb'])
        memory_allocated = float(ujson.loads(res1.text)[-1]['avg_allocatedmb'])
        memory_percent_usage = memory_allocated / total_memory  * 100

        # Get the number of workerNodes in cluster
        nodes_count = get_workdernode()

        return {'cpu_usage' : cpu_percent_usage,
                         'memory_usage' : memory_percent_usage,
                         'total_memory': total_memory,
                         'total_cores': total_cores,
                         'nodes_count' : nodes_count
                }
    except Exception as e:
            print(e)
            exit()


# Retrieve Running Jobs
def get_run():
    search_input = {
                    "appStatus":["R"],
                    "from":0,
                    "appTypes":["mr","hive","spark","cascading","pig"]
                   }

    try:
        response = s.post(login_uri,json=login_data)
        res = s.post(app_search_uri,json=search_input)
    except requests.exceptions.RequestException as e:
        print("Unable to connect to Unravel Server\n")
        print(e)
        exit()

    if res.status_code == 200:
            jobs_dict = {}
            parsed = ujson.loads(res.text)
            if parsed:
                # print("Running apps:")
                for job_num in range(len(parsed['results'])):
                    duration = parsed['results'][job_num]['duration_long']
                    app_id = parsed['results'][job_num]['id'].encode('utf-8')
                    # print(app_id,duration,len(parsed['results']))
                    jobs_dict[app_id] = duration
                # print(ujson.dumps(parsed, indent=4, sort_keys=True))
                return (jobs_dict)
    else:
            print("Search endpoint error")


def main():
    threshold_count = 0
    while True:
        if check_login:
            jobs_dict = get_run()
            resources_usage = get_resources()
            print(resources_usage)
            # print(jobs_dict)
            if jobs_dict:
                decision = check_threshold(threshold_count, resources_usage)
                print("decision: ", decision, "threshold_count: " + str(threshold_count), 'Workdernode: '+ str(resources_usage['nodes_count']))
                if decision == 'Up Scale threshold reach':
                    print('Threshold reach')
                    threshold_count += 1
                elif decision == 'Up Scaling':
                    print('More Resources Needed')
                    #Adding more resources command goes here

                    resizing = subprocess.Popen(['azure', 'hdinsight', 'cluster', 'resize', '-g', resource_group, '-c', cluster_name, str(resources_usage['nodes_count']+1)],stdout=subprocess.PIPE, bufsize=1)

                    for line in iter(resizing.stdout.readline, b''):
                        if (line.find('Operation state:  Succeeded') > -1):
                        	resizing_err = 1
                        print (line)
                    resizing.stdout.close()
                    resizing.wait()

                    if resizing_err:
                    	print('Resizing Success')
                    else:
                    	print('Resizing Fail')

                    #Adding more resources command goes here
                    threshold_count = 0
                elif decision == 'Down Scale threshold reach':
                    threshold_count -= 1
                elif decision == 'Down Scaling':
                    print('No Extra Resources Needed')
                    #Removing extra resources command goes here

                    resizing = subprocess.Popen(['azure', 'hdinsight', 'cluster', 'resize', '-g', resource_group, '-c', cluster_name, str(resources_usage['nodes_count']-1)],stdout=subprocess.PIPE, bufsize=1)

                    for line in iter(resizing.stdout.readline, b''):
                        if (line.find('Operation state:  Succeeded') > -1):
                        	resizing_err = 1
                        print (line)
                    resizing.stdout.close()
                    resizing.wait()

                    if resizing_err:
                    	print('Resizing Success')
                    else:
                    	print('Resizing Fail')

                    #Removing extra resources command goes here
                    threshold_count = 0
            else:
                print('No Jobs Running Extra Resources Can be Removed')
        else:
            print('Login Fail')

        sleep(60)

if __name__ == '__main__':
    main()
