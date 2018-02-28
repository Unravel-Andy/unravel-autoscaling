# unravel-autoscaling
This is a demo script to scale up or down cluster based on cluster cpu and memory usage
### Prerequisites
Install ujson and requests using pip:

`$ pip install ujson`

`$ pip install requests`

Install Azure CLI 1.0 (Azure CLI 2.0 does not support HDinsight cluster) [Click Link to see installlation insturction](https://docs.microsoft.com/en-us/azure/cli-install-nodejs)

After install Azure CLI 1.0 Run the following command to login:

`$ azure login`

Once you login azure you sure able to see existing HDinsight clusters using:

`$ azure hdinsight cluster list`

open azure.py and edit these variables:

**unravel_base_url** e.g. 'http://localhost:3000'

**memory_threshold** e.g. 80; scale up when memory_usage reach 80%

**cpu_threshold**  e.g. 10; scale up when cpu_usage reach 10%             

**min_nodes**      e.g. 4; initial number of worder nodes in the cluster

**max_nodes**      e.g. 10; max worker nodes user want to scale up to

**resource_group** e.g. 'UNRAVEL01'

**cluster_name**  e.g. 'estspk2rh75'

cluster name and resource group name can be retrieve using azure command:

`$ azure hdinsight cluster list`

![info](azure-autoscaling.png)

### Run auto scaling

Run the script:

`python azure.py`