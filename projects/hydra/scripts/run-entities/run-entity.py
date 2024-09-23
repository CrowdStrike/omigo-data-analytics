from omigo_hydra import cluster_services_v2
import sys

entity_type = sys.argv[1]

# if-else
if (entity_type == "master"):
    cluster_services_v2.run_master()
elif (entity_type == "resource_manager"):
    cluster_services_v2.run_resource_manager()
elif (entity_type == "job_manager"):
    cluster_services_v2.run_job_manager()
elif (entity_type == "task_manager"):
    cluster_services_v2.run_task_manager()
elif (entity_type == "swf_manager"):
    cluster_services_v2.run_swf_manager()
elif (entity_type == "wf_manager"):
    cluster_services_v2.run_wf_manager()
elif (entity_type == "agent"):
    cluster_services_v2.run_agent()
elif (entity_type == "worker"):
    cluster_services_v2.run_worker()
else:
    raise Exception("Invalid entity: {}".format(entity_type))
