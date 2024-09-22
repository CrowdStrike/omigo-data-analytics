from omigo_core import tsv, utils, tsvutils, funclib
import json
import time
import math

from omigo_hydra import cluster_common
from omigo_hydra import cluster_protocol
from omigo_hydra import cluster_ext
from dateutil import parser
import datetime

def list_dirs(path):
    for f in get_cluster_handler().list_dirs(path):
        if (path != ""):
            print("{}/{}".format(path, f))
        else:
            print(f)

def get_dirs(path):
    dirs = []
    for f in get_cluster_handler().list_dirs(path):
        if (path != ""):
            dirs.append("{}/{}".format(path, f))
        else:
            dirs.append(f)

    return dirs

def print_paths(path):
    for f in get_cluster_handler().list_all_recursive(path):
        if (path != ""):
            print("{}/{}".format(path, f))
        else:
            print(f)

def print_all_paths():
    for d in sorted(get_dirs(""), reverse = True):
        print_paths(d)

master_protocol = None
def get_master_protocol():
    global master_protocol
    if (master_protocol is not None):
        return master_protocol

    cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
    master_id = cluster_common.ClusterPaths.get_master_id()
    if (cluster_common.ClusterPaths.get_cluster_handler().dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.MASTER, master_id))):
        master_protocol = cluster_protocol.ClusterMasterProtocol(cluster_common.ClusterMaster.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(cluster_common.ClusterPaths.MASTER, master_id))))
    else:
        master_protocol = cluster_protocol.ClusterMasterProtocol(cluster_common.ClusterMaster.new_create_token(cluster_common.ClusterPaths.MASTER, master_id))

    return master_protocol

client_protocol = None
def get_client_protocol():
    global client_protocol
    if (client_protocol is not None):
        return client_protocol 

    cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
    client_id = cluster_common.ClusterPaths.get_client_id()
    if (cluster_common.ClusterPaths.get_cluster_handler().dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.CLIENT, client_id))):
        client_protocol = cluster_protocol.ClusterClient(cluster_common.ClusterClient.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(cluster_common.ClusterPaths.CLIENT, client_id))))
    else:
        client_protocol = cluster_protocol.ClusterClientProtocol(cluster_common.ClusterClient.new_create_token(cluster_common.ClusterPaths.CLIENT, client_id))

    return client_protocol

session_protocol = None
def get_session_protocol():
    global session_protocol
    if (session_protocol is not None):
        return session_protocol

    cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
    session_id = cluster_common.ClusterPaths.get_session_id()
    if (cluster_common.ClusterPaths.get_cluster_handler().dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.SESSION, session_id))):
        session_protocol = cluster_protocol.ClusterSessionProtocol(cluster_common.ClusterSession.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(cluster_common.ClusterPaths.SESSION, session_id))))
    else:
        session_protocol = cluster_protocol.ClusterSessionProtocol(cluster_common.ClusterSession.new_create_token(cluster_common.ClusterPaths.SESSION, session_id))

    return session_protocol

manager_protocol = None
def get_manager_protocol():
    global manager_protocol
    if (manager_protocol is not None):
        return manager_protocol

    cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
    manager_id = cluster_common.ClusterPaths.get_manager_id()
    if (cluster_common.ClusterPaths.get_cluster_handler().dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.MANAGER, manager_id))):
        manager_protocol = cluster_protocol.ClusterManagerProtocol(cluster_common.ClusterManager.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(cluster_common.ClusterPaths.MANAGER, manager_id))))
    else:
        manager_protocol = cluster_protocol.ClusterManagerProtocol(cluster_common.ClusterManager.new_create_token(cluster_common.ClusterPaths.MANAGER, manager_id))

    return manager_protocol

worker_protocol = None
def get_worker_protocol():
    global worker_protocol
    if (worker_protocol is not None):
        return worker_protocol

    cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
    worker_id = cluster_common.ClusterPaths.get_worker_id()
    if (cluster_common.ClusterPaths.get_cluster_handler().dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.WORKER, worker_id))):
        worker_protocol = cluster_protocol.ClusterWorkerProtocol(cluster_common.ClusterWorker.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(cluster_common.ClusterPaths.WORKER, worker_id))))
    else:
        worker_protocol = cluster_protocol.ClusterWorkerProtocol(cluster_common.ClusterWorker.new_create_token(cluster_common.ClusterPaths.WORKER, worker_id))

    return worker_protocol

def get_cluster_handler():
    return cluster_common.ClusterPaths.get_cluster_handler()

def construct_id(name, suffix, ts):
    return "{}{}-{}".format(name, suffix, ts)

ID_SUFFIX = "1"
def set_ids(suffix):
    global ID_SUFFIX
    ID_SUFFIX = suffix

    # add the timestamp to the entity id
    ts = funclib.get_utctimestamp_sec()

    cluster_common.ClusterPaths.set_master_id(construct_id("master_id", suffix, ts))
    cluster_common.ClusterPaths.set_manager_id(construct_id("manager_id", suffix, ts))
    cluster_common.ClusterPaths.set_client_id(construct_id("client_id", suffix, ts))
    cluster_common.ClusterPaths.set_session_id(construct_id("session_id", suffix, ts))
    cluster_common.ClusterPaths.set_worker_id(construct_id("worker_id", suffix, ts))

def init():
    # this is needed to start the cluster
    master_protocol = get_master_protocol()
 
    # start, run election, initialize cluster
    master_protocol.__start__()
    master_protocol.initialize_cluster()
    master_protocol.initialize_candidate()
    master_protocol.initialize()
    master_protocol.run_election()

def run1():
    global ID_SUFFIX
    # create all entities 
    master = cluster_common.ClusterMaster.new_create_token(cluster_common.ClusterPaths.MASTER, cluster_common.ClusterPaths.get_master_id())
    manager = cluster_common.ClusterManager.new_create_token(cluster_common.ClusterPaths.MANAGER, cluster_common.ClusterPaths.get_manager_id())
    client = cluster_common.ClusterClient.new_create_token(cluster_common.ClusterPaths.CLIENT, cluster_common.ClusterPaths.get_client_id())
    session = cluster_common.ClusterSession.new_create_token(cluster_common.ClusterPaths.SESSION, cluster_common.ClusterPaths.get_session_id())
    worker = cluster_common.ClusterWorker.new_create_token(cluster_common.ClusterPaths.WORKER, cluster_common.ClusterPaths.get_worker_id())

    # create all protocols
    master_protocol = cluster_protocol.ClusterMasterProtocol(master)
    manager_protocol = cluster_protocol.ClusterManagerProtocol(manager)
    client_protocol = cluster_protocol.ClusterClientProtocol(client)
    session_protocol = cluster_protocol.ClusterSessionProtocol(session)
    worker_protocol = cluster_protocol.ClusterWorkerProtocol(worker)

    # intialize the entities
    # master_protocol.initialize() # called in init
    manager_protocol.initialize()
    worker_protocol.initialize()
    client_protocol.initialize()
    session_protocol.initialize()

def run2():
    master_protocol = get_master_protocol()
    manager_protocol = get_manager_protocol()

    # update active
    master_protocol.update_active(cluster_common.ClusterPaths.MASTER)
    master_protocol.update_active(cluster_common.ClusterPaths.MANAGER)
    master_protocol.update_active(cluster_common.ClusterPaths.CLIENT)
    master_protocol.update_active(cluster_common.ClusterPaths.SESSION)
    master_protocol.update_active(cluster_common.ClusterPaths.WORKER)
   
    # update_registered
    master_protocol.update_registered(cluster_common.ClusterPaths.MASTER)
    master_protocol.update_registered(cluster_common.ClusterPaths.MANAGER)
    master_protocol.update_registered(cluster_common.ClusterPaths.CLIENT)
    master_protocol.update_registered(cluster_common.ClusterPaths.SESSION)
    master_protocol.update_registered(cluster_common.ClusterPaths.WORKER)

#def run3():
    # session_protocol = get_session_protocol()
    # job = TestData.create_job(cluster_common.ClusterPaths.get_client_id(), cluster_common.ClusterPaths.get_session_id(), "jobname", "func1", "job_id{}".format(ID_SUFFIX))
    # job_protocol = cluster_protocol.ClusterJobProtocol(job)
    # session_protocol.create_job(job)
    # master_protocol = get_master_protocol()

def run4(count = 1, wait_sec = 30):
    master_protocol = get_master_protocol()
    manager_protocol = get_manager_protocol()
    worker_protocol = get_worker_protocol()

    # iterate
    for i in range(count):
        master_protocol.process_jobs()
        master_protocol.process_managers()
        master_protocol.process_workers()
        manager_protocol.process_jobs()
        worker_protocol.process_jobs()
        manager_protocol.monitor_jobs()
        manager_protocol.monitor_dead_workers()
        master_protocol.cleanup_candidates()
        master_protocol.run_election()
        # master_protocol.recover_jobs_failsafe()

        utils.info("run4: {} sleeping for {} seconds".format(i + 1, wait_sec))
        time.sleep(wait_sec)

        # run cleanup only once in a while
        # if (i % 5 == 0):
        #    run4_cleanup()
  
def run4_cleanup():
    manager_protocol = get_manager_protocol()
    master_protocol = get_master_protocol()

    # run cleanup once in a while
    utils.info("run4: running cleanup")
    manager_protocol.cleanup_jobs()
    master_protocol.cleanup_jobs()
    master_protocol.process_cleanup()

def run5(count = 1):
    for i in range(count):
        x1 = tsv.read("https://github.com/CrowdStrike/omigo-data-analytics/raw/main/data/iris.tsv") \
            .extend_class(cluster_ext.ClusterExecutor) \
                .custom_func(tsv.TSV.regex_match, "class", ".*a") \
                .custom_func(tsv.TSV.gt_float, "petal_width", 2.0)
        print(json.dumps(x1.to_json()))
        x1.collect()

def remove_paths(path):
    get_cluster_handler().remove_dir_recursive(path, ignore_missing = True)

def read_json(path):
    return get_cluster_handler().read_json(path)

def read_most_recent_json(path):
    return get_cluster_handler().read_most_recent_json(path)

def read(path):
    return get_cluster_handler().read(path)

def remove_all_paths():
    for d in sorted(get_dirs(""), reverse = True):
        remove_paths(d)

    remove_paths("")

def init_client_session():
    client_protocol = get_client_protocol()
    session_protocol = get_session_protocol()
    client_protocol.initialize()
    session_protocol.initialize()

def get_job_data(job_id):
    # read job parameters
    cluster_handler = get_cluster_handler()
    # job = cluster_common.ClusterJob.from_json(cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))

    # check completion status
    if (cluster_handler.dir_exists(cluster_common.ClusterPaths.get_job_statuses_completed(job_id)) and cluster_handler.file_exists(cluster_common.ClusterPaths.get_data_job_output(job_id))):
        # debug
        utils.info("get_job_data: job_id: {} completed".format(job_id))

        # if completed, read the final output
        return cluster_common.TSVReference(cluster_handler.read_tsv(cluster_common.ClusterPaths.get_data_job_output(job_id))).read()
    else:
        # defensive programming
        if (cluster_handler.dir_exists(cluster_common.ClusterPaths.get_job_batches_statuses_incoming(job_id))):
            # if still running, check the completion status of the batches, and then read partial output
            batch_ids = cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_incoming(job_id))
            completed_batch_ids = cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id))

            # debug
            utils.info("get_job_data: job_id: {} is not completed yet. Status: {} / {}".format(job_id, len(completed_batch_ids), len(batch_ids)))

            # read partial output
            xtsvs = []
            for batch_id in completed_batch_ids:
                xtsvs.append(cluster_handler.read_tsv(cluster_common.ClusterPaths.get_data_job_batch_output(job_id, batch_id)))

            # return
            if (len(xtsvs) > 0):
                return tsvutils.merge(xtsvs)
            else:
                return None
        else:
            utils.info("get_job_data: job_id: {} is still initializing".format(job_id))
            return None
