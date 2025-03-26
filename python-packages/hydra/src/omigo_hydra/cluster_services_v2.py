from omigo_hydra import cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra import cluster_tsv
from omigo_hydra import cluster_arjun
from omigo_hydra import cluster_funcs
from omigo_hydra import cluster_shell_ext
from omigo_hydra.cluster_common_v2 import EntityType
from omigo_hydra.cluster_common_v2 import EntityState
from omigo_hydra.cluster_common_v2 import ClusterCapabilities
from omigo_hydra.cluster_common_v2 import ClusterPaths
from omigo_core import utils, timefuncs, etl
from omigo_ext import splunk_ext
# utils.enable_debug_mode()
import time
from omigo_core import tsv
import json
import datetime

# TODO: s3 and local are differing in deletion of all paths

ID_REF = None

# Entities
MASTER = None
RESOURCE_MANAGER = None
SWF_MANAGER = None
WF_MANAGER = None
JOB_MANAGER = None
TASK_MANAGER = None
WORKER = None
AGENT = None
DOUBLE_AGENT = None
INTELI_AGENT = None
CLIENT = None
SESSION = None
SWF = None
WF = None
JOB = None
TASK = None

# Protocols
MASTER_PROTOCOL = None
RESOURCE_MANAGER_PROTOCOL = None
SWF_MANAGER_PROTOCOL = None
WF_MANAGER_PROTOCOL = None
JOB_MANAGER_PROTOCOL = None
TASK_MANAGER_PROTOCOL = None
WORKER_PROTOCOL = None
AGENT_PROTOCOL = None
DOUBLE_AGENT_PROTOCOL = None
INTELI_AGENT_PROTOCOL = None
CLIENT_PROTOCOL = None
SESSION_PROTOCOL = None
SWF_PROTOCOL = None
WF_PROTOCOL = None
JOB_PROTOCOL = None
TASK_PROTOCOL = None

# Cluster Management Protocols
ALL_PROTOCOLS = None
CLUSTER_ADMIN = None
MASTER_ELECTION_PROTOCOL = None

def init(value):
    global ID_REF
    global MASTER
    global RESOURCE_MANAGER
    global SWF_MANAGER
    global WF_MANAGER
    global JOB_MANAGER
    global TASK_MANAGER
    global WORKER
    global AGENT
    global DOUBLE_AGENT
    global INTELI_AGENT
    global CLIENT
    global SESSION
    global SWF
    global WF
    global JOB
    global TASK

    global MASTER_PROTOCOL
    global RESOURCE_MANAGER_PROTOCOL
    global SWF_MANAGER_PROTOCOL
    global WF_MANAGER_PROTOCOL
    global JOB_MANAGER_PROTOCOL
    global TASK_MANAGER_PROTOCOL
    global WORKER_PROTOCOL
    global AGENT_PROTOCOL
    global DOUBLE_AGENT_PROTOCOL
    global INTELI_AGENT_PROTOCOL
    global CLIENT_PROTOCOL
    global SESSION_PROTOCOL
    global SWF_PROTOCOL
    global WF_PROTOCOL
    global JOB_PROTOCOL
    global TASK_PROTOCOL

    global ALL_PROTOCOLS
    global CLUSTER_ADMIN
    global MASTER_ELECTION_PROTOCOL

    ts = timefuncs.get_utctimestamp_sec()
    cluster_common_v2.ClusterIds.set_id(value)

    # Entities
    MASTER             = cluster_common_v2.ClusterEntityMaster.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.MASTER))
    RESOURCE_MANAGER   = cluster_common_v2.ClusterEntityResourceManager.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.RESOURCE_MANAGER))
    SWF_MANAGER        = cluster_common_v2.ClusterEntitySWFManager.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.SWF_MANAGER))
    WF_MANAGER         = cluster_common_v2.ClusterEntityWFManager.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.WF_MANAGER))
    JOB_MANAGER        = cluster_common_v2.ClusterEntityJobManager.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.JOB_MANAGER))
    TASK_MANAGER       = cluster_common_v2.ClusterEntityTaskManager.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.TASK_MANAGER))
    WORKER             = cluster_common_v2.ClusterEntityWorker.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.WORKER))
    AGENT              = cluster_common_v2.ClusterEntityAgent.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.AGENT))
    DOUBLE_AGENT       = cluster_common_v2.ClusterEntityDoubleAgent.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.DOUBLE_AGENT))
    INTELI_AGENT       = cluster_common_v2.ClusterEntityInteliAgent.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.INTELI_AGENT))
    CLIENT             = cluster_common_v2.ClusterEntityClient.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.CLIENT))
    SESSION            = cluster_common_v2.ClusterEntitySession.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.SESSION))

    # Protocols
    MASTER_PROTOCOL = cluster_protocol_v2.ClusterMasterProtocol(MASTER)
    RESOURCE_MANAGER_PROTOCOL = cluster_protocol_v2.ClusterResourceManagerProtocol(RESOURCE_MANAGER)
    SWF_MANAGER_PROTOCOL = cluster_protocol_v2.ClusterSWFManagerProtocol(SWF_MANAGER)
    WF_MANAGER_PROTOCOL  = cluster_protocol_v2.ClusterWFManagerProtocol(WF_MANAGER)
    JOB_MANAGER_PROTOCOL = cluster_protocol_v2.ClusterJobManagerProtocol(JOB_MANAGER)
    TASK_MANAGER_PROTOCOL = cluster_protocol_v2.ClusterTaskManagerProtocol(TASK_MANAGER)
    WORKER_PROTOCOL = cluster_protocol_v2.ClusterWorkerProtocol(WORKER)
    AGENT_PROTOCOL = cluster_protocol_v2.ClusterAgentProtocol(AGENT)
    DOUBLE_AGENT_PROTOCOL = cluster_protocol_v2.ClusterDoubleAgentProtocol(DOUBLE_AGENT)
    INTELI_AGENT_PROTOCOL = cluster_protocol_v2.ClusterInteliAgentProtocol(INTELI_AGENT)
    CLIENT_PROTOCOL = cluster_protocol_v2.ClusterClientProtocol(CLIENT)
    SESSION_PROTOCOL = cluster_protocol_v2.ClusterSessionProtocol(SESSION)
    # SWF_PROTOCOL = cluster_protocol_v2.ClusterSWFProtocol(SWF)
    # WF_PROTOCOL = cluster_protocol_v2.ClusterWFProtocol(WF)
    # JOB_PROTOCOL = cluster_protocol_v2.ClusterJobProtocol(JOB)
    # TASK_PROTOCOL = cluster_protocol_v2.ClusterTaskProtocol(TASK)

    ALL_PROTOCOLS = [MASTER_PROTOCOL, RESOURCE_MANAGER_PROTOCOL, SWF_MANAGER_PROTOCOL, WF_MANAGER_PROTOCOL, JOB_MANAGER_PROTOCOL, TASK_MANAGER_PROTOCOL, WORKER_PROTOCOL, AGENT_PROTOCOL,
        DOUBLE_AGENT_PROTOCOL, INTELI_AGENT_PROTOCOL, CLIENT_PROTOCOL, SESSION_PROTOCOL]
        # SWF_PROTOCOL, WF_PROTOCOL, JOB_PROTOCOL, TASK_PROTOCOL]

    # admin and master election
    CLUSTER_ADMIN = cluster_protocol_v2.ClusterAdmin()
    MASTER_ELECTION_PROTOCOL = cluster_protocol_v2.ClusterMasterElectionProtocol(MASTER.entity_id)

# Run1 with initialization
def run1():
    for p in ALL_PROTOCOLS:
        p.initialize()

    # run election and refresh cache if needed
    if (MASTER_ELECTION_PROTOCOL.run_election()):
        MASTER_PROTOCOL.refresh_master_cache()

# Run1 with initialization
def run1_client():
    SESSION_PROTOCOL.initialize()
    CLIENT_PROTOCOL.initialize()

# Run2 with supervisor assignment and monitoring
def run2(n = 10, wait_sec = 5):
    for i in range(n):
        print("Running iteration: {}".format(i+1))
        for p in ALL_PROTOCOLS:
            if (p.entity.entity_type == cluster_common_v2.EntityType.MASTER):
                if (MASTER_ELECTION_PROTOCOL.run_election()):
                    MASTER_PROTOCOL.refresh_master_cache()
                # monitor supervisor
                p.monitor_incoming_for_supervisor()

            # run active child supervisors
            if (p.entity.entity_type in cluster_common_v2.EntityActiveSupervisorTypes):
                p.monitor_active_children()

            # run passive child supervisors
            if (p.entity.entity_type in cluster_common_v2.EntityPassiveSupervisorTypes):
                p.monitor_passive_children()

            # run tasks
            if (p.entity.entity_type in cluster_common_v2.EntityExecutorTypes):
                p.monitor_execution_tasks()

        utils.info("Sleeping for {} seconds".format(wait_sec))
        time.sleep(wait_sec)

# Debugging
def get_cluster_handler():
    return ClusterPaths.get_cluster_handler()

def get_local_cluster_handler():
    return ClusterPaths.get_local_cluster_handler()

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
    for f in sorted(get_cluster_handler().list_all_recursive(path)):
        if (path != ""):
            print("{}/{}".format(path, f))
        else:
            print(f)

def print_all_paths():
    for d in sorted(ClusterPaths.get_base_paths(), reverse = True):
        print("print_all_paths: {}".format(d))
        print_paths(d)

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

def read_tsv(path):
    return get_cluster_handler().read_tsv(path)

def write_tsv(path, xtsv):
    return get_cluster_handler().write_tsv(path, xtsv)

def read_workflow_input(wf_id, max_duration = 3*86400, sleep_sec = 3):
    # output file
    input_file = cluster_common_v2.ClusterPaths.get_entity_data_input_file(cluster_common_v2.EntityType.WF, wf_id, "input_id1", 0)

    # return
    return read_workflow_file_path(input_file, max_duration = max_duration, sleep_sec = sleep_sec)

def read_workflow_output(wf_id, max_duration = 3600, sleep_sec = 3):
    # output file
    output_file = cluster_common_v2.ClusterPaths.get_entity_data_output_file(cluster_common_v2.EntityType.WF, wf_id, "output_id1", 0)

    # return
    return read_workflow_file_path(output_file, max_duration = max_duration, sleep_sec = sleep_sec)

def read_workflow_file_path(file_path, max_duration = 3*86400, sleep_sec = 3):
    # check if the file exists, and run in a loop with sleep
    duration = 0
    found = False

    # run in a loop
    utils.info("Waiting for the file to be present: {} ...".format(file_path))
    while (duration < max_duration):
      if (get_cluster_handler().file_exists(file_path) == True):
          time.sleep(sleep_sec)
          found = True
          break
      else:
          utils.trace("File: {} doesnt exists yet. Sleeping for {} seconds".format(file_path, sleep_sec))
          time.sleep(sleep_sec)
          duration = duration + sleep_sec

    # check if file was found
    if (found == True):
        try:
            xtsv = read_tsv(file_path)
        except Exception as e:
            utils.warn("Caught exception in reading file. Sleeping for {} seconds".format(sleep_sec))
            time.sleep(sleep_sec)
            xtsv = read_tsv(file_path)

        utils.info("Read file: {}, num_rows: {}".format(file_path, xtsv.num_rows()))
        return xtsv
    else:
        raise Exception("File not found: {}".format(file_path))

def scan_by_datetime_range(path, start_date_str, end_date_str, prefix):
    return etl.scan_by_datetime_range(get_cluster_handler().get_full_path(path), start_date_str, end_date_str, prefix)

#######################################################################################################################
# Script execution
#######################################################################################################################
def get_created_non_created_wf_ids(local_handler_flag = True, capabilities = []):
    # result
    created_wf_ids = []
    alive_wf_ids = []
    non_created_wf_ids = []
    capabilities = set(capabilities)
    handler = get_local_cluster_handler() if (local_handler_flag == True) else get_cluster_handler()

    # iterate over all states
    for state in cluster_common_v2.EntityState.get_all():
        # get all workflow ids
        ids = handler.list_dirs(ClusterPaths.get_entities_state_by_state(EntityType.WF, state))

        # read each workflow filter the workflows based on the capabilities matching requirements
        ids_allowed = []
        for id1 in ids:
            # read wf
            wf = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(ClusterPaths.get_entity(EntityType.WF, id1)))
            requirements = set(wf.collect_requirements())

            # check if all requirements are met
            if (len(capabilities) == 0 or len(requirements.difference(capabilities)) == 0):
                ids_allowed.append(id1)

        # check for created
        if (state in (EntityState.CREATED)):
            for id1 in ids_allowed:
                created_wf_ids.append(id1)

        # check for alive
        if (state in (EntityState.ALIVE)):
            for id1 in ids_allowed:
                alive_wf_ids.append(id1)

        # check for finished.
        if (state in (EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED)):
            for id1 in ids_allowed:
                non_created_wf_ids.append(id1)

    # debug
    utils.debug("get_created_non_created_wf_ids: created_wf_ids: {}, alive_wf_ids: {}, non_created_wf_ids: {}".format(created_wf_ids, alive_wf_ids, non_created_wf_ids))

    # effective ids
    effective_created_wf_ids = set(created_wf_ids).difference(set(non_created_wf_ids))
    effective_alive_wf_ids = set(alive_wf_ids).difference(set(non_created_wf_ids))
    effective_non_created_wf_ids = set(non_created_wf_ids)

    # return set difference
    return effective_created_wf_ids, effective_alive_wf_ids, effective_non_created_wf_ids

def print_created_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = False, capabilities = capabilities)
    for id1 in created_wf_ids:
        print(id1)

def print_alive_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = False, capabilities = capabilities)
    for id1 in alive_wf_ids:
        print(id1)

def print_non_created_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = False, capabilities = capabilities)
    for id1 in non_created_wf_ids:
        print(id1)

def print_local_created_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = True, capabilities = capabilities)
    for id1 in created_wf_ids:
        print(id1)

def print_local_alive_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = True, capabilities = capabilities)
    for id1 in alive_wf_ids:
        print(id1)

def print_local_non_created_wf_ids(capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(local_handler_flag = True, capabilities = capabilities)
    for id1 in non_created_wf_ids:
        print(id1)

# methods for external task execution
def print_shell_executor_scripts(wf_id):
    # read wf spec
    wf = cluster_common_v2.ClusterEntityWF.from_json(get_cluster_handler().read_most_recent_json(ClusterPaths.get_entity(EntityType.WF, wf_id)))

    # create wf protocol
    wf_protocol = cluster_protocol_v2.ClusterWFProtocol(wf)

    # read input. TODO: Use ctx api
    input_id = wf.entity_spec.input_ids[0]
    output_id = wf.entity_spec.output_ids[0]
    file_index = 0
    xinput = get_cluster_handler().read_tsv(ClusterPaths.get_entity_data_input_file(EntityType.WF, wf_id, input_id, file_index))

    # resolve the meta parameters for external task
    xinput_resolved = wf_protocol.resolve_external_task_meta_params(xinput, input_id, output_id, file_index)
    operations = wf_protocol.execute_single_round_get_operations(wf.entity_spec)

    # generate output
    xoutput = wf_protocol.execute_single_round(operations, xinput_resolved)

    # print the script
    print(utils.url_decode(xoutput.col_as_array("shell:output:url_encoded")[0]))

def print_wf_requirements(local_handler_flag = False):
    # get handler
    handler = get_local_cluster_handler() if (local_handler_flag == True) else get_cluster_handler()

    # get all wfs
    wf_ids = sorted(handler.list_dirs(ClusterPaths.get_entities_ids(EntityType.WF)))

    # iterate
    for wf_id in wf_ids:
        # use protocol class to get the current state
        xentity_state_protocol = cluster_protocol_v2.ClusterEntityStateProtocol(EntityType.WF, wf_id)
        effective_state = xentity_state_protocol.get_registered_state_resolved()

        # get wf details
        wf = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(ClusterPaths.get_entity(EntityType.WF, wf_id)))
        requirements = wf.collect_requirements()

        # print
        utils.info("wf_id: {}, cur_state: {}, requirements: {}".format(wf_id, effective_state, requirements))


class EntityRunner:
    def __init__(self, ident):
        self.ident = ident
        self.protocol = None
        self.wait_sec = 5

    def setup(self):
        cluster_common_v2.ClusterIds.set_id(self.ident)

    def run_step(self):
        # run active child supervisors
        if (self.protocol.entity.entity_type in cluster_common_v2.EntityActiveSupervisorTypes):
            self.protocol.monitor_active_children()

        # run passive child supervisors
        if (self.protocol.entity.entity_type in cluster_common_v2.EntityPassiveSupervisorTypes):
            self.protocol.monitor_passive_children()

        # run tasks
        if (self.protocol.entity.entity_type in cluster_common_v2.EntityExecutorTypes):
            self.protocol.monitor_execution_tasks()

    def run(self):
        while (True):
            # run step
            self.run_step()

            # sleep
            utils.info("{}: sleeping for {} seconds".format(self.protocol.entity.entity_type, self.wait_sec))
            time.sleep(self.wait_sec)

class EntityMasterRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()

        # create protocols
        self.protocol = cluster_protocol_v2.ClusterMasterProtocol(cluster_common_v2.ClusterEntityMaster.new(cluster_common_v2.ClusterIds.get_entity_id(EntityType.MASTER)))
        self.election_protocol = cluster_protocol_v2.ClusterMasterElectionProtocol(self.protocol.entity.entity_id)

        # initialize
        self.protocol.initialize()

    def run(self):
        while (True):
            # run election
            if (self.election_protocol.run_election() == True):
                self.protocol.refresh_master_cache()

            # monitor incoming
            self.protocol.monitor_incoming_for_supervisor()

            # run base class
            self.run_step()

            # sleep
            utils.info("{}: Sleeping for {} seconds".format(self.protocol.entity.entity_type, self.wait_sec))
            time.sleep(self.wait_sec)

class EntityResourceManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterResourceManagerProtocol(cluster_common_v2.ClusterEntityResourceManager.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.RESOURCE_MANAGER)))
        self.protocol.initialize()

class EntityJobManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterJobManagerProtocol(cluster_common_v2.ClusterEntityJobManager.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.JOB_MANAGER)))
        self.protocol.initialize()

class EntityTaskManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterTaskManagerProtocol(cluster_common_v2.ClusterEntityTaskManager.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.TASK_MANAGER)))
        self.protocol.initialize()

class EntitySWFManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterSWFManagerProtocol(cluster_common_v2.ClusterEntitySWFManager.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.SWF_MANAGER)))
        self.protocol.initialize()

class EntityWFManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterWFManagerProtocol(cluster_common_v2.ClusterEntityWFManager.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.WF_MANAGER)))
        self.protocol.initialize()

class EntityAgentRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterAgentProtocol(cluster_common_v2.ClusterEntityAgent.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.AGENT)))
        self.protocol.initialize()

class EntityWorkerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterWorkerProtocol(cluster_common_v2.ClusterEntityWorker.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.WORKER)))
        self.protocol.initialize()

def run_master(ident = 1):
    entity_runner = EntityMasterRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_resource_manager(ident = 1):
    entity_runner = EntityResourceManagerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_job_manager(ident = 1):
    entity_runner = EntityJobManagerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_task_manager(ident = 1):
    entity_runner = EntityTaskManagerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_swf_manager(ident = 1):
    entity_runner = EntitySWFManagerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_wf_manager(ident = 1):
    entity_runner = EntityWFManagerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_agent(ident = 1):
    entity_runner = EntityAgentRunner(ident)
    entity_runner.setup()
    entity_runner.run()

def run_worker(ident = 1):
    entity_runner = EntityWorkerRunner(ident)
    entity_runner.setup()
    entity_runner.run()

import multiprocessing

if __name__ == "__main__":
    multiprocessing.freeze_support()
    entity_name = sys.argv[1]
    if (entity_name == "master"):
        run_master()

