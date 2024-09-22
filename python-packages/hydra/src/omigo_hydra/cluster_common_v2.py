import datetime
from datetime import timezone
import json
import os
import time
import math 
import json 
import threading
from omigo_core import tsv, utils, tsvutils, etl, funclib
from omigo_hydra import cluster_data, cluster_class_reflection

# class that takes the base path in S3, and implement all distributed communication under that.
# takes care of protocol level things for future

# global constants. TODO        
if ("HYDRA_PATH" in os.environ.keys()):
    HYDRA_PATH = os.environ["HYDRA_PATH"]
else:
    utils.warn_once("Use HYDRA_PATH env variable")

if ("HYDRA_LOCAL_PATH" in os.environ.keys()):
    HYDRA_LOCAL_PATH = os.environ["HYDRA_LOCAL_PATH"]
else:
    utils.warn_once("Use HYDRA_LOCAL_PATH env variable")

# global variables
HYDRA_CLUSTER_HANDLER = None
HYDRA_LOCAL_CLUSTER_HANDLER = None

HYDRA_CLUSTER_HANDLER_LOCK = threading.Lock()
HYDRA_LOCAL_CLUSTER_HANDLER_LOCK = threading.Lock()

# some constants
MIN1_SECONDS = 60

# this is repeated in s3io_wrapper. TODO
DEFAULT_WAIT_SEC = 3
DEFAULT_ATTEMPTS = 3

def construct_dynamic_value_json():
    return "value.{}.json".format(funclib.get_utctimestamp_sec()) 

# Note: SHUTDOWN is implemented using?
# TODO: Why there is no Running state 
class EntityState:
    CREATED            = "created"
    ALIVE              = "alive"
    DEAD               = "dead"
    COMPLETED          = "completed"
    FAILED             = "failed"
    ABORTED            = "aborted"
    REASSIGNED         = "reassigned"
    CLEANUP            = "cleanup"

    def get_all():
        return [
            EntityState.CREATED,
            EntityState.ALIVE,
            EntityState.DEAD,
            EntityState.COMPLETED,
            EntityState.FAILED,
            EntityState.ABORTED,
            EntityState.REASSIGNED,
            EntityState.CLEANUP
        ]

class EntityCompletedState:
    ABORT_COMPLETED    = "abort-completed"
    REASSIGN_COMPLETED = "reassign-completed"
    CLEANUP_COMPLETED  = "cleanup-completed"

    def get_all():
        return [
            EntityCompletedState.ABORT_COMPLETED,
            EntityCompletedState.REASSIGN_COMPLETED,
            EntityCompletedState.CLEANUP_COMPLETED
        ]

class EntityType:
    MASTER             = "master"
    RESOURCE_MANAGER   = "resource-manager"
    SWF_MANAGER        = "swf-manager"
    WF_MANAGER         = "wf-manager"
    JOB_MANAGER        = "job-manager"
    TASK_MANAGER       = "task-manager"
    WORKER             = "worker"
    AGENT              = "agent"
    DOUBLE_AGENT       = "double-agent"
    INTELI_AGENT       = "inteli-agent"
    SWF                = "swf"
    WF                 = "wf"
    JOB                = "job"
    TASK               = "task"
    BATCH              = "batch"
    CLIENT             = "client"
    SESSION            = "session"

    def get_all():
        return [
            EntityType.MASTER,
            EntityType.RESOURCE_MANAGER,
            EntityType.SWF_MANAGER,
            EntityType.WF_MANAGER,
            EntityType.JOB_MANAGER,
            EntityType.TASK_MANAGER,
            EntityType.WORKER,
            EntityType.AGENT,
            EntityType.DOUBLE_AGENT,
            EntityType.INTELI_AGENT,
            EntityType.WF,
            EntityType.SWF,
            EntityType.JOB,
            EntityType.TASK,
            EntityType.BATCH,
            EntityType.CLIENT,
            EntityType.SESSION
        ]
            

# Entity Active or Not
EntityIsActiveMap = {}
EntityIsActiveMap[EntityType.MASTER] = True
EntityIsActiveMap[EntityType.RESOURCE_MANAGER] = True
EntityIsActiveMap[EntityType.SWF_MANAGER] = True
EntityIsActiveMap[EntityType.WF_MANAGER] = True
EntityIsActiveMap[EntityType.JOB_MANAGER] = True
EntityIsActiveMap[EntityType.TASK_MANAGER] = True
EntityIsActiveMap[EntityType.WORKER] = True
EntityIsActiveMap[EntityType.AGENT] = True
EntityIsActiveMap[EntityType.DOUBLE_AGENT] = True 
EntityIsActiveMap[EntityType.INTELI_AGENT] = True
EntityIsActiveMap[EntityType.SWF] = False
EntityIsActiveMap[EntityType.WF] = False
EntityIsActiveMap[EntityType.JOB] = False
EntityIsActiveMap[EntityType.TASK] = False
EntityIsActiveMap[EntityType.BATCH] = False
EntityIsActiveMap[EntityType.CLIENT] = True
EntityIsActiveMap[EntityType.SESSION] = True

# Create the map for supervisor 
EntitySupervisorMap = {}
EntitySupervisorMap[EntityType.MASTER] = EntityType.MASTER
EntitySupervisorMap[EntityType.RESOURCE_MANAGER] = EntityType.MASTER
EntitySupervisorMap[EntityType.SWF_MANAGER] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.WF_MANAGER] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.JOB_MANAGER] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.TASK_MANAGER] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.WORKER] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.AGENT] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.DOUBLE_AGENT] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.INTELI_AGENT] = EntityType.RESOURCE_MANAGER
EntitySupervisorMap[EntityType.SWF] = EntityType.SWF_MANAGER
EntitySupervisorMap[EntityType.WF] = EntityType.WF_MANAGER
EntitySupervisorMap[EntityType.JOB] = EntityType.JOB_MANAGER
EntitySupervisorMap[EntityType.TASK] = EntityType.TASK_MANAGER
EntitySupervisorMap[EntityType.BATCH] = EntityType.JOB_MANAGER
EntitySupervisorMap[EntityType.CLIENT] = EntityType.MASTER
EntitySupervisorMap[EntityType.SESSION] = EntityType.MASTER

# Create the map for active children
EntityActiveChildrenMap = {}
EntityActiveChildrenMap[EntityType.MASTER] = [EntityType.MASTER, EntityType.RESOURCE_MANAGER, EntityType.SESSION, EntityType.CLIENT] # TODO: Master belongs here because of active children entry
EntityActiveChildrenMap[EntityType.RESOURCE_MANAGER] = [EntityType.SWF_MANAGER, EntityType.WF_MANAGER, EntityType.JOB_MANAGER, EntityType.TASK_MANAGER, EntityType.WORKER, EntityType.AGENT, EntityType.DOUBLE_AGENT, EntityType.INTELI_AGENT]
EntityActiveChildrenMap[EntityType.SWF_MANAGER] = []
EntityActiveChildrenMap[EntityType.WF_MANAGER] = []
EntityActiveChildrenMap[EntityType.JOB_MANAGER] = []
EntityActiveChildrenMap[EntityType.TASK_MANAGER] = []
EntityActiveChildrenMap[EntityType.WORKER] = []
EntityActiveChildrenMap[EntityType.AGENT] = []
EntityActiveChildrenMap[EntityType.DOUBLE_AGENT] = []
EntityActiveChildrenMap[EntityType.INTELI_AGENT] = []
EntityActiveChildrenMap[EntityType.SWF] = []
EntityActiveChildrenMap[EntityType.WF] = []
EntityActiveChildrenMap[EntityType.JOB] = []
EntityActiveChildrenMap[EntityType.TASK] = []
EntityActiveChildrenMap[EntityType.BATCH] = []
EntityActiveChildrenMap[EntityType.CLIENT] = []
EntityActiveChildrenMap[EntityType.SESSION] = []

# Create the map for passive children
EntityPassiveChildrenMap = {}
EntityPassiveChildrenMap[EntityType.MASTER] = []
EntityPassiveChildrenMap[EntityType.RESOURCE_MANAGER] = []
EntityPassiveChildrenMap[EntityType.SWF_MANAGER] = [EntityType.SWF]
EntityPassiveChildrenMap[EntityType.WF_MANAGER] = [EntityType.WF]
EntityPassiveChildrenMap[EntityType.JOB_MANAGER] = [EntityType.JOB]
EntityPassiveChildrenMap[EntityType.TASK_MANAGER] = [EntityType.TASK, EntityType.BATCH]
EntityPassiveChildrenMap[EntityType.WORKER] = []
EntityPassiveChildrenMap[EntityType.AGENT] = []
EntityPassiveChildrenMap[EntityType.DOUBLE_AGENT] = []
EntityPassiveChildrenMap[EntityType.INTELI_AGENT] = []
EntityPassiveChildrenMap[EntityType.SWF] = []
EntityPassiveChildrenMap[EntityType.WF] = []
EntityPassiveChildrenMap[EntityType.JOB] = []
EntityPassiveChildrenMap[EntityType.TASK] = []
EntityPassiveChildrenMap[EntityType.BATCH] = []
EntityPassiveChildrenMap[EntityType.CLIENT] = []
EntityPassiveChildrenMap[EntityType.SESSION] = []

# Create the map for dependents.
# SESSION has dependency on all as either SWF, WF and JOB can be directly submitted for execution
EntityDependentsMap = {}
EntityDependentsMap[EntityType.MASTER] = []
EntityDependentsMap[EntityType.RESOURCE_MANAGER] = []
EntityDependentsMap[EntityType.SWF_MANAGER] = []
EntityDependentsMap[EntityType.WF_MANAGER] = []
EntityDependentsMap[EntityType.JOB_MANAGER] = []
EntityDependentsMap[EntityType.TASK_MANAGER] = []
EntityDependentsMap[EntityType.WORKER] = []
EntityDependentsMap[EntityType.AGENT] = []
EntityDependentsMap[EntityType.DOUBLE_AGENT] = []
EntityDependentsMap[EntityType.INTELI_AGENT] = []
EntityDependentsMap[EntityType.SWF] = [EntityType.WF]
EntityDependentsMap[EntityType.WF] = [EntityType.JOB]
EntityDependentsMap[EntityType.JOB] = [EntityType.TASK]
EntityDependentsMap[EntityType.TASK] = [EntityType.BATCH]
EntityDependentsMap[EntityType.BATCH] = []
EntityDependentsMap[EntityType.CLIENT] = [EntityType.SESSION]
EntityDependentsMap[EntityType.SESSION] = [EntityType.SWF, EntityType.WF, EntityType.JOB]

# Reverse Dependents
EntityReverseDependentsMap = {}
for k in EntityDependentsMap.keys():
    # get values
    vs = EntityDependentsMap[k]

    # iterate over each child
    for v in vs:
        # create key if not exists
        if (v not in EntityReverseDependentsMap.keys()):
            EntityReverseDependentsMap[v] = []

        # add to the list of parent
        EntityReverseDependentsMap[v].append(k)

# Entities that act as executors - worker, agents
EntityExecutorTypes = [EntityType.WORKER, EntityType.AGENT, EntityType.DOUBLE_AGENT, EntityType.INTELI_AGENT]
EntityExecutionTaskTypes = [EntityType.SWF, EntityType.WF, EntityType.JOB, EntityType.TASK, EntityType.BATCH]
EntityActiveSupervisorTypes = [EntityType.MASTER, EntityType.RESOURCE_MANAGER]
EntityPassiveSupervisorTypes = [EntityType.SWF_MANAGER, EntityType.WF_MANAGER, EntityType.JOB_MANAGER, EntityType.TASK_MANAGER]

# capacity map
ENTITY_CAPACITY_DEFAULT = 10
EntityCapacityMap = {}
EntityCapacityMap[EntityType.MASTER] = ENTITY_CAPACITY_DEFAULT 
EntityCapacityMap[EntityType.RESOURCE_MANAGER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.SWF_MANAGER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.WF_MANAGER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.JOB_MANAGER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.TASK_MANAGER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.WORKER] = ENTITY_CAPACITY_DEFAULT
EntityCapacityMap[EntityType.AGENT] = 1
EntityCapacityMap[EntityType.DOUBLE_AGENT] = 1
EntityCapacityMap[EntityType.INTELI_AGENT] = 1

# Constants for cluster capabilities
class ClusterCapabilities:
    SPARK       = "spark"
    PRESTO      = "presto"
    SPLUNK      = "splunk"
    SHELL       = "shell"

# primary entity class
class ClusterEntity(cluster_data.JsonSer):
    DEFAULT_ACTIVE_ENTITY_LEASE = 1 * MIN1_SECONDS
    DEFAULT_PASSIVE_ENTITY_LEASE = 2 * MIN1_SECONDS

    # constructor
    # is_alive: is a flag to tell if it is an active or passive entity
    # active_children: are active entities that are monitored for checking liveness
    # passive_children: are passive entities that are assigned for execution and periodically monitored for completion
    # dependents: are lifecycle dependents that if the parent is aborted, the dependents will be aborted too
    def __init__(self, entity_type, entity_id, ts, lease):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.ts = ts
        self.lease = lease

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterEntity.new(
            json_obj["entity_type"],
            json_obj["entity_id"],
            json_obj["ts"],
            json_obj["lease"]
        )

    def new(entity_type, entity_id, ts, lease):
        return ClusterEntity(entity_type, entity_id, ts, lease)

def deserialize_cluster_entity(json_obj):
    # read entity type
    entity_type = json_obj["entity_type"]

    # swtich case on entity type
    if (entity_type == EntityType.MASTER):
        return ClusterEntityMaster.from_json(json_obj)
    elif (entity_type == EntityType.RESOURCE_MANAGER):
        return ClusterEntityResourceManager.from_json(json_obj)
    elif (entity_type == EntityType.SWF_MANAGER):
        return ClusterEntitySWFManager.from_json(json_obj)
    elif (entity_type == EntityType.WF_MANAGER):
        return ClusterEntityWFManager.from_json(json_obj)
    elif (entity_type == EntityType.JOB_MANAGER):
        return ClusterEntityJobManager.from_json(json_obj)
    elif (entity_type == EntityType.TASK_MANAGER):
        return ClusterEntityTaskManager.from_json(json_obj)
    elif (entity_type == EntityType.WORKER):
        return ClusterEntityWorker.from_json(json_obj)
    elif (entity_type == EntityType.AGENT):
        return ClusterEntityAgent.from_json(json_obj)
    elif (entity_type == EntityType.DOUBLE_AGENT):
        return ClusterEntityDoubleAgent.from_json(json_obj)
    elif (entity_type == EntityType.INTELI_AGENT):
        return ClusterEntityInteliAgent.from_json(json_obj)
    elif (entity_type == EntityType.SWF):
        return ClusterEntitySWF.from_json(json_obj)
    elif (entity_type == EntityType.WF):
        return ClusterEntityWF.from_json(json_obj)
    elif (entity_type == EntityType.JOB):
        return ClusterEntityJob.from_json(json_obj)
    elif (entity_type == EntityType.TASK):
        return deserialize_cluster_task(json_obj)
    elif (entity_type == EntityType.BATCH):
        return ClusterEntityBatch.from_json(json_obj)
    elif (entity_type == EntityType.CLIENT):
        return ClusterEntityClient.from_json(json_obj)
    elif (entity_type == EntityType.SESSION):
        return ClusterEntitySession.from_json(json_obj)
    else:
        raise Exception("deserialize_cluster_entity: unknown entity type: {}".format(xentity_type))

# Master
class ClusterEntityMaster(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.MASTER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityMaster(entity_id, ts, lease)

# Resource Manager
class ClusterEntityResourceManager(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.RESOURCE_MANAGER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityResourceManager(entity_id, ts, lease)

# SWF Manager
class ClusterEntitySWFManager(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.SWF_MANAGER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntitySWFManager(entity_id, ts, lease)

# WF Manager
class ClusterEntityWFManager(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.WF_MANAGER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityWFManager(entity_id, ts, lease)

# Job Manager
class ClusterEntityJobManager(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.JOB_MANAGER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityJobManager(entity_id, ts, lease)

# Task Manager
class ClusterEntityTaskManager(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.TASK_MANAGER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityTaskManager(entity_id, ts, lease)

# Worker
class ClusterEntityWorker(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.WORKER, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityWorker(entity_id, ts, lease)

# Agent
class ClusterEntityAgent(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.AGENT, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityAgent(entity_id, ts, lease)

# Double Agent
class ClusterEntityDoubleAgent(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.DOUBLE_AGENT, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityDoubleAgent(entity_id, ts, lease)

# Inteli Agent
class ClusterEntityInteliAgent(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.INTELI_AGENT, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntityInteliAgent(entity_id, ts, lease)

# SWF
class ClusterEntitySWF(ClusterEntity):
    def __init__(self, entity_id, ts, lease, client_id, session_id, entity_spec):
        super().__init__(EntityType.SWF, entity_id, ts, lease)
        self.entity_spec = entity_spec
        self.client_id = client_id
        self.session_id = session_id

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterEntitySWF.new(
            json_obj["entity_id"],
            json_obj["client_id"],
            json_obj["session_id"],
            deserialize_cluster_spec(json_obj["entity_spec"]),
            json_obj["ts"],
            json_obj["lease"]
        )

    def new(entity_id, client_id, session_id, entity_spec, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntitySWF(entity_id, ts, lease, client_id, session_id, entity_spec)

# WF
class ClusterEntityWF(ClusterEntity):
    def __init__(self, entity_id, ts, lease, client_id, session_id, entity_spec):
        super().__init__(EntityType.WF, entity_id, ts, lease)
        self.client_id = client_id
        self.session_id = session_id
        self.entity_spec = entity_spec

    def collect_requirements(self):
        requirements = []
        for job_spec in self.entity_spec.jobs_specs:
            if (job_spec.map_task is not None):
                for map_op in job_spec.map_task.map_ops:
                    for r in map_op.requirements:
                        if (r not in requirements):
                            requirements.append(r)  

            if (job_spec.reduce_task is not None):
                for r in job_spec.reduce_task.reduce_op.requirements:
                    if (r not in requirements):
                        requirements.append(r)  

            if (job_spec.extend_class_def is not None):
                for r in job_spec.extend_class_def.extend_class_op.requirements:
                    if (r not in requirements):
                        requirements.append(r)  

        # return
        return requirements

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterEntityWF.new(
            json_obj["entity_id"],
            json_obj["client_id"],
            json_obj["session_id"],
            deserialize_cluster_spec(json_obj["entity_spec"]),
            json_obj["ts"],
            json_obj["lease"]
        )

    def new(entity_id, client_id, session_id, entity_spec, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntityWF(entity_id, ts, lease, client_id, session_id, entity_spec)

# Job
class ClusterEntityJob(ClusterEntity):
    def __init__(self, entity_id, ts, lease, client_id, session_id, entity_spec):
        super().__init__(EntityType.JOB, entity_id, ts, lease)
        self.entity_spec = entity_spec
        self.client_id = client_id
        self.session_id = session_id

    def new(entity_id, client_id, session_id, entity_spec, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntityJob(entity_id, ts, lease, client_id, session_id, entity_spec)

# Task 
class ClusterEntityTask(ClusterEntity):
    def __init__(self, entity_id, ts, lease, client_id, session_id, entity_spec):
        super().__init__(EntityType.TASK, entity_id, ts, lease)
        self.client_id = client_id
        self.session_id = session_id
        self.entity_spec = entity_spec

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterEntityTask.new(
            json_obj["entity_id"],
            json_obj["client_id"],
            json_obj["session_id"],
            deserialize_cluster_spec(json_obj["entity_spec"]),
            json_obj["ts"],
            json_obj["lease"]
        )

    def new(entity_id, client_id, session_id, entity_spec, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntityTask(entity_id, ts, lease, client_id, session_id, entity_spec)

# Batch
class ClusterEntityBatch(ClusterEntity):
    def __init__(self, entity_id, ts, lease, client_id, session_id, entity_spec):
        super().__init__(EntityType.BATCH, entity_id, ts, lease)
        self.client_id = client_id
        self.session_id = session_id
        self.entity_spec = entity_spec

    def new(entity_id, client_id, session_id, entity_spec, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntityBatch(entity_id, ts, lease, client_id, session_id, entity_spec)

# Client
class ClusterEntityClient(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.CLIENT, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_PASSIVE_ENTITY_LEASE):
        return ClusterEntityClient(entity_id, ts, lease)

# Session
class ClusterEntitySession(ClusterEntity):
    def __init__(self, entity_id, ts, lease):
        super().__init__(EntityType.SESSION, entity_id, ts, lease)

    def new(entity_id, ts = funclib.get_utctimestamp_sec(), lease = ClusterEntity.DEFAULT_ACTIVE_ENTITY_LEASE):
        return ClusterEntitySession(entity_id, ts, lease)

# define job, workflow and super-workflows as understood by public apis
class ClusterSpecBase(cluster_data.JsonSer):
    def __init__(self, entity_type, num_inputs, num_outputs):
        self.entity_type = entity_type
        self.num_inputs = num_inputs
        self.num_outputs = num_outputs

    def build(self):
        # do validation and create a valid compiled representations
        if (self.xinputs is None):
            raise Exception("ClusterSpecBase: build: xinputs is None")
        
        if (self.xoutputs is None):
            raise Exception("ClusterSpecBase: build: xoutputs is None")
            
    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecBase(
            json_obj["entity_type"],
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

# define constants
EXTEND_CLASS_OP = "extend_class_op"
MAP_OPS = "map_ops"
REDUCE_OP = "reduce_op"
SINGLETON_OP = "singleton_op"

# method to construct the correct ClusterSpec
def deserialize_cluster_spec(json_obj):
    # check for None
    if (json_obj is None):
        return None

    # read entity type
    entity_type = json_obj["entity_type"]

    # swtich case on entity_type
    if (entity_type == EntityType.SWF):
        return ClusterSpecSWF.from_json(json_obj)
    elif (entity_type == EntityType.WF):
        return ClusterSpecWF.from_json(json_obj)
    elif (entity_type == EntityType.JOB):
        return ClusterSpecJob.from_json(json_obj)
    elif (entity_type == EntityType.TASK):
        # TASK has multiple sub classes
        return deserialize_cluster_task_spec(json_obj)
    elif (entity_type == EntityType.BATCH):
        return ClusterSpecBatch.from_json(json_obj)
    else:
        raise Exception("deserialize_cluster_spec: unknown entity_type: {}".format(entity_type))
    
# SWF Spec
# TODO: this needs redesigning
class ClusterSpecSWF(ClusterSpecBase):
    def __init__(self, num_inputs, num_outputs, wfs_specs):
        super().__init__(EntityType.SWF, num_inputs, num_outputs)
        self.wfs_specs = wfs_specs
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # deserialize wfs 
        wfs_specs = []
        for wf_spec_obj in json_obj["wfs_specs"]:
            wfs_specs.append(ClusterSpecWF.from_json(wf_spec_obj))

        # return
        return ClusterSpecSWF.new(
            wfs_specs,
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(wfs_specs, num_inputs = 1, num_outputs = 1):
        return ClusterSpecSWF(wfs_specs, num_inputs, num_outputs)

# WF Spec
class ClusterSpecWF(ClusterSpecBase):
    def __init__(self, jobs_specs, is_live, is_remote, is_external, max_job_execution_time, interval, start_ts, use_full_data, duration, input_ids, output_ids):
        super().__init__(EntityType.WF, len(input_ids), len(output_ids))
        self.jobs_specs = jobs_specs
        self.is_live = is_live
        self.is_remote = is_remote
        self.is_external = is_external
        self.max_job_execution_time = max_job_execution_time
        self.interval = interval
        self.start_ts = start_ts
        self.use_full_data = use_full_data
        self.duration = duration
        self.input_ids = input_ids
        self.output_ids = output_ids 
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # deserialize jobs
        jobs_specs = []
        for job_spec_obj in json_obj["jobs_specs"]:
            jobs_specs.append(ClusterSpecJob.from_json(job_spec_obj))

        # return
        return ClusterSpecWF.new(
            jobs_specs,
            json_obj["is_live"],
            json_obj["is_remote"],
            json_obj["is_external"],
            json_obj["max_job_execution_time"],
            json_obj["interval"],
            json_obj["start_ts"],
            json_obj["use_full_data"],
            json_obj["duration"],
            json_obj["input_ids"],
            json_obj["output_ids"]
        )

    def new(jobs_specs, is_live = False, is_remote = False, is_external = False, max_job_execution_time = 600, interval = -1, start_ts = 0, use_full_data = False, duration = 0, input_ids = None, output_ids = None):
        # create defaults
        if (input_ids is None):
            raise Exception("ClusterSpecWF: input ids can not be None")

        if (output_ids is None):
            raise Exception("ClusterSpecWF: output ids can not be None")

        # return
        return ClusterSpecWF(jobs_specs, is_live, is_remote, is_external, max_job_execution_time, interval, start_ts, use_full_data, duration, input_ids, output_ids)
        
# Job Spec
class ClusterSpecJob(ClusterSpecBase):
    def __init__(self, map_partitioner, map_task, reduce_partitioner, reduce_task, extend_class_def, num_inputs, num_outputs):
        super().__init__(EntityType.JOB, num_inputs, num_outputs)
        self.map_partitioner = map_partitioner
        self.map_task = map_task
        self.reduce_partitioner = reduce_partitioner
        self.reduce_task = reduce_task
        self.extend_class_def = extend_class_def
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecJob.new(
            ClusterSpecPartitionTask.from_json(json_obj["map_partitioner"]),        
            ClusterSpecMapTask.from_json(json_obj["map_task"]),        
            ClusterSpecHashPartitionTask.from_json(json_obj["reduce_partitioner"]),        
            ClusterSpecReduceTask.from_json(json_obj["reduce_task"]),
            ClusterSpecExtendClassDef.from_json(json_obj["extend_class_def"]),
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(map_partitioner, map_task, reduce_partitioner, reduce_task, extend_class_def, num_inputs = 1, num_outputs = 1):
        return ClusterSpecJob(map_partitioner, map_task, reduce_partitioner, reduce_task, extend_class_def, num_inputs, num_outputs)

# Task Spec 
class ClusterSpecTask(ClusterSpecBase):
    def __init__(self, task_type, num_inputs, num_outputs):
        super().__init__(EntityType.TASK, num_inputs, num_outputs)
        self.task_type = task_type
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecTask.new(
            json_obj["task_type"],
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    # this method is not used because of derived classes
    def new(task_type, num_inputs = 1, num_outputs = 1):
        return ClusterSpecTask(task_type, num_inputs, num_outputs)

class ClusterSpecExtendClassDef(ClusterSpecTask):
    def __init__(self, extend_class_op, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.EXTEND_CLASS, num_inputs, num_outputs)
        self.extend_class_op = extend_class_op
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecExtendClassDef.new(
            ClusterExtendClassOperation.from_json(json_obj[EXTEND_CLASS_OP]),
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(extend_class_def, num_inputs = 1, num_outputs = 1):
        return ClusterSpecExtendClassDef(extend_class_def, num_inputs, num_outputs)

class ClusterSpecMapTask(ClusterSpecTask):
    def __init__(self, map_ops, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.MAP, num_inputs, num_outputs)
        self.map_ops = map_ops
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # operations
        map_ops = []
        for op in json_obj[MAP_OPS]:
            map_ops.append(ClusterMapOperation.from_json(op))

        # return
        return ClusterSpecMapTask.new(
            map_ops,
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(map_ops, num_inputs = 1, num_outputs = 1):
        return ClusterSpecMapTask(map_ops, num_inputs, num_outputs)

class ClusterSpecReduceTask(ClusterSpecTask):
    def __init__(self, reduce_op, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.REDUCE, num_inputs, num_outputs)
        self.reduce_op = reduce_op
        
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecReduceTask.new(
            ClusterReduceOperation.from_json(json_obj[REDUCE_OP]),
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(reduce_op, num_inputs = 1, num_outputs = 1):
        return ClusterSpecReduceTask(reduce_op, num_inputs, num_outputs)

class ClusterSpecSingletonTask(ClusterSpecTask):
    def __init__(self, singleton_op, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.SINGLETON, num_inputs, num_outputs)
        self.singleton_op = singleton_op

    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecSingletonTask.new(
            ClusterSingletonOperation.from_json(json_obj[SINGLETON_OP]),
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(singleton_op, num_inputs = 1, num_outputs = 1):
        return ClusterSpecSingletonTask(singleton_op, num_inputs, num_outputs)

class ClusterSpecPartitionTask(ClusterSpecTask):
    def __init__(self, num_splits, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.PARTITION, num_inputs, num_outputs)
        self.num_splits = num_splits
 
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecPartitionTask.new(
            json_obj["num_splits"],
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(num_splits, num_inputs = 1, num_outputs = 1):
        return ClusterSpecPartitionTask(num_splits, num_inputs, num_outputs)

class ClusterSpecHashPartitionTask(ClusterSpecTask):
    def __init__(self, num_splits, hash_cols, num_inputs, num_outputs):
        super().__init__(ClusterTaskType.HASH_PARTITION, num_inputs, num_outputs)
        self.num_splits = num_splits
        self.hash_cols = hash_cols
 
    def build(self):
        super().build()

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # return
        return ClusterSpecHashPartitionTask.new(
            json_obj["num_splits"],
            json_obj["hash_cols"],
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(num_splits, hash_cols, num_inputs = 1, num_outputs = 1):
        return ClusterSpecHashPartitionTask(num_splits, hash_cols, num_inputs, num_outputs)
       
def deserialize_cluster_task_spec(json_obj):
    # check for None
    if (json_obj is None):
        return None

    # get task type
    task_type = json_obj["task_type"]
    
    # switch case
    if (task_type == ClusterTaskType.EXTEND_CLASS):
        return ClusterSpecExtendClassDef.from_json(json_obj)
    elif (task_type == ClusterTaskType.MAP):
        return ClusterSpecMapTask.from_json(json_obj)
    elif (task_type == ClusterTaskType.REDUCE):
        return ClusterSpecReduceTask.from_json(json_obj)
    elif (task_type == ClusterTaskType.SINGLETON):
        return ClusterSpecSingletonTask.from_json(json_obj)
    elif (task_type == ClusterTaskType.PARTITION):
        return ClusterSpecPartitionTask.from_json(json_obj)
    elif (task_type == ClusterTaskType.HASH_PARTITION):
        return ClusterSpecHashPartitionTask.from_json(json_obj)
    else:
        raise Exception("Unknown task type: {}".format(task_type))

# Batch Spec. TODO: Batch is an Entity?.
class ClusterSpecBatch(ClusterSpecBase):
    def __init__(self, num_inputs, num_outputs):
        super().__init__(EntityType.BATCH, num_inputs, num_outputs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # create spec
        return ClusterSpecBase.new(
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(num_inputs = 1, num_outputs = 1):
        return ClusterSpecBase(entity_type, num_inputs, num_outputs)
        

# Copied from cluster_common_v1
# This is meant to take arbitrary arguments including lambda function or list which dont have standard serialization
# thats why need to convert to cluster operand directly
class ClusterOperation(cluster_data.JsonSer):
    def __init__(self, name, requirements, *args, **kwargs):
        # check if the name is a string name or actual function call
        if (isinstance(name, str)):
            # name of the operation
            self.name = name
        else:
            self.name = cluster_class_reflection.get_fully_qualified_name(name)

        # set the requirements
        self.requirements = requirements

        # this is an array of orthogonal types. dont assume homogenous types here
        jargs = []
        for x in args:
            jargs.append(cluster_data.cluster_operand_serializer(x))
        self.args = cluster_data.ClusterArrayObject(jargs)

        # dictionary is non homogenous types by default
        self.kwargs = cluster_data.cluster_operand_serializer(kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # initialize variables. TODO: why calling load_native_objects
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))

        # return
        return ClusterOperation(name, requirements, *args, **kwargs)

# TODO: this is partially shared between ClusterTaskOperation and ClusterSpecBase
class ClusterTaskType:
    EXTEND_CLASS = "extend_class"
    MAP = "map"
    REDUCE = "reduce"
    SINGLETON = "singleton"
    PARTITION = "partition"
    HASH_PARTITION = "hash_partition"

class ClusterTaskOperation(ClusterOperation):
    def __init__(self, task_type, name, requirements, *args, **kwargs):
        super().__init__(name, requirements, *args, **kwargs)
        self.task_type = task_type

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # this parsing is tricky and has to be done inline
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))
        return ClusterTaskOperation(json_obj["task_type"], name, requirements, *args, **kwargs)

class ClusterExtendClassOperation(ClusterTaskOperation):
    def __init__(self, name, requirements, *args, **kwargs):
        super().__init__(ClusterTaskType.EXTEND_CLASS, name, requirements, *args, **kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # this parsing is tricky and has to be done inline
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))
        return ClusterExtendClassOperation(name, requirements, *args, **kwargs)

# TODO: follow the same design as ClusterOperand with mulitple derived classes
class ClusterMapOperation(ClusterTaskOperation):
    def __init__(self, name, requirements, *args, **kwargs):
        super().__init__(ClusterTaskType.MAP, name, requirements, *args, **kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # this parsing is tricky and has to be done inline
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))
        return ClusterMapOperation(name, requirements, *args, **kwargs)

# TODO: follow the same design as ClusterOperand with mulitple derived classes
class ClusterReduceOperation(ClusterTaskOperation):
    def __init__(self, grouping_cols, num_splits, name, requirements, *args, **kwargs):
        super().__init__(ClusterTaskType.REDUCE, name, requirements, *args, **kwargs)
        self.grouping_cols = grouping_cols 
        self.num_splits = num_splits

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # this parsing is tricky and has to be done inline
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))

        # return
        return ClusterReduceOperation(json_obj["grouping_cols"], json_obj["num_splits"], name, requirements, *args, **kwargs)

# TODO: follow the same design as ClusterOperand with mulitple derived classes
class ClusterSingletonOperation(ClusterTaskOperation):
    def __init__(self, name, requirements, *args, **kwargs):
        super().__init__(ClusterTaskType.SINGLETON, name, requirements, *args, **kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # this parsing is tricky and has to be done inline
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))

        # return
        return ClusterSingletonOperation(name, requirements, *args, **kwargs)

def deserialize_cluster_task_operation(json_obj):
    # check for None
    if (json_obj is None):
        return None

    # read task_type
    task_type = json_obj["task_type"]

    # do switch case on valid task types
    if (task_type == ClusterTaskType.EXTEND_CLASS):
        return ClusterExtendClassOperation.from_json(json_obj)
    elif (task_type == ClusterTaskType.MAP):
        return ClusterMapOperation.from_json(json_obj)
    elif (task_type == ClusterTaskType.REDUCE):
        return ClusterReduceOperation.from_json(json_obj)
    elif (task_type == ClusterTaskType.SINGLETON):
        return ClusterSingletonOperation.from_json(json_obj)
    else:
        raise Exception("deserialize_cluster_task_operation: unknown task_type for ClusterTaskOperation: {}".format(task_type))
 
# TODO: follow the same design as ClusterOperand with mulitple derived classes
class ClusterInmemoryOperation(ClusterOperation):
    def __init__(self, name, requirements, *args, **kwargs):
        super().__init__(name, requirements, *args, **kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # parsing
        name = json_obj["name"]
        requirements = json_obj["requirements"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))

        # return
        return ClusterInmemoryOperation(name, requirements, *args, **kwargs)

# Entity Heartbeat
class ClusterHearbeat(cluster_data.JsonSer):
    def __init__(self, ts, lease):
        self.ts = ts
        self.lease = lease

    def is_alive(self):
        # compute time diff from current time
        cur_ts = funclib.get_utctimestamp_sec()
        time_diff = abs(cur_ts - self.ts)

        # check time diff against lease
        if (time_diff <= self.lease):
            return True
        else:
            return False

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterHearbeat.new(
            json_obj["ts"],
            json_obj["lease"]
        )

    def new(ts, lease):
        return ClusterHearbeat(ts, lease)

class ClusterUpdateTime(cluster_data.JsonSer):
    def __init__(self, ts):
        self.ts = ts

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterUpdateTime.new(
            json_obj["ts"]
        )

    def new(ts):
        return ClusterUpdateTime(ts)

class ClusterEntityRef(cluster_data.JsonSer):
    def __init__(self, entity_type, entity_id):
        self.entity_type = entity_type
        self.entity_id = entity_id

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterEntityRef.new(
            json_obj["entity_type"],
            json_obj["entity_id"]
        )

    def new(entity_type, entity_id):
        return ClusterEntityRef(entity_type, entity_id)

# ClusterFileHandler
class ClusterFileHandler(cluster_data.JsonSer):
    def __init__(self, base_path):
        super().__init__()

        # validation
        if (base_path is None or len(base_path) == 0):
            raise Exception("Base path is empty")

        # assign variables
        self.base_path = base_path
        self.fs = s3io_wrapper.S3FSWrapper()
        
    def __makepath__(self, path):
        # validation
        if (path is None):
            raise Exception("__makepath__: path is None")

        # remove any trailing '/'
        if (path.endswith("/")):
            path = path[0:-1]

        # remove any leading '/'
        if (path.startswith("/")):
            path = path[1:] 

        # check for empty string
        if (path == ""):
            return self.base_path

        # return
        return "{}/{}".format(self.base_path, path)
    
    def __strip_leading_trailing_slashes__(self, path):
        # normalize
        if (path.startswith("/")):
            path = path[1:]

        if (path.endswith("/")):
            path = path[0:-1]

        return path

    def create(self, path, verify = True):
        utils.warn_once("create: api is using levels which points to some issue in protocol")
        # create only if absent
        if (self.dir_exists(path) == False):
            utils.info("create      : {}".format(path))
            # TODO: this levels can lead to silent bugs
            levels = min(len(path.split("/")), 1)
            self.fs.makedirs(self.__makepath__(path), levels = levels)

        # after creation wait for confirmation
        if (verify == True and self.dir_exists_with_wait(path) == False):
            raise Exception("create: path: {}, failed to verify".format(path))        

    def list_files(self, path):
        utils.debug("list_files : {}".format(path))
        return self.fs.list_files(self.__makepath__(path))

    def list_dirs(self, path):
        utils.debug("list_dirs  : {}".format(path))
        return self.fs.list_dirs(self.__makepath__(path))

    def list_all_recursive(self, path):
        # create result
        results = []

        # normalize
        path = self.__strip_leading_trailing_slashes__(path)

        # iterate over all files and directories
        for f in self.fs.ls(self.__makepath__(path)):
            # append to results
            results.append(f)

            # create full path
            pathf = "{}/{}".format(path, f)
            utils.debug("pathf: {}".format(pathf))

            # recursive call for directory
            if (self.fs.is_directory(self.__makepath__(pathf))):
                for f2 in self.list_all_recursive(pathf):
                    results.append("{}/{}".format(f, f2))

        # return
        return sorted(results)

    def remove_file(self, path, ignore_if_missing = False, verify = True, ignore_logging = False):
        if (ignore_logging == False):
            utils.info("remove_file : {}".format(path))
        else:
            utils.debug("remove_file : {}".format(path))

        self.fs.delete_file_with_wait(self.__makepath__(path), ignore_if_missing = ignore_if_missing)

        # check for commit
        if (verify == True and self.file_not_exists_with_wait(path) == False):
            raise Exception("remove_file: path: {}, verify commit failed".format(path))
        
    def remove_dir(self, path, ignore_if_missing = False, verify = True, ignore_logging = False):
        # debug
        if (ignore_logging == False):
            utils.info("remove_dir  : {}".format(path))
        else:
            utils.debug("remove_dir  : {}".format(path))

        # check for ignore_if_missing
        if (self.dir_exists(path) == False):
            if (ignore_if_missing == True):
                utils.warn("remove_dir: path missing: {}".format(path))
                return
            else:
                if (self.dir_exists_with_wait(path) == False):
                    raise Exception("remove_dir: path not found: {}".format(path))
        else:
            self.fs.delete_dir_with_wait(self.__makepath__(path), ignore_if_missing = ignore_if_missing)

        # check for commit
        if (verify == True and self.file_not_exists_with_wait(path) == False):
            raise Exception("remove_dir: path: {}, verify commit failed".format(path))

    def remove_dir_recursive(self, path, ignore_if_missing = False, verify = True):
        # debug
        utils.info("remove_dir_r: {}".format(path))

        # check for ignore_if_missing
        if (self.dir_exists(path) == False):
            if (ignore_if_missing == True):
                utils.warn("remove_dir_r: path missing: {}, ignore_if_missing: {}".format(path, ignore_if_missing))
            else:
               if (self.dir_exists_with_wait(path) == False):
                   raise Exception("remove_dir_r: path doesnt exist: {}, ignore_if_missing: {}".format(path, ignore_if_missing))
        else:
            # get all the files and directories
            listings = self.list_all_recursive(path)

            # split into files and directories
            files = []
            dirs = []
            for listing in listings:
                # get the path
                listing_path = "{}/{}".format(path, listing)

                # check for file or directory
                if (self.is_directory(listing_path)):
                    dirs.append(listing_path)
                elif (self.is_file(listing_path)):
                    files.append(listing_path)
                else:
                    raise Exception("remove_dir_r: path: {}, not file or directory: {}".format(path, listing_path))

            # debug
            # utils.info("remove_dir_recursive: path: {}, files: {}, dirs: {}".format(path, files, dirs))

            # delete all the files first
            utils.info("remove_dir_r: files: {}".format(files))
            for f in files:
                self.remove_file(f, ignore_if_missing = ignore_if_missing)

            # now delete all directories
            dirs = sorted(dirs, reverse = True)

            # now delete directories
            utils.info("remove_dir_r: dirs: {}".format(dirs))
            for d in dirs:
                self.remove_dir(d, ignore_if_missing = ignore_if_missing)
            
            # now remove the path
            self.remove_dir(path, ignore_if_missing = ignore_if_missing)

        # check for commit
        if (verify == True and self.file_not_exists_with_wait(path) == False):
            raise Exception("remove_dir_r: path: {}, verify commit failed".format(path))

    def write_text(self, path, text):
        # validation
        if (text is None):
            raise Exception("update: Null text: {}".format(path))
 
        utils.info("write_text  : {}".format(path))
        self.fs.write_text_file(self.__makepath__(path), text)

    # TODO: Change the order of input parameters
    def write_tsv(self, path, xtsv):
        # validation
        if (xtsv is None):
            raise Exception("update: Null xtsv: {}".format(path))
 
        utils.info("write_tsv   : {}, num_rows: {}, num_cols: {}".format(path, xtsv.num_rows(), xtsv.num_cols()))
        tsv.write(xtsv, self.__makepath__(path))

    def update(self, path, msg, verify = True, ignore_logging = False):
        # validation
        if (msg is None):
            raise Exception("update: Null msg: {}".format(path))
 
        if (ignore_logging == False):
            utils.info("update      : {}".format(path))
        else:
            utils.debug("update      : {}".format(path))

        # write
        self.update_json(path, msg.to_json(), verify = verify, ignore_logging = ignore_logging)

    def update_json(self, path, json_obj, verify = True, ignore_logging = False):
        # validation
        if (json_obj is None):
            raise Exception("update_json: Null json_obj: {}".format(path))
 
        self.fs.write_text_file(self.__makepath__(path), json.dumps(json_obj))
        if (ignore_logging == False):
            utils.info("update_json : {}".format(path))
        else:
            utils.debug("update_json : {}".format(path))

        # check for commit
        if (verify == True and self.file_exists_with_wait(path) == False):
            raise Exception("update_json: path: {}, verify commit failed".format(path))

    def update_dynamic_value_json(self, path, json_obj, verify = True, ignore_logging = False, max_keep = 2):
        # create path
        dynamic_value_path = "{}/{}".format(path, construct_dynamic_value_json())

        # get the most recent files
        files = self.get_recent_files(path)

        # write the value
        self.update_json(dynamic_value_path, json_obj, verify = verify, ignore_logging = ignore_logging)

        # sort the files
        if (files is not None):
            sorted_files = sorted(files)

            # check how many to delete
            if (len(sorted_files) > max_keep):
                # iterate and delete
                for f in sorted_files[0:-max_keep]:
                    self.remove_file("{}/{}".format(path, f), ignore_logging = ignore_logging)
    
    def update_dynamic_value(self, path, msg, verify = True, ignore_logging = False, max_keep = 2):
        self.update_dynamic_value_json(path, msg.to_json(), verify = verify, ignore_logging = ignore_logging, max_keep = max_keep)

    # this special api doesnt use timestamp but sequence number to guarantee update and avoid race conditions 
    # because of eventual consistency
    def update_dynamic_seq_update(self, path, msg, verify = True, ignore_logging = False, max_keep = 2):
        utils.warn_once("update_dynamic_seq_update: not implemented yet. Using update_dynamic_value")
        self.update_dynamic_value(path, msg, verify = verify, ignore_logging = ignore_logging, max_keep = max_keep)

    # TODO: this api needs rethinking coz of eventual consistency    
    def file_not_exists(self, path):
        utils.debug("file_not_exists : {}".format(path))
        return self.fs.file_not_exists(self.__makepath__(path))

    def file_not_exists_with_wait(self, path):
        return self.fs.file_not_exists_with_wait(self.__makepath__(path))

    def dir_not_exists_with_wait(self, path):
        return self.fs.dir_not_exists_with_wait(self.__makepath__(path))

    # TODO: this api needs rethinking coz of eventual consistency    
    def file_exists(self, path):
        utils.debug("file_exists    : {}".format(path))
        return self.fs.file_exists(self.__makepath__(path))

    def file_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, ignore_if_missing = False):
        # add ignore_if_missing logic
        try:
            return self.fs.file_exists_with_wait(self.__makepath__(path), wait_sec = wait_sec, attempts = attempts)
        except Exception as e:
            # raise exception if ignore_if_missing is False
            if (ignore_if_missing == False):
                raise e
            else:
                return False

    def dir_exists(self, path):
        return self.fs.dir_exists(self.__makepath__(path))

    def dir_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, ignore_if_missing = False):
        # add ignore_if_missing logic
        try:
            return self.fs.dir_exists_with_wait(self.__makepath__(path), wait_sec = wait_sec, attempts = attempts)
        except Exception as e:
            # raise exception if ignore_if_missing is False 
            if (ignore_if_missing == False):
                raise e
            else:
                return False

    def is_file(self, path):
        return self.fs.is_file(self.__makepath__(path))

    def is_directory(self, path):
        return self.fs.is_directory(self.__makepath__(path))

    def is_non_empty_dir(self, path):
        # check for directory
        if (self.is_directory(path) == False):
            return False

        # get all entries
        entries = self.fs.ls(self.__makepath__(path))
        if (entries is not None and len(entries) > 0):
            return True
        else:
            return False
    
    def get_parent(self, path):
        # normalize
        path = self.__strip_leading_trailing_slashes__(path)

        # split and join
        if (path.find("/") != -1):
            return "/".join(path.split("/")[0:-1])
        else:
            return ""

    def __normalize_path__(self, path):
        if (path.startswith("/")):
            path = path[1:]
        if (path.endswith("/")):
            path = path[:-1]

        return path

    def read(self, path):
        path = self.__normalize_path__(path)
        utils.debug("read: {}".format(path))
        return self.fs.read_file_contents_as_text_with_wait(self.__makepath__(path))
   
    # TODO: Race condition . Create method with wait suffix
    def read_json(self, path, retries = 5, wait_sec = 1):
        path = self.__normalize_path__(path)

        content = self.read(path)
        try:
            return json.loads(content)
        except Exception as e:
            # some race condition it seems. EventualConsistency probably. TODO. This was the case with s3fs probably
            utils.warn("read_json: some race condition: {}, {}".format(path, content))
            if (retries > 0):
                return self.read_json(path, retries = retries - 1, wait_sec = wait_sec * 2)
            else:
                raise e

    # TODO: remove tsvutils dependency
    def read_tsv(self, path_or_paths):
        paths = utils.get_argument_as_array(path_or_paths)
        full_paths = list([self.__makepath__(self.__normalize_path__(p)) for p in utils.get_argument_as_array(path_or_paths)])
        return tsv.read(full_paths)

    # this is a special method that reads all the files in a directory. The format of the filename is <something>.TIMESTAMP.EXTENSION
    # the filename with the highest timestamp will be returned
    def get_recent_files(self, path):
        path = self.__normalize_path__(path)

        # get all the files in the directory
        files = self.list_files(path)

        # boundary conditions
        if (files is None or len(files) == 0):
            utils.debug("get_recent_files: path: {}, no file found.".format(path))
            return None

        # return
        return files

    def get_most_recent_file(self, path):
        # get all files
        files = self.get_recent_files(path)

        # check for None
        if (files is None):
            return None

        # sort the files
        sorted_files = sorted(files)

        # return
        return sorted_files[-1] 

    def dynamic_value_exists(self, path):
        return self.get_most_recent_file(path) is not None

    def read_most_recent(self, path):
        path = self.__normalize_path__(path)

        # get the most recent file
        filename = self.get_most_recent_file(path)

        # check for None
        if (filename is None):
            return None

        # call read_json
        new_path = "{}/{}".format(path, filename)
        return self.read(new_path)

    def read_most_recent_server_timestamp(self, path):
        path = self.__normalize_path__(path)

        # get the most recent file
        filename = self.get_most_recent_file(path)

        # check for None
        if (filename is None):
            return None

        # call read_json
        new_path = "{}/{}".format(path, filename)

        # get the timestamp
        return self.fs.get_last_modified_timestamp(self.__makepath__(new_path))

    # TODO: For json parsing error, just do a simple retry first.
    # JSONDecodeError
    def read_most_recent_json(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        content = self.read_most_recent(path)
        if (content is not None):
            try:
                return json.loads(content)
            except Exception as e:
                utils.error("read_most_recent_json: caught exception in parsing json: {}, error: {}, attempts: {}, wait_sec: {}".format(content, e, attempts, wait_sec))
                if (attempts > 0):
                    time.sleep(wait_sec)
                    return self.read_most_recent_json(path, wait_sec = wait_sec, attempts = attempts - 1)
                else:
                    raise e
        else:
            return None

    def read_most_recent_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, ignore_if_missing = False):
        # read
        content = self.read_most_recent(path)

        # return if valid response
        if (content is not None):
            return content 

        # check if attempts are left
        if (attempts > 0):
            utils.info("read_most_recent_with_wait: path: {}, data not found. attempts: {}, waiting: {} seconds".format(path, attempts, wait_sec))
            time.sleep(wait_sec)
            return self.read_most_recent_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)
        else:
            if (ignore_if_missing == False):
                raise Exception("read_most_recent_with_wait: path: {}, data not found. attempts: over".format(path))
            else:
                return None

    def read_most_recent_json_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, ignore_if_missing = False):
        content = self.read_most_recent_with_wait(path, wait_sec = wait_sec, attempts = attempts, ignore_if_missing = ignore_if_missing)
        if (content is not None):
            return json.loads(content)
        else:
            return None

    def is_json(self, path):
        return path.endswith(".json")
 
    def to_json(self, transient_keys = []):
        transient_keys2 = list([x for x in transient_keys])
        transient_keys2.append("s3")
        return super().to_json(transient_keys = transient_keys2)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterFileHandler(json_obj["base_path"])

    def get_full_path(self, path):
        return self.__makepath__(path)
    
    def new(base_path):
        return ClusterFileHandler(base_path)

# protocol paths
class ClusterPaths:
    DEFAULT_OUTPUT_PREFIX = "output"

    # global constants
    def get_base_path():
        global HYDRA_PATH
        if (HYDRA_PATH is None):
            raise Exception("HYDRA_PATH is None")

        return HYDRA_PATH

    def set_base_path(path):
        global HYDRA_PATH
        HYDRA_PATH = path

    def get_cluster_handler():
        # refer global variables
        global HYDRA_CLUSTER_HANDLER
        global HYDRA_CLUSTER_HANDLER_LOCK

        # use lock for thread safety
        with HYDRA_CLUSTER_HANDLER_LOCK:
            if (HYDRA_CLUSTER_HANDLER is None):
                HYDRA_CLUSTER_HANDLER = ClusterFileHandler.new(ClusterPaths.get_base_path())

        # return
        return HYDRA_CLUSTER_HANDLER

    # global constants
    def get_local_base_path():
        global HYDRA_LOCAL_PATH
        if (HYDRA_LOCAL_PATH is None):
            raise Exception("HYDRA_LOCAL_PATH is None")

        return HYDRA_LOCAL_PATH

    def set_local_base_path(path):
        global HYDRA_LOCAL_PATH
        HYDRA_LOCAL_PATH = path

    def get_local_cluster_handler():
        # refer global variables
        global HYDRA_LOCAL_CLUSTER_HANDLER
        global HYDRA_LOCAL_CLUSTER_HANDLER_LOCK

        # use lock for thread safety
        with HYDRA_LOCAL_CLUSTER_HANDLER_LOCK:
            if (HYDRA_LOCAL_CLUSTER_HANDLER is None):
                HYDRA_LOCAL_CLUSTER_HANDLER = ClusterFileHandler.new(ClusterPaths.get_local_base_path())

        # return
        return HYDRA_LOCAL_CLUSTER_HANDLER

    #########################################################################################
    # all entities that need life cycle management
    #########################################################################################
    def __get_entities_ids_base_path__():
        return "/entities-ids"
    
    def __get_entities_base_path__():
        return "/entities-details"

    def __get_entities_state_base_path__():
        return "/entities-state"

    def __get_entities_active_children_base_path__():
        return "/entities-active-children"

    def __get_entities_passive_children_base_path__():
        return "/entities-passive-children"

    def __get_entities_dependents_base_path__():
        return "/entities-dependents"

    def __get_entities_heartbeat_base_path__():
        return "/entities-heartbeat"

    def __get_entities_incoming__():
        return "/entities-incoming"

    def __get_entities_assigned_supervisor__():
        return "/entities-assigned-supervisor"

    def __get_entities_assigned_executors__():
        return "/entities-assigned-executors"

    def __get_entities_assigned_execution_tasks__():
        return "/entities-assigned-execution-tasks"

    def __get_entities_data__():
        return "/entities-data"

    def __get_current_master_base_path__():
        return "/current-master"

    def get_base_paths():
        return [
            ClusterPaths.__get_entities_ids_base_path__(),
            ClusterPaths.__get_entities_base_path__(),
            ClusterPaths.__get_entities_state_base_path__(),
            ClusterPaths.__get_entities_active_children_base_path__(),
            ClusterPaths.__get_entities_passive_children_base_path__(),
            ClusterPaths.__get_entities_dependents_base_path__(),
            ClusterPaths.__get_entities_heartbeat_base_path__(),
            ClusterPaths.__get_entities_incoming__(),
            ClusterPaths.__get_entities_assigned_supervisor__(),
            ClusterPaths.__get_entities_assigned_executors__(),
            ClusterPaths.__get_entities_assigned_execution_tasks__(),
            ClusterPaths.__get_entities_data__(),
            ClusterPaths.__get_current_master_base_path__()
        ]

    # /entities
    def get_entities_ids(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_ids_base_path__(), entity_type)

    def get_entity_id(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_ids(entity_type), entity_id)

    def get_entities(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_base_path__(), entity_type) 

    def get_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities(entity_type), entity_id)

    def get_entities_state(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_state_base_path__(), entity_type)

    def get_entities_state_by_state(entity_type, entity_state):
        return "{}/{}".format(ClusterPaths.get_entities_state(entity_type), entity_state)

    def get_entities_state_by_id(entity_type, entity_state, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_state_by_state(entity_type, entity_state), entity_id)

    def get_entities_active_children(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_active_children_base_path__(), entity_type)

    def get_entity_active_children(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_active_children(entity_type), entity_id)

    def get_entity_active_children_by_child_type(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_active_children(entity_type, entity_id), child_entity_type)

    def get_entity_active_children_by_id(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_active_children_by_child_type(entity_type, entity_id, child_entity_type), child_entity_id)

    def get_entities_passive_children(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_passive_children_base_path__(), entity_type)

    def get_entity_passive_children(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_passive_children(entity_type), entity_id)

    def get_entity_passive_children_by_child_type(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_passive_children(entity_type, entity_id), child_entity_type)

    def get_entity_passive_children_by_id(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_passive_children_by_child_type(entity_type, entity_id, child_entity_type), child_entity_id)

    def get_entities_dependents(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_dependents_base_path__(), entity_type)

    def get_entity_dependents(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_dependents(entity_type), entity_id)

    def get_entity_dependents_by_child_type(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_dependents(entity_type, entity_id), child_entity_type)

    def get_entity_dependents_by_id(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_dependents_by_child_type(entity_type, entity_id, child_entity_type), child_entity_id)

    def get_entities_heartbeat(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_heartbeat_base_path__(), entity_type)

    def get_entity_heartbeat(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_heartbeat(entity_type), entity_id)

    def get_entities_incoming(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_incoming__(), entity_type)

    def get_entity_incoming(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_incoming(entity_type), entity_id)

    def get_entities_assigned_supervisor(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_assigned_supervisor__(), entity_type)

    def get_entity_assigned_supervisor(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_assigned_supervisor(entity_type), entity_id)

    def get_entities_assigned_executors(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_assigned_executors__(), entity_type)

    def get_entity_assigned_executors(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_assigned_executors(entity_type), entity_id)

    def get_entity_assigned_executors_by_child_type(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_assigned_executors(entity_type, entity_id), child_entity_type)

    def get_entity_assigned_executors_by_id(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_assigned_executors_by_child_type(entity_type, entity_id, child_entity_type), child_entity_id)

    def get_entities_assigned_execution_tasks(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_assigned_execution_tasks__(), entity_type)

    def get_entity_assigned_execution_tasks(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_assigned_execution_tasks(entity_type), entity_id)

    def get_entity_assigned_execution_tasks_by_child_type(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_assigned_execution_tasks(entity_type, entity_id), child_entity_type)

    def get_entity_assigned_execution_tasks_by_id(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_assigned_execution_tasks_by_child_type(entity_type, entity_id, child_entity_type), child_entity_id)

    # /current-master
    def get_current_master():
        return "{}".format(ClusterPaths.__get_current_master_base_path__())

    # define data for each entity
    def get_entities_data(entity_type):
        return "{}/{}s".format(ClusterPaths.__get_entities_data__(), entity_type)

    def get_entity_data(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_entities_data(entity_type), entity_id) 
       
    def get_entity_data_child_entities(entity_type, entity_id, child_entity_type):
        return "{}/{}s".format(ClusterPaths.get_entity_data(entity_type, entity_id), child_entity_type) 

    def get_entity_data_child_entity(entity_type, entity_id, child_entity_type, child_entity_id):
        return "{}/{}".format(ClusterPaths.get_entity_data_child_entities(entity_type, entity_id, child_entity_type), child_entity_id)

    def get_entity_data_inputs(entity_type, entity_id):
        return "{}/inputs".format(ClusterPaths.get_entity_data(entity_type, entity_id))

    def get_entity_data_input(entity_type, entity_id, input_id):
        return "{}/{}".format(ClusterPaths.get_entity_data_inputs(entity_type, entity_id), input_id)

    def get_entity_data_input_file(entity_type, entity_id, input_id, file_index):
        return "{}/{}.tsv.gz".format(ClusterPaths.get_entity_data_input(entity_type, entity_id, input_id), file_index)

    def get_entity_data_outputs(entity_type, entity_id):
        return "{}/outputs".format(ClusterPaths.get_entity_data(entity_type, entity_id))

    def get_entity_data_output(entity_type, entity_id, output_id):
        return "{}/{}".format(ClusterPaths.get_entity_data_outputs(entity_type, entity_id), output_id)

    def get_entity_data_output_file(entity_type, entity_id, output_id, file_index):
        return "{}/{}.tsv.gz".format(ClusterPaths.get_entity_data_output(entity_type, entity_id, output_id), file_index)

    def get_entity_data_output_etl_dt(entity_type, entity_id, output_id, start_ts):
        return "{}/dt={}".format(ClusterPaths.get_entity_data_output(entity_type, entity_id, output_id), etl.get_etl_file_date_str_from_ts(start_ts))

    def get_entity_data_output_etl_file(entity_type, entity_id, output_id, start_ts, end_ts):
        return "{}/{}.tsv.gz".format(ClusterPaths.get_entity_data_output_etl_dt(entity_type, entity_id, output_id, start_ts),
            etl.get_etl_file_base_name_by_ts(ClusterPaths.DEFAULT_OUTPUT_PREFIX, start_ts, end_ts))

    def get_entity_data_input_batches(entity_type, entity_id, input_id):
        return "{}/batches".format(ClusterPaths.get_entity_data_input(entity_type, entity_id, input_id))

    def get_entity_data_input_batch(entity_type, entity_id, input_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_entity_data_input_batches(entity_type, entity_id, input_id), batch_id)

    def get_entity_data_input_batch_hashes(entity_type, entity_id, input_id, batch_id):
        return "{}/hashes".format(ClusterPaths.get_entity_data_input_batch(entity_type, entity_id, input_id, batch_id))

    def get_entity_data_input_batch_hash(entity_type, entity_id, input_id, batch_id, hash_id):
        return "{}/{}".format(ClusterPaths.get_entity_data_input_batch_hashes(entity_type, entity_id, input_id, batch_id), hash_id)

class ClusterExtendClass(cluster_data.JsonSer):
    def __init__(self, name_or_class_ref, *args, **kwargs):
        # take fully qualified name
        if (isinstance(name_or_class_ref, str)): 
            self.name = name_or_class_ref
        else:
            self.name = cluster_class_reflection.get_fully_qualified_class_name(name_or_class_ref)

        # this is an array of orthogonal types. dont assume homogenous types here
        jargs = []
        for x in args:
            jargs.append(cluster_data.cluster_operand_serializer(x))
        self.args = cluster_data.ClusterArrayObject(jargs)

        # dictionary is non homogenous types by default
        self.kwargs = cluster_data.cluster_operand_serializer(kwargs)

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # initialize variables
        name = json_obj["name"]
        args = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["args"]))
        kwargs = cluster_data.load_native_objects(cluster_data.cluster_operand_deserializer(json_obj["kwargs"]))

        # return
        return ClusterExtendClass.new(name, *args, **kwargs)

    def new(name, *args, **kwargs):
        return ClusterExtendClass(name, *args, **kwargs)

class ClusterOperationJob(cluster_data.JsonSer):
    def __init__(self, map_ops, reduce_op, singleton_op, extend_class_op):
        self.map_ops = map_ops
        self.reduce_op = reduce_op
        self.singleton_op = singleton_op
        self.extend_class_op = extend_class_op

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # create
        map_ops = []
        reduce_op = None
        extend_class_def = None

        # add maps
        for op in json_obj[MAP_OPS]:
            map_ops.append(ClusterMapOperation.from_json(op))

        # check for None or empty in reduce_op
        if (REDUCE_OP in json_obj.keys()):
            value = json_obj[REDUCE_OP]
            if (value is not None and value != ""):
                reduce_op = ClusterReduceOperation.from_json(value)

        # check for None or empty in singleton_op
        if (SINGLETON_OP in json_obj.keys()):
            value = json_obj[SINGLETON_OP]
            if (value is not None and value != ""):
                singleton_op = ClusterSingletonOperation.from_json(value)

        # check for None for extend_class_def
        if (EXTEND_CLASS_OP in json_obj.keys()):
            value = json_obj[EXTEND_CLASS_OP]
            if (value is not None and value != ""):
                extend_class_op = ClusterExtendClass.from_json(value)

        # return
        return ClusterOperationJob.new(map_ops, reduce_op, singleton_op, extend_class_op)

    def new(map_ops, reduce_op, singleton_op, extend_class_op):
        # return
        return ClusterOperationJob(map_ops, reduce_op, singleton_op, extend_class_op)

class ClusterIds:
    ID_SUFFIX = 0
    SWF_COUNTER = 0
    WF_COUNTER = 0
    JOB_COUNTER = 0
    TASK_COUNTER = 0
    TIMESTAMP = funclib.get_utctimestamp_sec()

    def set_id(ident):
        ClusterIds.ID_SUFFIX = ident

    def generate_swf_id():
        global ID_SUFFIX
        ClusterIds.WF_COUNTER = ClusterIds.WF_COUNTER + 1
        wf_id = "swf{:02}-{:04d}-{}".format(ClusterIds.ID_SUFFIX, ClusterIds.SWF_COUNTER, ClusterIds.TIMESTAMP)

        # return
        return wf_id

    def generate_wf_id():
        global ID_SUFFIX
        ClusterIds.WF_COUNTER = ClusterIds.WF_COUNTER + 1
        wf_id = "wf{:02}-{:04d}-{}".format(ClusterIds.ID_SUFFIX, ClusterIds.WF_COUNTER, ClusterIds.TIMESTAMP)

        # return
        return wf_id

    def generate_job_id():
        global ID_SUFFIX
        ClusterIds.JOB_COUNTER = ClusterIds.JOB_COUNTER + 1
        job_id = "job{:02}-{:04d}-{}".format(ClusterIds.ID_SUFFIX, ClusterIds.JOB_COUNTER, ClusterIds.TIMESTAMP)

        # return
        return job_id 

    def generate_task_id(task_type):
        global ID_SUFFIX
        ClusterIds.TASK_COUNTER = ClusterIds.TASK_COUNTER + 1
        task_id = "{}-task{:02}-{:04d}-{}".format(task_type, ClusterIds.ID_SUFFIX, ClusterIds.TASK_COUNTER, ClusterIds.TIMESTAMP)

        # return
        return task_id

    def get_entity_id(entity_type):
        return "{}-id{:02}-{}".format(entity_type, ClusterIds.ID_SUFFIX, ClusterIds.TIMESTAMP)

    def get_client_id():
        return ClusterIds.get_entity_id(EntityType.CLIENT)

    def get_session_id():
        return ClusterIds.get_entity_id(EntityType.SESSION)

def load_extend_class_obj(extend_class_op, header, data):
    # take the parameters
    class_name = extend_class_op.name
    args = cluster_data.load_native_objects(extend_class_op.args)
    kwargs = cluster_data.load_native_objects(extend_class_op.kwargs)

    # debug
    utils.debug("load_extend_class_obj: class_name: {}, header: {}, data: {}, args: {}, kwargs: {}".format(class_name, len(header), len(data), args, kwargs))

    # take class reference
    class_ref = cluster_class_reflection.load_class_ref_by_name(class_name)

    # call constructor
    return class_ref(header, data, *args, **kwargs)

class TSVReference:
    OMIGO_REFERENCE_PATH = ".omigo.reference.path"
    def __init__(self, xtsv):
        self.xtsv = xtsv

    def read(self, num_par = 0):
        # check if this is a reference file
        if (TSVReference.OMIGO_REFERENCE_PATH in self.xtsv.get_columns()):
            # get the index of path
            index = self.xtsv.get_column_index(TSVReference.OMIGO_REFERENCE_PATH)

            # create paths
            tasks = []

            # iterate through each reference path
            for line in self.xtsv.get_data():
                fields = line.split("\t", -1)
                tasks.append(utils.ThreadPoolTask(tsv.read, fields[index]))

            # read the paths. merge with union
            results = utils.run_with_thread_pool(tasks, num_par = num_par)
            return tsvutils.merge_union(results)
        else:
            return self.xtsv

# ClusterTSV Spec
