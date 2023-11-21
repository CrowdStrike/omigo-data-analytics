import datetime
from datetime import timezone
import json
import os
import time
import math 
import json 
import threading
from omigo_core import tsv, utils, tsvutils, etl, funclib

# class that takes the base path in S3, and implement all distributed communication under that.
# takes care of protocol level things for future

# global constants. TODO        
# if ("HYDRA_PATH" in os.environ.keys()):
#     HYDRA_PATH = os.environ["HYDRA_PATH"]
# else:
if ("HYDRA_PATH" in os.environ.keys()):
    HYDRA_PATH = os.environ["HYDRA_PATH"]
else:
    # HYDRA_PATH = ""
    raise Exception("Use HYDRA_PATH env variable")

HYDRA_LOCAL_PATH = ""

# global variables
HYDRA_CLUSTER_HANDLER = None
HYDRA_LOCAL_CLUSTER_HANDLER = None

HYDRA_CLUSTER_HANDLER_LOCK = threading.Lock()
HYDRA_LOCAL_CLUSTER_HANDLER_LOCK = threading.Lock()

# some constants
MIN1_SECONDS = 60

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
            ClusterExtendClassOperation.from_json(json_obj["extend_class_op"]),
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
        for op in json_obj["map_ops"]:
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
            ClusterReduceOperation.from_json(json_obj["reduce_op"]),
            json_obj["num_inputs"],
            json_obj["num_outputs"]
        )

    def new(reduce_op, num_inputs = 1, num_outputs = 1):
        return ClusterSpecReduceTask(reduce_op, num_inputs, num_outputs)

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
    else:
        raise Exception("deserialize_cluster_task_operation: unknown task_type for ClusterTaskOperation: {}".format(task_type))
 
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
                HYDRA_CLUSTER_HANDLER = old_cluster_common.ClusterFileHandler.new(ClusterPaths.get_base_path())

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
                HYDRA_LOCAL_CLUSTER_HANDLER = old_cluster_common.ClusterFileHandler.new(ClusterPaths.get_local_base_path())

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
    def __init__(self, map_ops, reduce_op, extend_class_op):
        self.map_ops = map_ops
        self.reduce_op = reduce_op
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
        for op in json_obj["map_ops"]:
            map_ops.append(ClusterMapOperation.from_json(op))

        # check for None or empty in reduce_op
        if ("reduce_op" in json_obj.keys()):
            value = json_obj["reduce_op"]
            if (value is not None and value != ""):
                reduce_op = ClusterReduceOperation.from_json(value)

        # check for None for extend_class_def
        if ("extend_class_op" in json_obj.keys()):
            value = json_obj["extend_class_op"]
            if (value is not None and value != ""):
                extend_class_op = ClusterExtendClass.from_json(value)

        # return
        return ClusterOperationJob.new(map_ops, reduce_op, extend_class_op)

    def new(map_ops, reduce_op, extend_class_op):
        # return
        return ClusterOperationJob(map_ops, reduce_op, extend_class_op)

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
