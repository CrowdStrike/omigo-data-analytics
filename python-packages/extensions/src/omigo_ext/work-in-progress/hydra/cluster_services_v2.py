from omigo_ajaiswal_ext.hydra import cluster_common_v2
from omigo_ajaiswal_ext.hydra import cluster_protocol_v2
from omigo_ajaiswal_ext.hydra import cluster_tsv
from omigo_ajaiswal_ext.hydra import cluster_funcs
from omigo_ajaiswal_ext.hydra.cluster_common_v2 import EntityType
from omigo_ajaiswal_ext.hydra.cluster_common_v2 import EntityState
from omigo_core import utils, funclib, etl
# utils.enable_debug_mode()
import time
from omigo_core import tsv
import json

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
 
    ts = funclib.get_utctimestamp_sec() 
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
        # , SWF_PROTOCOL, WF_PROTOCOL, JOB_PROTOCOL, TASK_PROTOCOL]

    # admin and master election
    CLUSTER_ADMIN = cluster_protocol_v2.ClusterAdmin()
    MASTER_ELECTION_PROTOCOL = cluster_protocol_v2.ClusterMasterElectionProtocol(MASTER.entity_id)


