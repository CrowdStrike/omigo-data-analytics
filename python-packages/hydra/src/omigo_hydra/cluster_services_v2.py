from omigo_hydra import cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra import cluster_arjun
from omigo_hydra import cluster_data
from omigo_hydra import cluster_funcs
from omigo_hydra import cluster_shell_ext
from omigo_hydra import etl
from omigo_hydra import hydra
from omigo_hydra import message_bus
from omigo_hydra import cluster_df
from omigo_hydra.cluster_common_v2 import EntityType
from omigo_hydra.cluster_common_v2 import EntityState
from omigo_hydra.cluster_common_v2 import ClusterCapabilities
from omigo_hydra.cluster_common_v2 import ClusterPaths
from omigo_core import utils, timefuncs
from omigo_ext import splunk_ext
# utils.enable_debug_mode()
import time
from omigo_core import dataframe, utils
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
    RESOURCE_MANAGER_PROTOCOL = cluster_protocol_v2.ClusterRMShardProtocol(RESOURCE_MANAGER)
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
        # Master assigns shards to RMs on first startup
        MASTER_PROTOCOL.assign_shards_to_rms()

# Run1 with initialization
def run1_client():
    SESSION_PROTOCOL.initialize()
    CLIENT_PROTOCOL.initialize()

# Run2 with supervisor assignment and monitoring
def run2(n = 10, wait_sec = 5, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "run2")
    for i in range(n):
        print("Running iteration: {}".format(i+1))
        for p in ALL_PROTOCOLS:
            # Master: election + incoming (MASTER and RM only) + shard assignment
            if (p.entity.entity_type == cluster_common_v2.EntityType.MASTER):
                if (MASTER_ELECTION_PROTOCOL.run_election()):
                    MASTER_PROTOCOL.refresh_master_cache()
                # monitor supervisor (V2: only MASTER and RM types)
                p.monitor_incoming_for_supervisor()
                # monitor and reassign shards if RMs died
                p.monitor_shard_assignments()

            # RM: read shard assignments from master + process incoming
            if (p.entity.entity_type == cluster_common_v2.EntityType.RESOURCE_MANAGER):
                p.monitor_shard_incoming()

            # run active child supervisors
            if (p.entity.entity_type in cluster_common_v2.EntityActiveSupervisorTypes):
                p.monitor_active_children()

            # run passive child supervisors
            if (p.entity.entity_type in cluster_common_v2.EntityPassiveSupervisorTypes):
                p.monitor_passive_children()

            # run tasks
            if (p.entity.entity_type in cluster_common_v2.EntityExecutorTypes):
                p.monitor_execution_tasks()

        utils.info("{}: Sleeping for {} seconds".format(dmsg, wait_sec))
        time.sleep(wait_sec)

# Debugging
def get_cluster_handler():
    return ClusterPaths.get_cluster_handler()

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

def read_df(path):
    return get_cluster_handler().read_df(path)

def write_df(path, xdf):
    # TODO: add native DataFrame write support to cluster handler
    return get_cluster_handler().write_text_file(path, xdf.to_string())

def read_workflow_input(namespace, wf_id, max_duration = 3*86400, sleep_sec = 3):
    # output file
    input_file = cluster_common_v2.ClusterPaths.get_passive_entity_data_input_default_file_path(namespace, cluster_common_v2.EntityType.WF, wf_id, "input_id1")

    # return
    return read_workflow_file_path(input_file, max_duration = max_duration, sleep_sec = sleep_sec)

def read_workflow_output(namespace, wf_id, max_duration = 3600, sleep_sec = 3):
    # output file
    output_file = cluster_common_v2.ClusterPaths.get_passive_entity_data_output_default_file_path(namespace, cluster_common_v2.EntityType.WF, wf_id, "output_id1")

    # return
    return read_workflow_file_path(output_file, max_duration = max_duration, sleep_sec = sleep_sec)

def read_swf_output(namespace, swf_id, output_id, max_duration = 3600, sleep_sec = 3):
    # find child WFs of this SWF
    handler = get_cluster_handler()
    wf_ids = handler.list_dirs(cluster_common_v2.ClusterPaths.get_entity_dependents_by_child_type(
        cluster_common_v2.EntityType.SWF, swf_id, cluster_common_v2.EntityType.WF))

    # find the WF that has the matching output_id
    for wf_id in wf_ids:
        output_path = cluster_common_v2.ClusterPaths.get_passive_entity_data_output_path(namespace, cluster_common_v2.EntityType.WF, wf_id, output_id)
        if (handler.dir_exists(output_path)):
            output_file = cluster_common_v2.ClusterPaths.get_passive_entity_data_output_default_file_path(namespace, cluster_common_v2.EntityType.WF, wf_id, output_id)
            return read_workflow_file_path(output_file, max_duration = max_duration, sleep_sec = sleep_sec)

    raise Exception("read_swf_output: output_id '{}' not found in any child WF of SWF '{}'".format(output_id, swf_id))

def read_swf_live_output(namespace, swf_id, output_id, sleep_sec = 3):
    # find child WFs of this SWF
    handler = get_cluster_handler()
    wf_ids = handler.list_dirs(cluster_common_v2.ClusterPaths.get_entity_dependents_by_child_type(
        cluster_common_v2.EntityType.SWF, swf_id, cluster_common_v2.EntityType.WF))

    # find the WF that has the matching output_id
    for wf_id in wf_ids:
        output_path = cluster_common_v2.ClusterPaths.get_passive_entity_data_output_path(namespace, cluster_common_v2.EntityType.WF, wf_id, output_id)
        if (handler.dir_exists(output_path)):
            return read_workflow_live_output(namespace, wf_id, output_id, sleep_sec = sleep_sec)

    raise Exception("read_swf_live_output: output_id '{}' not found in any child WF of SWF '{}'".format(output_id, swf_id))

class SWFBuilder:
    """Builder for Super-Workflows with automatic DAG inference.

    Accepts a ClusterExecutorContext to determine execution mode:
    - If session_protocol is None: executes DAG locally in topological order (inmemory mode)
    - If session_protocol is set: submits to cluster

    WFs can be:
    - HydraDF reference (new pattern): operations extracted via to_wf_spec()
    - WFBlueprint instance (legacy): use directly
    - Callable (legacy): fn(upstream_outputs) -> WFBlueprint

    Usage (new):
        ctx = ClusterExecutorContext(namespace = "ns", session_protocol = sp)
        hydra_df_a = ctx.from_maps([...]).extend_class(HydraDF, ctx).select([...]).checkpoint("stats")
        hydra_df_b = ctx.read_df("A").join(ctx.read_df("stats"), ...)

        swf_id = SWFBuilder(ctx) \\
            .add_wf("A", hydra_df_a) \\
            .add_wf("B", hydra_df_b) \\
            .materialize()

    Edge inference:
        - Output: WF name is default output_id. Checkpoints add additional output_ids.
        - Input: Inferred from proxy markers (ctx.read_df) and WFBlueprint join operands.
        - Explicit input_ids/output_ids override inference when provided (backward compat).
    """

    def __init__(self, ctx, session_protocol = None, tags = {}, params = {}):
        """
        Args:
            ctx: ClusterExecutorContext instance.
            session_protocol: optional override
            tags: metadata tags for the SWF
            params: SWF-level params (Dict[str, str]). Inherited by all WFs; each WF can override.
        """
        from omigo_core.context import ExecutorContext as BaseExecutorContext
        if (not isinstance(ctx, BaseExecutorContext)):
            raise Exception("SWFBuilder: first argument must be an ExecutorContext. Got: {}".format(type(ctx)))

        self.ctx = ctx
        self.namespace = ctx.namespace if (hasattr(ctx, 'namespace')) else None
        self.session_protocol = session_protocol if (session_protocol is not None) else (ctx.session_protocol if (hasattr(ctx, 'session_protocol')) else None)

        self.tags = tags
        self.params = params
        self.wf_entries = []

    def add_wf(self, name, wf_blueprint, input_ids = [], output_ids = [], is_live = False, bucket_interval = -1, ticks_interval = None, duration = 0,
               start_ts = None, use_full_data = False, max_job_execution_time = 600, num_splits = None, tags = {}, event_ts_col = None, params = {}):
        """Register a WF with per-WF execution config. Returns self for chaining.

        wf_blueprint can be:
        - HydraDF instance (new pattern): extract operations via to_wf_spec()
        - WFBlueprint instance (legacy): use directly
        - Callable (legacy): fn(upstream_outputs) -> WFBlueprint

        input_ids/output_ids: Optional. When both are empty (default), edges are inferred
        at materialize() time from the blueprint's proxy markers and checkpoint operations.
        When provided explicitly, inference is skipped for this entry.
        """
        from omigo_hydra.cluster_df import HydraBaseDF
        # Convert HydraDF to WFBlueprint for internal processing
        if (isinstance(wf_blueprint, HydraBaseDF)):
            blueprint = wf_blueprint.to_wf_spec()
        else:
            blueprint = wf_blueprint

        is_live = (bucket_interval > 0) if (not is_live) else is_live
        if (ticks_interval is None):
            ticks_interval = bucket_interval

        self.wf_entries.append({
            "name": name,
            "blueprint": blueprint,
            "input_ids": input_ids,
            "output_ids": output_ids,
            "is_live": is_live,
            "bucket_interval": bucket_interval,
            "ticks_interval": ticks_interval,
            "duration": duration,
            "start_ts": start_ts,
            "use_full_data": use_full_data,
            "max_job_execution_time": max_job_execution_time,
            "num_splits": num_splits,
            "tags": tags,
            "event_ts_col": event_ts_col,
            "params": params,
        })
        return self

    def __infer_dependencies__(self, entries_by_name):
        """Infer depends_on for each WF by matching input_ids to output_ids across WFs."""
        # build output_id -> wf_name map
        output_to_wf = {}
        for name, entry in entries_by_name.items():
            for output_id in entry["output_ids"]:
                if (output_id in output_to_wf):
                    raise Exception("SWFBuilder: duplicate output_id '{}' in WFs '{}' and '{}'".format(output_id, output_to_wf[output_id], name))
                output_to_wf[output_id] = name

        # infer depends_on for each WF using unified input_ids (includes join operands)
        for name, entry in entries_by_name.items():
            deps = set()
            dep_ids = entry["input_ids"]
            for input_id in dep_ids:
                if (input_id in output_to_wf):
                    dep_name = output_to_wf[input_id]
                    if (dep_name == name):
                        raise Exception("SWFBuilder: WF '{}' has input_id '{}' that matches its own output_id".format(name, input_id))
                    deps.add(dep_name)
            entry["depends_on"] = list(deps)

    def __infer_edges__(self, entry):
        """Infer input_ids and output_ids from the WFBlueprint.

        Output inference:
        - WF name is the default output_id.
        - Each ClusterCheckpointOperation in jobs_operations adds an additional output_id.

        Input inference (unified: primary + operands):
        - OMIGO_ARJUN_PROXY_DATAFRAME_ID in blueprint.xinput -> primary input_id.
        - WFBlueprint/proxy operands in operation args -> additional input_ids.
        - primary_input_id explicitly identifies the per-iteration pipeline input.
        """
        bp = entry["blueprint"]

        # --- Output inference ---
        output_ids = [entry["name"]]
        for job in bp.jobs_operations:
            if (job.checkpoint_op is not None):
                cp_name = job.checkpoint_op.name
                if (cp_name in output_ids):
                    raise Exception("SWFBuilder.__infer_edges__(): WF '{}' has duplicate output_id '{}' (checkpoint name collides with WF name or another checkpoint)".format(entry["name"], cp_name))
                output_ids.append(cp_name)
        entry["output_ids"] = output_ids

        # --- Input inference (unified: primary + operands) ---
        input_ids = []
        primary_input_id = None
        input_params_config = {}

        # primary input from xinput proxy marker
        if (cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID in bp.xinput.header_fields):
            proxy_id = bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID)[0]
            input_ids.append(proxy_id)
            primary_input_id = proxy_id

            # extract params enrichment metadata from proxy marker if present
            import json
            proxy_params_config = {}
            if (cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS in bp.xinput.header_fields):
                proxy_params_config["params"] = json.loads(bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS)[0])
            if (cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS_PREFIX in bp.xinput.header_fields):
                proxy_params_config["prefix"] = bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS_PREFIX)[0]
            if (len(proxy_params_config) > 0):
                input_params_config[proxy_id] = proxy_params_config

        # join operands: scan operations for proxy/WFBlueprint args
        for job in bp.jobs_operations:
            wf_input_ids = self.__extract_operand_input_ids__(job)
            input_ids.extend(wf_input_ids)

        # root WFs (no upstream dependency) keep a synthetic input_id
        # so the executor can read seed data from the standard input path
        if (len(input_ids) == 0):
            input_ids.append("seed_input")
            primary_input_id = "seed_input"

        entry["input_ids"] = input_ids
        entry["primary_input_id"] = primary_input_id
        entry["input_params_config"] = input_params_config

    def __extract_operand_input_ids__(self, job):
        """Scan a ClusterOperationJob's operations for proxy/WFBlueprint operands.

        Returns a list of input_ids found in operation arguments.
        Looks at singleton_op args (join arguments), reduce_op args, and map_op args.
        """
        input_ids = []

        def scan_operands(args_obj):
            """Extract proxy ids from a ClusterArrayObject or list of operands."""
            if (args_obj is None):
                return
            # args_obj is a ClusterArrayObject with .value list (ClusterOperand stores data in .value)
            values = args_obj.value if (hasattr(args_obj, "value") and isinstance(args_obj.value, list)) else (args_obj if isinstance(args_obj, list) else [])
            for operand in values:
                if (not hasattr(operand, "data_type")):
                    continue
                if (operand.data_type == cluster_data.PROXY_DATAFRAME_DATA_TYPE):
                    # ClusterProxyDataFrame: value is the proxy id
                    input_ids.append(operand.value)
                elif (operand.data_type == "WFBlueprint"):
                    # Nested WFBlueprint: check its xinput for proxy marker
                    nested_bp = operand.value
                    if (hasattr(nested_bp, "xinput") and cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID in nested_bp.xinput.header_fields):
                        input_ids.append(nested_bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID)[0])

        # scan singleton_op (join operations)
        if (job.singleton_op is not None):
            scan_operands(job.singleton_op.args)

        # scan reduce_op
        if (job.reduce_op is not None):
            scan_operands(job.reduce_op.args)

        # scan map_ops
        for map_op in (job.map_ops or []):
            scan_operands(map_op.args)

        return input_ids

    def __build_proxy_marker__(self, input_id):
        """Build a single-row proxy marker DataFrame for a given input_id.
        The proxy marker contains OMIGO_ARJUN_PROXY_DATAFRAME_ID as the column,
        with the input_id as the value. At runtime, execute_live reads this file
        and detects the proxy column to classify the input as upstream-dependent."""
        from omigo_core import dataframe
        return dataframe.from_maps([{cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID: input_id}])

    def __resolve_seed_input_content__(self, xinput, entry):
        """Determine the seed_input content for a root WF.

        Shared by both local and cluster modes to ensure consistent behavior.
        For from_maps root WFs (xinput is actual data, no proxy marker), returns the inline data.
        For proxy/seed-based root WFs (xinput has proxy marker), returns params-based seed_input."""
        if (cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID not in xinput.header_fields):
            # from_maps root: the inline data IS the seed input
            return xinput
        else:
            # proxy root: build params-based seed_input
            merged_params = {**self.params, **entry["params"]}
            return self.__build_seed_input_from_params__(merged_params)

    def __build_seed_input_from_params__(self, params):
        """Build a single-row seed_input DataFrame from merged params + timestamp placeholders.
        Params keys become columns, values become the single row. Timestamp placeholder columns
        (omigo.arjun.start_ts, omigo.arjun.end_ts) are added for resolution at execution time via resolve_meta().

        IMPORTANT: The timestamp columns contain placeholder template strings (e.g. '{omigo.arjun.start_ts}'),
        NOT actual values. The raw seed_input is read once from disk at WF setup, then resolve_meta() expands
        these placeholders per iteration with the current window's cur_start_ts / cur_end_ts. Each iteration
        gets different resolved values. The raw template DataFrame must never be assumed to hold concrete
        timestamps — it is a template that is re-resolved every iteration."""
        from omigo_core import dataframe
        row = {
            cluster_arjun.OMIGO_ARJUN_START_TS: cluster_arjun.OMIGO_ARJUN_START_TS_TEMPLATE,
            cluster_arjun.OMIGO_ARJUN_END_TS: cluster_arjun.OMIGO_ARJUN_END_TS_TEMPLATE,
        }
        for k, v in params.items():
            row[k] = str(v)
        return dataframe.from_maps([row])

    def __topological_phases__(self, entries_by_name):
        """Return a list of phases (each phase is a list of WF names that can run in parallel)."""
        remaining = set(entries_by_name.keys())
        satisfied = set()
        phases = []
        while (len(remaining) > 0):
            phase = []
            for name in list(remaining):
                deps = set(entries_by_name[name]["depends_on"])
                if (deps.issubset(satisfied)):
                    phase.append(name)
            if (len(phase) == 0):
                raise Exception("SWFBuilder: circular dependency detected among: {}".format(remaining))
            for name in phase:
                remaining.remove(name)
                satisfied.add(name)
            phases.append(phase)
        return phases

    def __materialize_local__(self):
        """Execute DAG locally (inmemory): topological sort, run each WF eagerly, wire outputs through ctx store.

        For each WF in topological order:
        1. If blueprint has proxy input, resolve from ctx.read_output()
        2. Execute operations using cluster_executor.__execute_operations_local__()
        3. Store result under output_ids via ctx.store_output()

        Returns: "local_swf_complete"
        """
        from omigo_hydra import cluster_executor

        # build name -> entry map
        entries_by_name = {}
        for i, entry in enumerate(self.wf_entries):
            if (entry["name"] is None or entry["name"] == ""):
                raise Exception("SWFBuilder.__materialize_local__(): WF at index {} has empty name.".format(i))
            entries_by_name[entry["name"]] = entry

        # infer edges for entries that don't have explicit input_ids/output_ids
        for entry in self.wf_entries:
            if (len(entry["input_ids"]) == 0 and len(entry["output_ids"]) == 0):
                if (not callable(entry["blueprint"])):
                    self.__infer_edges__(entry)

        # infer dependencies
        self.__infer_dependencies__(entries_by_name)

        # validate bucket_interval multiples: downstream.bucket_interval % upstream.ticks_interval == 0
        # build output_id -> entry map for upstream lookups
        output_id_to_entry = {}
        for name, entry in entries_by_name.items():
            for output_id in entry["output_ids"]:
                output_id_to_entry[output_id] = entry

        for name, entry in entries_by_name.items():
            if (entry["bucket_interval"] <= 0):
                continue
            for input_id in entry["input_ids"]:
                upstream_entry = output_id_to_entry.get(input_id)
                if (upstream_entry is None):
                    continue
                upstream_ticks = upstream_entry["ticks_interval"]
                if (upstream_ticks <= 0):
                    continue
                if (entry["bucket_interval"] % upstream_ticks != 0):
                    raise Exception("SWFBuilder.__materialize_local__(): WF '{}' bucket_interval ({}) is not a multiple of upstream '{}' ticks_interval ({}). "
                        "Downstream bucket_interval must be an exact multiple of all upstream ticks_intervals.".format(
                            name, entry["bucket_interval"], input_id, upstream_ticks))
                if (entry["bucket_interval"] < upstream_ticks):
                    raise Exception("SWFBuilder.__materialize_local__(): WF '{}' bucket_interval ({}) is less than upstream '{}' ticks_interval ({}). "
                        "Downstream cannot have smaller bucket than upstream ticks.".format(
                            name, entry["bucket_interval"], input_id, upstream_ticks))

        # topological sort
        phases = self.__topological_phases__(entries_by_name)

        # execute each WF in DAG order
        for phase_names in phases:
            for wf_name in phase_names:
                entry = entries_by_name[wf_name]
                bp = entry["blueprint"]

                # resolve callable blueprints
                if (callable(bp)):
                    upstream_outputs = {}
                    for input_id in entry["input_ids"]:
                        if (self.ctx.has_output(input_id)):
                            upstream_outputs[input_id] = self.ctx.read_output(input_id)
                    bp = bp(upstream_outputs)

                # get the input DataFrame
                xinput = bp.xinput

                # resolve proxy input: substitute from ctx store or build seed_input from params
                if (cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID in xinput.header_fields):
                    proxy_input_id = xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID)[0]
                    if (proxy_input_id == "seed_input"):
                        # seed-based root WF (e.g. ctx.read_df("seed_input")): resolve through shared path
                        xinput = self.__resolve_seed_input_content__(xinput, entry)
                        # resolve timestamp templates to current time (local mode has no per-iteration windows)
                        now = timefuncs.get_utctimestamp_sec()
                        now_str = timefuncs.utctimestamp_to_datetime_str(now)
                        xinput = xinput.transform_inline(xinput.get_columns(),
                            lambda x: x.replace(cluster_arjun.OMIGO_ARJUN_START_TS_TEMPLATE, now_str).replace(cluster_arjun.OMIGO_ARJUN_END_TS_TEMPLATE, now_str),
                            ignore_if_missing = True)
                    elif (not self.ctx.has_output(proxy_input_id)):
                        raise Exception("SWFBuilder.__materialize_local__(): WF '{}' proxy input '{}' not in store. Available: {}".format(
                            wf_name, proxy_input_id, self.ctx.get_output_ids()))
                    else:
                        xinput = self.ctx.read_output(proxy_input_id)
                else:
                    # Root from_maps WF: resolve through the same shared path that cluster mode uses.
                    # This ensures any bug in seed_input resolution is caught in both modes.
                    xinput = self.__resolve_seed_input_content__(xinput, entry)

                # execute operations eagerly on xinput
                result = cluster_executor.__execute_operations_local__(xinput, bp.jobs_operations, self.ctx)

                # store result under all output_ids
                self.ctx.store_output(result, entry["output_ids"])

        return "local_swf_complete"

    def __build_wf_spec__(self, entry, bp, sp):
        """Build a ClusterSpecWF from a resolved blueprint."""
        num_splits = entry["num_splits"] if (entry["num_splits"] is not None) else bp.num_splits

        # resolve start_ts: user-provided datetime strings are converted; numeric (already resolved by materialize) pass through
        start_ts = entry["start_ts"]
        if (start_ts is not None and isinstance(start_ts, str)):
            start_ts = timefuncs.datetime_to_utctimestamp(start_ts)

        # merge params: SWF-level params + WF-level overrides
        merged_params = {**self.params, **entry["params"]}

        ctx = cluster_protocol_v2.ClusterExecutorContext(
            self.namespace,
            sp,
            is_live = entry["is_live"],
            is_external = False,
            max_job_execution_time = entry["max_job_execution_time"],
            bucket_interval = entry["bucket_interval"],
            ticks_interval = entry["ticks_interval"],
            duration = entry["duration"],
        )

        wf_spec = ctx.build_wf_spec(
            bp.jobs_operations, name = entry["name"], input_ids = entry["input_ids"], output_ids = entry["output_ids"],
            primary_input_id = entry.get("primary_input_id"),
            start_ts = start_ts, use_full_data = entry["use_full_data"], num_splits = num_splits, tags = entry["tags"],
            event_ts_col = entry["event_ts_col"], params = merged_params, input_params_config = entry.get("input_params_config", {}),
        )

        return wf_spec

    def __read_wf_outputs__(self, namespace, wf_id, output_ids, is_live):
        """Read all outputs of a completed WF. Returns dict of output_id -> dataframe."""
        outputs = {}
        for output_id in output_ids:
            if (is_live):
                outputs[output_id] = read_workflow_live_output(namespace, wf_id, output_id)
            else:
                output_file = ClusterPaths.get_passive_entity_data_output_default_file_path(namespace, EntityType.WF, wf_id, output_id)
                outputs[output_id] = read_workflow_file_path(output_file)
        return outputs

    def materialize(self):
        """Execute the DAG. Dispatches to inmemory or cluster based on session_protocol."""
        # Resolve session protocol
        sp = self.session_protocol if (self.session_protocol is not None) else SESSION_PROTOCOL

        # Inmemory mode: no session protocol available, execute locally
        if (sp is None and self.ctx is not None):
            return self.__materialize_local__()

        # Cluster mode: existing logic below
        if (sp is None):
            raise Exception("SWFBuilder.materialize(): session_protocol is None. Call cluster_services_v2.init() first or pass session_protocol.")

        # validate namespace exists
        self.cluster_handler = get_cluster_handler()
        if (self.cluster_handler.dir_exists(ClusterPaths.get_namespace_path(self.namespace)) == False):
            raise Exception("SWFBuilder.materialize(): namespace '{}' does not exist. Register it before submitting.".format(self.namespace))

        # build name -> entry map and validate
        entries_by_name = {}
        for i, entry in enumerate(self.wf_entries):
            if (entry["name"] is None or entry["name"] == ""):
                raise Exception("SWFBuilder.materialize(): WF at index {} has empty or None name. Every WF must have a non-empty name.".format(i))
            if (entry["name"] in entries_by_name):
                raise Exception("SWFBuilder.materialize(): duplicate WF name '{}'".format(entry["name"]))
            entries_by_name[entry["name"]] = entry

        # infer edges for entries that don't have explicit input_ids/output_ids
        for i, entry in enumerate(self.wf_entries):
            if (len(entry["input_ids"]) == 0 and len(entry["output_ids"]) == 0):
                if (not callable(entry["blueprint"])):
                    self.__infer_edges__(entry)
            # validate output_ids after inference
            if (len(entry["output_ids"]) == 0):
                raise Exception("SWFBuilder.materialize(): WF '{}' at index {} has empty output_ids after inference. Every WF must have non-empty output_ids.".format(entry["name"], i))

        # infer dependencies from input_id/output_id matching
        self.__infer_dependencies__(entries_by_name)

        # topological sort into phases
        phases = self.__topological_phases__(entries_by_name)

        # validate root WFs: proxy blueprints cannot be roots (they need upstream input to resolve)
        # callable blueprints are skipped — they always depend on upstream outputs
        # "seed_input" is a reserved proxy name for params-based input — exempt from this check
        for entry in self.wf_entries:
            bp = entry["blueprint"]
            if (callable(bp)):
                continue
            if (cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID in bp.xinput.header_fields and len(entry["depends_on"]) == 0):
                proxy_id = bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID)[0]
                if (proxy_id != "seed_input"):
                    raise Exception("SWFBuilder.materialize(): root WF '{}' has a proxy input_id '{}' but no upstream dependency".format(
                        entry["name"], proxy_id))

        # === RESOLVE start_ts: WFs with start_ts=None get a single shared now() ===
        # This ensures all WFs in the SWF share the same effective start, so upstream_inputs
        # metadata (frozen at build time) matches what execute_live uses at execution time.
        # Clock-aligned bucket boundaries (floor_to_bucket) require consistent start_ts across
        # the DAG — otherwise downstream windows can fall outside the upstream's recorded range.
        swf_default_start_ts = timefuncs.get_utctimestamp_sec()
        for entry in self.wf_entries:
            if (entry["start_ts"] is None):
                entry["start_ts"] = swf_default_start_ts
                utils.info("SWFBuilder: WF '{}' start_ts=None, resolved to {}".format(entry["name"], swf_default_start_ts))

        # === BUILD PHASE: resolve all blueprints and build wf_specs in DAG order ===
        upstream_outputs = {}
        built_wfs = []  # list of (wf_name, wf_spec, xinput, entry)

        utils.info("SWFBuilder: build phase: {} phases: {}".format(len(phases), phases))

        for phase_index, phase_names in enumerate(phases):
            utils.info("SWFBuilder: building phase {} with WFs: {}".format(phase_index, phase_names))

            for wf_name in phase_names:
                entry = entries_by_name[wf_name]
                bp = entry["blueprint"]

                # resolve callable blueprints: invoke with upstream outputs to get actual blueprint
                if (callable(bp)):
                    bp = bp(upstream_outputs)
                    entry["blueprint"] = bp

                # resolve proxy DataFrame: substitute actual upstream xinput
                # "seed_input" is reserved — it resolves from params, not upstream output
                if (cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID in bp.xinput.header_fields):
                    proxy_input_id = bp.xinput.col_as_array(cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID)[0]
                    if (proxy_input_id == "seed_input"):
                        pass  # seed_input resolved at execution time from params
                    elif (proxy_input_id not in upstream_outputs):
                        raise Exception("SWFBuilder.materialize(): WF '{}' has proxy input_id '{}' but no upstream WF produces it".format(
                            wf_name, proxy_input_id))
                    else:
                        bp.xinput = upstream_outputs[proxy_input_id]

                # build wf_spec (no cluster persistence yet)
                wf_spec = self.__build_wf_spec__(entry, bp, sp)
                built_wfs.append((wf_name, wf_spec, bp.xinput, entry))

                # register this WF's xinput as placeholder for downstream
                for output_id in entry["output_ids"]:
                    upstream_outputs[output_id] = bp.xinput

                utils.info("SWFBuilder: built WF '{}' (phase {})".format(wf_name, phase_index))

        # === SUBMIT PHASE: build protocols and submit atomically via SWFProtocol ===
        all_wf_specs = [wf_spec for _, wf_spec, _, _ in built_wfs]

        # generate all WF entities first so we can resolve upstream_inputs
        wf_entities = []
        for wf_name, wf_spec, xinput, entry in built_wfs:
            wf_entity = cluster_common_v2.ClusterEntityWF.new(
                self.namespace,
                cluster_protocol_v2.ClusterIds.generate_wf_id(),
                cluster_protocol_v2.ClusterIds.get_client_id(),
                cluster_protocol_v2.ClusterIds.get_session_id(),
                wf_spec,
            )
            wf_entities.append(wf_entity)
            utils.info("SWFBuilder: generated entity for WF '{}' as {}".format(wf_name, wf_entity.entity_id))

        # build output_id -> entity index map for upstream resolution
        output_id_to_idx = {}
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            for output_id in entry["output_ids"]:
                output_id_to_idx[output_id] = idx

        # populate upstream_inputs on each wf_spec whose input_ids match an upstream output_id
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            resolved_upstream = {}
            for dep_id in entry["input_ids"]:
                if (dep_id in output_id_to_idx):
                    upstream_idx = output_id_to_idx[dep_id]
                    upstream_entity = wf_entities[upstream_idx]
                    upstream_spec = upstream_entity.entity_spec
                    etl_path = cluster_common_v2.ClusterPaths.get_passive_entity_data_output_path(
                        upstream_entity.namespace, upstream_entity.entity_type, upstream_entity.entity_id, dep_id)
                    # start_ts is always concrete here (resolved by materialize if originally None)
                    upstream_start_ts = upstream_spec.start_ts if (upstream_spec.start_ts is not None and upstream_spec.start_ts > 0) else timefuncs.get_utctimestamp_sec()
                    resolved_upstream[dep_id] = {
                        "etl_path": etl_path,
                        "bucket_interval": upstream_spec.bucket_interval,
                        "ticks_interval": upstream_spec.ticks_interval,
                        "start_ts": upstream_start_ts,
                        "duration": upstream_spec.duration
                    }
                    utils.info("SWFBuilder: WF '{}' input '{}' -> upstream entity {} (bucket_interval={}, ticks_interval={})".format(
                        wf_name, dep_id, upstream_entity.entity_id, upstream_spec.bucket_interval, upstream_spec.ticks_interval))

            # overwrite with a fresh dict so we never mutate the shared default
            wf_spec.upstream_inputs = resolved_upstream

        # Validate bucket_interval multiples: downstream.bucket_interval % upstream.ticks_interval == 0
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            if (wf_spec.bucket_interval <= 0):
                continue  # static workflows don't have interval constraints
            for input_id, upstream_meta in wf_spec.upstream_inputs.items():
                upstream_ticks_interval = upstream_meta.get("ticks_interval", upstream_meta["bucket_interval"])
                if (upstream_ticks_interval <= 0):
                    continue  # upstream is static
                if (wf_spec.bucket_interval % upstream_ticks_interval != 0):
                    raise Exception("SWFBuilder.materialize(): WF '{}' bucket_interval ({}) is not a multiple of upstream '{}' ticks_interval ({}). "
                        "Downstream bucket_interval must be an exact multiple of all upstream ticks_intervals.".format(
                            wf_name, wf_spec.bucket_interval, input_id, upstream_ticks_interval))
                if (wf_spec.bucket_interval < upstream_ticks_interval):
                    raise Exception("SWFBuilder.materialize(): WF '{}' bucket_interval ({}) is less than upstream '{}' ticks_interval ({}). "
                        "Downstream cannot have smaller bucket than upstream ticks.".format(
                            wf_name, wf_spec.bucket_interval, input_id, upstream_ticks_interval))

        # Warn if a sliding-window WF's output is consumed by a downstream WF
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            if (wf_spec.bucket_interval <= 0):
                continue
            for input_id, upstream_meta in wf_spec.upstream_inputs.items():
                upstream_idx = output_id_to_idx.get(input_id)
                if (upstream_idx is not None):
                    upstream_wf_spec = built_wfs[upstream_idx][1]
                    if (upstream_wf_spec.ticks_interval < upstream_wf_spec.bucket_interval):
                        utils.warn("SWFBuilder.materialize(): WF '{}' reads from upstream '{}' which is a sliding-window WF "
                            "(bucket_interval={}, ticks_interval={}). Downstream resolution of sliding-window outputs is not yet supported. "
                            "Results may be incomplete.".format(
                                wf_name, input_id, upstream_wf_spec.bucket_interval, upstream_wf_spec.ticks_interval))

        # write input files — SWFBuilder owns this since it has full context
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            wf_entity = wf_entities[idx]

            # create data and inputs directories
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_inputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))

            for input_id in wf_spec.input_ids:
                if (input_id in wf_spec.upstream_inputs):
                    # upstream dependency (primary or join operand): proxy marker with this input_id
                    proxy_df = self.__build_proxy_marker__(input_id)
                elif (input_id == "seed_input"):
                    # seed_input: resolve through shared method (from_maps data vs params)
                    proxy_df = self.__resolve_seed_input_content__(xinput, entry)
                else:
                    # legacy: original xinput (seed data or template)
                    proxy_df = xinput
                self.cluster_handler.create(ClusterPaths.get_passive_entity_data_input_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id))
                path = ClusterPaths.get_passive_entity_data_input_default_file_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id)
                self.cluster_handler.write_df(path, proxy_df)

            # write seed_input for every WF (params + timestamp placeholders) if not already in input_ids
            if ("seed_input" not in wf_spec.input_ids):
                seed_df = self.__build_seed_input_from_params__(wf_spec.params)
                self.cluster_handler.create(ClusterPaths.get_passive_entity_data_input_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, "seed_input"))
                seed_path = ClusterPaths.get_passive_entity_data_input_default_file_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, "seed_input")
                self.cluster_handler.write_df(seed_path, seed_df)

        # build protocols — no xinputs, no file writes in submit()
        wf_protocols = []
        all_wf_ids = []
        for idx, (wf_name, wf_spec, xinput, entry) in enumerate(built_wfs):
            wf_entity = wf_entities[idx]
            wf_protocols.append(cluster_protocol_v2.ClusterWFProtocol(wf_entity))
            all_wf_ids.append(wf_entity.entity_id)
            utils.info("SWFBuilder: built WF protocol '{}' as {}".format(wf_name, wf_entity.entity_id))

        # build SWF protocol with children attached and submit
        swf_spec = cluster_common_v2.ClusterSpecSWF.new(all_wf_specs, tags = self.tags, params = self.params)
        swf_entity = cluster_common_v2.ClusterEntitySWF.new(
            self.namespace,
            cluster_protocol_v2.ClusterIds.generate_swf_id(),
            cluster_protocol_v2.ClusterIds.get_client_id(),
            cluster_protocol_v2.ClusterIds.get_session_id(),
            swf_spec,
        )
        swf_protocol = cluster_protocol_v2.ClusterSWFProtocol(swf_entity, wf_protocols)
        swf_protocol.submit()
        swf_id = swf_entity.entity_id

        utils.info("SWFBuilder: submitted SWF {} with {} wfs".format(swf_id, len(all_wf_ids)))

        # register SWF as dependent of session
        self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_id(
            EntityType.SESSION, cluster_protocol_v2.ClusterIds.get_session_id(), EntityType.SWF, swf_id))

        utils.info("SWFBuilder: SWF {} completed with {} wfs".format(swf_id, len(all_wf_ids)))
        return swf_id

TERMINAL_STATES = {EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED}

def get_entity_state(namespace, entity_type, entity_id):
    """Return the current resolved state of an entity (WF, SWF, JOB, etc.).
    For passive entities, namespace is required to resolve the state path."""
    if (cluster_common_v2.is_passive_entity(entity_type)):
        state_protocol = cluster_protocol_v2.ClusterPassiveEntityStateProtocol(namespace, entity_type, entity_id)
    else:
        state_protocol = cluster_protocol_v2.ClusterActiveEntityStateProtocol(entity_type, entity_id)
    return state_protocol.get_registered_state_resolved()

def get_wf_state(namespace, wf_id):
    """Return the current resolved state of a WF."""
    return get_entity_state(namespace, EntityType.WF, wf_id)

def get_swf_state(namespace, swf_id):
    """Return the current resolved state of an SWF."""
    return get_entity_state(namespace, EntityType.SWF, swf_id)

def is_completed(namespace, entity_type, entity_id):
    """Check if an entity has completed successfully."""
    return get_entity_state(namespace, entity_type, entity_id) == EntityState.COMPLETED

def is_wf_completed(namespace, wf_id):
    """Check if a WF has completed successfully."""
    return is_completed(namespace, EntityType.WF, wf_id)

def is_swf_completed(namespace, swf_id):
    """Check if an SWF has completed successfully."""
    return is_completed(namespace, EntityType.SWF, swf_id)

def get_swf_child_wf_ids(swf_id):
    """Return the list of child WF ids for an SWF."""
    handler = get_cluster_handler()
    return handler.list_dirs(cluster_common_v2.ClusterPaths.get_entity_dependents_by_child_type(
        EntityType.SWF, swf_id, EntityType.WF))

def get_swf_status(namespace, swf_id):
    """Return a dict with SWF state and per-child-WF states.

    Returns:
        {
            "swf_id": <str>,
            "swf_state": <str>,
            "is_successful": <bool>,
            "wfs": [{"wf_id": <str>, "name": <str>, "state": <str>}, ...]
        }
    """
    swf_state = get_swf_state(namespace, swf_id)
    handler = get_cluster_handler()
    wf_ids = get_swf_child_wf_ids(swf_id)
    wfs = []
    for wf_id in wf_ids:
        wf_name = ""
        try:
            wf_state = get_wf_state(namespace, wf_id)
        except Exception:
            wf_state = "unknown"
        try:
            wf_entity = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(
                cluster_common_v2.ClusterPaths.get_passive_entity_details_path(namespace, EntityType.WF, wf_id)))
            wf_name = wf_entity.entity_spec.name
        except Exception:
            pass
        wfs.append({"wf_id": wf_id, "name": wf_name, "state": wf_state})

    return {
        "swf_id": swf_id,
        "swf_state": swf_state,
        "is_successful": swf_state == EntityState.COMPLETED,
        "wfs": wfs,
    }

def get_swf_dag_edges(namespace, swf_id):
    """Return DAG edges for an SWF as a list of (source_output_id, dest_wf_name) tuples.

    Reads each child WF's entity spec and matches input_ids to output_ids across WFs.
    """
    handler = get_cluster_handler()
    wf_ids = get_swf_child_wf_ids(swf_id)

    # read all WF specs
    wf_specs = []
    for wf_id in wf_ids:
        try:
            wf_entity = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(
                cluster_common_v2.ClusterPaths.get_passive_entity_details_path(namespace, EntityType.WF, wf_id)))
            wf_specs.append(wf_entity.entity_spec)
        except Exception:
            continue

    # build output_id -> wf_name map
    output_to_wf = {}
    for spec in wf_specs:
        for output_id in spec.output_ids:
            output_to_wf[output_id] = spec.name

    # build edges: (source_wf_name, dest_wf_name) for each input that matches an output
    edges = []
    for spec in wf_specs:
        for input_id in spec.input_ids:
            if (input_id in output_to_wf):
                edges.append((output_to_wf[input_id], spec.name))

    return edges

def wait_for_completion(namespace, entity_type, entity_id, max_duration=3600, sleep_sec=5):
    """Poll entity state until it reaches a terminal state (COMPLETED, FAILED, ABORTED).

    Returns the final state string. Raises on timeout.
    """
    duration = 0
    utils.info("wait_for_completion: waiting for {} {} ...".format(entity_type, entity_id))
    while (duration < max_duration):
        try:
            cur_state = get_entity_state(namespace, entity_type, entity_id)
            if (cur_state in TERMINAL_STATES):
                utils.info("wait_for_completion: {} {} reached terminal state: {}".format(entity_type, entity_id, cur_state))
                return cur_state
            utils.trace("wait_for_completion: {} {} current state: {}. Sleeping {} seconds".format(entity_type, entity_id, cur_state, sleep_sec))
        except Exception as e:
            utils.trace("wait_for_completion: {} {} state check error: {}. Sleeping {} seconds".format(entity_type, entity_id, str(e), sleep_sec))
        time.sleep(sleep_sec)
        duration = duration + sleep_sec

    raise Exception("wait_for_completion: {} {} did not reach terminal state within {} seconds".format(entity_type, entity_id, max_duration))

def wait_for_swf_completion(namespace, swf_id, max_duration=3600, sleep_sec=5):
    """Wait for an SWF to reach a terminal state. Returns the final state string."""
    return wait_for_completion(namespace, EntityType.SWF, swf_id, max_duration=max_duration, sleep_sec=sleep_sec)

def wait_for_wf_completion(namespace, wf_id, max_duration=3600, sleep_sec=5):
    """Wait for a WF to reach a terminal state. Returns the final state string."""
    return wait_for_completion(namespace, EntityType.WF, wf_id, max_duration=max_duration, sleep_sec=sleep_sec)

def submit_message_bus_rollup_wf():
    """Build and submit the message bus rollup SWF. Runs every 60s for 1 week."""
    namespace = message_bus.MESSAGE_BUS_DEFAULT_NAMESPACE
    xinput = dataframe.new_with_cols(["rollup_status"], [["pending"]])

    wf_blueprint = xinput \
        .extend_class(cluster_df.HydraDF) \
        .extend_class(message_bus.MessageBusRollupDF) \
        .rollup_cascade() \
        .to_wf_spec()

    swf_id = (
        SWFBuilder(namespace)
            .add_wf("message_bus_rollup", wf_blueprint,
                     input_ids=["rollup_trigger"], output_ids=["rollup_status"],
                     is_live=True, interval=60, duration=604800)
            .materialize()
    )

    utils.info("submit_message_bus_rollup_wf: submitted SWF {}".format(swf_id))
    return swf_id

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
            xdf = read_df(file_path)
        except Exception as e:
            utils.warn("Caught exception in reading file. Sleeping for {} seconds".format(sleep_sec))
            time.sleep(sleep_sec)
            xdf = read_df(file_path)

        utils.info("Read file: {}, num_rows: {}".format(file_path, xdf.num_rows()))
        return xdf
    else:
        raise Exception("File not found: {}".format(file_path))

def scan_by_datetime_range(path, start_date_str, end_date_str, prefix):
    return etl.scan_by_datetime_range(get_cluster_handler().get_full_path(path), start_date_str, end_date_str, prefix)

def read_workflow_live_output(namespace, wf_id, output_id, sleep_sec = 3):
    handler = get_cluster_handler()
    output_path = ClusterPaths.get_passive_entity_data_output_path(namespace, EntityType.WF, wf_id, output_id)

    # wait for dt= child directories to appear
    utils.info("read_workflow_live_output: waiting for output directories: {} ...".format(output_path))
    while (True):
        dt_dirs = handler.list_dirs(output_path)
        if (len(dt_dirs) > 0):
            break
        utils.trace("read_workflow_live_output: no directories yet. Sleeping for {} seconds".format(sleep_sec))
        time.sleep(sleep_sec)

    # collect all file paths across all dt directories
    file_paths = []
    for dt_dir in dt_dirs:
        dt_full_path = handler.get_full_path("{}/{}".format(output_path, dt_dir))
        for f in handler.list_files("{}/{}".format(output_path, dt_dir)):
            file_paths.append("{}/{}".format(dt_full_path, f))

    utils.info("read_workflow_live_output: wf_id: {}, found {} files across {} directories".format(wf_id, len(file_paths), len(dt_dirs)))

    # read all files
    return hydra.read(file_paths, do_union = True)

#######################################################################################################################
# Script execution
#######################################################################################################################
def get_created_non_created_wf_ids(namespace, local_handler_flag = True, capabilities = []):
    # result
    created_wf_ids = []
    alive_wf_ids = []
    non_created_wf_ids = []
    capabilities = set(capabilities)
    handler = get_cluster_handler()

    # iterate over all states
    for state in cluster_common_v2.EntityState.get_all():
        # get all workflow ids using namespace-aware state path
        ids = handler.list_dirs(ClusterPaths.get_passive_entities_state_by_state(namespace, EntityType.WF, state))

        # read each workflow filter the workflows based on the capabilities matching requirements
        ids_allowed = []
        for id1 in ids:
            # read wf using namespace-aware details path
            wf = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(
                ClusterPaths.get_passive_entity_details_path(namespace, EntityType.WF, id1)))
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

def print_created_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = False, capabilities = capabilities)
    for id1 in created_wf_ids:
        print(id1)

def print_alive_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = False, capabilities = capabilities)
    for id1 in alive_wf_ids:
        print(id1)

def print_non_created_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = False, capabilities = capabilities)
    for id1 in non_created_wf_ids:
        print(id1)

def print_local_created_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = True, capabilities = capabilities)
    for id1 in created_wf_ids:
        print(id1)

def print_local_alive_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = True, capabilities = capabilities)
    for id1 in alive_wf_ids:
        print(id1)

def print_local_non_created_wf_ids(namespace, capabilities = []):
    created_wf_ids, alive_wf_ids, non_created_wf_ids = get_created_non_created_wf_ids(namespace, local_handler_flag = True, capabilities = capabilities)
    for id1 in non_created_wf_ids:
        print(id1)

# methods for external task execution
def print_shell_executor_scripts(namespace, wf_id):
    # read wf spec
    wf = cluster_common_v2.ClusterEntityWF.from_json(get_cluster_handler().read_most_recent_json(
        ClusterPaths.get_passive_entity_details_path(namespace, EntityType.WF, wf_id)))

    # create wf protocol
    wf_protocol = cluster_protocol_v2.ClusterWFProtocol(wf)

    # read input. TODO: Use ctx api
    input_id = wf.entity_spec.input_ids[0]
    output_id = wf.entity_spec.output_ids[0]
    xinput = read_df(ClusterPaths.get_passive_entity_data_input_default_file_path(namespace, EntityType.WF, wf_id, input_id))

    # resolve the meta parameters for external task
    xinput_resolved = wf_protocol.resolve_external_task_meta_params(xinput, input_id, output_id)
    operations = wf_protocol.execute_single_round_get_operations(wf.entity_spec)

    # generate output
    xoutput = wf_protocol.execute_single_round(operations, xinput_resolved)

    # print the script
    print(utils.url_decode(xoutput.col_as_array("shell:output:url_encoded")[0]))

def print_wf_requirements(namespace, local_handler_flag = False):
    # get handler
    handler = get_cluster_handler()

    # get all wfs using namespace-scoped entity IDs path
    wf_ids = sorted(handler.list_dirs("{}/{}".format(ClusterPaths.get_entities_ids(EntityType.WF), namespace)))

    # iterate
    for wf_id in wf_ids:
        # use protocol class to get the current state
        xentity_state_protocol = cluster_protocol_v2.ClusterPassiveEntityStateProtocol(namespace, EntityType.WF, wf_id)
        effective_state = xentity_state_protocol.get_registered_state_resolved()

        # get wf details using namespace-aware path
        wf = cluster_common_v2.ClusterEntityWF.from_json(handler.read_most_recent_json(
            ClusterPaths.get_passive_entity_details_path(namespace, EntityType.WF, wf_id)))
        requirements = wf.collect_requirements()

        # print
        utils.info("wf_id: {}, cur_state: {}, requirements: {}".format(wf_id, effective_state, requirements))


# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
# Workflow Code Reconstruction
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

def reconstruct_code(entity_detail):
    """Reverse-engineer a WF entity detail JSON into reconstructed Python code.

    Reads entity_detail["entity_spec"]["jobs_specs"] and rebuilds the
    equivalent DataFrame operation chain.
    """
    if (entity_detail is None):
        return None

    entity_spec = entity_detail.get("entity_spec")
    if (entity_spec is None):
        return None

    jobs_specs = entity_spec.get("jobs_specs")
    if (not jobs_specs):
        return None

    lines = ["df \\"]

    for job_idx, job_spec in enumerate(jobs_specs):
        # Determine which task types are present for the comment
        task_labels = []
        if (job_spec.get("extend_class_def") is not None):
            task_labels.append("extend_class")
        if (job_spec.get("map_task") is not None):
            task_labels.append("map")
        if (job_spec.get("reduce_task") is not None):
            task_labels.append("reduce")
        if (job_spec.get("singleton_task") is not None):
            task_labels.append("singleton")

        label = " + ".join(task_labels) if task_labels else "empty"
        lines.append("    # Job {} ({})".format(job_idx, label))

        # Extend class
        extend_class_def = job_spec.get("extend_class_def")
        if (extend_class_def is not None):
            op = extend_class_def.get("extend_class_op")
            if (op is not None):
                class_name = __reconstruct_extract_method_name__(op.get("name", ""))
                args_str = __reconstruct_format_args__(op)
                if (args_str):
                    lines.append("    .extend_class({}, {}) \\".format(class_name, args_str))
                else:
                    lines.append("    .extend_class({}) \\".format(class_name))

        # Map operations
        map_task = job_spec.get("map_task")
        if (map_task is not None):
            map_ops = map_task.get("map_ops", [])
            for op in map_ops:
                task_type = op.get("task_type", "")
                if (task_type == "extend_class"):
                    class_name = __reconstruct_extract_method_name__(op.get("name", ""))
                    args_str = __reconstruct_format_args__(op)
                    if (args_str):
                        lines.append("    .extend_class({}, {}) \\".format(class_name, args_str))
                    else:
                        lines.append("    .extend_class({}) \\".format(class_name))
                else:
                    method_name = __reconstruct_extract_method_name__(op.get("name", ""))
                    args_str = __reconstruct_format_args__(op)
                    lines.append("    .{}({}) \\".format(method_name, args_str))

        # Reduce operation
        reduce_task = job_spec.get("reduce_task")
        if (reduce_task is not None):
            op = reduce_task.get("reduce_op")
            if (op is not None):
                method_name = __reconstruct_extract_method_name__(op.get("name", ""))
                args_str = __reconstruct_format_args__(op)
                lines.append("    .{}({}) \\".format(method_name, args_str))

        # Singleton operation
        singleton_task = job_spec.get("singleton_task")
        if (singleton_task is not None):
            op = singleton_task.get("singleton_op")
            if (op is not None):
                method_name = __reconstruct_extract_method_name__(op.get("name", ""))
                args_str = __reconstruct_format_args__(op)
                lines.append("    .{}({}) \\".format(method_name, args_str))

        # Checkpoint operation
        checkpoint_def = job_spec.get("checkpoint_def")
        if (checkpoint_def is not None):
            cp_op = checkpoint_def.get("checkpoint_op")
            if (cp_op is not None):
                cp_name = cp_op.get("name", "")
                cp_overwrite = cp_op.get("overwrite", False)
                if (cp_overwrite == True):
                    lines.append('    .checkpoint("{}", overwrite=True) \\'.format(cp_name))
                else:
                    lines.append('    .checkpoint("{}") \\'.format(cp_name))

    # Remove trailing backslash from the last operation line
    if (len(lines) > 1):
        last = lines[-1]
        if (last.endswith(" \\")):
            lines[-1] = last[:-2]

    return "\n".join(lines)


def __reconstruct_extract_method_name__(fqn):
    """Extract short method name from fully qualified name.

    E.g. 'omigo_core.dataframe.DataFrame.select' -> 'select'
         'omigo_ext.graph_ext.VisualDF' -> 'VisualDF'
    """
    if (not fqn):
        return "unknown"
    parts = fqn.rsplit(".", 1)
    return parts[-1] if len(parts) > 1 else fqn


def __reconstruct_format_args__(op):
    """Format the args and kwargs of an operation into a Python-like argument string."""
    parts = []

    # Positional args
    args_operand = op.get("args")
    if (args_operand is not None):
        values = args_operand.get("value", [])
        if (isinstance(values, list)):
            for v in values:
                parts.append(__reconstruct_format_operand__(v))

    # Keyword args
    kwargs_operand = op.get("kwargs")
    if (kwargs_operand is not None):
        kw_values = kwargs_operand.get("value", {})
        if (isinstance(kw_values, dict)):
            for k, v in kw_values.items():
                parts.append("{} = {}".format(k, __reconstruct_format_operand__(v)))

    return ", ".join(parts)


def __reconstruct_format_operand__(operand):
    """Convert a ClusterOperand dict into a Python-like string representation."""
    if (operand is None):
        return "None"

    if (not isinstance(operand, dict)):
        return repr(operand)

    data_type = operand.get("data_type", "")
    value = operand.get("value")

    if (data_type == "str"):
        return '"{}"'.format(value)
    elif (data_type in ("int", "float")):
        return str(value)
    elif (data_type == "bool"):
        return "True" if value else "False"
    elif (data_type == "array_object"):
        if (isinstance(value, list)):
            items = [__reconstruct_format_operand__(v) for v in value]
            return "[{}]".format(", ".join(items))
        return str(value)
    elif (data_type == "dict"):
        if (isinstance(value, dict)):
            items = ["{}: {}".format(repr(k), __reconstruct_format_operand__(v)) for k, v in value.items()]
            return "{{{}}}".format(", ".join(items))
        return str(value)
    elif (data_type == "proxy_dataframe"):
        return 'ctx.read_df("{}")'.format(value)
    elif (data_type == "function"):
        func_type = operand.get("func_type", "")
        name = operand.get("name", "")
        if (func_type == "library"):
            short = __reconstruct_shorten_func_name__(name)
            return short
        elif (func_type == "lambda"):
            return "<lambda>"
        elif (func_type == "javascript"):
            return "<javascript>"
        return "<func:{}>".format(name)
    elif (data_type == "pyobject"):
        return "<pyobject>"
    else:
        # Fallback: if it looks like a plain value, just repr it
        if (value is not None):
            return repr(value)
        return str(operand)


def __reconstruct_shorten_func_name__(fqn):
    """Shorten a fully qualified function name for display.

    E.g. 'omigo_ext.udfs.maxfloat' -> 'udfs.maxfloat'
         'omigo_core.dataframe.DataFrame.apply' -> 'DataFrame.apply'
    """
    if (not fqn):
        return "unknown"
    parts = fqn.split(".")
    if (len(parts) <= 2):
        return fqn
    # Keep last two segments
    return ".".join(parts[-2:])


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

    def run(self, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "EntityRunner: {}: {}".format(self.protocol.entity.entity_type, self.ident))
        while (True):
            # run step
            self.run_step()

            # sleep
            utils.info("{}: Sleeping for {} seconds".format(dmsg, self.wait_sec))
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

    def run(self, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "EntityMasterRunner: {}: {}".format(self.protocol.entity.entity_type, self.ident))
        while (True):
            # run election
            if (self.election_protocol.run_election() == True):
                self.protocol.refresh_master_cache()

            # monitor incoming
            self.protocol.monitor_incoming_for_supervisor()

            # assign/reassign shards to alive RMs
            self.protocol.monitor_shard_assignments()

            # run base class
            self.run_step()

            # sleep
            utils.info("{}: Sleeping for {} seconds".format(dmsg, self.wait_sec))
            time.sleep(self.wait_sec)

class EntityResourceManagerRunner(EntityRunner):
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterRMShardProtocol(
            cluster_common_v2.ClusterEntityResourceManager.new(
                cluster_common_v2.ClusterIds.get_entity_id(EntityType.RESOURCE_MANAGER)))
        self.protocol.initialize()

    def run(self):
        while (True):
            # read shard assignments from master and process incoming
            self.protocol.monitor_shard_incoming()

            # base class monitoring
            self.run_step()

            # sleep
            utils.info("{}: Sleeping for {} seconds".format(self.protocol.entity.entity_type, self.wait_sec))
            time.sleep(self.wait_sec)

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

class MessageBusAgentRunner(EntityRunner):
    """Stop-gap: runs message bus ETL rollup inline in the system agent run loop.
    Not suitable for multi-node — only one instance should run rollup at a time.
    Long-term: replace with SWF-based scheduling via submit_message_bus_rollup_wf()."""
    def __init__(self, ident):
        super().__init__(ident)

    def setup(self):
        super().setup()
        self.protocol = cluster_protocol_v2.ClusterSystemAgentProtocol(cluster_common_v2.ClusterEntitySystemAgent.new(
            cluster_common_v2.ClusterIds.get_entity_id(EntityType.SYSTEM_AGENT)))
        self.protocol.initialize()

    def run_step(self):
        # standard monitoring (active children, passive children)
        super().run_step()

        # message bus rollup
        cluster_handler = cluster_common_v2.ClusterPaths.get_cluster_handler()
        namespace = message_bus.MESSAGE_BUS_DEFAULT_NAMESPACE
        rollup = message_bus.BucketRollup()
        rollup.rollup_all_levels(cluster_handler, namespace, dmsg = "MessageBusAgentRunner")
        rollup.fill_empty_windows(cluster_handler, namespace, dmsg = "MessageBusAgentRunner")

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

def run_message_bus_agent(ident = 1):
    entity_runner = MessageBusAgentRunner(ident)
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

