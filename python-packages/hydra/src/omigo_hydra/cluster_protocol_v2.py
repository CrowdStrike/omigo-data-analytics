import time
import os
import random
import math
import json
# import threading
import multiprocessing
from omigo_core import dataframe, utils, timefuncs
from omigo_core.context import ExecutorContext
from omigo_hydra import cluster_data, cluster_class_reflection, cluster_tsv, cluster_common_v2, cluster_arjun, etl, message_bus
from omigo_hydra.cluster_bvar import BroadcastContext
from omigo_hydra.cluster_common_v2 import EntityType, EntityState, ClusterTaskType, ClusterIds, ClusterPaths

class ClusterHeartbeatProtocol:
    MAX_HEARTBEAT_WAIT = 30
    HEARTBEAT_SEC = 5
    MAX_HEARTBEAT_CACHE = 10

    def __init__(self, entity):
        self.entity = entity
        self.last_heartbeat_cache = None
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def get_entity(self):
        return self.entity

    def get_entity_id(self):
        return self.get_entity().entity_id

    def get_entity_type(self):
        return self.get_entity().entity_type

    def __get_heartbeat_path__(self):
        if (cluster_common_v2.is_passive_entity(self.get_entity_type())):
            return ClusterPaths.get_passive_entity_heartbeat_path(self.entity.namespace, self.get_entity_type(), self.get_entity_id())
        else:
            return ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id())

    def update_cache_ts(self, ts):
        self.last_heartbeat_cache = ts

    def get_cache_ts(self):
        return self.last_heartbeat_cache

    def start_heartbeat_process(self):
        heartbeat_process = multiprocessing.Process(target = self.update_heartbeat, name = "CLUSTER_ENTITY: {}".format(self.entity.entity_type), args = (), daemon = True)
        heartbeat_process.start()

    def update_heartbeat(self):
        while True:
            if (self.update_heartbeat_inner() == True):
                time.sleep(ClusterHeartbeatProtocol.HEARTBEAT_SEC)
            else:
                # TODO
                utils.info("ClusterHeartbeatProtocol {}: update_heartbeat exiting".format(self.get_entity_id()))
                return

    def is_alive_cached(self):
        # check the heartbeat
        cur_time = timefuncs.get_utctimestamp_sec()

        # update cache if needed
        if (self.get_cache_ts() is None or cur_time - self.get_cache_ts() > ClusterHeartbeatProtocol.MAX_HEARTBEAT_CACHE):
            # read the entity heartbeat. TODO: dont update the cache here
            heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(self.__get_heartbeat_path__()))

            # check for none
            if (heartbeat is None):
                utils.warn("ClusterHeartbeatProtocol: is_alive_cached: heartbeat missing for {}. Returning False".format(self.get_entity_id()))
                return False

            # update
            self.update_cache_ts(heartbeat.ts)

        # compute the time diff
        time_diff = cur_time - self.get_cache_ts()
        if (time_diff <=  ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT):
            # valid heartbeat
            return True
        else:
            utils.info("ClusterHeartbeatProtocol: is_alive_cached expired: {}. Time Diff: {} seconds. Returning False".format(self.get_entity_id(), time_diff))
            return False

    def __is_alive__(self):
        # check if the directories exists
        if (self.cluster_handler.dir_exists(self.__get_heartbeat_path__()) == False):
            utils.warn("ClusterHeartbeatProtocol: __is_alive__: entity missing: {}".format(self.get_entity_id()))
            return False

        # read the entity value
        heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(self.__get_heartbeat_path__()))
        heartbeat_server_timestamp = self.cluster_handler.read_most_recent_server_timestamp(self.__get_heartbeat_path__())

        # check for None
        if (heartbeat is None or heartbeat_server_timestamp is None):
            utils.warn("ClusterHeartbeatProtocol: __is_alive__: entity details missing: {}".format(self.get_entity_id()))
            return False

        # check the diff
        clock_skew = heartbeat.ts - heartbeat_server_timestamp
        if (abs(clock_skew) > ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT):
            raise Exception("ClusterHeartbeatProtocol: __is_alive__: {}, clock skew detected. entity ts: {}, server ts: {}, skew: {}. Raising exception for debugging".format(
                self.get_entity_id(), heartbeat.ts, heartbeat_server_timestamp))

        # check if the timestamp has already expired
        cur_time = timefuncs.get_utctimestamp_sec()
        time_diff = cur_time - heartbeat_server_timestamp

        # check against max time
        if (time_diff > ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT):
            utils.warn("ClusterHeartbeatProtocol: __is_alive__: {}, heartbeat too old. Time Diff: {} seconds. Exiting".format(self.get_entity_id(), time_diff))
            return False
        else:
            return True

    # inner heartbeat method
    def update_heartbeat_inner(self):
        # check if the entity is alive
        if (self.__is_alive__() == False):
            utils.warn("ClusterHeartbeatProtocol: update_heartbeat_inner: {}, heartbeat missing or too old. Exiting".format(self.get_entity_id()))
            return False

        # check if the entity is part of cleanup already. TODO: consider all possible states
        if (cluster_common_v2.is_passive_entity(self.get_entity_type())):
            xentity_state_protocol = ClusterPassiveEntityStateProtocol(self.entity.namespace, self.get_entity_type(), self.get_entity_id())
        else:
            xentity_state_protocol = ClusterActiveEntityStateProtocol(self.get_entity_type(), self.get_entity_id())
        if (xentity_state_protocol.has_cleanup_state() or xentity_state_protocol.has_aborted_state()):
            utils.info("ClusterHeartbeatProtocol: update_heartbeat_inner: {}, entity in aborted or cleanup. Exiting.".format(self.get_entity_id()))
            return False

        # construct new heartbeat
        ts = timefuncs.get_utctimestamp_sec()
        heartbeat = cluster_common_v2.ClusterHearbeat.new(ts, self.get_entity().lease)

        # 1. update on cluster
        self.cluster_handler.update_dynamic_value(self.__get_heartbeat_path__(), heartbeat, ignore_logging = True)

        # 2. update local cache
        self.update_cache_ts(ts)

        # return
        return True

# protocol to check and resolve current persisted state — base class with all shared logic
class ClusterEntityStateProtocolBase:
    def __init__(self, entity_type, entity_id):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def __get_state_path__(self, state):
        raise Exception("Derived class must implement __get_state_path__")

    def __get_heartbeat_path__(self):
        raise Exception("Derived class must implement __get_heartbeat_path__")

    def __get_all_states__(self):
        # create map of states
        result = {}

        # get all states
        for state in EntityState.get_all():
            result[state] = self.cluster_handler.dir_exists(self.__get_state_path__(state))

        # return
        return result

    # get all states that exist along with their timestamps for timestamp-aware resolution
    def __get_all_states_with_timestamps__(self):
        result = []

        for state in EntityState.get_all():
            entity_state_path = self.__get_state_path__(state)
            if (self.cluster_handler.dir_exists(entity_state_path)):
                update_time = cluster_common_v2.ClusterUpdateTime.from_json(
                    self.cluster_handler.read_most_recent_json(entity_state_path))
                # use timestamp 0 if state dir exists but has no timestamp yet
                ts = update_time.ts if update_time is not None else 0
                result.append({"state": state, "ts": ts})

        return result

    def has_any_state(self):
        # check if entity has any registered state directories at all.
        # returns False for entities that have been fully removed from the cluster.
        states = self.__get_all_states__()
        for state, exists in states.items():
            if (exists == True):
                return True
        return False

    def is_finished(self):
        # find all states
        states = self.__get_all_states__()
        finished_states = [EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP]

        # check if any finished state directory actually exists
        for state in finished_states:
            if (states.get(state, False) == True):
                return True

        return False

    def has_heartbeat(self):
        # check if heartbeat directory exists (may have been deleted by cleanup)
        if (self.cluster_handler.dir_exists(self.__get_heartbeat_path__()) == False):
            return False

        # check if they are alive or not
        heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(self.__get_heartbeat_path__()))

        # check for None - missing heartbeat means not alive
        if (heartbeat is None):
            return False

        # check if entity is alive or not
        return heartbeat.is_alive()

    def has_dead_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.DEAD))):
            return True
        else:
            return False

    def has_reassigned_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.REASSIGNED))):
            return True
        else:
            return False

    def has_aborted_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.ABORTED))):
            return True
        else:
            return False

    def has_completed_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.COMPLETED))):
            return True
        else:
            return False

    def has_failed_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.FAILED))):
            return True
        else:
            return False

    def has_cleanup_state(self):
        if (self.cluster_handler.dir_exists(self.__get_state_path__(EntityState.CLEANUP))):
            return True
        else:
            return False

    def get_registered_state_resolved(self):
        # get all states with their timestamps for timestamp-aware resolution.
        # states are sorted by timestamp descending (newest first). when timestamps are equal,
        # priority index is used as tiebreaker (lower index = higher priority).
        # this solves the REASSIGNED->ALIVE loop: when ALIVE gets a newer timestamp than REASSIGNED, ALIVE wins.
        states_with_ts = self.__get_all_states_with_timestamps__()

        # check if any states exist
        if (len(states_with_ts) == 0):
            raise Exception("ClusterEntityStateProtocolBase: get_registered_state_resolved: invalid: {}, no states found".format(self.entity_id))

        # get priority index for tiebreaking (lower index = higher priority)
        priority_index = EntityState.get_priority_index()

        # log all states with timestamps for debugging
        utils.trace("ClusterEntityStateProtocolBase: get_registered_state_resolved: {}: states_with_ts: {}".format(
            self.entity_id, [{"state": entry["state"], "ts": entry["ts"]} for entry in states_with_ts]))

        # find the winning state: newest timestamp first, then highest priority (lowest index) as tiebreaker
        winning_entry = min(states_with_ts, key = lambda x: (-x["ts"], priority_index.get(x["state"], 999)))
        return winning_entry["state"]

    def is_alive(self):
        # get registered resolved state. if no state files exist (e.g., after laptop sleep/wake where state was cleaned up),
        # the entity is definitionally not alive — return False instead of crashing.
        try:
            cur_registered_state = self.get_registered_state_resolved()
        except Exception:
            return False

        # check if it is alive
        if (cur_registered_state != EntityState.ALIVE):
            return False

        # check if it is active entity and doesnt have heartbeat
        if (cluster_common_v2.EntityIsActiveMap[self.entity_type] == True):
            if (self.has_heartbeat() == True):
                return True
            else:
                return False
        else:
            return True

    def get_state_update_time(self, state):
        # get the timestamp at which the state was determined. TODO: add wait
        update_time = cluster_common_v2.ClusterUpdateTime.from_json(self.cluster_handler.read_most_recent_json(self.__get_state_path__(state)))

        # return
        return update_time


# Active entity state protocol — no namespace needed
class ClusterActiveEntityStateProtocol(ClusterEntityStateProtocolBase):
    def __init__(self, entity_type, entity_id):
        super().__init__(entity_type, entity_id)

    def __get_state_path__(self, state):
        return ClusterPaths.get_entities_state_by_id(self.entity_type, state, self.entity_id)

    def __get_heartbeat_path__(self):
        return ClusterPaths.get_entity_heartbeat(self.entity_type, self.entity_id)


# Passive entity state protocol — namespace is required
class ClusterPassiveEntityStateProtocol(ClusterEntityStateProtocolBase):
    def __init__(self, namespace, entity_type, entity_id):
        if (namespace is None):
            raise Exception("ClusterPassiveEntityStateProtocol: namespace cannot be None for passive entity type: {}".format(entity_type))
        super().__init__(entity_type, entity_id)
        self.namespace = namespace

    def __get_state_path__(self, state):
        return ClusterPaths.get_passive_entity_state_path(self.namespace, self.entity_type, state, self.entity_id)

    def __get_heartbeat_path__(self):
        return ClusterPaths.get_passive_entity_heartbeat_path(self.namespace, self.entity_type, self.entity_id)


# main protocol class for entity operations
class ClusterEntityProtocol:
    MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD = 86400*1000

    # minimum wait at DEAD for distributed state convergence before checking Gate 1 conditions
    MAX_ACTIVE_ENTITY_DEAD_THRESHOLD = 60*1

    # minimum wait at CLEANUP before running do_cleanup (in addition to Gate 3 condition)
    MAX_ACTIVE_ENTITY_FINISHED_THRESHOLD = 120

    def __init__(self, entity):
        self.entity = entity
        self.cluster_handler = ClusterPaths.get_cluster_handler()
        self.heartbeat_protocol = ClusterHeartbeatProtocol(self.entity)
        self.namespaces = self.__read_namespaces__()

    def __read_namespaces__(self):
        """Read registered namespaces from the namespace registry."""
        ns_base = ClusterPaths.get_namespaces_base_path()
        if (self.cluster_handler.dir_exists(ns_base)):
            return self.cluster_handler.list_dirs(ns_base)
        return []

    def get_entity(self):
        return self.entity

    def get_entity_id(self):
        return self.get_entity().entity_id

    def get_entity_type(self):
        return self.get_entity().entity_type

    def initialize(self):
        # create entries into appropriate places
        self.cluster_handler.create(ClusterPaths.get_entity_id(self.get_entity_type(), self.get_entity_id()))
        self.cluster_handler.create(ClusterPaths.get_entity(self.get_entity_type(), self.get_entity_id()))
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_entity(self.get_entity_type(), self.get_entity_id()), self.entity)
        self.cluster_handler.create(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.CREATED, self.get_entity_id()))
        update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.CREATED, self.get_entity_id()), update_time)

        # create active children
        self.cluster_handler.create(ClusterPaths.get_entity_active_children(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityActiveChildrenMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_active_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # create passive children
        self.cluster_handler.create(ClusterPaths.get_entity_passive_children(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityPassiveChildrenMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_passive_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))
            # Create namespace subdirectory for each registered namespace
            for ns in self.namespaces:
                self.cluster_handler.create(ClusterPaths.get_entity_passive_children_by_namespace(
                    ns, self.get_entity_type(), self.get_entity_id(), xchild_type))

        # create dependents
        self.cluster_handler.create(ClusterPaths.get_entity_dependents(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityDependentsMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # initialize heartbeat location
        heartbeat_path = self.heartbeat_protocol.__get_heartbeat_path__()
        self.cluster_handler.create(heartbeat_path)

        # create initial heartbeat
        heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), self.get_entity().lease)
        self.cluster_handler.update_dynamic_value(heartbeat_path, heartbeat)

        # if this is active entity, start heartbeat thread
        if (cluster_common_v2.EntityIsActiveMap[self.get_entity_type()] == True):
            utils.info("ClusterEntityProtocol: {} initialize: active entity: starting heartbeat process".format(self.get_entity_id()))
            self.heartbeat_protocol.start_heartbeat_process()
        else:
            utils.debug("ClusterEntityProtocol: {} initialize: passive entity: not starting heartbeat process".format(self.get_entity_id()))

    # the frequency to monitor active and passive children needs to be different. TODO: find that logic at sometime
    # for active entities, the clock synchronization and validation is needed. for passive, only when job starts, they are set to alive
    # completed, failed states are not for active entities
    def monitor_active_children(self):
        # take current time
        cur_ts = timefuncs.get_utctimestamp_sec()

        # at this point, the current state is as per in the cluster
        for xchild_entity_type in cluster_common_v2.EntityActiveChildrenMap[self.get_entity_type()]:
            # debug
            utils.debug("ClusterEntityProtocol: monitor_active_children: {}, xchild_entity_type: {}".format(self.get_entity_id(), xchild_entity_type))

            # get entity ids
            xchild_entity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_active_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

            # sort the entities
            xchild_entity_ids = sorted(xchild_entity_ids)

            # iterate over each xchild_entity_ids
            for xchild_entity_id in xchild_entity_ids:
                # get the entity
                xchild_entity = cluster_common_v2.deserialize_cluster_entity(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(xchild_entity_type, xchild_entity_id)))

                # get effective state
                xentity_state_protocol = ClusterActiveEntityStateProtocol(xchild_entity_type, xchild_entity_id)
                cur_registered_state = xentity_state_protocol.get_registered_state_resolved()
                cur_state_update_time = xentity_state_protocol.get_state_update_time(cur_registered_state)
                cur_time_diff = cur_ts - cur_state_update_time.ts

                # do a switch case and call the methods for each possible transition
                if (cur_registered_state == EntityState.CREATED):
                    # move to ASSIGNED state (supervisor acknowledges the active child)
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ASSIGNED)
                elif (cur_registered_state == EntityState.ASSIGNED):
                    # do verification, clock synchronization and move to ALIVE
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ALIVE)
                elif (cur_registered_state == EntityState.ALIVE):
                    # check for heartbeat
                    if (xentity_state_protocol.has_heartbeat() == False):
                        self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.DEAD)
                    else:
                        # do monitoring, record state
                        utils.info("monitor_active_children: {} : active child entity {} is ALIVE".format(self.get_entity_id(), xchild_entity_id))
                elif (cur_registered_state == EntityState.DEAD):
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_ACTIVE_ENTITY_DEAD_THRESHOLD):
                        # run reassignment work (idempotent, safe to run every cycle)
                        self.reassign_dead_child_active_children(xchild_entity_type, xchild_entity_id)
                        self.reassign_dead_child_passive_children(xchild_entity_type, xchild_entity_id)

                        # Gate 1: only transition when all children have new ALIVE supervisors
                        if self.all_children_have_alive_supervisor(xchild_entity_type, xchild_entity_id):
                            utils.info("monitor_active_children: {}: child {}/{} DEAD for {}s, all children reassigned, transitioning to REASSIGNED".format(
                                self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff))
                            self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.REASSIGNED)
                        else:
                            utils.info("monitor_active_children: {}: child {}/{} DEAD for {}s, waiting for children to be reassigned".format(
                                self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff))
                    else:
                        utils.info("monitor_active_children: {}: child {}/{} DEAD for {}s, waiting for convergence threshold {}s".format(
                            self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff,
                            ClusterEntityProtocol.MAX_ACTIVE_ENTITY_DEAD_THRESHOLD))
                elif (cur_registered_state == EntityState.REASSIGNED):
                    # Gate 2: REASSIGNED means all children verified at Gate 1. Transition immediately.
                    utils.info("monitor_active_children: {}: child {}/{} REASSIGNED, children verified, transitioning to CLEANUP".format(
                        self.get_entity_id(), xchild_entity_type, xchild_entity_id))
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.ABORTED):
                    # check if aborted is finished or not
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.CLEANUP):
                    # Gate 3: minimum time at CLEANUP AND all children must be fully deleted
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_ACTIVE_ENTITY_FINISHED_THRESHOLD):
                        if self.all_children_deleted(xchild_entity_type, xchild_entity_id):
                            utils.info("monitor_active_children: {}: child {}/{} CLEANUP for {}s, all children deleted, running do_cleanup".format(
                                self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff))
                            self.do_cleanup(xchild_entity)
                        else:
                            utils.info("monitor_active_children: {}: child {}/{} CLEANUP for {}s, waiting for children to be deleted".format(
                                self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff))
                    else:
                        utils.info("monitor_active_children: {}: child {}/{} CLEANUP for {}s, waiting for threshold {}s".format(
                            self.get_entity_id(), xchild_entity_type, xchild_entity_id, cur_time_diff,
                            ClusterEntityProtocol.MAX_ACTIVE_ENTITY_FINISHED_THRESHOLD))
                else:
                    raise Exception("ClusterEntityProtocol: monitor_active_children: invalid state: {}".format(cur_registered_state))

    # TODO: there needs to be a method in between to check if the state change is needed
    # state update time is not useful for doing final state change
    def do_active_child_state_change(self, xchild_entity, cur_state, state_update_time, target_state):
        # define the placeholders for all possible state transitions
        if (cur_state == EntityState.CREATED and target_state == EntityState.ASSIGNED):
            # CREATED to ASSIGNED (supervisor acknowledges)
            self.do_active_child_state_change_created_to_assigned(xchild_entity)
        elif (cur_state == EntityState.ASSIGNED and target_state == EntityState.ALIVE):
            # ASSIGNED to ALIVE
            self.do_active_child_state_change_assigned_to_alive(xchild_entity)
        elif (cur_state == EntityState.CREATED and target_state == EntityState.ALIVE):
            # CREATED to ALIVE (legacy direct path)
            self.do_active_child_state_change_created_to_alive(xchild_entity)
        elif (cur_state == EntityState.ALIVE and target_state == EntityState.DEAD):
            # ALIVE to DEAD
            self.do_active_child_state_change_alive_to_dead(xchild_entity)
        elif (cur_state == EntityState.DEAD and target_state == EntityState.REASSIGNED):
            # DEAD to REASSIGNED
            self.do_active_child_state_change_dead_to_reassigned(xchild_entity)
        elif (cur_state == EntityState.CREATED and target_state == EntityState.ABORTED):
            # CREATED to ABORTED
            self.do_active_child_state_change_created_to_aborted(xchild_entity)
        elif (cur_state == EntityState.ASSIGNED and target_state == EntityState.ABORTED):
            # ASSIGNED to ABORTED
            self.do_active_child_state_change_assigned_to_aborted(xchild_entity)
        elif (cur_state == EntityState.ALIVE and target_state == EntityState.ABORTED):
            # ALIVE to ABORTED
            self.do_active_child_state_change_alive_to_aborted(xchild_entity)
        elif (cur_state == EntityState.DEAD and target_state == EntityState.ABORTED):
            # DEAD to ABORTED
            self.do_active_child_state_change_dead_to_aborted(xchild_entity)
        elif (cur_state == EntityState.REASSIGNED and target_state == EntityState.ABORTED):
            # REASSIGNED to ABORTED
            self.do_active_child_state_change_reassigned_to_aborted(xchild_entity)
        elif (cur_state == EntityState.REASSIGNED and target_state == EntityState.CLEANUP):
            # REASSIGNED to CLEANUP
            self.do_active_child_state_change_reassigned_to_cleanup(xchild_entity)
        elif (cur_state == EntityState.ABORTED and target_state == EntityState.CLEANUP):
            # ABORTED to CLEANUP
            self.do_active_child_state_change_aborted_to_cleanup(xchild_entity)
        else:
            raise Exception("do_active_child_state_change: {}: invalid state change: {}, {}, {}".format(self.entity_id, xchild_entity_id, cur_state, target_state))

    def __do_child_entity_state_change__(self, xchild_entity, state):
        # create new update time
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())

        # take path - use namespace for passive entities, regular path for active entities
        if (cluster_common_v2.is_passive_entity(xchild_entity.entity_type)):
            entity_state_path = ClusterPaths.get_passive_entity_state_path(xchild_entity.namespace, xchild_entity.entity_type, state, xchild_entity.entity_id)
        else:
            entity_state_path = ClusterPaths.get_entities_state_by_id(xchild_entity.entity_type, state, xchild_entity.entity_id)

        # update timestamp and the value
        if (self.cluster_handler.dir_exists(entity_state_path) == False):
            self.cluster_handler.create(entity_state_path)
            self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)
        else:
            utils.warn("ClusterEntityProtocol: __do_child_entity_state_change__: {}, get_entities_state_by_id: {} exists.".format(self.get_entity_id(), xchild_entity.entity_id))
            if (self.cluster_handler.is_non_empty_dir(entity_state_path) == True):
                # allow re-writing REASSIGNED state with a newer timestamp (e.g., entity reassigned multiple times)
                if (state == EntityState.REASSIGNED):
                    utils.warn("ClusterEntityProtocol: __do_child_entity_state_change__: {}: re-reassigning {}, updating timestamp".format(self.get_entity_id(), xchild_entity.entity_id))
                    self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)
                else:
                    raise Exception("ClusterEntityProtocol: __do_child_entity_state_change__: {}: state transition not allowed as update timestamp already exists: {}".format(self.get_entity_id(), xchild_entity.entity_id))
            else:
                self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

    def do_active_child_state_change_created_to_assigned(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ASSIGNED)

    def do_active_child_state_change_assigned_to_alive(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ALIVE)

    def do_active_child_state_change_created_to_alive(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ALIVE)

    def do_active_child_state_change_alive_to_dead(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.DEAD)

    def do_active_child_state_change_dead_to_reassigned(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.REASSIGNED)

    def do_active_child_state_change_created_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_active_child_state_change_assigned_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_active_child_state_change_alive_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_active_child_state_change_dead_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_active_child_state_change_reassigned_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_active_child_state_change_reassigned_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    def do_active_child_state_change_aborted_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    def __sort_passive_children_by_creation_time__(self, namespace, xchild_entity_type, xchild_entity_ids):
        """Sort passive child entity IDs by their CREATED state timestamp (oldest first)."""
        entity_ts_pairs = []
        for xchild_entity_id in xchild_entity_ids:
            xentity_state_protocol = ClusterPassiveEntityStateProtocol(namespace, xchild_entity_type, xchild_entity_id)
            update_time = xentity_state_protocol.get_state_update_time(EntityState.CREATED)
            if (update_time is not None):
                entity_ts_pairs.append((xchild_entity_id, update_time.ts))
            else:
                # fallback: no CREATED state found, use 0 so it sorts first
                entity_ts_pairs.append((xchild_entity_id, 0))

        # sort by timestamp ascending (oldest first)
        entity_ts_pairs.sort(key = lambda x: x[1])
        return [pair[0] for pair in entity_ts_pairs]

    # monitor passive children
    def monitor_passive_children(self):
        # take current time
        cur_ts = timefuncs.get_utctimestamp_sec()

        # at this point, the current state is as per in the cluster
        for xchild_entity_type in cluster_common_v2.EntityPassiveChildrenMap[self.get_entity_type()]:
            # debug
            utils.debug("ClusterEntityProtocol: {} monitor_passive_children: xchild_entity_type: {}".format(self.get_entity_id(), xchild_entity_type))

            # iterate over namespaces
            for namespace in self.namespaces:
                # get entity ids within this namespace
                xchild_entity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_passive_children_by_namespace(
                    namespace, self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

                # sort the entities by creation time (oldest first) so WFs are assigned to agents in submission order
                xchild_entity_ids = self.__sort_passive_children_by_creation_time__(namespace, xchild_entity_type, xchild_entity_ids)

                # iterate over each xchild_entity_ids
                for xchild_entity_id in xchild_entity_ids:
                    # get the entity using passive path
                    xchild_entity = cluster_common_v2.deserialize_cluster_entity(self.cluster_handler.read_most_recent_json(
                        ClusterPaths.get_passive_entity_details_path(namespace, xchild_entity_type, xchild_entity_id)))

                    # get effective state
                    xentity_state_protocol = ClusterPassiveEntityStateProtocol(namespace, xchild_entity_type, xchild_entity_id)
                    cur_registered_state = xentity_state_protocol.get_registered_state_resolved()
                    cur_state_update_time = xentity_state_protocol.get_state_update_time(cur_registered_state)
                    cur_time_diff = cur_ts - cur_state_update_time.ts

                    # do a switch case and call the methods for each possible transition
                    if (cur_registered_state == EntityState.CREATED):
                        # move to ASSIGNED state (supervisor acknowledges the passive child)
                        self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ASSIGNED)
                    elif (cur_registered_state == EntityState.ASSIGNED):
                        # manager attempts execution; only move to ALIVE if an agent was actually assigned
                        if self.do_execute_passive_child(xchild_entity):
                            utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is ASSIGNED, moving to ALIVE for execution".format(
                                self.get_entity_id(), xchild_entity_id))
                            self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ALIVE)
                        else:
                            utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is ASSIGNED, waiting for available agent".format(
                                self.get_entity_id(), xchild_entity_id))
                    elif (cur_registered_state == EntityState.ALIVE):
                        # do monitoring, record state. Update heartbeat so UI and supervisor see fresh timestamp
                        utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is ALIVE".format(self.get_entity_id(), xchild_entity_id))
                        heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), xchild_entity.lease)
                        self.cluster_handler.update_dynamic_value(
                            ClusterPaths.get_passive_entity_heartbeat_path(namespace, xchild_entity.entity_type, xchild_entity.entity_id), heartbeat)
                        self.do_execute_passive_child(xchild_entity)
                    elif (cur_registered_state == EntityState.REASSIGNED):
                        # passive child's previous supervisor died. Check if execution already completed.
                        # Note: REASSIGNED > COMPLETED in priority, so resolved state stays REASSIGNED even after
                        # execution marks COMPLETED. We check explicitly and move to CLEANUP.
                        if (xentity_state_protocol.has_completed_state() or xentity_state_protocol.has_failed_state()):
                            utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is REASSIGNED but already executed, moving to CLEANUP".format(
                                self.get_entity_id(), xchild_entity_id))
                            self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                        else:
                            # attempt execution; only move to ALIVE if agent was assigned
                            if self.do_execute_passive_child(xchild_entity):
                                utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is REASSIGNED, moving to ALIVE for execution".format(
                                    self.get_entity_id(), xchild_entity_id))
                                self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ALIVE)
                            else:
                                utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is REASSIGNED, waiting for available agent".format(
                                    self.get_entity_id(), xchild_entity_id))
                    elif (cur_registered_state == EntityState.COMPLETED):
                        # handle terminal state (idempotent)
                        self.handle_passive_child_completed(xchild_entity)
                        # after a wait period, move to cleanup
                        if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                            self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                    elif (cur_registered_state == EntityState.FAILED):
                        # handle terminal state (idempotent)
                        self.handle_passive_child_completed(xchild_entity)
                        # after a wait period, move to cleanup. no retries
                        if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                            self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                    elif (cur_registered_state == EntityState.ABORTED):
                        # handle terminal state (idempotent)
                        self.handle_passive_child_completed(xchild_entity)
                        # aborted entity move to cleanup
                        if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                            self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                    elif (cur_registered_state == EntityState.CLEANUP):
                        # check if exceeded has threshold for doing cleanup
                        self.do_cleanup(xchild_entity)
                    else:
                        raise Exception("ClusterEntityProtocol: {} monitor_passive_children: invalid state: {}".format(self.get_entity_id(), cur_registered_state))

    # do_passive_child_state_change
    def do_passive_child_state_change(self, xchild_entity, cur_state, state_update_time, target_state):
        # define the placeholders for all possible state transitions
        if (cur_state == EntityState.CREATED and target_state == EntityState.ASSIGNED):
            # CREATED to ASSIGNED
            self.do_passive_child_state_change_created_to_assigned(xchild_entity)
        elif (cur_state == EntityState.ASSIGNED and target_state == EntityState.ALIVE):
            # ASSIGNED to ALIVE (manager picks up for execution)
            self.do_passive_child_state_change_assigned_to_alive(xchild_entity)
        elif (cur_state == EntityState.ALIVE and target_state == EntityState.COMPLETED):
            # ALIVE to COMPLETED
            self.do_passive_child_state_change_alive_to_completed(xchild_entity)
        elif (cur_state == EntityState.ALIVE and target_state == EntityState.FAILED):
            # ALIVE to FAILED
            self.do_passive_child_state_change_alive_to_failed(xchild_entity)
        elif (cur_state == EntityState.ALIVE and target_state == EntityState.ABORTED):
            # ALIVE to ABORTED
            self.do_passive_child_state_change_alive_to_aborted(xchild_entity)
        elif (cur_state == EntityState.COMPLETED and target_state == EntityState.CLEANUP):
            # COMPLETED to CLEANUP
            self.do_passive_child_state_change_completed_to_cleanup(xchild_entity)
        elif (cur_state == EntityState.FAILED and target_state == EntityState.CLEANUP):
            # FAILED to CLEANUP
            self.do_passive_child_state_change_failed_to_cleanup(xchild_entity)
        elif (cur_state == EntityState.ABORTED and target_state == EntityState.CLEANUP):
            # ABORTED to CLEANUP
            self.do_passive_child_state_change_aborted_to_cleanup(xchild_entity)
        elif (cur_state == EntityState.REASSIGNED and target_state == EntityState.ALIVE):
            # REASSIGNED to ALIVE (passive child re-adopted by new supervisor)
            self.do_passive_child_state_change_reassigned_to_alive(xchild_entity)
        elif (cur_state == EntityState.REASSIGNED and target_state == EntityState.CLEANUP):
            # REASSIGNED to CLEANUP (passive child already completed/failed before reassignment)
            self.do_passive_child_state_change_reassigned_to_cleanup(xchild_entity)
        else:
            raise Exception("ClusterEntityProtocol: {}: do_passive_child_state_change: invalid state change: {}, {}, {}".format(
                self.get_entity_id(), xchild_entity.entity_id, cur_state, target_state))

    # do_passive_child_state_change_created_to_assigned: supervisor acknowledges the passive child
    def do_passive_child_state_change_created_to_assigned(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ASSIGNED)

    # do_passive_child_state_change_assigned_to_alive: manager picks up for execution, assigns resources
    def do_passive_child_state_change_assigned_to_alive(self, xchild_entity):
        # check if there are enough resources - workers, agents to assign. if the resources are there, then
        # assign and move to alive state
        # if (cluster_common_v2.EntityIsActiveMap[xchild_entity.entity_type] == False):
        #     # check available resources
        #     if (xchild_entity.entity_type == EntityType.WF):
        #         # selected agent id
        #         selected_agent_id = None

        #         # if it is live, then assign agent else execute each job
        #         if (xchild_entity.entity_spec.is_live == True):
        #             alive_agent_ids = sorted(self.__get_alive_entity_ids__(EntityType.AGENT))
        #             available_agent_ids = sorted(self.__get_available_entity_ids__(EntityType.AGENT, alive_agent_ids))

        #             # check if there are any available agents
        #             if (len(available_agent_ids) > 0):
        #                 # pick one randomly
        #                 # random.shuffle(alive_agent_ids)
        #                 selected_agent_id = available_agent_ids[-1]

        #                 # assign agent id to the wf
        #                 self.cluster_handler.create(ClusterPaths.get_entity_assigned_executors_by_id(xchild_entity.entity_type, xchild_entity.entity_id, EntityType.AGENT,
        #                     selected_agent_id))

        #                 # create entry into agent id for this wf
        #                 self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_id(EntityType.AGENT, selected_agent_id, xchild_entity.entity_type,
        #                     xchild_entity.entity_id))

        # do the state change
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ALIVE)

    def do_passive_child_state_change_alive_to_completed(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.COMPLETED)

    def do_passive_child_state_change_alive_to_failed(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.FAILED)

    def do_passive_child_state_change_alive_to_aborted(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ABORTED)

    def do_passive_child_state_change_completed_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    def do_passive_child_state_change_failed_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    def do_passive_child_state_change_aborted_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    def do_passive_child_state_change_reassigned_to_alive(self, xchild_entity):
        # ALIVE state marker may already exist from the original execution before the supervisor died.
        # We cannot use __do_child_entity_state_change__ because it raises an exception if the timestamp
        # already exists. Instead, update the existing ALIVE timestamp directly to refresh it.
        # Note: the ALIVE directory may not exist if the entity was reassigned before it ever became alive
        # (e.g., the previous supervisor died before executing the entity).
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())
        # Use namespace-aware path for passive entities
        if (cluster_common_v2.is_passive_entity(xchild_entity.entity_type)):
            entity_state_path = ClusterPaths.get_passive_entity_state_path(xchild_entity.namespace, xchild_entity.entity_type, EntityState.ALIVE, xchild_entity.entity_id)
        else:
            entity_state_path = ClusterPaths.get_entities_state_by_id(xchild_entity.entity_type, EntityState.ALIVE, xchild_entity.entity_id)
        if (self.cluster_handler.dir_exists(entity_state_path) == False):
            self.cluster_handler.create(entity_state_path)
        self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

    def do_passive_child_state_change_reassigned_to_cleanup(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.CLEANUP)

    # reassign the dead child's active children to new live supervisors by ensuring they are in the incoming queue
    def reassign_dead_child_active_children(self, dead_child_entity_type, dead_child_entity_id):
        # get the active children types that the dead child was supervising
        dead_child_active_children_types = cluster_common_v2.EntityActiveChildrenMap.get(dead_child_entity_type, [])

        for grandchild_type in dead_child_active_children_types:
            # read the dead child's active children directory
            grandchild_ids = self.cluster_handler.list_dirs(
                ClusterPaths.get_entity_active_children_by_child_type(dead_child_entity_type, dead_child_entity_id, grandchild_type))

            for grandchild_id in grandchild_ids:
                # skip grandchildren whose entity details have already been cleaned up
                if (self.cluster_handler.dir_exists(ClusterPaths.get_entity(grandchild_type, grandchild_id)) == False):
                    utils.info("reassign_dead_child_active_children: {}: active child {}/{} entity details already deleted, skipping".format(
                        self.get_entity_id(), grandchild_type, grandchild_id))
                    continue

                # V2: route to shard incoming queue (except MASTER and RM which use global)
                if grandchild_type in (EntityType.MASTER, EntityType.RESOURCE_MANAGER):
                    incoming_path = ClusterPaths.get_entity_incoming(grandchild_type, grandchild_id)
                else:
                    shard_id = cluster_common_v2.get_shard_for_entity(grandchild_id)
                    incoming_path = ClusterPaths.get_shard_entity_incoming(shard_id, grandchild_type, grandchild_id)

                if (self.cluster_handler.dir_exists(incoming_path) == False):
                    self.cluster_handler.create(incoming_path)

                utils.info("reassign_dead_child_active_children: {}: dead child {}/{} had active child {}/{}, ensured in incoming queue".format(
                    self.get_entity_id(), dead_child_entity_type, dead_child_entity_id, grandchild_type, grandchild_id))

    # reassign the dead child's passive children to new live supervisors
    # ALIVE and ASSIGNED passive children are marked REASSIGNED (they need re-execution by new supervisor)
    # CREATED passive children stay CREATED (not yet acknowledged by any supervisor)
    # all non-terminal are ensured in the incoming queue for reassignment to a new supervisor
    def reassign_dead_child_passive_children(self, dead_child_entity_type, dead_child_entity_id):
        # get the passive children types that the dead child was supervising
        dead_child_passive_children_types = cluster_common_v2.EntityPassiveChildrenMap.get(dead_child_entity_type, [])

        for grandchild_type in dead_child_passive_children_types:
            for namespace in self.namespaces:
                # read the dead child's passive children directory scoped by namespace
                grandchild_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_entity_passive_children_by_namespace(namespace, dead_child_entity_type, dead_child_entity_id, grandchild_type))

                for grandchild_id in grandchild_ids:
                    # skip grandchildren whose entity details have already been cleaned up
                    if (self.cluster_handler.dir_exists(ClusterPaths.get_passive_entity_details_path(namespace, grandchild_type, grandchild_id)) == False):
                        utils.info("reassign_dead_child_passive_children: {}: passive child {}/{} entity details already deleted in namespace {}, skipping".format(
                            self.get_entity_id(), grandchild_type, grandchild_id, namespace))
                        continue

                    # read entity details to get namespace for state path resolution
                    grandchild_json = self.cluster_handler.read_most_recent_json(ClusterPaths.get_passive_entity_details_path(namespace, grandchild_type, grandchild_id))
                    if (grandchild_json is None):
                        utils.warn("reassign_dead_child_passive_children: {}: passive child {}/{} entity details unreadable (race) in namespace {}, skipping".format(
                            self.get_entity_id(), grandchild_type, grandchild_id, namespace))
                        continue
                    grandchild_entity = cluster_common_v2.deserialize_cluster_entity(grandchild_json)

                    # check current state
                    grandchild_state_protocol = ClusterPassiveEntityStateProtocol(namespace, grandchild_type, grandchild_id)
                    if (grandchild_state_protocol.has_any_state() == False):
                        utils.warn("reassign_dead_child_passive_children: {}: dead child {}/{} had passive child {}/{} with no states (removed), skipping".format(
                            self.get_entity_id(), dead_child_entity_type, dead_child_entity_id, grandchild_type, grandchild_id))
                        continue
                    cur_state = grandchild_state_protocol.get_registered_state_resolved()

                    # skip terminal states -- these are done
                    if (cur_state in (EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP)):
                        utils.info("reassign_dead_child_passive_children: {}: dead child {}/{} had passive child {}/{} in terminal state {}, skipping".format(
                            self.get_entity_id(), dead_child_entity_type, dead_child_entity_id, grandchild_type, grandchild_id, cur_state))
                        continue

                    # ALIVE and ASSIGNED passive children get marked REASSIGNED (need re-execution by new supervisor)
                    if (cur_state in (EntityState.ALIVE, EntityState.ASSIGNED)):
                        self.__do_child_entity_state_change__(grandchild_entity, EntityState.REASSIGNED)
                        utils.info("reassign_dead_child_passive_children: {}: dead child {}/{} had {} passive child {}/{}, marked REASSIGNED".format(
                            self.get_entity_id(), dead_child_entity_type, dead_child_entity_id, cur_state, grandchild_type, grandchild_id))

                    # CREATED passive children stay CREATED -- just ensure in incoming queue
                    elif (cur_state == EntityState.CREATED):
                        utils.info("reassign_dead_child_passive_children: {}: dead child {}/{} had CREATED passive child {}/{}, keeping CREATED".format(
                            self.get_entity_id(), dead_child_entity_type, dead_child_entity_id, grandchild_type, grandchild_id))

                    # ensure entity is in incoming queue (idempotent)
                    # V2: route to shard incoming queue — passive entities use namespace-aware shard incoming
                    shard_id = cluster_common_v2.get_shard_for_entity(grandchild_id)
                    incoming_path = ClusterPaths.get_passive_shard_entity_incoming(namespace, shard_id, grandchild_type, grandchild_id)

                    if (self.cluster_handler.dir_exists(incoming_path) == False):
                        self.cluster_handler.create(incoming_path)

    def all_children_have_alive_supervisor(self, dead_entity_type, dead_entity_id):
        """Gate 1: Check that all children of a dead entity have been reassigned to new ALIVE supervisors.

        For active children: assigned supervisor must not be the dead entity and must be ALIVE.
        For passive children: must be in a terminal state OR have a new ALIVE supervisor.

        Returns True only if ALL children pass. Returns False if any child fails.
        """
        # check active children
        for grandchild_type in cluster_common_v2.EntityActiveChildrenMap.get(dead_entity_type, []):
            grandchild_ids = self.cluster_handler.list_dirs(
                ClusterPaths.get_entity_active_children_by_child_type(dead_entity_type, dead_entity_id, grandchild_type))

            for grandchild_id in grandchild_ids:
                # skip self-referential entry (master assigns itself as its own active child)
                if (grandchild_type == dead_entity_type and grandchild_id == dead_entity_id):
                    continue

                # if the child entity no longer exists in the system, it has already been fully cleaned up.
                # remove the stale reference from the dead entity's active children and continue.
                if (self.cluster_handler.dir_exists(ClusterPaths.get_entity(grandchild_type, grandchild_id)) == False):
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} no longer exists in system, removing stale reference".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    self.cluster_handler.remove_dir_recursive(
                        ClusterPaths.get_entity_active_children_by_id(dead_entity_type, dead_entity_id, grandchild_type, grandchild_id),
                        ignore_if_missing = True)
                    continue

                # read assigned supervisor (directory may not exist for newly created entities)
                supervisor_path = ClusterPaths.get_entity_assigned_supervisor(grandchild_type, grandchild_id)
                if (self.cluster_handler.dir_exists(supervisor_path) == False):
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} has no assigned supervisor directory".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    return False

                supervisor_json = self.cluster_handler.read_most_recent_json(supervisor_path)
                supervisor_ref = cluster_common_v2.ClusterEntityRef.from_json(supervisor_json)

                # must have a supervisor
                if supervisor_ref is None:
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} has no assigned supervisor".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    return False

                # supervisor must not be the dead entity
                if supervisor_ref.entity_id == dead_entity_id:
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} still assigned to dead entity".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    return False

                # supervisor must be ALIVE
                supervisor_state_protocol = ClusterActiveEntityStateProtocol(supervisor_ref.entity_type, supervisor_ref.entity_id)
                if (supervisor_state_protocol.has_any_state() == False):
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} supervisor {}/{} has no states (removed)".format(
                        dead_entity_id, grandchild_type, grandchild_id, supervisor_ref.entity_type, supervisor_ref.entity_id))
                    return False
                supervisor_state = supervisor_state_protocol.get_registered_state_resolved()
                if supervisor_state != EntityState.ALIVE:
                    utils.info("all_children_have_alive_supervisor: {}: active child {}/{} supervisor {}/{} is {} not ALIVE".format(
                        dead_entity_id, grandchild_type, grandchild_id, supervisor_ref.entity_type, supervisor_ref.entity_id, supervisor_state))
                    return False

        # check passive children
        for grandchild_type in cluster_common_v2.EntityPassiveChildrenMap.get(dead_entity_type, []):
            for namespace in self.namespaces:
                grandchild_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_entity_passive_children_by_namespace(namespace, dead_entity_type, dead_entity_id, grandchild_type))

                for grandchild_id in grandchild_ids:
                    # if the child entity no longer exists in the system, it has already been fully cleaned up.
                    # remove the stale reference from the dead entity's passive children and continue.
                    if (self.cluster_handler.dir_exists(ClusterPaths.get_passive_entity_details_path(namespace, grandchild_type, grandchild_id)) == False):
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} no longer exists in namespace {}, removing stale reference".format(
                            dead_entity_id, grandchild_type, grandchild_id, namespace))
                        self.cluster_handler.remove_dir_recursive(
                            ClusterPaths.get_entity_passive_children_by_ns_id(namespace, dead_entity_type, dead_entity_id, grandchild_type, grandchild_id),
                            ignore_if_missing = True)
                        continue

                    # read entity details using namespace-aware path
                    grandchild_json = self.cluster_handler.read_most_recent_json(ClusterPaths.get_passive_entity_details_path(namespace, grandchild_type, grandchild_id))
                    if (grandchild_json is None):
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} entity details unreadable (race) in namespace {}, cannot verify".format(
                            dead_entity_id, grandchild_type, grandchild_id, namespace))
                        return False
                    grandchild_entity = cluster_common_v2.deserialize_cluster_entity(grandchild_json)

                    # check state
                    grandchild_state_protocol = ClusterPassiveEntityStateProtocol(namespace, grandchild_type, grandchild_id)
                    if (grandchild_state_protocol.has_any_state() == False):
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} has no states (removed) in namespace {}, cannot verify".format(
                            dead_entity_id, grandchild_type, grandchild_id, namespace))
                        return False
                    grandchild_state = grandchild_state_protocol.get_registered_state_resolved()

                    # terminal states pass — nothing to reassign
                    if grandchild_state in (EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP):
                        continue

                    # non-terminal: must have a new ALIVE supervisor
                    supervisor_path = ClusterPaths.get_passive_entity_assigned_supervisor_path(namespace, grandchild_type, grandchild_id)
                    if (self.cluster_handler.dir_exists(supervisor_path) == False):
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} in state {} has no assigned supervisor directory".format(
                            dead_entity_id, grandchild_type, grandchild_id, grandchild_state))
                        return False

                    supervisor_json = self.cluster_handler.read_most_recent_json(supervisor_path)
                    supervisor_ref = cluster_common_v2.ClusterEntityRef.from_json(supervisor_json)

                    if supervisor_ref is None:
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} in state {} has no assigned supervisor".format(
                            dead_entity_id, grandchild_type, grandchild_id, grandchild_state))
                        return False

                    if supervisor_ref.entity_id == dead_entity_id:
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} still assigned to dead entity".format(
                            dead_entity_id, grandchild_type, grandchild_id))
                        return False

                    supervisor_state_protocol = ClusterActiveEntityStateProtocol(supervisor_ref.entity_type, supervisor_ref.entity_id)
                    if (supervisor_state_protocol.has_any_state() == False):
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} supervisor {}/{} has no states (removed)".format(
                            dead_entity_id, grandchild_type, grandchild_id, supervisor_ref.entity_type, supervisor_ref.entity_id))
                        return False
                    supervisor_state = supervisor_state_protocol.get_registered_state_resolved()
                    if supervisor_state != EntityState.ALIVE:
                        utils.info("all_children_have_alive_supervisor: {}: passive child {}/{} supervisor {}/{} is {} not ALIVE".format(
                            dead_entity_id, grandchild_type, grandchild_id, supervisor_ref.entity_type, supervisor_ref.entity_id, supervisor_state))
                        return False

        # all children verified
        return True

    def all_children_deleted(self, dead_entity_type, dead_entity_id):
        """Gate 3: Check that all children of a dead entity have been fully deleted.

        Uses list_dirs() on the dead entity's active and passive children directories.
        For each child still referenced, checks if the child entity actually exists in the system.
        If the child has been fully cleaned up (no entity details), removes the stale reference.

        Returns True only if ALL children are gone. Returns False if any live child remains.
        """
        # check active children
        for grandchild_type in cluster_common_v2.EntityActiveChildrenMap.get(dead_entity_type, []):
            grandchild_ids = self.cluster_handler.list_dirs(
                ClusterPaths.get_entity_active_children_by_child_type(dead_entity_type, dead_entity_id, grandchild_type))

            for grandchild_id in grandchild_ids:
                # skip self-referential entry (master assigns itself as its own active child)
                if (grandchild_type == dead_entity_type and grandchild_id == dead_entity_id):
                    continue

                # check if this child still exists in the system
                if (self.cluster_handler.dir_exists(ClusterPaths.get_entity(grandchild_type, grandchild_id)) == False):
                    # child has been fully cleaned up by its new supervisor. remove the stale reference.
                    utils.info("all_children_deleted: {}: active child {}/{} no longer exists in system, removing stale reference".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    self.cluster_handler.remove_dir_recursive(
                        ClusterPaths.get_entity_active_children_by_id(dead_entity_type, dead_entity_id, grandchild_type, grandchild_id),
                        ignore_if_missing = True)
                else:
                    utils.info("all_children_deleted: {}: active child {}/{} still exists in system".format(
                        dead_entity_id, grandchild_type, grandchild_id))
                    return False

        # check passive children
        # NOTE: passive entities have long retention (e.g., 10 days for WFs), so we cannot wait for physical
        # deletion. Gate 1 already verified all children have alive supervisors and reassigned them.
        # By this point, the dead entity's references to passive children are stale — just remove them.
        for grandchild_type in cluster_common_v2.EntityPassiveChildrenMap.get(dead_entity_type, []):
            for namespace in self.namespaces:
                grandchild_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_entity_passive_children_by_namespace(namespace, dead_entity_type, dead_entity_id, grandchild_type))

                for grandchild_id in grandchild_ids:
                    utils.info("all_children_deleted: {}: removing stale passive child reference {}/{} in namespace {}".format(
                        dead_entity_id, grandchild_type, grandchild_id, namespace))
                    self.cluster_handler.remove_dir_recursive(
                        ClusterPaths.get_entity_passive_children_by_ns_id(namespace, dead_entity_type, dead_entity_id, grandchild_type, grandchild_id),
                        ignore_if_missing = True)

        # all children gone
        return True

    # TODO: move this to separate class
    # this cleanup is only run by the supervisor which has references in active, passive and dependents. These references need to be deleted
    def do_cleanup(self, xchild_entity):
        # delete from active children
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_active_children_by_id(self.get_entity_type(), self.get_entity_id(),
            xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # delete from passive children - use namespace path for passive entities
        if (cluster_common_v2.is_passive_entity(xchild_entity.entity_type)):
            self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_passive_children_by_ns_id(xchild_entity.namespace, self.get_entity_type(), self.get_entity_id(),
                xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)
        else:
            self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_passive_children_by_id(self.get_entity_type(), self.get_entity_id(),
                xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # delete from dependents
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_dependents_by_id(self.get_entity_type(), self.get_entity_id(),
            xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # run cleanup that is common for all entities
        if (cluster_common_v2.is_passive_entity(xchild_entity.entity_type)):
            entity_cleanup_protocol = ClusterPassiveEntityCleanupProtocol(xchild_entity.namespace, xchild_entity.entity_type, xchild_entity.entity_id)
        else:
            entity_cleanup_protocol = ClusterActiveEntityCleanupProtocol(xchild_entity.entity_type, xchild_entity.entity_id)
        entity_cleanup_protocol.cleanup()

    def is_alive_cached(self):
        return self.heartbeat_protocol.is_alive_cached()

    # monitor_execution_tasks
    def monitor_execution_tasks(self):
        # at this point, the current state is as per in the cluster
        for xchild_entity_type in cluster_common_v2.EntityExecutionTaskTypes:
            # debug
            utils.debug("ClusterEntityProtocol: {} monitor_execution_tasks: xchild_entity_type: {}".format(self.get_entity_id(), xchild_entity_type))

            # iterate over namespaces (execution tasks are passive entities stored under namespace dirs)
            for namespace in self.namespaces:
                # get entity ids within this namespace
                xchild_entity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_assigned_execution_tasks_by_namespace(
                    namespace, self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

                # sort the entities
                xchild_entity_ids = sorted(xchild_entity_ids)

                # iterate over each xchild_entity_ids
                for xchild_entity_id in xchild_entity_ids:
                    # get the entity using passive (namespace-aware) path
                    xchild_entity = cluster_common_v2.deserialize_cluster_entity(self.cluster_handler.read_most_recent_json(
                        ClusterPaths.get_passive_entity_details_path(namespace, xchild_entity_type, xchild_entity_id)))

                    # get effective state
                    xentity_state_protocol = ClusterPassiveEntityStateProtocol(namespace, xchild_entity_type, xchild_entity_id)
                    cur_registered_state = xentity_state_protocol.get_registered_state_resolved()
                    cur_state_update_time = xentity_state_protocol.get_state_update_time(cur_registered_state)

                    # do a switch case and call the methods for each possible transition
                    if (cur_registered_state == EntityState.CREATED):
                        pass
                    elif (cur_registered_state == EntityState.ALIVE):
                        # run the wf
                        if (xchild_entity_type == EntityType.WF):
                            self.do_execute_passive_child(xchild_entity)
                        else:
                            raise Exception("ClusterEntityProtocol {}: monitor_execution_tasks: not implemented for this entity_type: {}".format(self.get_entity_id(), xchild_entity_type))
                    elif (cur_registered_state == EntityState.COMPLETED):
                        pass
                    elif (cur_registered_state == EntityState.FAILED):
                        pass
                    elif (cur_registered_state == EntityState.ABORTED):
                        pass
                    elif (cur_registered_state == EntityState.REASSIGNED):
                        # execution task's previous supervisor died and it was reassigned.
                        # Check if the task already completed/failed before the reassignment.
                        if (xentity_state_protocol.has_completed_state() or xentity_state_protocol.has_failed_state()):
                            utils.info("ClusterEntityProtocol: {} monitor_execution_tasks: execution task {} is REASSIGNED but already executed, skipping".format(
                                self.get_entity_id(), xchild_entity_id))
                        else:
                            # re-execute the task under the new supervisor
                            utils.info("ClusterEntityProtocol: {} monitor_execution_tasks: execution task {} is REASSIGNED, re-executing".format(
                                self.get_entity_id(), xchild_entity_id))
                            if (xchild_entity_type == EntityType.WF):
                                self.do_execute_passive_child(xchild_entity)
                            else:
                                raise Exception("ClusterEntityProtocol {}: monitor_execution_tasks: not implemented for this entity_type: {}".format(self.get_entity_id(), xchild_entity_type))
                    elif (cur_registered_state == EntityState.CLEANUP):
                        pass
                    else:
                        raise Exception("ClusterEntityProtocol: {} monitor_execution_tasks: invalid state: {}".format(self.get_entity_id(), cur_registered_state))

    def do_execute_passive_child(self, xchild_entity):
        """Returns True if execution was successfully initiated, False otherwise."""
        raise Exception("Derived class should implement this")

    def handle_passive_child_completed(self, xchild_entity):
        """Called when a passive child reaches a terminal state (COMPLETED, FAILED, ABORTED).
        Must be idempotent — called every monitoring cycle until state moves to CLEANUP.
        Override in derived classes to perform disassociation or other cleanup."""
        pass

    # def do_execute_execution_task_wf(self, xchild_entity):
    #     utils.warn_once("TODO: do_execute_execution_task_wf: not assigning the workflow to agents or workers and executing inline")

    #     # parse entity_spec into appropriate derived class
    #     entity_spec = xchild_entity.entity_spec

    #     # execute inline. TODO
    #     self.do_execute_execution_task_wf_inline(xchild_entity)

    # def do_execute_execution_task_wf_inline(self, wf_entity):
    #     # instantiate protocol
    #     wf_protocol = ClusterWFProtocol(wf_entity)
    #     utils.info("ClusterEntityProtocol: {} do_execute_execution_task_wf_inline: executing workflow: {}".format(self.get_entity_id(), wf_entity.entity_id))
    #     wf_protocol.execute()

    def __get_alive_entity_ids__(self, xentity_type):
        # create result
        result = []

        # get entity ids
        xentity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_ids(xentity_type))

        # iterate
        for xentity_id in xentity_ids:
            # get the effective state of each entity
            xentity_state_protocol = ClusterActiveEntityStateProtocol(xentity_type, xentity_id)

            # skip entities whose states have been deleted (being cleaned up)
            if (xentity_state_protocol.has_any_state() == False):
                continue

            # check if the entity is alive
            if (xentity_state_protocol.is_alive()):
                result.append(xentity_id)

        # return
        return result

    # TODO: this is very inefficient
    def __get_available_entity_ids__(self, xentity_type, xentity_ids):
        # create result
        result = []

        # for each entity, count unfinished tasks across all task types
        for xentity_id in xentity_ids:
            # find the capacity of each entity. TODO: make this part of entity class
            total_capacity = cluster_common_v2.EntityCapacityMap[xentity_type]
            used_capacity = 0

            # check for each ExectionTask type
            for execution_task_type in cluster_common_v2.EntityExecutionTaskTypes:
                # iterate over namespaces (execution tasks are stored under namespace dirs)
                for namespace in self.namespaces:
                    # find all assigned tasks within this namespace
                    assigned_task_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_assigned_execution_tasks_by_namespace(
                        namespace, xentity_type, xentity_id, execution_task_type))

                    # for each assigned task, check if the task is finished
                    for assigned_task_id in assigned_task_ids:
                        xentity_state_protocol = ClusterPassiveEntityStateProtocol(namespace, execution_task_type, assigned_task_id)
                        is_task_finished = xentity_state_protocol.is_finished()
                        # count each unfinished task towards used capacity
                        if (is_task_finished == False):
                            used_capacity = used_capacity + 1
                            utils.debug("__get_available_entity_ids__: entity: {}, unfinished task: {}/{}/{}, used_capacity: {}/{}".format(
                                xentity_id, execution_task_type, namespace, assigned_task_id, used_capacity, total_capacity))

            # check if there is free capacity
            if (used_capacity < total_capacity):
                result.append(xentity_id)
            else:
                utils.debug("__get_available_entity_ids__: entity: {} has no free capacity: {}/{}".format(
                    xentity_id, used_capacity, total_capacity))

        # return
        return result

    # check if a supervisor has free capacity (non-terminal active + passive children < capacity)
    def __has_free_capacity__(self, xsupervisor_entity_type, xsupervisor_id):
        non_terminal_count = 0

        # count non-terminal active children
        active_children_types = cluster_common_v2.EntityActiveChildrenMap.get(xsupervisor_entity_type, [])
        for xchild_type in active_children_types:
            xchild_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_active_children_by_child_type(xsupervisor_entity_type, xsupervisor_id, xchild_type))
            for xchild_id in xchild_ids:
                xchild_state_protocol = ClusterActiveEntityStateProtocol(xchild_type, xchild_id)
                if (xchild_state_protocol.is_finished() == False):
                    non_terminal_count = non_terminal_count + 1

        # count non-terminal passive children
        passive_children_types = cluster_common_v2.EntityPassiveChildrenMap.get(xsupervisor_entity_type, [])
        for xchild_type in passive_children_types:
            for namespace in self.namespaces:
                xchild_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_passive_children_by_namespace(
                    namespace, xsupervisor_entity_type, xsupervisor_id, xchild_type))
                for xchild_id in xchild_ids:
                    xchild_state_protocol = ClusterPassiveEntityStateProtocol(namespace, xchild_type, xchild_id)
                    if (xchild_state_protocol.is_finished() == False):
                        non_terminal_count = non_terminal_count + 1

        # check against capacity
        total_capacity = cluster_common_v2.EntityCapacityMap[xsupervisor_entity_type]
        has_capacity = non_terminal_count < total_capacity
        utils.debug("__has_free_capacity__: {}/{}, non_terminal_count: {}, capacity: {}, has_capacity: {}".format(
            xsupervisor_entity_type, xsupervisor_id, non_terminal_count, total_capacity, has_capacity))
        return has_capacity

# separate protocol class for cleanup — active entities
class ClusterActiveEntityCleanupProtocol:
    def __init__(self, entity_type, entity_id):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def cleanup(self):
        # delete entity heartbeat
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_heartbeat(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete from incoming FIRST (V2: clean both global and shard)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_incoming(self.entity_type, self.entity_id), ignore_if_missing = True)
        if (self.entity_type not in (EntityType.MASTER, EntityType.RESOURCE_MANAGER)):
            shard_id = cluster_common_v2.get_shard_for_entity(self.entity_id)
            self.cluster_handler.remove_dir_recursive(
                ClusterPaths.get_shard_entity_incoming(shard_id, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete from assigned
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_supervisor(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_executors(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_execution_tasks(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete active, passive and dependents
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_active_children(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_passive_children(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_dependents(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete data
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_data(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entity details (after incoming is gone, so no reader can race here)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entities state except CLEANUP
        for state in EntityState.get_all():
            if (state != EntityState.CLEANUP):
                self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entities_state_by_id(self.entity_type, state, self.entity_id), ignore_if_missing = True)

        # delete CLEANUP
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.CLEANUP, self.entity_id), ignore_if_missing = True)

        # delete entity_id at the end
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_id(self.entity_type, self.entity_id), ignore_if_missing = True)

# separate protocol class for cleanup — passive entities (namespace required)
class ClusterPassiveEntityCleanupProtocol:
    def __init__(self, namespace, entity_type, entity_id):
        if (namespace is None):
            raise Exception("ClusterPassiveEntityCleanupProtocol: namespace is required for passive entity cleanup. entity_type: {}, entity_id: {}".format(entity_type, entity_id))
        self.namespace = namespace
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def cleanup(self):
        namespace = self.namespace

        # delete entity heartbeat
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_heartbeat_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete from incoming (global + shard)
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_incoming_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)
        shard_id = cluster_common_v2.get_shard_for_entity(self.entity_id)
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_shard_entity_incoming(namespace, shard_id, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete assigned-supervisor
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_assigned_supervisor_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete assigned-executors
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_assigned_executors_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete dependents
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_dependents(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete data
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_data_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entity details
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_details_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entities state except CLEANUP
        for state in EntityState.get_all():
            if (state != EntityState.CLEANUP):
                self.cluster_handler.remove_dir_recursive(
                    ClusterPaths.get_passive_entity_state_path(namespace, self.entity_type, state, self.entity_id), ignore_if_missing = True)

        # delete CLEANUP state
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_state_path(namespace, self.entity_type, EntityState.CLEANUP, self.entity_id), ignore_if_missing = True)

        # delete entity-id last
        self.cluster_handler.remove_dir_recursive(
            ClusterPaths.get_passive_entity_id_path(namespace, self.entity_type, self.entity_id), ignore_if_missing = True)

# Intermediate Abstract Class: Executor entities (AGENT, WORKER, DOUBLE_AGENT, INTELI_AGENT)
class ClusterExecutorEntityProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # create assigned execution tasks paths
        self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks(self.get_entity_type(), self.get_entity_id()))
        for xchild_entity_type in cluster_common_v2.EntityExecutionTaskTypes:
            self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))
            # Create namespace subdirectory for each registered namespace
            for ns in self.namespaces:
                self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_namespace(
                    ns, self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

# Intermediate Abstract Class: Execution task entities (SWF, WF, JOB, TASK, BATCH)
class ClusterExecutionTaskEntityProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        namespace = self.entity.namespace

        # Passive entity initialization with namespace paths
        self.cluster_handler.create(ClusterPaths.get_passive_entity_id_path(namespace, self.get_entity_type(), self.get_entity_id()))
        self.cluster_handler.create(ClusterPaths.get_passive_entity_details_path(namespace, self.get_entity_type(), self.get_entity_id()))
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_passive_entity_details_path(namespace, self.get_entity_type(), self.get_entity_id()), self.entity)
        self.cluster_handler.create(ClusterPaths.get_passive_entity_state_path(namespace, self.get_entity_type(), EntityState.CREATED, self.get_entity_id()))
        update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_passive_entity_state_path(namespace, self.get_entity_type(), EntityState.CREATED, self.get_entity_id()), update_time)

        # create dependents
        self.cluster_handler.create(ClusterPaths.get_entity_dependents(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityDependentsMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # create assigned executors paths with namespace
        self.cluster_handler.create(ClusterPaths.get_passive_entity_assigned_executors_path(namespace, self.get_entity_type(), self.get_entity_id()))
        for xchild_entity_type in cluster_common_v2.EntityExecutorTypes:
            self.cluster_handler.create(ClusterPaths.get_passive_entity_assigned_executors_by_child_type_path(
                namespace, self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

        # initialize heartbeat location for passive entity
        heartbeat_path = ClusterPaths.get_passive_entity_heartbeat_path(namespace, self.get_entity_type(), self.get_entity_id())
        self.cluster_handler.create(heartbeat_path)

        # create initial heartbeat
        heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), self.get_entity().lease)
        self.cluster_handler.update_dynamic_value(heartbeat_path, heartbeat)

        # shard-based incoming routing with namespace
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_passive_shard_entity_incoming(namespace, shard_id, self.get_entity_type(), self.get_entity_id()))

# Master Protocol
class ClusterMasterProtocol(ClusterEntityProtocol):
    MAX_CUR_MASTER_CACHE = 30
    def __init__(self, entity):
        super().__init__(entity)
        self.is_cur_master_cache = False
        self.cur_master_cache_ts = 0
        self.election_protocol = ClusterMasterElectionProtocol(self.get_entity_id())

    # initialize
    def initialize(self):
        # call base class initialization first
        super().initialize()

        # global incoming routing
        self.cluster_handler.create(ClusterPaths.get_entity_incoming(self.get_entity_type(), self.get_entity_id()))

        # Only MASTER move to ALIVE state by default
        if (self.get_entity_type() == EntityType.MASTER):
            utils.info("ClusterMasterProtocol: {} initialize: Master moved to ALIVE as part of initialization".format(self.get_entity_id()))
            self.cluster_handler.create(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.ALIVE, self.get_entity_id()))
            self.cluster_handler.update_dynamic_value(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.ALIVE, self.get_entity_id()),
                cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec()))

    # check if this is the current master. TODO: Why there are 2 methods in Election and here
    def is_current_master(self):
       # find time difference
       cur_time = timefuncs.get_utctimestamp_sec()
       time_diff = abs(cur_time - self.cur_master_cache_ts)

       # update cache if needed
       if (self.cur_master_cache_ts == 0 or time_diff > ClusterMasterProtocol.MAX_CUR_MASTER_CACHE):
           self.is_cur_master_cache = self.election_protocol.is_current_master()
           self.cur_master_cache_ts = cur_time

       # return
       return self.is_cur_master_cache

    def refresh_master_cache(self):
       self.is_cur_master_cache = self.election_protocol.is_current_master()
       self.cur_master_cache_ts = timefuncs.get_utctimestamp_sec()

    # monitor the incoming entities for assignment to their supervisors
    def monitor_incoming_for_supervisor(self):
        # check if this is current master or not
        if (self.is_current_master() == False):
            utils.info("ClusterMasterProtocol: {}: monitor_incoming_for_supervisor: not the current master. Returning".format(self.get_entity_id()))
            return

        # V2: Master only handles MASTER and RESOURCE_MANAGER incoming
        for xentity_type in [EntityType.MASTER, EntityType.RESOURCE_MANAGER]:
            # get all incoming entity ids
            xincoming_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_incoming(xentity_type))

            # get all assigned supervisor entity ids
            xassigned_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_assigned_supervisor(xentity_type))

            # find which ones are not assigned yet
            xnon_assigned_ids = list(set(xincoming_ids).difference(set(xassigned_ids)))

            # also find entities whose assigned supervisor is no longer alive (need reassignment)
            for xassigned_id in set(xincoming_ids).intersection(set(xassigned_ids)):
                xsupervisor_ref = cluster_common_v2.ClusterEntityRef.from_json(
                    self.cluster_handler.read_most_recent_json(
                        ClusterPaths.get_entity_assigned_supervisor(xentity_type, xassigned_id)))
                if (xsupervisor_ref is not None):
                    if (ClusterActiveEntityStateProtocol(xsupervisor_ref.entity_type, xsupervisor_ref.entity_id).is_alive() == False):
                        utils.info("monitor_incoming_for_supervisor: {}: entity {}/{} has dead supervisor {}/{}, adding to reassignment".format(
                            self.get_entity_id(), xentity_type, xassigned_id, xsupervisor_ref.entity_type, xsupervisor_ref.entity_id))
                        xnon_assigned_ids.append(xassigned_id)

            # sort the entities
            xnon_assigned_ids = sorted(xnon_assigned_ids)

            # iterate and assign
            for xnon_assigned_id in xnon_assigned_ids:
                # based on the type of the entity, construct the protocol
                utils.info("ClusterMasterProtocol: {} monitor_incoming_for_supervisor: xnon_assigned_id: {}".format(self.get_entity_id(), xnon_assigned_id))
                self.assign_entity_to_supervisor(xentity_type, xnon_assigned_id)

    # assign entity to the next available supervisor
    def assign_entity_to_supervisor(self, xentity_type, xentity_id):
        # get entity
        xentity = cluster_common_v2.ClusterEntity.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(xentity_type, xentity_id)))

        # check if entity is active or passive
        xentity_is_active = cluster_common_v2.EntityIsActiveMap[xentity_type]

        # get supervisor
        xsupevisor_entity_ref = self.__select_assigned_supervisor_entity__(xentity.entity_type)

        # check if there was a valid assigned_id
        if (xsupevisor_entity_ref is not None):
            # create entry in entity assigned
            self.cluster_handler.create(ClusterPaths.get_entity_assigned_supervisor(xentity_type, xentity_id))
            self.cluster_handler.update_dynamic_seq_update(ClusterPaths.get_entity_assigned_supervisor(xentity_type, xentity_id), xsupevisor_entity_ref)

            # create entry under the supervisor too
            utils.info("calling assign_entity_to_supervisor: {}, {}, {}, active : {}".format(xsupevisor_entity_ref.entity_id, xentity_type, xentity_id, xentity_is_active))
            if (xentity_is_active == True):
                self.cluster_handler.create(ClusterPaths.get_entity_active_children_by_id(xsupevisor_entity_ref.entity_type, xsupevisor_entity_ref.entity_id, xentity_type, xentity_id))
            else:
                self.cluster_handler.create(ClusterPaths.get_entity_passive_children_by_id(xsupevisor_entity_ref.entity_type, xsupevisor_entity_ref.entity_id, xentity_type, xentity_id))
        else:
            utils.info("ClusterMasterProtocol: {}: assign_entity_to_supervisor: not able to find any alive available supervisor: xentity_type: {}, xentity_id: {}".format(self.get_entity_id(), xentity_type, xentity_id))

    # TODO: Optimize this
    def __select_assigned_supervisor_entity__(self, xentity_type):
        # find the supervisor entity type
        xsupervisor_entity_type = cluster_common_v2.EntitySupervisorMap[xentity_type]

        # if the supervisor is master, then assign current master, else select randomly
        if (xsupervisor_entity_type == EntityType.MASTER):
            # current master
            return cluster_common_v2.ClusterEntityRef.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_current_master()))
        else:
            # find the list of entities that are alive
            xsupervisor_ids = sorted(self.cluster_handler.list_dirs(ClusterPaths.get_entities(xsupervisor_entity_type)))

            # find which ones are alive
            xalive_entity_ids = []
            for xsupervisor_id in xsupervisor_ids:
                if (ClusterActiveEntityStateProtocol(xsupervisor_entity_type, xsupervisor_id).is_alive()):
                    xalive_entity_ids.append(xsupervisor_id)

            # debug
            utils.info("__select_assigned_supervisor_entity__: xentity_type: {}, xsupervisor_ids: {}, xalive_entity_ids: {}".format(xentity_type, xsupervisor_ids, xalive_entity_ids))

            # check if there are any
            if (len(xalive_entity_ids) > 0):
                utils.info("ClusterMasterProtocol: {}: __select_assigned_entity__: xentity_type: {}, supervisor candidates: xalive_entity_ids: {}".format(self.get_entity_id(), xentity_type, xalive_entity_ids))

                # filter for supervisors that have free passive capacity
                xfree_entity_ids = list(filter(lambda t: self.__has_free_capacity__(xsupervisor_entity_type, t), xalive_entity_ids))
                utils.info("__select_assigned_supervisor_entity__: xentity_type: {}, xfree_entity_ids: {}".format(xentity_type, xfree_entity_ids))

                # only assign to supervisors with free capacity
                if (len(xfree_entity_ids) == 0):
                    utils.info("__select_assigned_supervisor_entity__: xentity_type: {}, no supervisors with free capacity".format(xentity_type))
                    return None

                xcandidates = xfree_entity_ids

                # TODO: workaround to not select master if workers are available
                if (len(xcandidates) > 1):
                    xcandidate_non_master_node_ids = list(filter(lambda t: self.__is_same_entity_node_as_current_master__(t) == False, xcandidates))
                    xcandidate_non_master_node_ids = utils.random_shuffle(xcandidate_non_master_node_ids, seed = timefuncs.get_utctimestamp_millis())
                    return cluster_common_v2.ClusterEntityRef.new(xsupervisor_entity_type, xcandidate_non_master_node_ids[0])
                else:
                    return cluster_common_v2.ClusterEntityRef.new(xsupervisor_entity_type, xcandidates[0])
            else:
               # return None
               return None

    def __is_same_entity_node_as_current_master__(self, xentity_id):
        utils.warn_once("TODO: check if the current node is also the current master and assign to a different supervisor")
        entity_id_ts = self.get_entity_id().split("-")[-1]
        xentity_id_ts = xentity_id.split("-")[-1]
        if (entity_id_ts == xentity_id_ts):
            return True
        else:
            return False

    def assign_shards_to_rms(self):
        """Master assigns shards to alive RMs using round-robin."""
        if not self.is_current_master():
            return

        alive_rm_ids = sorted(self.__get_alive_entity_ids__(EntityType.RESOURCE_MANAGER))

        if len(alive_rm_ids) == 0:
            utils.warn("ClusterMasterProtocol: {}: no alive RMs for shard assignment".format(self.get_entity_id()))
            return

        # Round-robin assignment: distribute shards evenly across RMs
        for shard_id in range(cluster_common_v2.NUM_SHARDS):
            assigned_rm_id = alive_rm_ids[shard_id % len(alive_rm_ids)]

            # Check if current assignment is different
            current_assignment = None
            if self.cluster_handler.is_non_empty_dir(ClusterPaths.get_shard_assigned_rm(shard_id)):
                current_ref = cluster_common_v2.ClusterEntityRef.from_json(
                    self.cluster_handler.read_most_recent_json(ClusterPaths.get_shard_assigned_rm(shard_id)))
                if current_ref:
                    current_assignment = current_ref.entity_id

            # Only update if assignment changed
            if current_assignment != assigned_rm_id:
                rm_ref = cluster_common_v2.ClusterEntityRef.new(EntityType.RESOURCE_MANAGER, assigned_rm_id)
                self.cluster_handler.update_dynamic_seq_update(
                    ClusterPaths.get_shard_assigned_rm(shard_id), rm_ref)
                utils.info("ClusterMasterProtocol: {}: assigned shard {} to RM {}".format(
                    self.get_entity_id(), shard_id, assigned_rm_id))

    def monitor_shard_assignments(self):
        """Monitor and reassign shards if their assigned RM is dead or if no assignments exist."""
        if not self.is_current_master():
            return

        needs_assignment = False

        # Check if any shard lacks assignment or has a dead RM
        for shard_id in range(cluster_common_v2.NUM_SHARDS):
            if self.cluster_handler.is_non_empty_dir(ClusterPaths.get_shard_assigned_rm(shard_id)):
                rm_ref = cluster_common_v2.ClusterEntityRef.from_json(
                    self.cluster_handler.read_most_recent_json(ClusterPaths.get_shard_assigned_rm(shard_id)))
                if rm_ref:
                    if ClusterActiveEntityStateProtocol(rm_ref.entity_type, rm_ref.entity_id).is_alive() == False:
                        utils.info("ClusterMasterProtocol: {}: shard {} has dead RM {}, triggering reassignment".format(
                            self.get_entity_id(), shard_id, rm_ref.entity_id))
                        needs_assignment = True
                        break
                else:
                    # Empty assignment
                    needs_assignment = True
                    break
            else:
                # No assignment exists
                needs_assignment = True
                break

        if needs_assignment:
            self.assign_shards_to_rms()

# Resource Manager Protocol
class ClusterResourceManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # global incoming routing
        self.cluster_handler.create(ClusterPaths.get_entity_incoming(self.get_entity_type(), self.get_entity_id()))

# SWF Manager Protocol
class ClusterSWFManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

    # Override: SWF child WFs are already submitted by ClusterSessionProtocol.submit_swf().
    # The SWF manager just needs to acknowledge the SWF entity so it moves to ALIVE.
    # Once ALIVE, this method is called each monitoring cycle to check if child WFs have finished.
    def do_execute_passive_child(self, xchild_entity):
        xchild_entity_type = xchild_entity.entity_type

        if (xchild_entity_type == EntityType.SWF):
            # get the current state of the SWF
            swf_state_protocol = ClusterPassiveEntityStateProtocol(xchild_entity.namespace, EntityType.SWF, xchild_entity.entity_id)
            cur_swf_state = swf_state_protocol.get_registered_state_resolved()

            # if SWF is not yet ALIVE, acknowledge it so it moves from ASSIGNED to ALIVE
            # but only if there are enough available agents to handle all child WFs
            if (cur_swf_state in (EntityState.CREATED, EntityState.ASSIGNED)):
                # count child WFs
                wf_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_entity_dependents_by_child_type(EntityType.SWF, xchild_entity.entity_id, EntityType.WF))
                num_child_wfs = len(wf_ids)

                # count available agents
                alive_agent_ids = self.__get_alive_entity_ids__(EntityType.AGENT)
                available_agent_ids = self.__get_available_entity_ids__(EntityType.AGENT, alive_agent_ids)
                num_available = len(available_agent_ids)

                if (num_available < num_child_wfs):
                    utils.info("ClusterSWFManagerProtocol {}: do_execute_passive_child: SWF {} needs {} agents but only {} available, waiting".format(
                        self.get_entity_id(), xchild_entity.entity_id, num_child_wfs, num_available))
                    return False

                utils.info("ClusterSWFManagerProtocol {}: do_execute_passive_child: acknowledging SWF {} ({} agents available for {} child WFs)".format(
                    self.get_entity_id(), xchild_entity.entity_id, num_available, num_child_wfs))
                return True

            # if SWF is ALIVE, check child WF states to determine if SWF should transition
            if (cur_swf_state == EntityState.ALIVE):
                self.__check_swf_child_wf_states__(xchild_entity)
                return True

            return True

        return False

    def __check_swf_child_wf_states__(self, swf_entity):
        """Check all child WF states. If any WF has FAILED, abort all non-terminal sibling WFs.
        Once all WFs reach terminal state, transition SWF accordingly:
        All COMPLETED -> COMPLETED. Any FAILED -> FAILED. Any ABORTED -> ABORTED."""

        # get child WF ids from dependents
        wf_ids = self.cluster_handler.list_dirs(
            ClusterPaths.get_entity_dependents_by_child_type(EntityType.SWF, swf_entity.entity_id, EntityType.WF))

        # if no child WFs registered yet, nothing to do
        if (len(wf_ids) == 0):
            utils.debug("ClusterSWFManagerProtocol {}: __check_swf_child_wf_states__: SWF {} has no child WFs yet".format(
                self.get_entity_id(), swf_entity.entity_id))
            return

        # collect states of all child WFs
        num_completed = 0
        num_failed = 0
        num_aborted = 0
        num_pending = 0
        pending_wf_ids = []

        for wf_id in wf_ids:
            wf_state_protocol = ClusterPassiveEntityStateProtocol(swf_entity.namespace, EntityType.WF, wf_id)

            if (wf_state_protocol.has_any_state() == False):
                # WF entity may have been cleaned up already, treat as pending
                num_pending += 1
                pending_wf_ids.append(wf_id)
            elif (wf_state_protocol.has_completed_state()):
                num_completed += 1
            elif (wf_state_protocol.has_failed_state()):
                num_failed += 1
            elif (wf_state_protocol.has_aborted_state()):
                num_aborted += 1
            else:
                num_pending += 1
                pending_wf_ids.append(wf_id)

        utils.info("ClusterSWFManagerProtocol {}: __check_swf_child_wf_states__: SWF {}: total={}, completed={}, failed={}, aborted={}, pending={}".format(
            self.get_entity_id(), swf_entity.entity_id, len(wf_ids), num_completed, num_failed, num_aborted, num_pending))

        # if any WF has failed or been aborted and there are still pending WFs, abort the pending ones
        if (num_pending > 0):
            if (num_failed > 0 or num_aborted > 0):
                utils.info("ClusterSWFManagerProtocol {}: __check_swf_child_wf_states__: SWF {}: detected failed/aborted WFs, aborting {} pending sibling WFs".format(
                    self.get_entity_id(), swf_entity.entity_id, num_pending))
                cluster_admin = ClusterAdmin()
                for pending_wf_id in pending_wf_ids:
                    utils.info("ClusterSWFManagerProtocol {}: __check_swf_child_wf_states__: SWF {}: aborting sibling WF {}".format(
                        self.get_entity_id(), swf_entity.entity_id, pending_wf_id))
                    cluster_admin.abort_entity(swf_entity.namespace, EntityType.WF, pending_wf_id)
            return

        # all child WFs have finished. determine the SWF terminal state
        # failed takes precedence over aborted: sibling WFs are aborted *because* one failed
        if (num_failed > 0):
            target_state = EntityState.FAILED
        elif (num_aborted > 0):
            target_state = EntityState.ABORTED
        else:
            target_state = EntityState.COMPLETED

        utils.info("ClusterSWFManagerProtocol {}: __check_swf_child_wf_states__: SWF {} all child WFs finished, transitioning to {}".format(
            self.get_entity_id(), swf_entity.entity_id, target_state))

        # update the SWF state (namespace-aware path for passive entities)
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())
        entity_state_path = ClusterPaths.get_passive_entity_state_path(swf_entity.namespace, EntityType.SWF, target_state, swf_entity.entity_id)
        self.cluster_handler.create(entity_state_path)
        self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

# WF Manager Protocol
class ClusterWFManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

    # Override
    def do_execute_passive_child(self, xchild_entity):
        utils.warn_once("TODO: ClusterWFManagerProtocol: do_execute_passive_child: the semantics are not clear")

        # check the type of the child
        xchild_entity_type = xchild_entity.entity_type

        # switch case for different kinds of passive children
        if (xchild_entity_type == EntityType.WF):
            return self.__do_execute_passive_wf__(xchild_entity)

        return False

    def __do_execute_passive_wf__(self, wf_entity):
        utils.info("ClusterWFManagerProtocol {}: __do_execute_passive_wf__: delegating to agent: {}".format(
            self.get_entity_id(), wf_entity.entity_id))

        # idempotent: skip if already assigned to an agent
        if self.__is_wf_assigned_to_agent__(wf_entity):
            utils.debug("ClusterWFManagerProtocol {}: WF {} already assigned to agent, skipping".format(
                self.get_entity_id(), wf_entity.entity_id))
            return True

        # find an available agent
        selected_agent_ref = self.__select_available_agent__()
        if selected_agent_ref is None:
            utils.warn("ClusterWFManagerProtocol {}: no available agent for WF {}. Will retry next cycle.".format(
                self.get_entity_id(), wf_entity.entity_id))
            return False

        utils.info("ClusterWFManagerProtocol {}: assigning WF {} to agent {}".format(
            self.get_entity_id(), wf_entity.entity_id, selected_agent_ref.entity_id))

        # write WF as execution task under agent (namespace-aware path)
        self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_ns_id(
            wf_entity.namespace, EntityType.AGENT, selected_agent_ref.entity_id, wf_entity.entity_type, wf_entity.entity_id))

        # write agent as executor under WF (namespace-aware path)
        self.cluster_handler.create(ClusterPaths.get_passive_entity_assigned_executors_by_id_path(
            wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, EntityType.AGENT, selected_agent_ref.entity_id))

        return True

    def __select_available_agent__(self):
        """Find any alive agent with free capacity."""
        alive_agent_ids = sorted(self.__get_alive_entity_ids__(EntityType.AGENT))
        utils.debug("__select_available_agent__: alive_agent_ids: {}".format(alive_agent_ids))

        if len(alive_agent_ids) == 0:
            return None

        available_agent_ids = sorted(self.__get_available_entity_ids__(EntityType.AGENT, alive_agent_ids))
        utils.debug("__select_available_agent__: available_agent_ids: {}".format(available_agent_ids))
        if len(available_agent_ids) == 0:
            return None

        return cluster_common_v2.ClusterEntityRef.new(EntityType.AGENT, available_agent_ids[0])

    def __is_wf_assigned_to_agent__(self, wf_entity):
        """Check if WF already has a live agent assigned. Cleans up stale references to dead agents."""
        agent_ids = self.cluster_handler.list_dirs(
            ClusterPaths.get_passive_entity_assigned_executors_by_child_type_path(
                wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, EntityType.AGENT))

        for agent_id in agent_ids:
            agent_state_protocol = ClusterActiveEntityStateProtocol(EntityType.AGENT, agent_id)
            if (agent_state_protocol.has_any_state() and agent_state_protocol.is_alive()):
                return True
            else:
                # stale executor reference: agent is dead, gone, or fully removed. Remove it so a new agent can be assigned.
                utils.warn("__is_wf_assigned_to_agent__: {}: removing stale executor reference to dead agent {}".format(
                    wf_entity.entity_id, agent_id))
                stale_path = ClusterPaths.get_passive_entity_assigned_executors_by_id_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, EntityType.AGENT, agent_id)
                self.cluster_handler.remove_dir_recursive(stale_path, ignore_if_missing = True)

        return False

    # Override: disassociate agent from WF when WF reaches terminal state
    def handle_passive_child_completed(self, xchild_entity):
        """Remove agent assignment when WF reaches terminal state. Idempotent."""
        if (xchild_entity.entity_type != EntityType.WF):
            return

        # find assigned agents from the WF's assigned-executors path
        agent_ids = self.cluster_handler.list_dirs(
            ClusterPaths.get_passive_entity_assigned_executors_by_child_type_path(
                xchild_entity.namespace, xchild_entity.entity_type, xchild_entity.entity_id, EntityType.AGENT))

        for agent_id in agent_ids:
            # remove WF from agent's assigned-execution-tasks
            utils.info("ClusterWFManagerProtocol {}: handle_passive_child_completed: removing WF {} from agent {} execution tasks".format(
                self.get_entity_id(), xchild_entity.entity_id, agent_id))
            self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_execution_tasks_by_ns_id(
                xchild_entity.namespace, EntityType.AGENT, agent_id, xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

class ClusterJobManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

# Task Manager Protocol
class ClusterTaskManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

# SWF Protocol
class ClusterSWFProtocol(ClusterExecutionTaskEntityProtocol):
    def __init__(self, entity, wf_protocols=None):
        super().__init__(entity)
        self.wf_protocols = wf_protocols if (wf_protocols is not None) else []

    def submit(self):
        """Submit SWF and all child WFs atomically."""
        # set parent_swf_id on each child WF spec
        for wf_protocol in self.wf_protocols:
            wf_protocol.entity.entity_spec.parent_swf_id = self.get_entity_id()

        # submit each child WF
        for wf_protocol in self.wf_protocols:
            wf_protocol.submit()

        # register WFs as dependents of SWF
        for wf_protocol in self.wf_protocols:
            self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_id(
                self.get_entity_type(), self.get_entity_id(),
                EntityType.WF, wf_protocol.get_entity_id()))

        # persist SWF entity
        self.initialize()

# WF Protocol
class ClusterWFProtocol(ClusterExecutionTaskEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def submit(self):
        """Persist the WF entity to cluster paths. Input files are written by SWFBuilder."""
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # initialize output directories
        self.cluster_handler.create(ClusterPaths.get_passive_entity_data_outputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
        for i in range(len(wf_spec.output_ids)):
            output_id = wf_spec.output_ids[i]
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id))

        # persist entity to cluster paths (entity IDs, details, state, children, dependents, heartbeat)
        self.initialize()

    # commands to execute the workflow
    def execute(self):
        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # TODO
        utils.warn_once("ClusterWFProtocol: execute: this is doing state management locally. Need to use ClusterEntityStateProtocol")

        # check if it is live or not
        if (wf_spec.is_live == False):
            self.execute_static()
        else:
            self.execute_live()

    def __update_wf_state__(self, cluster_handler_ref, entity_state):
        # use namespace-aware path for passive entities (WF is passive)
        state_path = ClusterPaths.get_passive_entity_state_path(self.entity.namespace, self.get_entity_type(), entity_state, self.get_entity_id())
        cluster_handler_ref.create(state_path)
        cluster_handler_ref.update_dynamic_value(state_path,
            cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec()))

    # execute wf statically as map reduce task
    def execute_static(self):
        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # debug
        utils.info("ClusterWFProtocol: execute_static: {}".format(self.get_entity_id()))

        # change the state to ALIVE
        self.__update_wf_state__(self.cluster_handler, EntityState.ALIVE)

        # run under try-catch block to handle exceptions and set the final state
        try:
            # read all inputs and union them
            input_ids = wf_spec.input_ids
            xinput = self.cluster_handler.read_df(ClusterPaths.get_passive_entity_data_input_default_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_ids[0]))
            for input_idx in range(1, len(input_ids)):
                xinput_next = self.cluster_handler.read_df(ClusterPaths.get_passive_entity_data_input_default_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_ids[input_idx]))
                xinput = xinput.union(xinput_next)
            utils.info("ClusterWFProtocol: {}: execute_static: num rows: {}, num_cols: {}".format(self.get_entity_id(), xinput.num_rows(), xinput.num_cols()))

            # resolve start_ts. TODO: This needs to fall on some boundaries of timestamps
            wf_spec_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else timefuncs.get_utctimestamp_sec()
            wf_spec_end_ts = wf_spec_start_ts + wf_spec.duration

            # check if entire input is to be used
            wf_spec_use_full_data = wf_spec.use_full_data

            # define some constants
            if (wf_spec.bucket_interval > 0):
                raise Exception("ClusterWFProtocol: static workflow dont support bucket_interval: {}".format(wf_spec.bucket_interval))

            # parse operations from the wf_spec as segments preserving job boundaries for checkpoint support
            segments = self.execute_live_single_round_get_operations(wf_spec)

            # debug
            utils.info("ClusterWFProtocol: {}: execute_static: duration: {}: start_ts: {}, end_ts: {}".format(
                self.get_entity_id(), wf_spec.duration, wf_spec_start_ts, wf_spec_end_ts))

            # resolve the input: use upstream ETL files if primary_input_id has upstream mapping
            primary_input_id = wf_spec.primary_input_id
            is_primary_upstream = (primary_input_id is not None and primary_input_id in wf_spec.upstream_inputs)
            if (is_primary_upstream):
                xinput_resolved = self.__resolve_upstream_data__(primary_input_id, wf_spec, wf_spec_start_ts, wf_spec_start_ts, wf_spec_end_ts, dmsg = "execute_static")
                if (xinput_resolved is None):
                    utils.info("ClusterWFProtocol: {}: execute_static: upstream inputs not ready, skipping".format(self.get_entity_id()))
            else:
                xinput_resolved = self.resolve_meta(xinput, wf_spec_start_ts, wf_spec_use_full_data, wf_spec_start_ts, wf_spec_end_ts)

            # check if this is external execution or not
            if (wf_spec.is_external == False):
                if (xinput_resolved is not None):
                    # execute and read the output
                    xoutput = self.execute_live_single_round(segments, xinput_resolved, wf_entity, wf_spec_start_ts, wf_spec_end_ts)

                    # write the output. TODO: single output only
                    output_id = wf_entity.entity_spec.output_ids[0]
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_outputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id))
                    self.cluster_handler.write_df(ClusterPaths.get_passive_entity_data_output_default_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id), xoutput)

                    # publish write event to message bus
                    message_bus.publish_event(self.cluster_handler, wf_entity.entity_type, wf_entity.entity_id, "write",
                        {"start_ts": wf_spec_start_ts, "end_ts": wf_spec_end_ts, "num_rows": xoutput.num_rows()}, output_id = output_id, dmsg = "execute_static")
            else:
                # wait for the presence of output. TODO
                output_id = wf_entity.entity_spec.output_ids[0]
                total_wait_time = 0
                while (total_wait_time < wf_spec.max_job_execution_time):
                    # check for local cluster state
                    if (self.cluster_handler.file_exists(ClusterPaths.get_passive_entity_data_output_default_file_path(wf_entity.namespace, self.get_entity_type(), self.get_entity_id(), output_id))):
                        # debug and wait
                        utils.info("ClusterWFProtocol: {}: execute_static: is_external is True. Output file found. Waiting for 10 seconds".format(self.get_entity_id()))
                        time.sleep(10)

                        # break loop
                        break
                    else:
                        # wait for some time
                        utils.info("ClusterWFProtocol: {}: execute_static: is_external is True. Waiting for 10 seconds".format(self.get_entity_id()))
                        time.sleep(10)
                        total_wait_time = total_wait_time + 10

            # set the final state
            self.__update_wf_state__(self.cluster_handler, EntityState.COMPLETED)

            # debug
            utils.info("ClusterWFProtocol: {}: execute_static: wf: {}. finished".format(self.get_entity_id(), wf_entity.entity_id))
        except Exception as e:
            # set the final state
            self.__update_wf_state__(self.cluster_handler, EntityState.FAILED)
            raise e

    # execute as live task
    def execute_live(self, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ClusterWFProtocol: execute_live: {}".format(self.get_entity_id()))

        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # change state to ALIVE
        self.__update_wf_state__(self.cluster_handler, EntityState.ALIVE)

        # run under try-catch block to handle exceptions and set the final state
        try:
            # --- One-time setup: resolve inputs ---
            ctx = ClusterExecutorContext(wf_entity.namespace, None)

            # identify primary input by name (from wf_spec.primary_input_id, set at build time)
            primary_input_id = wf_spec.primary_input_id

            # determine if primary input is upstream-dependent or seed data
            is_primary_upstream = (primary_input_id in wf_spec.upstream_inputs)

            # for seed inputs, read the seed data file once (one-time discovery)
            primary_input_df = None
            if (not is_primary_upstream):
                primary_input_df = ctx.resolve_input(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, primary_input_id)

            # resolve start_ts and align to bucket boundary
            wf_spec_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else timefuncs.get_utctimestamp_sec()
            wf_spec_use_full_data = wf_spec.use_full_data

            # bucket-align: floor start_ts to nearest ticks_interval boundary.
            # Each WF stays on its own interval grid — no clamping to upstream's grid.
            # __resolve_upstream_data__ handles partial-overlap windows via effective_start/effective_end,
            # so a downstream's first bucket may get fewer upstream sub-buckets and that is correct.
            # Clamping to a different-interval upstream would shift the output file grid, making this
            # WF's output files unresolvable by further downstream WFs on the same interval.
            bucket_start = etl.floor_to_bucket(wf_spec_start_ts, wf_spec.ticks_interval)

            # number of iterations based on ticks_interval
            effective_end = wf_spec_start_ts + wf_spec.duration
            num_iter = int(math.ceil((effective_end - bucket_start) / wf_spec.ticks_interval))

            # parse operations from the wf_spec as segments preserving job boundaries for checkpoint support
            segments = self.execute_live_single_round_get_operations(wf_spec)

            # initialize: window width is bucket_interval, advances by ticks_interval
            cur_start_ts = bucket_start
            cur_end_ts = bucket_start + wf_spec.bucket_interval

            # iterate
            for iter_count in range(num_iter):
                # check if wf has reached a terminal state (aborted, completed, failed) — exit immediately
                wf_state_protocol = ClusterPassiveEntityStateProtocol(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id)
                if (wf_state_protocol.is_finished()):
                    utils.info("{}: execute_live: iteration: {} / {}: WF is in terminal state, exiting".format(dmsg, iter_count, num_iter))
                    return

                # wait until current time passes the window end
                cur_ts = timefuncs.get_utctimestamp_sec()

                # debug
                utils.info("{}: execute_live: duration: {}, bucket_interval: {}, ticks_interval: {}, iteration: {} / {}: start_ts: {}, end_ts: {}, cur_ts: {}".format(
                    dmsg, wf_spec.duration, wf_spec.bucket_interval, wf_spec.ticks_interval, iter_count, num_iter, cur_start_ts, cur_end_ts, cur_ts))

                # check if it is time to run the loop
                if (cur_ts <= cur_end_ts):
                    # wait
                    utils.info("{}: execute_live: iteration: {} / {}: sleeping for {} seconds".format(dmsg, iter_count, num_iter, (cur_end_ts - cur_ts)))
                    time.sleep(cur_end_ts - cur_ts)

                # --- Per-iteration input resolution ---
                if (is_primary_upstream):
                    # primary input is a proxy: resolve from upstream ETL paths
                    upstream_attempts = 0
                    xinput_resolved = None
                    while (xinput_resolved is None):
                        xinput_resolved = self.__resolve_upstream_data__(primary_input_id, wf_spec, wf_spec_start_ts, cur_start_ts, cur_end_ts, dmsg = dmsg)
                        if (xinput_resolved is None):
                            upstream_attempts += 1
                            utils.info("{}: execute_live: iteration: {} / {}: upstream not ready, attempt: {}, sleeping {} seconds".format(
                                dmsg, iter_count, num_iter, upstream_attempts, cluster_common_v2.DEFAULT_UPSTREAM_WAIT_10_SEC))
                            time.sleep(cluster_common_v2.DEFAULT_UPSTREAM_WAIT_10_SEC)

                            # periodically check if WF has been aborted/finished
                            if (upstream_attempts % cluster_common_v2.DEFAULT_UPSTREAM_RETRY_BEFORE_STATE_CHECK == 0):
                                wf_state_check = ClusterPassiveEntityStateProtocol(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id)
                                if (wf_state_check.is_finished()):
                                    utils.info("{}: execute_live: iteration: {} / {}: WF is in terminal state during upstream wait, exiting".format(
                                        dmsg, iter_count, num_iter))
                                    return
                else:
                    # primary input is seed data: resolve via meta (template expansion, ETL scan, etc.)
                    xinput_resolved = self.resolve_meta(primary_input_df, wf_spec_start_ts, wf_spec_use_full_data, cur_start_ts, cur_end_ts)

                # enrich primary input with params from seed_input if configured
                xinput_resolved = self.__enrich_with_params__(xinput_resolved, primary_input_id, wf_entity, wf_spec, ctx)

                # output file timestamps are bucket boundaries -- fully deterministic
                output_id = wf_entity.entity_spec.output_ids[0]
                output_etl_file = ClusterPaths.get_passive_entity_data_output_etl_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id, cur_start_ts, cur_end_ts)

                # checkpoint: skip if output already exists
                if (xinput_resolved is not None and self.cluster_handler.file_exists(output_etl_file) == False):
                    # call a single run. live means that the files are ETL formatted and not based on batchids
                    xoutput = self.execute_live_single_round(segments, xinput_resolved, wf_entity, cur_start_ts, cur_end_ts)

                    # populate OMIGO_ARJUN_EVENT_TS from event_ts_col or bucket start time
                    xoutput = self.__populate_event_ts__(xoutput, wf_spec, cur_start_ts, dmsg = dmsg)

                    # write the output
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_outputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id))
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_etl_dt_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, output_id, cur_start_ts))
                    self.cluster_handler.write_df(output_etl_file, xoutput)

                    # publish write event to message bus
                    message_bus.publish_event(self.cluster_handler, wf_entity.entity_type, wf_entity.entity_id, "write",
                        {"start_ts": cur_start_ts, "end_ts": cur_end_ts, "num_rows": xoutput.num_rows()}, output_id = output_id, dmsg = dmsg)
                else:
                    # debug
                    utils.debug("{}: output etl file already exists: {}. Skipping ...".format(dmsg, output_etl_file))

                # update heartbeat after each batch so supervisor knows WF is making progress
                heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), wf_entity.lease)
                self.cluster_handler.update_dynamic_value(ClusterPaths.get_passive_entity_heartbeat_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id), heartbeat)

                # inter-iteration delay to avoid bombarding upstream data systems
                if (iter_count < num_iter - 1):
                    time.sleep(cluster_common_v2.DEFAULT_INTER_ITERATION_DELAY_5_SEC)

                # advance window by ticks_interval (slide)
                cur_start_ts = cur_start_ts + wf_spec.ticks_interval
                cur_end_ts = cur_start_ts + wf_spec.bucket_interval

            # set state to COMPLETED
            self.__update_wf_state__(self.cluster_handler, EntityState.COMPLETED)

            # debug
            utils.info("{}: execute_live: wf: {}. finished".format(dmsg, wf_entity.entity_id))
        except Exception as e:
            # set the final state
            self.__update_wf_state__(self.cluster_handler, EntityState.FAILED)
            raise e

    def __resolve_upstream_data__(self, input_id, wf_spec, wf_start_ts, cur_start_ts, cur_end_ts, dmsg = ""):
        """Resolve upstream ETL data for a single input_id using upstream_inputs metadata.
        Works uniformly for primary inputs and join operands.
        Returns a DataFrame (merge_union of all bucket files), or None if data not ready."""
        dmsg = utils.extend_inherit_message(dmsg, "__resolve_upstream_data__({})".format(input_id))

        if (input_id not in wf_spec.upstream_inputs):
            raise Exception("{}: input_id '{}' not found in upstream_inputs. Available: {}".format(
                dmsg, input_id, list(wf_spec.upstream_inputs.keys())))

        upstream_meta = wf_spec.upstream_inputs[input_id]
        etl_path = upstream_meta["etl_path"]
        upstream_bucket_interval = upstream_meta["bucket_interval"]
        upstream_ticks_interval = upstream_meta.get("ticks_interval", upstream_bucket_interval)
        upstream_start_ts = upstream_meta["start_ts"]
        upstream_duration = upstream_meta["duration"]

        # Runtime assertion (defensive)
        if (wf_spec.bucket_interval > 0 and upstream_ticks_interval > 0):
            if (wf_spec.bucket_interval % upstream_ticks_interval != 0):
                raise Exception("{}: downstream bucket_interval {} is not a multiple of upstream ticks_interval {}".format(
                    dmsg, wf_spec.bucket_interval, upstream_ticks_interval))

        # Clamp window to upstream's actual bucket range
        upstream_bucket_start = etl.floor_to_bucket(upstream_start_ts, upstream_ticks_interval)
        upstream_num_iter = int(math.ceil((upstream_start_ts + upstream_duration - upstream_bucket_start) / upstream_ticks_interval))
        upstream_last_end = upstream_bucket_start + upstream_num_iter * upstream_ticks_interval

        effective_start = max(cur_start_ts, upstream_bucket_start)
        effective_end = min(cur_end_ts, upstream_last_end)

        if (effective_start >= effective_end):
            utils.info("{}: no upstream files for '{}' in window [{}, {}), upstream range [{}, {})".format(
                dmsg, input_id, cur_start_ts, cur_end_ts, upstream_bucket_start, upstream_last_end))
            return None

        # Compute expected file paths for the clamped window
        expected_paths = etl.get_expected_upstream_etl_file_paths(etl_path, upstream_bucket_interval, effective_start, effective_end, ticks_interval = upstream_ticks_interval)
        utils.info("{}: etl_path: {}, upstream_bucket_interval: {}, upstream_ticks_interval: {}, window: [{}, {}), expected_paths: {}".format(
            dmsg, etl_path, upstream_bucket_interval, upstream_ticks_interval, effective_start, effective_end, len(expected_paths)))

        if (len(expected_paths) == 0):
            utils.info("{}: no expected paths computed".format(dmsg))
            return None

        # Wait for last file (upstream writes sequentially)
        last_path = expected_paths[-1]
        if (self.cluster_handler.file_exists(last_path) == False):
            utils.info("{}: upstream last file not ready: {}. Skipping.".format(dmsg, last_path))
            return None

        # Read all files with retry, merge_union
        dfs = []
        for path in expected_paths:
            xdf = self.__read_upstream_file_with_retry__(path, dmsg)
            dfs.append(xdf)
            utils.info("{}: read file: {}, rows: {}".format(dmsg, path, xdf.num_rows()))

        # merge_union all DataFrames (handles schema differences across buckets)
        result = dataframe.merge_union(dfs)

        # strip system column added by executor at write time — each WF produces its own event_ts,
        # carrying the upstream's through causes duplicate column errors in downstream joins
        if (cluster_arjun.OMIGO_ARJUN_EVENT_TS in result.get_columns()):
            result = result.drop_cols([cluster_arjun.OMIGO_ARJUN_EVENT_TS])

        utils.info("{}: resolved, total rows: {}".format(dmsg, result.num_rows()))
        return result

    def __read_upstream_file_with_retry__(self, path, dmsg = ""):
        """Read an upstream ETL file. If the read returns 0 cols/rows or throws an exception,
        retry once after 30 seconds to handle race conditions where file_exists returns
        True but the file is not fully written yet."""
        try:
            xdf = self.cluster_handler.read_df(path)
            if (xdf.num_cols() == 0):
                raise Exception("missing data (0 cols), trying retry once")
            return xdf
        except Exception as e:
            utils.info("{}: __read_upstream_file_with_retry__: {}: {}, retrying after {} sec".format(
                dmsg, path, e, cluster_common_v2.DEFAULT_UPSTREAM_READ_DELAY_30SEC))

        # single retry after wait
        time.sleep(cluster_common_v2.DEFAULT_UPSTREAM_READ_DELAY_30SEC)
        return self.cluster_handler.read_df(path)

    def __enrich_with_params__(self, xdf, input_id, wf_entity, wf_spec, ctx):
        """Enrich a DataFrame with params from the seed_input file based on input_params_config.
        Reads the seed_input file, cherry-picks the requested param keys, and adds them as
        constant columns via add_const() with the configured prefix ({prefix}:{key})."""
        config = wf_spec.input_params_config.get(input_id, {})
        param_keys = config.get("params", [])
        if (len(param_keys) == 0):
            return xdf

        prefix = config.get("prefix", wf_spec.name)

        # read the seed_input file to get param values
        seed_df = ctx.resolve_input(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, "seed_input")

        # enrich: add each requested param key as a constant column
        for key in param_keys:
            if (seed_df.has_col(key)):
                value = seed_df.col_as_array(key)[0]
                col_name = "{}:{}".format(prefix, key)
                xdf = xdf.add_const(col_name, value)
            else:
                utils.warn("__enrich_with_params__: WF {}: param key '{}' not found in seed_input. Available: {}".format(
                    wf_entity.entity_id, key, seed_df.get_columns()))

        return xdf

    # resolve meta parameters
    def resolve_meta(self, xdf, wf_start_ts, use_full_data, start_ts, end_ts):
        # resolve params first
        xdf1 = self.__resolve_meta_params__(xdf, start_ts, end_ts)

        # resolve data and do etl scan if needed
        xdf2 = self.__resolve_reference_paths__(xdf1, wf_start_ts, use_full_data, start_ts, end_ts)

        # resolve meta params again TODO
        xdf3 = self.__resolve_meta_params__(xdf2, start_ts, end_ts)

        # return
        return xdf3

    # TODO: these need to be defined properly
    def __resolve_reference_paths__(self, xdf, wf_start_ts, use_full_data, start_ts, end_ts):
        # check if DFReference is defined. TODO
        if (xdf.has_col(cluster_common_v2.DFReference.OMIGO_REFERENCE_PATH)):
            return self.__resolve_reference_paths__(cluster_common_v2.DFReference.read(xdf), wf_start_ts, use_full_data, start_ts, end_ts)

        # check if etl path is defined
        etl_path_col = None
        for c in xdf.get_columns():
            if (c.startswith(cluster_arjun.OMIGO_ARJUN_ETL_PATH_PREFIX)):
                utils.info("ClusterWFProtocol: __resolve_reference_paths__: found etl path: {}, {}".format(c, xdf.get_columns()))
                etl_path_col = c
                break

        # if etl path is defined, resolve it
        if (etl_path_col is not None):
            etsvs = []
            mps = xdf.to_maps()

            # iterate over each row and resolve the tsvs
            for mp in mps:
                # read all data. resolve the full path
                etl_full_path = self.cluster_handler.get_full_path(mp[etl_path_col])

                # get the start and end timestamps
                etl_start_ts = mp[cluster_arjun.OMIGO_ARJUN_ETL_START_TS]
                etl_end_ts = mp[cluster_arjun.OMIGO_ARJUN_ETL_END_TS]

                # TODO: HACK. Synchronizing dependencies
                etl_input_paths = etl.get_file_paths_by_datetime_range(etl_full_path, etl_start_ts, etl_end_ts, cluster_arjun.OMIGO_ARJUN_ETL_FILE_PREFIX)
                remaining_attempts = 30
                while ((etl_input_paths is None or len(etl_input_paths) == 0) and remaining_attempts > 0):
                    utils.info("__resolve_reference_paths__: etl input path is empty. Waiting for 10 seconds. Remaining Attempts: {}. path: {}, start_ts: {}, end_ts: {}, wf_start_ts: {}, use_full_data: {}".format(
                        remaining_attempts, etl_full_path, etl_start_ts, etl_end_ts, wf_start_ts, use_full_data))
                    time.sleep(10)
                    etl_input_paths = etl.get_file_paths_by_datetime_range(etl_full_path, etl_start_ts, etl_end_ts, cluster_arjun.OMIGO_ARJUN_ETL_FILE_PREFIX)
                    remaining_attempts = remaining_attempts - 1

                # if the delta input is still empty, print a warning
                if (etl_input_paths is None or len(etl_input_paths) == 0):
                    utils.warn("__resolve_reference_paths__: etl input path is empty even after waiting. Possible data loss")

                # resolve the effective start ts. Use the string representation
                effective_etl_start_ts = timefuncs.utctimestamp_to_datetime_str(wf_start_ts) if (use_full_data == True) else etl_start_ts

                # read and append the data
                etsv = etl.scan_by_datetime_range(etl_full_path, effective_etl_start_ts, etl_end_ts, cluster_arjun.OMIGO_ARJUN_ETL_FILE_PREFIX)

                # check if empty. TODO
                if (etsv.has_empty_header() == False):
                    # add all the columns except the etl path to this
                    for k in mp.keys():
                        if (k != etl_path_col):
                            etsv = etsv.add_const(str(k), str(mp[k]))

                    # append to the list
                    etsvs.append(etsv)

            # return after doing merge
            return self.__resolve_reference_paths__(tsv.merge_union(etsvs), wf_start_ts, use_full_data, start_ts, end_ts)

        # final fallback
        return xdf

    # internal method to resolve meta parameters
    def __resolve_meta_params__(self, xdf, start_ts, end_ts):
        def __resolve_meta_params_inner__(x):
            # column values
            start_ts_str = timefuncs.utctimestamp_to_datetime_str(start_ts)
            end_ts_str = timefuncs.utctimestamp_to_datetime_str(end_ts)

            # list of columns to replace
            cols = {
                cluster_arjun.OMIGO_ARJUN_START_TS_TEMPLATE: start_ts_str,
                cluster_arjun.OMIGO_ARJUN_END_TS_TEMPLATE: end_ts_str
            }

            # resolve
            for k in cols.keys():
                x = x.replace(k, cols[k])

            # return
            return x

        # apply the tranformation on each column
        xdf = xdf.transform_inline(xdf.get_columns(), lambda x: __resolve_meta_params_inner__(x), ignore_if_missing = True)

        # return
        return xdf

    # resolve meta parameters for external task
    def resolve_external_task_meta_params(self, xdf, input_id, output_id):
        return self.__resolve_external_task_meta_params__(xdf, input_id, output_id)

    # internal method to resolve meta parameters for external task
    def __resolve_external_task_meta_params__(self, xdf, input_id, output_id):
        def __resolve_external_task_meta_params_inner__(x):
            # list of columns to replace
            cols = {
                cluster_arjun.OMIGO_ARJUN_BASE_PATH_TEMPLATE: ClusterPaths.get_base_path(),
                cluster_arjun.OMIGO_ARJUN_INPUT_FILE_TEMPLATE: ClusterPaths.get_passive_entity_data_input_default_file_path(self.entity.namespace, self.get_entity_type(), self.get_entity_id(), input_id),
                cluster_arjun.OMIGO_ARJUN_OUTPUT_FILE_TEMPLATE: ClusterPaths.get_passive_entity_data_output_default_file_path(self.entity.namespace, self.get_entity_type(), self.get_entity_id(), output_id)
            }

            # resolve
            for k in cols.keys():
                x = x.replace(k, cols[k])

            # return
            return x

        # apply the tranformation on each column
        xdf = xdf.transform_inline(xdf.get_columns(), lambda x: __resolve_external_task_meta_params_inner__(x), ignore_if_missing = True)

        # return
        return xdf

    # resolve timestamp for etl file
    def __populate_event_ts__(self, xdf, wf_spec, cur_start_ts, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ClusterWFProtocol: __populate_event_ts__")

        # skip if the column already exists
        if (xdf.has_col(cluster_arjun.OMIGO_ARJUN_EVENT_TS)):
            utils.debug("{}: OMIGO_ARJUN_EVENT_TS already exists. Skipping".format(dmsg))
            return xdf

        # check if event_ts_col is specified and the source column exists
        event_ts_col = wf_spec.event_ts_col
        if (event_ts_col is not None and xdf.has_col(event_ts_col)):
            # normalize via datetime_to_utctimestamp_sec then back to datetime string for consistency
            def __normalize_event_ts__(v):
                ts_sec = timefuncs.datetime_to_utctimestamp_sec(v)
                return timefuncs.utctimestamp_to_datetime_str(ts_sec)

            xdf = xdf.transform([event_ts_col], __normalize_event_ts__, cluster_arjun.OMIGO_ARJUN_EVENT_TS)
        else:
            # fallback: use bucket start time as constant for all rows
            bucket_start_str = timefuncs.utctimestamp_to_datetime_str(cur_start_ts)
            xdf = xdf.add_const(cluster_arjun.OMIGO_ARJUN_EVENT_TS, bucket_start_str)

        return xdf

    def __resolve_etl_file_timestamp__(self, xdf, start_ts, end_ts):
        # check for no data
        if (xdf.num_rows() == 0):
            return (start_ts, end_ts)

        # check if the special column is defined
        if (xdf.has_col(cluster_arjun.OMIGO_ARJUN_EVENT_TS)):
            # take the min max values
            col_values = sorted(xdf.col_as_array_uniq(cluster_arjun.OMIGO_ARJUN_EVENT_TS))
            event_start_ts, event_end_ts = (col_values[0], col_values[-1])

            # resolve the value of the timestamp to numeric seconds
            event_start_ts = timefuncs.datetime_to_utctimestamp(event_start_ts)
            event_end_ts = timefuncs.datetime_to_utctimestamp(event_end_ts)

            # return
            return (event_start_ts, event_end_ts)

        # return the default
        return (start_ts, end_ts)

    # execute single round of live wf
    def execute_live_single_round(self, segments, xinput_resolved, wf_entity, cur_start_ts, cur_end_ts, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ClusterWFProtocol: execute_live_single_round")

        # if the input is empty, return empty output
        if (xinput_resolved.num_rows() == 0):
            utils.warn_once("execute_live_single_round: empty tsv. continuing. The empty tsv can lead to unpredictable behavior")

        # read the input as the base
        otsv = xinput_resolved

        # iterate over each job segment
        for operations in segments:
            # check if last operation is a checkpoint -- if so, check for fast-forward
            last_op = operations[-1][0] if (len(operations) > 0) else None
            if (last_op is not None and last_op.task_type == ClusterTaskType.CHECKPOINT):
                checkpoint_file = ClusterPaths.get_passive_entity_data_output_etl_file_path(
                    wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, last_op.name, cur_start_ts, cur_end_ts)

                # if checkpoint exists and overwrite is not set, read from checkpoint and skip this segment
                if (last_op.overwrite == False and self.cluster_handler.file_exists(checkpoint_file)):
                    utils.info("{}: checkpoint '{}' exists, skipping segment".format(dmsg, last_op.name))
                    otsv = self.cluster_handler.read_df(checkpoint_file)
                    continue

            # execute all operations in this segment
            for (operation, extend_class_op) in operations:
                # checkpoint operation: write intermediate output to ETL path
                if (operation.task_type == ClusterTaskType.CHECKPOINT):
                    checkpoint_file = ClusterPaths.get_passive_entity_data_output_etl_file_path(
                        wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, operation.name, cur_start_ts, cur_end_ts)
                    utils.info("{}: writing checkpoint '{}' to {}".format(dmsg, operation.name, checkpoint_file))
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, operation.name))
                    self.cluster_handler.create(ClusterPaths.get_passive_entity_data_output_etl_dt_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, operation.name, cur_start_ts))
                    self.cluster_handler.write_df(checkpoint_file, otsv)

                    # publish checkpoint event to message bus
                    message_bus.publish_event(self.cluster_handler, wf_entity.entity_type, wf_entity.entity_id, "checkpoint",
                        {"start_ts": cur_start_ts, "end_ts": cur_end_ts, "num_rows": otsv.num_rows()}, output_id = operation.name, dmsg = dmsg)

                    continue

                # call the function. and set the job status correctly
                try:
                    # if extend_class_op is defined, then instantiate the object
                    extend_class_obj = cluster_common_v2.load_extend_class_obj(extend_class_op, otsv.get_header_fields(), otsv.get_data_fields()) if (extend_class_op is not None) else None
                    utils.trace("{}: execute_live_single_round: extend_class_op: {}, extend_class_obj: {}".format(dmsg, extend_class_op, extend_class_obj))

                    # read function parameters
                    args = cluster_data.load_native_objects(operation.args)
                    kwargs = cluster_data.load_native_objects(operation.kwargs)

                    # resolve proxy DataFrame operands in args/kwargs (e.g. ctx.read_df("X") used as join arg)
                    args = self.__resolve_proxy_operands__(args, wf_entity, cur_start_ts, cur_end_ts, dmsg)
                    kwargs = self.__resolve_proxy_operands_dict__(kwargs, wf_entity, cur_start_ts, cur_end_ts, dmsg)

                    # define func, class_reference, class_func accordingly
                    class_func = None

                    # check if extend_class_obj is defined
                    if (extend_class_obj is not None):
                        # TODO: this can break
                        func_base_name = operation.name.split(".")[-1]
                        class_func = getattr(extend_class_obj, func_base_name)
                    else:
                        # lookup the function to call
                        func = cluster_class_reflection.load_fully_qualified_func(operation.name)
                        class_reference = cluster_class_reflection.get_class_that_defined_method(func)
                        class_func = getattr(otsv.extend_class(class_reference), func.__name__)

                    # debug
                    utils.trace("{}: execute_live_single_round: custom_func: name: {}, args: {}, kwargs: {}, extend_class_obj: {}, func: {}".format(
                        dmsg, operation.name, args, kwargs, extend_class_obj, class_func))

                    # note that custom_func is not called as the context is already in the function.
                    otsv = class_func(*args, **kwargs)
                except Exception as e:
                    utils.error("{}: execute_live_single_round: Found error while running the method: {}".format(dmsg, e))
                    raise e

        # return output
        return otsv

    def __resolve_proxy_operands__(self, args, wf_entity, cur_start_ts, cur_end_ts, dmsg = ""):
        """Resolve proxy DataFrame dicts in operation args."""
        wf_spec = wf_entity.entity_spec
        wf_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else cur_start_ts
        resolved = []
        for arg in args:
            if (isinstance(arg, dict) and cluster_data.PROXY_DATAFRAME_MARKER_KEY in arg):
                proxy_id = arg[cluster_data.PROXY_DATAFRAME_MARKER_KEY]
                xdf = None
                proxy_attempts = 0
                while (xdf is None):
                    xdf = self.__resolve_upstream_data__(proxy_id, wf_spec, wf_start_ts, cur_start_ts, cur_end_ts, dmsg)
                    if (xdf is None):
                        proxy_attempts += 1
                        utils.info("{}: proxy operand '{}' not ready at window [{}, {}), sleeping {} seconds".format(
                            dmsg, proxy_id, cur_start_ts, cur_end_ts, cluster_common_v2.DEFAULT_WAIT_SEC))
                        heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), wf_entity.lease)
                        self.cluster_handler.update_dynamic_value(
                            ClusterPaths.get_passive_entity_heartbeat_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id), heartbeat)
                        time.sleep(cluster_common_v2.DEFAULT_WAIT_SEC)

                        # periodically check if WF has been aborted/finished
                        if (proxy_attempts % cluster_common_v2.DEFAULT_UPSTREAM_RETRY_BEFORE_STATE_CHECK == 0):
                            wf_state_check = ClusterPassiveEntityStateProtocol(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id)
                            if (wf_state_check.is_finished()):
                                utils.info("{}: proxy operand '{}': WF is in terminal state during upstream wait, raising".format(dmsg, proxy_id))
                                raise Exception("{}: proxy operand '{}': WF reached terminal state while waiting for upstream data".format(dmsg, proxy_id))
                resolved.append(xdf)
            else:
                resolved.append(arg)
        return resolved

    def __resolve_proxy_operands_dict__(self, kwargs, wf_entity, cur_start_ts, cur_end_ts, dmsg = ""):
        """Resolve proxy DataFrame dicts in kwargs values."""
        wf_spec = wf_entity.entity_spec
        wf_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else cur_start_ts
        resolved = {}
        for k, v in kwargs.items():
            if (isinstance(v, dict) and cluster_data.PROXY_DATAFRAME_MARKER_KEY in v):
                proxy_id = v[cluster_data.PROXY_DATAFRAME_MARKER_KEY]
                xdf = None
                proxy_attempts = 0
                while (xdf is None):
                    xdf = self.__resolve_upstream_data__(proxy_id, wf_spec, wf_start_ts, cur_start_ts, cur_end_ts, dmsg)
                    if (xdf is None):
                        proxy_attempts += 1
                        utils.info("{}: proxy operand '{}' not ready at window [{}, {}), sleeping {} seconds".format(
                            dmsg, proxy_id, cur_start_ts, cur_end_ts, cluster_common_v2.DEFAULT_WAIT_SEC))
                        heartbeat = cluster_common_v2.ClusterHearbeat.new(timefuncs.get_utctimestamp_sec(), wf_entity.lease)
                        self.cluster_handler.update_dynamic_value(
                            ClusterPaths.get_passive_entity_heartbeat_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id), heartbeat)
                        time.sleep(cluster_common_v2.DEFAULT_WAIT_SEC)

                        # periodically check if WF has been aborted/finished
                        if (proxy_attempts % cluster_common_v2.DEFAULT_UPSTREAM_RETRY_BEFORE_STATE_CHECK == 0):
                            wf_state_check = ClusterPassiveEntityStateProtocol(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id)
                            if (wf_state_check.is_finished()):
                                utils.info("{}: proxy operand '{}': WF is in terminal state during upstream wait, raising".format(dmsg, proxy_id))
                                raise Exception("{}: proxy operand '{}': WF reached terminal state while waiting for upstream data".format(dmsg, proxy_id))
                resolved[k] = xdf
            else:
                resolved[k] = v
        return resolved

    # get all operations for single round of live wf. returns list of operations lists preserving job boundaries
    def execute_live_single_round_get_operations(self, wf_spec):
        # create segments: each segment is a list of operations for one job
        segments = []

        # flatten the wf spec into tasks. ignore all partitioning tasks. then execute sequentially
        jobs_specs = wf_spec.jobs_specs
        for job_spec in jobs_specs:
            # operations for this job segment
            operations = []

            # check if a custom class is called
            extend_class_op = None

            # instantiate the class object
            if (job_spec.extend_class_def is not None):
                extend_class_op = job_spec.extend_class_def.extend_class_op

            # add map task
            if (job_spec.map_task is not None):
                # iterate and add all operations
                for op in job_spec.map_task.map_ops:
                    # capture inline extend_class for the next operation
                    if (op.task_type == ClusterTaskType.EXTEND_CLASS):
                        extend_class_op = op
                        continue

                    # add the operation and the extend class
                    operations.append((op, extend_class_op))

                    # only the first operation is under extend class. rest are normal tsv
                    extend_class_op = None

            # add reduce task
            if (job_spec.reduce_task is not None):
                operations.append((job_spec.reduce_task.reduce_op, extend_class_op))
                # only the first operation is under extend class. rest are normal tsv
                extend_class_op = None

            # add singleton task
            if (job_spec.singleton_task is not None):
                operations.append((job_spec.singleton_task.singleton_op, extend_class_op))
                # only the first operation is under extend class. rest are normal tsv
                extend_class_op = None

            # add checkpoint as the last operation in the segment (if defined)
            if (job_spec.checkpoint_def is not None):
                operations.append((job_spec.checkpoint_def.checkpoint_op, None))

            # add segment
            segments.append(operations)

        # return
        return segments

# Job Protocol
class ClusterJobProtocol(ClusterExecutionTaskEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Task Protocol
class ClusterTaskProtocol(ClusterExecutionTaskEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Batch Protocol
class ClusterBatchProtocol(ClusterExecutionTaskEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Worker Protocol
class ClusterWorkerProtocol(ClusterExecutorEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Agent Protocol
class ClusterAgentProtocol(ClusterExecutorEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    # Override
    def do_execute_passive_child(self, xchild_entity):
        utils.warn_once("TODO: ClusterAgentProtocol: do_execute_passive_child: the semantics are not clear")

        # check the type of the child
        xchild_entity_type = xchild_entity.entity_type

        # switch case for different kinds of passive children
        if (xchild_entity_type == EntityType.WF):
            self.__do_execute_passive_wf__(xchild_entity)
            return True

        return False

    def __do_execute_passive_wf__(self, wf_entity):
        utils.info("ClusterWFManagerProtocol {}: __do_execute_passive_wf__: {}".format(self.get_entity_id(), wf_entity.entity_id))
        wf_protocol = ClusterWFProtocol(wf_entity)
        wf_protocol.execute()

# Double Agent Protocol
class ClusterDoubleAgentProtocol(ClusterExecutorEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Inteli Agent Protocol
class ClusterInteliAgentProtocol(ClusterExecutorEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# System Agent Protocol - active entity with heartbeat but no WF execution
class ClusterSystemAgentProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing so RM can discover and assign this entity
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

# Client Protocol
class ClusterClientProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

# Session Protocol
class ClusterSessionProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def initialize(self):
        super().initialize()

        # shard-based incoming routing
        shard_id = cluster_common_v2.get_shard_for_entity(self.get_entity_id())
        self.cluster_handler.create(ClusterPaths.get_shard_entity_incoming(shard_id, self.get_entity_type(), self.get_entity_id()))

    # method for submitting a super-workflow to the cluster
    def submit_swf(self, namespace, swf_spec, xinputs_per_wf):
        # validate namespace exists
        if (self.cluster_handler.dir_exists(ClusterPaths.get_namespace_path(namespace)) == False):
            raise Exception("ClusterSessionProtocol: submit_swf: namespace '{}' does not exist. Register it before submitting.".format(namespace))

        # build child WF protocols
        wf_protocols = []
        for i in range(len(swf_spec.wfs_specs)):
            wf_spec_i = swf_spec.wfs_specs[i]
            xinputs = xinputs_per_wf[i]
            wf_entity = cluster_common_v2.ClusterEntityWF.new(namespace, ClusterIds.generate_wf_id(), ClusterIds.get_client_id(), ClusterIds.get_session_id(), wf_spec_i)
            # write input files directly (SWFBuilder normally handles this but this is the legacy path)
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_inputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
            for j in range(len(wf_spec_i.input_ids)):
                input_id = wf_spec_i.input_ids[j]
                self.cluster_handler.create(ClusterPaths.get_passive_entity_data_input_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id))
                self.cluster_handler.write_df(ClusterPaths.get_passive_entity_data_input_default_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id), xinputs[j])
            wf_protocols.append(ClusterWFProtocol(wf_entity))

        # build SWF protocol with children attached
        swf_entity = cluster_common_v2.ClusterEntitySWF.new(namespace, ClusterIds.generate_swf_id(), ClusterIds.get_client_id(), ClusterIds.get_session_id(), swf_spec)
        swf_protocol = ClusterSWFProtocol(swf_entity, wf_protocols)
        swf_protocol.submit()

        # register SWF as dependent of session
        self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_id(self.get_entity_type(), self.get_entity_id(), swf_entity.entity_type, swf_entity.entity_id))

        # return swf_id
        return swf_entity.entity_id

    # method for submitting a workflow to the cluster
    def submit_wf(self, namespace, wf_spec, xinputs):
        # validate namespace exists
        if (self.cluster_handler.dir_exists(ClusterPaths.get_namespace_path(namespace)) == False):
            raise Exception("ClusterSessionProtocol: submit_wf: namespace '{}' does not exist. Register it before submitting.".format(namespace))

        # create wf entity and protocol
        wf_entity = cluster_common_v2.ClusterEntityWF.new(namespace, ClusterIds.generate_wf_id(), ClusterIds.get_client_id(), ClusterIds.get_session_id(), wf_spec)
        # write input files directly (SWFBuilder normally handles this but this is the legacy path)
        self.cluster_handler.create(ClusterPaths.get_passive_entity_data_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
        self.cluster_handler.create(ClusterPaths.get_passive_entity_data_inputs_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id))
        for j in range(len(wf_spec.input_ids)):
            input_id = wf_spec.input_ids[j]
            self.cluster_handler.create(ClusterPaths.get_passive_entity_data_input_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id))
            self.cluster_handler.write_df(ClusterPaths.get_passive_entity_data_input_default_file_path(wf_entity.namespace, wf_entity.entity_type, wf_entity.entity_id, input_id), xinputs[j])
        wf_protocol = ClusterWFProtocol(wf_entity)
        wf_protocol.submit()

        # add as dependent of session
        self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_id(self.get_entity_type(), self.get_entity_id(), wf_entity.entity_type, wf_entity.entity_id))

        # return wf_id
        return wf_entity.entity_id

# Master Election Protocol
class ClusterMasterElectionProtocol:
    def __init__(self, entity_id):
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def get_elected_master(self):
        # check if master current exists.
        if (self.cluster_handler.is_non_empty_dir(ClusterPaths.get_current_master())):
            # read the value of elected master
            current_master_ref = cluster_common_v2.ClusterEntityRef.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_current_master()))

            # check if current master exists
            current_master_entity_path = ClusterPaths.get_entity(current_master_ref.entity_type, current_master_ref.entity_id)
            if (self.cluster_handler.dir_exists(current_master_entity_path) == False):
                utils.warn("ClusterMasterElectionProtocol: {}: has_elected_master: current master entry exists but the entity is missing: {}".format(self.entity_id, current_master_ref.entity_id))
                return None

            # check if the master is alive
            current_master = cluster_common_v2.ClusterEntity.from_json(self.cluster_handler.read_most_recent_json(current_master_entity_path))

            # check alive
            xentity_state_protocol = ClusterActiveEntityStateProtocol(current_master.entity_type, current_master.entity_id)

            # return false if the master exists but is not alive
            if (xentity_state_protocol.is_alive() == True):
                return current_master.entity_id
            else:
                return None
        else:
            # no elected master found
            return None

    # check if there is elected master
    def has_elected_master(self):
        if (self.get_elected_master() is not None):
            return True
        else:
            return False

    # check if its current master
    def is_current_master(self):
        # check if the master is already elected
        if (self.has_elected_master()):
            # read the value of elected master
            current_master_ref = cluster_common_v2.ClusterEntityRef.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_current_master()))

            # check if the master is alive
            current_master = cluster_common_v2.ClusterEntity.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(current_master_ref.entity_type, current_master_ref.entity_id)))

            # compare the ids
            if (current_master.entity_id == self.entity_id):
                utils.info("ClusterMasterElectionProtocol: {}: is_current_master: already the current master".format(self.entity_id))
                return True
            else:
                utils.info("ClusterMasterElectionProtocol: {}: is_current_master: another master exists: {}".format(self.entity_id, current_master.entity_id))
                return False
        else:
            # all other scenarios, its false
            utils.info("ClusterMasterElectionProtocol: {}: is_current_master: no current master found.".format(self.entity_id))
            return False

    # run master election
    def run_election(self):
        # check if master already exists and is valid
        current_master_id = self.get_elected_master()
        if (current_master_id is not None):
            # check if already the current master
            if (current_master_id == self.entity_id):
                utils.info("ClusterMasterElectionProtocol: {}: run_election: already the current master".format(self.entity_id))
            else:
                utils.info("ClusterMasterElectionProtocol: {}: run_election: another master exists: {}".format(self.entity_id, current_master_id))

            # return
            return False

        # get the current master id
        utils.info("ClusterMasterElectionProtocol: {}: run_election: ".format(self.entity_id))

        # get other masters
        candidate_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities(EntityType.MASTER))

        # get all the candidates. pick the one with least timestamp
        min_ts = -1
        min_ts_candidate = None

        # check if there are any candidates
        if (candidate_ids is None or len(candidate_ids) == 0):
            utils.info("ClusterMasterElectionProtocol: {}: run_election: : no candidates found.".format(self.entity_id))
            return False

        # debug
        utils.info("ClusterMasterElectionProtocol: {}: run_election: list of candidates found (before checking alive): {}".format(self.entity_id, candidate_ids))

        # iterate over all candidates
        for candidate_id in candidate_ids:
            # get candidate information
            candidate = cluster_common_v2.ClusterEntity.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(EntityType.MASTER, candidate_id)))

            # only consider if the candidate is alive
            xentity_state_protocol = ClusterActiveEntityStateProtocol(candidate.entity_type, candidate.entity_id)
            if (xentity_state_protocol.is_alive() == False):
                utils.info("ClusterMasterElectionProtocol: {}: run_election: found dead candidate: {}. Ignoring.".format(self.entity_id, candidate_id))
                continue

            # find the one with the min
            if (min_ts == -1 or min_ts > candidate.ts):
                min_ts = candidate.ts
                min_ts_candidate = candidate
            elif (min_ts == candidate.ts):
                if (candidate.entity_id < min_ts_candidate.entity_id):
                    min_ts_candidate = candidate

        # check if there was any valid candidate
        if (min_ts_candidate is None):
            utils.warn("ClusterMasterElectionProtocol: {}: run_election: no alive candidates found. Returning".format(self.entity_id))
            return False

        # pick the winner. Check if the winner is the same as the current
        if (min_ts_candidate.entity_id == self.entity_id):
            # create reference
            min_ts_candidate_ref = cluster_common_v2.ClusterEntityRef.new(min_ts_candidate.entity_type, min_ts_candidate.entity_id)

            # elected as winner. Create an entry into the /master/current
            self.cluster_handler.update_dynamic_seq_update(ClusterPaths.get_current_master(), min_ts_candidate_ref)
            utils.info("ClusterMasterElectionProtocol: {}: run_election: Won".format(self.entity_id))
            return True
        else:
            utils.info("ClusterMasterElectionProtocol: {}: run_election: Lost election. Found another winner: {}".format(self.entity_id, min_ts_candidate.entity_id))
            return False

# admin to create and delete cluster
class ClusterAdmin:
    def __init__(self):
        self.cluster_handler = ClusterPaths.get_cluster_handler()
        self.namespaces = self.__read_namespaces__()

    def __read_namespaces__(self):
        """Read registered namespaces from the namespace registry."""
        ns_base = ClusterPaths.get_namespaces_base_path()
        if (self.cluster_handler.dir_exists(ns_base)):
            return self.cluster_handler.list_dirs(ns_base)
        return []

    def bootstrap_namespace(self, namespace):
        """Create namespace registry entry and all per-passive-type directory scaffolding."""
        # Create namespace registry entry
        self.cluster_handler.create(ClusterPaths.get_namespace_path(namespace))
        ns_metadata = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_namespace_path(namespace), ns_metadata)

        # Create per-passive-type directories under each relevant base path
        tasks = []
        for ptype in cluster_common_v2.EntityPassiveTypes:
            # entities-ids/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities_ids(ptype), namespace)))
            # entities-details/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities(ptype), namespace)))
            # entities-incoming/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities_incoming(ptype), namespace)))
            # entities-assigned-supervisor/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities_assigned_supervisor(ptype), namespace)))
            # entities-assigned-executors/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities_assigned_executors(ptype), namespace)))
            # entities-data/{type}s/{namespace}/
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                "{}/{}".format(ClusterPaths.get_entities_data(ptype), namespace)))
            # entities-state/{type}s/{state}/{namespace}/ for every state
            for state in EntityState.get_all():
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                    ClusterPaths.get_passive_entities_state_by_state(namespace, ptype, state)))
            # shards/shard-{N}/entities-incoming/{type}s/{namespace}/ for every shard
            for shard_id in range(cluster_common_v2.NUM_SHARDS):
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                    "{}/{}".format(ClusterPaths.get_shard_incoming(shard_id, ptype), namespace)))

        # Create message bus scaffolding (single flat location, not per-namespace)
        tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
            ClusterPaths.get_message_bus_base()))
        tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
            ClusterPaths.get_message_bus_incoming_current()))
        tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
            ClusterPaths.get_message_bus_incoming_late()))
        tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
            ClusterPaths.get_message_bus_completed()))
        tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
            ClusterPaths.get_message_bus_buckets()))
        for bucket_level in message_bus.MESSAGE_BUS_BUCKET_LEVELS:
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create,
                ClusterPaths.get_message_bus_bucket(bucket_level)))

        utils.run_with_thread_pool(tasks, num_par = 10)

    # create cluster
    def create_cluster(self, create_remote_flag = False, namespaces = ["platform"]):
        # print the cluster base path
        utils.info("create_cluster: Using base path: {}".format(cluster_common_v2.HYDRA_PATH))

        # validation
        if (namespaces is None or len(namespaces) == 0):
            raise Exception("create_cluster: namespace can not be empty")

        # create root path
        self.cluster_handler.create("")

        # iterate over base paths
        for base_path in ClusterPaths.get_base_paths():
            self.cluster_handler.create(base_path)

        # Create namespaces base directory and bootstrap each namespace
        self.cluster_handler.create(ClusterPaths.get_namespaces_base_path())
        for namespace in namespaces:
            self.bootstrap_namespace(namespace)

        # create tasks
        tasks = []

        # iterate over all entity types
        for xentity_type in EntityType.get_all():
            # create states
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_ids(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_state(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_active_children(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_passive_children(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_dependents(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_heartbeat(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_incoming(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_assigned_supervisor(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_assigned_executors(xentity_type)))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_data(xentity_type)))

            # update for all states
            for xentity_state in EntityState.get_all():
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_state_by_state(xentity_type, xentity_state)))

        # TODO: create root path
        if (create_remote_flag == True):
            utils.warn_once("create_cluster: hack to create remote cluster")
            self.cluster_handler.create("")

            # # iterate over base paths
            for base_path in ClusterPaths.get_base_paths():
                self.cluster_handler.create(base_path)

            # iterate over all entity types
            for xentity_type in EntityType.get_all():
                # create states
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_ids(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_state(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_active_children(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_passive_children(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_dependents(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_heartbeat(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_incoming(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_assigned_supervisor(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_assigned_executors(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_data(xentity_type)))

                # update for all states
                for xentity_state in EntityState.get_all():
                    tasks.append(utils.ThreadPoolTask(self.cluster_handler.create, ClusterPaths.get_entities_state_by_state(xentity_type, xentity_state)))

        # run under thread pool
        utils.run_with_thread_pool(tasks, num_par = 10)

    # remove all entities. TODO: implement logic to revoke any validation tokens for failsafe stopping of entities
    def remove_entities(self, only_dead = False):
        # find the list of entities to remove: (namespace_or_none, entity_type, entity_id)
        remove_entities = []

        # iterate over all entity types
        for xentity_type in EntityType.get_all():
            if (cluster_common_v2.is_passive_entity(xentity_type)):
                # passive entities: iterate over namespaces
                for namespace in self.namespaces:
                    xentity_ids = self.cluster_handler.list_dirs(
                        "{}/{}".format(ClusterPaths.get_entities_ids(xentity_type), namespace))
                    for xentity_id in xentity_ids:
                        if (only_dead == True):
                            xentity_state_protocol = ClusterPassiveEntityStateProtocol(namespace, xentity_type, xentity_id)
                            if (xentity_state_protocol.is_alive() == False):
                                remove_entities.append((namespace, xentity_type, xentity_id))
                                utils.info("ClusterAdmin: remove_entities: {}/{}/{}".format(namespace, xentity_type, xentity_id))
                        else:
                            remove_entities.append((namespace, xentity_type, xentity_id))
                            utils.info("ClusterAdmin: remove_entities: {}/{}/{}".format(namespace, xentity_type, xentity_id))
            else:
                # active entities: existing logic
                xentity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_ids(xentity_type))
                for xentity_id in xentity_ids:
                    if (only_dead == True):
                        xentity_state_protocol = ClusterActiveEntityStateProtocol(xentity_type, xentity_id)
                        if (xentity_state_protocol.is_alive() == False):
                            remove_entities.append((None, xentity_type, xentity_id))
                            utils.info("ClusterAdmin: remove_entities: {}".format(xentity_id))
                    else:
                        remove_entities.append((None, xentity_type, xentity_id))
                        utils.info("ClusterAdmin: remove_entities: {}".format(xentity_id))

        # wait for max heartbeat timeout.
        if (len(remove_entities) > 0):
            utils.info("ClusterAdmin: waiting for {} seconds for stopping any heartbeat threads. enable it again".format(ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT))
            # time.sleep(ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT)

        # inner method for threadpool
        def __remove_entities_inner__(namespace, xentity_type, xentity_id):
            if (cluster_common_v2.is_passive_entity(xentity_type)):
                ClusterPassiveEntityCleanupProtocol(namespace, xentity_type, xentity_id).cleanup()
            else:
                ClusterActiveEntityCleanupProtocol(xentity_type, xentity_id).cleanup()

        # iterate over all entities again and remove
        tasks = []
        for (namespace, xentity_type, xentity_id) in remove_entities:
            # add the task
            tasks.append(utils.ThreadPoolTask(__remove_entities_inner__, namespace, xentity_type, xentity_id))

        # run under thread pool
        utils.run_with_thread_pool(tasks, num_par = 10)

    def remove_cluster(self):
        # remove all entities
        self.remove_entities()

        # create tasks
        tasks = []

        # remove entities state
        for xentity_type in EntityType.get_all():
            # delete at the level of entity type
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_active_children(xentity_type), ignore_if_missing = True))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_passive_children(xentity_type), ignore_if_missing = True))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_dependents(xentity_type), ignore_if_missing = True))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_incoming(xentity_type), ignore_if_missing = True))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_assigned_supervisor(xentity_type), ignore_if_missing = True))
            tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_assigned_executors(xentity_type), ignore_if_missing = True))

            # update for all states
            for xentity_state in EntityState.get_all():
                tasks.append(utils.ThreadPoolTask(self.cluster_handler.remove_dir_recursive, ClusterPaths.get_entities_state_by_state(xentity_type, xentity_state), ignore_if_missing = True))

        # run under thread pool
        utils.run_with_thread_pool(tasks, num_par = 10)

        # iterate over base paths and remove
        for base_path in ClusterPaths.get_base_paths():
            self.cluster_handler.remove_dir_recursive(base_path, ignore_if_missing = True)

        # remove current master
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_current_master(), ignore_if_missing = True)

    # TODO: this is only temporary
    def do_active_entity_state_change(self, xentity_type, xentity_id, target_state):
        utils.info("ClusterAdmin: do_active_entity_state_change: {}, {}, {}".format(xentity_type, xentity_id, target_state))

        # create new update time
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())

        # take path
        entity_state_path = ClusterPaths.get_entities_state_by_id(xentity_type, target_state, xentity_id)
        self.cluster_handler.create(entity_state_path)
        self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

    def do_passive_entity_state_change(self, namespace, xentity_type, xentity_id, target_state):
        utils.info("ClusterAdmin: do_passive_entity_state_change: {}, {}, {}, {}".format(namespace, xentity_type, xentity_id, target_state))

        # create new update time
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(timefuncs.get_utctimestamp_sec())

        # take path
        entity_state_path = ClusterPaths.get_passive_entity_state_path(namespace, xentity_type, target_state, xentity_id)
        self.cluster_handler.create(entity_state_path)
        self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

    def __force_delete_passive_entity__(self, namespace, entity_type, entity_id):
        """Recursively force-delete a passive entity and all its dependents (bottom-up).
        If non-terminal, writes ABORTED state first. Then runs physical cleanup."""
        utils.warn("ClusterAdmin: __force_delete_passive_entity__: STOP-GAP — directly mutating state for {}/{}/{}. Bypasses SWF manager ownership. Needs proper abort-request protocol.".format(namespace, entity_type, entity_id))
        # recursively delete dependents first (bottom-up)
        dep_types = cluster_common_v2.EntityDependentsMap.get(entity_type, [])
        for dep_type in dep_types:
            dep_path = ClusterPaths.get_entity_dependents_by_child_type(entity_type, entity_id, dep_type)
            if (self.cluster_handler.dir_exists(dep_path)):
                dep_ids = self.cluster_handler.list_dirs(dep_path)
                for dep_id in dep_ids:
                    utils.info("ClusterAdmin: __force_delete_passive_entity__: {}/{}/{}: recursing into dependent {}/{}".format(
                        namespace, entity_type, entity_id, dep_type, dep_id))
                    self.__force_delete_passive_entity__(namespace, dep_type, dep_id)

        # check current state and abort if non-terminal
        state_protocol = ClusterPassiveEntityStateProtocol(namespace, entity_type, entity_id)
        terminal_states = {EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP}

        if (state_protocol.has_any_state()):
            cur_state = state_protocol.get_registered_state_resolved()
            if (cur_state not in terminal_states):
                utils.info("ClusterAdmin: __force_delete_passive_entity__: {}/{}/{}: state {} -> ABORTED".format(
                    namespace, entity_type, entity_id, cur_state))
                self.do_passive_entity_state_change(namespace, entity_type, entity_id, EntityState.ABORTED)

        # physical cleanup
        utils.info("ClusterAdmin: __force_delete_passive_entity__: {}/{}/{}: running cleanup".format(
            namespace, entity_type, entity_id))
        ClusterPassiveEntityCleanupProtocol(namespace, entity_type, entity_id).cleanup()

    def abort_entity(self, namespace, entity_type, entity_id):
        """Abort a passive entity and all its dependents recursively (bottom-up).
        Sets state to ABORTED for any entity not already in a terminal state.
        Does NOT perform physical cleanup — entities remain on disk in ABORTED state.

        WARNING: This method directly mutates SWF/WF state, bypassing the SWF manager that owns the entity.
        This can cause inconsistency if the SWF manager is concurrently managing the entity lifecycle.
        Proper design: abort requests should be communicated to the SWF manager via the protocol layer,
        and the SWF manager should perform the state change itself. This direct approach is a stop-gap."""
        # validate existence
        id_path = ClusterPaths.get_passive_entity_id_path(namespace, entity_type, entity_id)
        if (self.cluster_handler.dir_exists(id_path) == False):
            utils.warn("ClusterAdmin: abort_entity: {}/{} not found in namespace {}".format(entity_type, entity_id, namespace))
            return

        utils.warn("ClusterAdmin: abort_entity: STOP-GAP — directly mutating state for {}/{}/{}. This bypasses the SWF manager. Needs proper abort-request protocol.".format(namespace, entity_type, entity_id))
        utils.info("ClusterAdmin: abort_entity: aborting {}/{}/{}".format(namespace, entity_type, entity_id))
        self.__abort_entity_recursive__(namespace, entity_type, entity_id)
        utils.info("ClusterAdmin: abort_entity: {}/{}/{} aborted".format(namespace, entity_type, entity_id))

    def __abort_entity_recursive__(self, namespace, entity_type, entity_id):
        """Recursively abort a passive entity and all its dependents (bottom-up)."""
        utils.warn("ClusterAdmin: __abort_entity_recursive__: STOP-GAP — directly aborting {}/{}/{}. Bypasses SWF manager ownership. Needs proper abort-request protocol.".format(namespace, entity_type, entity_id))
        # recursively abort dependents first (bottom-up)
        dep_types = cluster_common_v2.EntityDependentsMap.get(entity_type, [])
        for dep_type in dep_types:
            dep_path = ClusterPaths.get_entity_dependents_by_child_type(entity_type, entity_id, dep_type)
            if (self.cluster_handler.dir_exists(dep_path)):
                dep_ids = self.cluster_handler.list_dirs(dep_path)
                for dep_id in dep_ids:
                    utils.info("ClusterAdmin: __abort_entity_recursive__: {}/{}/{}: recursing into dependent {}/{}".format(
                        namespace, entity_type, entity_id, dep_type, dep_id))
                    self.__abort_entity_recursive__(namespace, dep_type, dep_id)

        # check current state and abort if non-terminal
        state_protocol = ClusterPassiveEntityStateProtocol(namespace, entity_type, entity_id)
        terminal_states = {EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP}

        if (state_protocol.has_any_state()):
            cur_state = state_protocol.get_registered_state_resolved()
            if (cur_state not in terminal_states):
                utils.info("ClusterAdmin: __abort_entity_recursive__: {}/{}/{}: state {} -> ABORTED".format(
                    namespace, entity_type, entity_id, cur_state))
                self.do_passive_entity_state_change(namespace, entity_type, entity_id, EntityState.ABORTED)
            else:
                utils.info("ClusterAdmin: __abort_entity_recursive__: {}/{}/{}: already in terminal state {}".format(
                    namespace, entity_type, entity_id, cur_state))
        else:
            # no state exists yet, write ABORTED directly
            utils.info("ClusterAdmin: __abort_entity_recursive__: {}/{}/{}: no state found, writing ABORTED".format(
                namespace, entity_type, entity_id))
            self.do_passive_entity_state_change(namespace, entity_type, entity_id, EntityState.ABORTED)

        # remove agent association so the agent is freed immediately
        agent_ids = self.cluster_handler.list_dirs(
            ClusterPaths.get_passive_entity_assigned_executors_by_child_type_path(
                namespace, entity_type, entity_id, EntityType.AGENT))
        for agent_id in agent_ids:
            utils.info("ClusterAdmin: __abort_entity_recursive__: {}/{}/{}: removing from agent {} execution tasks".format(
                namespace, entity_type, entity_id, agent_id))
            self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_execution_tasks_by_ns_id(
                namespace, EntityType.AGENT, agent_id, entity_type, entity_id), ignore_if_missing = True)

    def delete_swf(self, namespace, swf_id):
        """Force-delete a SWF and all its dependents (WFs -> JOBs -> TASKs -> BATCHes)."""
        id_path = ClusterPaths.get_passive_entity_id_path(namespace, EntityType.SWF, swf_id)
        if (self.cluster_handler.dir_exists(id_path) == False):
            utils.warn("ClusterAdmin: delete_swf: {}/{} not found in namespace {}".format(EntityType.SWF, swf_id, namespace))
            return

        utils.info("ClusterAdmin: delete_swf: deleting SWF {}/{}".format(namespace, swf_id))
        self.__force_delete_passive_entity__(namespace, EntityType.SWF, swf_id)
        utils.info("ClusterAdmin: delete_swf: SWF {}/{} deleted".format(namespace, swf_id))

    def delete_wf(self, namespace, wf_id):
        """Force-delete a WF and all its dependents (JOBs -> TASKs -> BATCHes)."""
        id_path = ClusterPaths.get_passive_entity_id_path(namespace, EntityType.WF, wf_id)
        if (self.cluster_handler.dir_exists(id_path) == False):
            utils.warn("ClusterAdmin: delete_wf: {}/{} not found in namespace {}".format(EntityType.WF, wf_id, namespace))
            return

        utils.info("ClusterAdmin: delete_wf: deleting WF {}/{}".format(namespace, wf_id))
        self.__force_delete_passive_entity__(namespace, EntityType.WF, wf_id)
        utils.info("ClusterAdmin: delete_wf: WF {}/{} deleted".format(namespace, wf_id))

# Cluster execution context: from_maps/read_df return HydraDF (lazy operation chains).
# Supports both inmemory (dict-based resolution) and filesystem/cluster execution.
# materialize() on HydraDF submits to cluster; SWFBuilder handles DAG orchestration.
class ClusterExecutorContext(ExecutorContext):
    def __init__(self, namespace, session_protocol, is_live = False, is_external = False, max_job_execution_time = 600, bucket_interval = 60, ticks_interval = None, duration = 300):
        self.namespace = namespace
        self.cluster_handler = ClusterPaths.get_cluster_handler()
        self.session_protocol = session_protocol
        self.is_live = is_live
        self.is_external = is_external
        self.max_job_execution_time = max_job_execution_time
        self.bucket_interval = bucket_interval
        self.ticks_interval = ticks_interval if (ticks_interval is not None) else bucket_interval
        self.duration = duration
        self.__store__ = {}  # inmemory store for input/output id resolution
        self.broadcast_context = BroadcastContext()

    def get_broadcast_context(self):
        """Return the BroadcastContext for defining broadcast variables."""
        return self.broadcast_context

    def read_df(self, input_id, params = [], prefix = None):
        """Return a HydraDF proxy for a single named input_id, resolved at execution time.

        Args:
            input_id: Name of the input to read (e.g. "tickets", "seed_input").
            params: List of param keys to enrich from seed_input via add_const(). Default [].
            prefix: Prefix for param column names ({prefix}:{key}). Defaults to WF name.
        """
        if (isinstance(input_id, list)):
            raise Exception("read_df: multiple input_ids are not supported yet. Got: {}".format(input_id))
        from omigo_hydra import cluster_df
        import json
        row = {cluster_arjun.OMIGO_ARJUN_PROXY_DATAFRAME_ID: input_id}
        if (len(params) > 0):
            row[cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS] = json.dumps(params)
        if (prefix is not None):
            row[cluster_arjun.OMIGO_ARJUN_PROXY_PARAMS_PREFIX] = prefix
        proxy = dataframe.from_maps([row])
        return proxy.extend_class(cluster_df.HydraDF, self)

    def from_maps(self, mps, accepted_cols = None, excluded_cols = None, url_encoded_cols = None):
        """Create HydraDF from maps for lazy workflow serialization."""
        from omigo_hydra import cluster_df
        xdf = dataframe.from_maps(mps, accepted_cols = accepted_cols,
            excluded_cols = excluded_cols, url_encoded_cols = url_encoded_cols)
        return xdf.extend_class(cluster_df.HydraDF, self)

    def new_with_cols(self, header_fields, data_fields = None):
        """Create DataFrame with explicit columns. Returns plain DataFrame (not HydraDF)."""
        if (data_fields is None):
            data_fields = []
        return dataframe.new_with_cols(header_fields, data_fields)

    def create_empty(self):
        """Create empty DataFrame. Returns plain DataFrame (not HydraDF)."""
        return dataframe.create_empty()

    def store_output(self, df, output_ids):
        """Store output in inmemory dict. Used by __materialize_local__ in SWFBuilder."""
        for output_id in output_ids:
            self.__store__[output_id] = df

    def read_output(self, output_id):
        """Read a previously stored output DataFrame by output_id."""
        if (output_id not in self.__store__):
            raise Exception("ClusterExecutorContext.read_output: '{}' not found. Available: {}".format(
                output_id, list(self.__store__.keys())))
        return self.__store__[output_id]

    def get_output_ids(self):
        """Return list of all stored output_ids."""
        return list(self.__store__.keys())

    def has_output(self, output_id):
        """Check if an output_id has been stored."""
        return output_id in self.__store__

    def resolve_input(self, namespace, entity_type, entity_id, input_id):
        """In cluster mode, read from the input folder on disk.
        Path resolution via ClusterPaths (global utility). Fully explicit — no mutable state."""
        path = ClusterPaths.get_passive_entity_data_input_default_file_path(
            namespace, entity_type, entity_id, input_id)
        return self.cluster_handler.read_df(path)

    # ---- cluster-specific methods (not in ExecutorContext spec) ----

    # build wf spec without submitting
    def build_wf_spec(self, jobs_operations, name = "", input_ids = [], output_ids = [], primary_input_id = None, start_ts = None, use_full_data = False, num_splits = 10, tags = {}, event_ts_col = None, params = {}, input_params_config = {}):
        wf_spec = self.__create_wf_spec__(jobs_operations, name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, num_splits = num_splits, tags = tags, event_ts_col = event_ts_col, params = params, input_params_config = input_params_config)
        wf_spec.primary_input_id = primary_input_id
        return wf_spec

    # execute multiple jobs as workflow
    def execute_jobs(self, xdf, jobs_operations, name = "", input_ids = [], output_ids = [], start_ts = None, use_full_data = False, num_splits = 10, tags = {}):
        wf_spec = self.__create_wf_spec__(jobs_operations, name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, num_splits = num_splits, tags = tags)
        xinputs = [xdf]
        wf_id = self.session_protocol.submit_wf(self.namespace, wf_spec, xinputs)
        return wf_id

    # execute multiple jobs as a single-WF super workflow
    def execute_jobs_as_swf(self, xdf, jobs_operations, name = "", input_ids = [], output_ids = [], start_ts = None, use_full_data = False, num_splits = 10, tags = {}):
        wf_spec = self.__create_wf_spec__(jobs_operations, name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, num_splits = num_splits, tags = tags)
        swf_spec = cluster_common_v2.ClusterSpecSWF.new([wf_spec], tags = tags)
        xinputs_per_wf = [[xdf]]
        swf_id = self.session_protocol.submit_swf(self.namespace, swf_spec, xinputs_per_wf)
        return swf_id

    # create job spec
    def __create_job_spec__(self, job_operation, num_splits):
        # initialize variables
        map_partitioner = None
        map_task = None
        reduce_partitioner = None
        reduce_task = None
        singleton_task = None
        extend_class_def = None

        # check if map is present
        if (job_operation.map_ops is not None and len(job_operation.map_ops) > 0):
            # create partitioner task
            map_partitioner = cluster_common_v2.ClusterSpecPartitionTask.new(num_splits)

            # create the map task
            map_task = cluster_common_v2.ClusterSpecMapTask.new(job_operation.map_ops)

        # check if reduce is present
        if (job_operation.reduce_op is not None):
            # create partitioner task
            reduce_partitioner = cluster_common_v2.ClusterSpecHashPartitionTask.new(job_operation.reduce_op.num_splits, job_operation.reduce_op.grouping_cols)

            # create reduce task
            reduce_task = cluster_common_v2.ClusterSpecReduceTask.new(job_operation.reduce_op)

        # check if reduce is present
        if (job_operation.singleton_op is not None):
            # create partitioner task
            singleton_partitioner = cluster_common_v2.ClusterSpecSingletonPartitionTask.new()

            # create reduce task
            singleton_task = cluster_common_v2.ClusterSpecSingletonTask.new(job_operation.singleton_op)

        # check if extend class is present
        if (job_operation.extend_class_op is not None):
            # create extend class reference
            extend_class_def = cluster_common_v2.ClusterSpecExtendClassDef.new(job_operation.extend_class_op)

        # check if checkpoint is present
        checkpoint_def = None
        if (job_operation.checkpoint_op is not None):
            checkpoint_def = cluster_common_v2.ClusterSpecCheckpointDef.new(job_operation.checkpoint_op)

        # create job_spec
        job_spec = cluster_common_v2.ClusterSpecJob.new(map_partitioner, map_task, reduce_partitioner, reduce_task, singleton_task, extend_class_def, checkpoint_def=checkpoint_def)

        # return
        return job_spec

    # create wf spec
    def __create_wf_spec__(self, jobs_operations, name = "", input_ids = [], output_ids = [], start_ts = None, use_full_data = False, num_splits = 10, tags = {}, event_ts_col = None, params = {}, input_params_config = {}):
        # jobs_specs
        jobs_specs = []

        # for each job, get map task, reduce task and add job
        for i in range(len(jobs_operations)):
            # get job
            job_operation = jobs_operations[i]

            # create job spec. TODO: pass output_id here
            job_spec = self.__create_job_spec__(job_operation, num_splits)

            # add to the list
            jobs_specs.append(job_spec)

        # create workflow
        wf_spec = cluster_common_v2.ClusterSpecWF.new(jobs_specs, is_live = self.is_live, is_external = self.is_external, max_job_execution_time = self.max_job_execution_time,
            bucket_interval = self.bucket_interval, ticks_interval = self.ticks_interval, start_ts = start_ts, use_full_data = use_full_data, duration = self.duration, input_ids = input_ids, output_ids = output_ids, name = name, tags = tags, event_ts_col = event_ts_col, params = params, input_params_config = input_params_config)

        # return
        return wf_spec

    def get_workflow_input_path(self, namespace, workflow_id, input_id):
        return cluster_common_v2.ClusterPaths.get_passive_entity_data_input_path(namespace, EntityType.WF, workflow_id, input_id)

    def get_workflow_output_path(self, namespace, workflow_id, output_id):
        return cluster_common_v2.ClusterPaths.get_passive_entity_data_output_path(namespace, EntityType.WF, workflow_id, output_id)

# RM Shard Protocol (V2)
class ClusterRMShardProtocol(ClusterEntityProtocol):
    """RM protocol for V2 sharded model. RM reads shard assignments from master."""

    def __init__(self, entity):
        super().__init__(entity)
        self.assigned_shards = set()
        self.assignment_check_ts = 0

    def initialize(self):
        super().initialize()

        # global incoming routing (Master discovers RMs via global incoming, not shard incoming)
        self.cluster_handler.create(ClusterPaths.get_entity_incoming(self.get_entity_type(), self.get_entity_id()))

    def get_my_assigned_shards(self):
        """Read which shards master has assigned to me."""
        cur_time = timefuncs.get_utctimestamp_sec()
        # Cache for 30 seconds
        if self.assignment_check_ts == 0 or abs(cur_time - self.assignment_check_ts) > 30:
            self.assigned_shards = set()
            for shard_id in range(cluster_common_v2.NUM_SHARDS):
                if self.cluster_handler.is_non_empty_dir(ClusterPaths.get_shard_assigned_rm(shard_id)):
                    rm_ref = cluster_common_v2.ClusterEntityRef.from_json(
                        self.cluster_handler.read_most_recent_json(ClusterPaths.get_shard_assigned_rm(shard_id)))
                    if rm_ref and rm_ref.entity_id == self.get_entity_id():
                        self.assigned_shards.add(shard_id)
            self.assignment_check_ts = cur_time
            if len(self.assigned_shards) > 0:
                utils.info("ClusterRMShardProtocol: {}: assigned to shards: {}".format(
                    self.get_entity_id(), sorted(self.assigned_shards)))
        return self.assigned_shards

    def has_assigned_shards(self):
        """Check if this RM has any assigned shards."""
        return len(self.get_my_assigned_shards()) > 0

    def monitor_shard_incoming(self):
        """Process incoming for assigned shards only."""
        assigned_shards = self.get_my_assigned_shards()
        if len(assigned_shards) == 0:
            return

        for shard_id in sorted(assigned_shards):
            self.__monitor_shard_incoming_for_shard__(shard_id)

    def __monitor_shard_incoming_for_shard__(self, shard_id):
        """Pick up entities from a specific shard's incoming queue and assign to supervisors."""
        for xentity_type in EntityType.get_all():
            # skip MASTER and RESOURCE_MANAGER -- Master handles those globally
            if (xentity_type in (EntityType.MASTER, EntityType.RESOURCE_MANAGER)):
                continue

            xis_passive = cluster_common_v2.is_passive_entity(xentity_type)

            # read this shard's incoming queue
            # passive entities are stored under namespace subdirectories; active entities are stored directly
            if (xis_passive):
                # passive entities: process per-namespace so supervisor paths use the namespace-aware layout
                for namespace in self.namespaces:
                    ns_incoming_path = "{}/{}".format(ClusterPaths.get_shard_incoming(shard_id, xentity_type), namespace)
                    xincoming_ids = self.cluster_handler.list_dirs(ns_incoming_path)

                    # read already-assigned within this namespace
                    ns_assigned_path = "{}/{}".format(ClusterPaths.get_entities_assigned_supervisor(xentity_type), namespace)
                    xassigned_ids = self.cluster_handler.list_dirs(ns_assigned_path)

                    # find unassigned
                    xnon_assigned_ids = list(set(xincoming_ids).difference(set(xassigned_ids)))

                    # also check for dead-supervisor reassignment
                    for xassigned_id in set(xincoming_ids).intersection(set(xassigned_ids)):
                        supervisor_path = ClusterPaths.get_passive_entity_assigned_supervisor_path(namespace, xentity_type, xassigned_id)
                        if (self.cluster_handler.dir_exists(supervisor_path) == False):
                            continue
                        xsupervisor_ref = cluster_common_v2.ClusterEntityRef.from_json(
                            self.cluster_handler.read_most_recent_json(supervisor_path))
                        if (xsupervisor_ref is not None):
                            if (ClusterActiveEntityStateProtocol(xsupervisor_ref.entity_type, xsupervisor_ref.entity_id).is_alive() == False):
                                xnon_assigned_ids.append(xassigned_id)

                    xnon_assigned_ids = sorted(xnon_assigned_ids)

                    for xnon_assigned_id in xnon_assigned_ids:
                        self.__assign_entity_to_shard_supervisor__(xentity_type, xnon_assigned_id, shard_id)
            else:
                xincoming_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_shard_incoming(shard_id, xentity_type))

                # read already-assigned
                xassigned_ids = self.cluster_handler.list_dirs(
                    ClusterPaths.get_entities_assigned_supervisor(xentity_type))

                # find unassigned
                xnon_assigned_ids = list(set(xincoming_ids).difference(set(xassigned_ids)))

                # also check for dead-supervisor reassignment
                for xassigned_id in set(xincoming_ids).intersection(set(xassigned_ids)):
                    # skip entities whose assigned-supervisor details have been cleaned up (race with cleanup)
                    if (self.cluster_handler.dir_exists(ClusterPaths.get_entity_assigned_supervisor(xentity_type, xassigned_id)) == False):
                        continue
                    xsupervisor_ref = cluster_common_v2.ClusterEntityRef.from_json(
                        self.cluster_handler.read_most_recent_json(
                            ClusterPaths.get_entity_assigned_supervisor(xentity_type, xassigned_id)))
                    if (xsupervisor_ref is not None):
                        if (ClusterActiveEntityStateProtocol(xsupervisor_ref.entity_type, xsupervisor_ref.entity_id).is_alive() == False):
                            xnon_assigned_ids.append(xassigned_id)

                xnon_assigned_ids = sorted(xnon_assigned_ids)

                for xnon_assigned_id in xnon_assigned_ids:
                    self.__assign_entity_to_shard_supervisor__(xentity_type, xnon_assigned_id, shard_id)

    def __assign_entity_to_shard_supervisor__(self, xentity_type, xentity_id, shard_id):
        """Assign entity to a supervisor within the given shard."""
        xis_passive = cluster_common_v2.is_passive_entity(xentity_type)

        # resolve namespace for passive entities by scanning registered namespaces
        namespace = None
        if (xis_passive):
            for ns in self.namespaces:
                if (self.cluster_handler.dir_exists(ClusterPaths.get_passive_entity_details_path(ns, xentity_type, xentity_id))):
                    namespace = ns
                    break
            if (namespace is None):
                utils.info("ClusterRMShardProtocol: {}: passive entity {}/{} not found in any namespace, removing stale shard incoming entry".format(
                    self.get_entity_id(), xentity_type, xentity_id))
                for ns in self.namespaces:
                    self.cluster_handler.remove_dir_recursive(
                        ClusterPaths.get_passive_shard_entity_incoming(ns, shard_id, xentity_type, xentity_id), ignore_if_missing = True)
                return

        # skip entities whose details have already been cleaned up and remove stale shard incoming entry
        if (xis_passive):
            xentity_details_path = ClusterPaths.get_passive_entity_details_path(namespace, xentity_type, xentity_id)
        else:
            xentity_details_path = ClusterPaths.get_entity(xentity_type, xentity_id)

        if (self.cluster_handler.dir_exists(xentity_details_path) == False):
            utils.info("ClusterRMShardProtocol: {}: entity {}/{} details already deleted, removing stale shard incoming entry".format(
                self.get_entity_id(), xentity_type, xentity_id))
            if (xis_passive):
                self.cluster_handler.remove_dir_recursive(
                    ClusterPaths.get_passive_shard_entity_incoming(namespace, shard_id, xentity_type, xentity_id), ignore_if_missing = True)
            else:
                self.cluster_handler.remove_dir_recursive(
                    ClusterPaths.get_shard_entity_incoming(shard_id, xentity_type, xentity_id), ignore_if_missing = True)
            return

        xentity = cluster_common_v2.ClusterEntity.from_json(
            self.cluster_handler.read_most_recent_json(xentity_details_path))
        if (xentity is None):
            return

        xentity_is_active = cluster_common_v2.EntityIsActiveMap[xentity_type]
        xsupervisor_entity_type = cluster_common_v2.EntitySupervisorMap[xentity_type]

        # if supervisor type is RESOURCE_MANAGER, use this RM (the shard winner)
        if (xsupervisor_entity_type == EntityType.RESOURCE_MANAGER):
            xsupervisor_ref = cluster_common_v2.ClusterEntityRef.new(
                EntityType.RESOURCE_MANAGER, self.get_entity_id())
        else:
            # find alive supervisors of the correct type within this shard
            xsupervisor_ref = self.__select_shard_supervisor__(xsupervisor_entity_type, shard_id)

        if (xsupervisor_ref is None):
            utils.info("ClusterRMShardProtocol: {}: no supervisor for {}/{} in shard {}".format(
                self.get_entity_id(), xentity_type, xentity_id, shard_id))
            return

        # write assignment — use namespace-aware path for passive entities
        if (xis_passive):
            supervisor_path = ClusterPaths.get_passive_entity_assigned_supervisor_path(namespace, xentity_type, xentity_id)
        else:
            supervisor_path = ClusterPaths.get_entity_assigned_supervisor(xentity_type, xentity_id)
        self.cluster_handler.create(supervisor_path)
        self.cluster_handler.update_dynamic_seq_update(supervisor_path, xsupervisor_ref)

        # add under supervisor's children
        if (xentity_is_active):
            self.cluster_handler.create(ClusterPaths.get_entity_active_children_by_id(
                xsupervisor_ref.entity_type, xsupervisor_ref.entity_id, xentity_type, xentity_id))
        else:
            # register under namespace-scoped passive children so supervisor's monitor_passive_children finds it
            self.cluster_handler.create(ClusterPaths.get_entity_passive_children_by_ns_id(
                namespace, xsupervisor_ref.entity_type, xsupervisor_ref.entity_id, xentity_type, xentity_id))

    def __select_shard_supervisor__(self, xsupervisor_entity_type, shard_id):
        """Find alive supervisor of given type. Prefers the target shard but falls back to any shard."""
        all_ids = sorted(self.cluster_handler.list_dirs(ClusterPaths.get_entities_ids(xsupervisor_entity_type)))

        # filter to alive first (needed for both shard-local and fallback)
        all_alive_ids = [eid for eid in all_ids
                         if ClusterActiveEntityStateProtocol(xsupervisor_entity_type, eid).is_alive()]

        if not all_alive_ids:
            return None

        # prefer supervisors in the target shard
        shard_alive_ids = [eid for eid in all_alive_ids
                           if cluster_common_v2.get_shard_for_entity(eid) == shard_id]

        # fall back to any alive supervisor if none in target shard
        alive_ids = shard_alive_ids if shard_alive_ids else all_alive_ids

        # filter by capacity
        free_ids = [eid for eid in alive_ids if self.__has_free_capacity__(xsupervisor_entity_type, eid)]
        candidates = free_ids if free_ids else alive_ids

        return cluster_common_v2.ClusterEntityRef.new(xsupervisor_entity_type, candidates[0])

if __name__ == "__main__":
    freeze_support()
