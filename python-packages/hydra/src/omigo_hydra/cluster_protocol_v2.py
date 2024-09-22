import time
import os
import random
import math
import threading
from omigo_core import tsv, utils, funclib, etl
from omigo_hydra import cluster_data, cluster_class_reflection, cluster_tsv, cluster_common_v2, cluster_arjun
from omigo_hydra.cluster_common_v2 import EntityType, EntityState, ClusterTaskType, ClusterIds, ClusterPaths

class ClusterHeartbeatProtocol:
    MAX_HEARTBEAT_WAIT = 30
    HEARTBEAT_SEC = 5
    MAX_HEARTBEAT_CACHE = 10

    def __init__(self, entity):
        self.entity = entity
        self.last_heartbeat_cache = None
        self._lock = threading.Lock()
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def get_entity(self):
        return self.entity

    def get_entity_id(self):
        return self.get_entity().entity_id

    def get_entity_type(self):
        return self.get_entity().entity_type

    def update_cache_ts(self, ts):
        with self._lock:
            self.last_heartbeat_cache = ts

    def get_cache_ts(self):
        with self._lock:
            return self.last_heartbeat_cache

    def start_heartbeat_thread(self):
        # sleep for few seconds
        # utils.info("ClusterHeartbeatProtocol {}: start_heartbeat_thread: sleeping for 5 seconds before starting thread".format(self.get_entity_id()))
        # time.sleep(5)

        # create the thread to update the heartbeat
        thread = threading.Thread(target = self.update_heartbeat, daemon = True)
        thread.start()

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
        cur_time = funclib.get_utctimestamp_sec()

        # update cache if needed
        if (self.get_cache_ts() is None or cur_time - self.get_cache_ts() > ClusterHeartbeatProtocol.MAX_HEARTBEAT_CACHE):
            # read the entity heartbeat. TODO: dont update the cache here
            heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id())))
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
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id())) == False):
            utils.warn("ClusterHeartbeatProtocol: __is_alive__: entity missing: {}".format(self.get_entity_id()))
            return False

        # read the entity value
        heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id())))
        heartbeat_server_timestamp = self.cluster_handler.read_most_recent_server_timestamp(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id()))

        # check for None
        if (heartbeat_server_timestamp is None):
            utils.warn("ClusterHeartbeatProtocol: __is_alive__: entity details missing: {}".format(self.get_entity_id()))
            return False

        # check the diff
        clock_skew = heartbeat.ts - heartbeat_server_timestamp 
        if (abs(clock_skew) > ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT):
            raise Exception("ClusterHeartbeatProtocol: __is_alive__: {}, clock skew detected. entity ts: {}, server ts: {}, skew: {}. Raising exception for debugging".format(
                self.get_entity_id(), heartbeat.ts, heartbeat_server_timestamp)) 

        # check if the timestamp has already expired
        cur_time = funclib.get_utctimestamp_sec()
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
            utils.warn("ClusterHeartbeatProtocol: update_heartbeat: {}, heartbeat missing or too old. Exiting".format(self.get_entity_id()))
            return False

        # check if the entity is part of cleanup already. TODO: consider all possible states
        xentity_state_protocol = ClusterEntityStateProtocol(self.get_entity_type(), self.get_entity_id())
        if (xentity_state_protocol.has_cleanup_state() or xentity_state_protocol.has_aborted_state()):
            utils.info("ClusterHeartbeatProtocol: update_heartbeat: {}, entity in aborted or cleanup. Exiting.".format(self.get_entity_id()))
            return False

        # construct new heartbeat 
        ts = funclib.get_utctimestamp_sec()
        heartbeat = cluster_common_v2.ClusterHearbeat.new(ts, self.get_entity().lease)

        # 1. update on cluster
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id()), heartbeat, ignore_logging = True)

        # 2. update local cache 
        self.update_cache_ts(ts)

        # return
        return True

# protocol to check and resolve current persisted state
class ClusterEntityStateProtocol:
    def __init__(self, entity_type, entity_id):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def __get_all_states__(self):
        # create map of states
        result = {}

        # get all states
        for state in EntityState.get_all():
            result[state] = self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, state, self.entity_id))
        
        # return
        return result

    def is_finished(self):
        # find all states
        states = self.__get_all_states__()
        finished_states = [EntityState.COMPLETED, EntityState.FAILED, EntityState.ABORTED, EntityState.CLEANUP]

        # take intersection
        common_states = set(finished_states).intersection(set(states))

        # if any of finished states present return True
        if (len(common_states) > 0):
            return True
        else:
            return False

    def has_heartbeat(self):
        # check if they are alive or not
        heartbeat = cluster_common_v2.ClusterHearbeat.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity_heartbeat(self.entity_type, self.entity_id)))

        # check if entity is alive or not
        return heartbeat.is_alive()

    def has_dead_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.DEAD, self.entity_id))):
            return True
        else:
            return False

    def has_reassigned_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.REASSIGNED, self.entity_id))):
            return True
        else:
            return False

    def has_aborted_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.ABORTED, self.entity_id))):
            return True
        else:
            return False

    def has_completed_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.COMPLETED, self.entity_id))):
            return True
        else:
            return False

    def has_failed_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.FAILED, self.entity_id))):
            return True
        else:
            return False

    def has_cleanup_state(self):
        # check for presence in dead
        if (self.cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.CLEANUP, self.entity_id))):
            return True
        else:
            return False

    def get_registered_state_resolved(self):
        # get all states values
        states = self.__get_all_states__()

        # priority order
        priority_order = [EntityState.CLEANUP, EntityState.ABORTED, EntityState.REASSIGNED, EntityState.DEAD, EntityState.COMPLETED, EntityState.FAILED, EntityState.ALIVE, EntityState.CREATED]

        # check in the order
        for state in priority_order:
            if (states[state] == True):
                return state

        # invalid
        raise Exception("ClusterEntityStateProtocol: get_registered_state_resolved: invalid: {}, states: {}".format(self.entity_id, states))

    def is_alive(self):
        # get registered resolved state
        cur_registered_state = self.get_registered_state_resolved()

        # check if it is alive
        if (cur_registered_state != EntityState.ALIVE):
            return False

        # check if it is active entity and doesnt have heartbeat
        if (cluster_common_v2.EntityIsActiveMap[self.entity_type] == True and self.has_heartbeat() == False):
            return False

        # return
        return True 

    def get_state_update_time(self, state):
        # get the timestamp at which the state was determined. TODO: add wait
        update_time = cluster_common_v2.ClusterUpdateTime.from_json(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entities_state_by_id(self.entity_type, state, self.entity_id)))
      
        # return 
        return update_time

# main protocol class for entity operations
class ClusterEntityProtocol:
    # thresholds for wait time before moving to cleanup state
    MAX_ACTIVE_ENTITY_FINISHED_THRESHOLD = 3600*10
    MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD = 86400*10

    def __init__(self, entity):
        self.entity = entity
        self.cluster_handler = ClusterPaths.get_cluster_handler()
        self.heartbeat_protocol = ClusterHeartbeatProtocol(self.entity)
        self._lock = threading.Lock()
        self.local_cluster_handler = ClusterPaths.get_local_cluster_handler() 

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
        update_time = cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec())
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.CREATED, self.get_entity_id()), update_time)

        # create active children
        self.cluster_handler.create(ClusterPaths.get_entity_active_children(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityActiveChildrenMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_active_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # create passive children
        self.cluster_handler.create(ClusterPaths.get_entity_passive_children(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityPassiveChildrenMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_passive_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # create dependents 
        self.cluster_handler.create(ClusterPaths.get_entity_dependents(self.get_entity_type(), self.get_entity_id()))
        for xchild_type in cluster_common_v2.EntityDependentsMap[self.get_entity_type()]:
            self.cluster_handler.create(ClusterPaths.get_entity_dependents_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_type))

        # initialize heartbeat location
        self.cluster_handler.create(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id()))

        # create initial heartbeat
        heartbeat = cluster_common_v2.ClusterHearbeat.new(funclib.get_utctimestamp_sec(), self.get_entity().lease)
        self.cluster_handler.update_dynamic_value(ClusterPaths.get_entity_heartbeat(self.get_entity_type(), self.get_entity_id()), heartbeat)

        # if this is active entity, start heartbeat thread
        if (cluster_common_v2.EntityIsActiveMap[self.get_entity_type()] == True):
            utils.info("ClusterEntityProtocol: {} initialize: active entity: starting heartbeat thread".format(self.get_entity_id()))
            self.heartbeat_protocol.start_heartbeat_thread()
        else:
            utils.debug("ClusterEntityProtocol: {} initialize: passive entity: not starting heartbeat thread".format(self.get_entity_id()))

        # check if this entity is one of executors
        if (self.get_entity_type() in cluster_common_v2.EntityExecutorTypes):
            # create base entry
            self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks(self.get_entity_type(), self.get_entity_id()))

            # create entry for assigned executors
            for xchild_entity_type in cluster_common_v2.EntityExecutionTaskTypes:
                self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

        # check if this entity is one of exection task types
        if (self.get_entity_type() in cluster_common_v2.EntityExecutionTaskTypes):
            # create base entry
            self.cluster_handler.create(ClusterPaths.get_entity_assigned_executors(self.get_entity_type(), self.get_entity_id()))

            # create entry for assigned executors
            for xchild_entity_type in cluster_common_v2.EntityExecutorTypes:
                self.cluster_handler.create(ClusterPaths.get_entity_assigned_executors_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

        # create entry into incoming
        self.cluster_handler.create(ClusterPaths.get_entity_incoming(self.get_entity_type(), self.get_entity_id()))

    # the frequency to monitor active and passive children needs to be different. TODO: find that logic at sometime
    # for active entities, the clock synchronization and validation is needed. for passive, only when job starts, they are set to alive
    # completed, failed states are not for active entities
    def monitor_active_children(self):
        # take current time
        cur_ts = funclib.get_utctimestamp_sec()

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
                xentity_state_protocol = ClusterEntityStateProtocol(xchild_entity_type, xchild_entity_id)
                cur_registered_state = xentity_state_protocol.get_registered_state_resolved()
                cur_state_update_time = xentity_state_protocol.get_state_update_time(cur_registered_state)
                cur_time_diff = cur_ts - cur_state_update_time.ts

                # do a switch case and call the methods for each possible transition
                if (cur_registered_state == EntityState.CREATED):
                    # do verification, clock synchronization and move to ALIVE. TODO this state transition is stuck on master
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ALIVE)
                elif (cur_registered_state == EntityState.ALIVE):
                    # check for heartbeat
                    if (xentity_state_protocol.has_heartbeat() == False):
                        self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.DEAD)
                    else:
                        # do monitoring, record state
                        utils.info("monitor_active_children: {} : active child entity {} is ALIVE".format(self.get_entity_id(), xchild_entity_id))
                elif (cur_registered_state == EntityState.DEAD):
                    # check if has exceeded the threshold for doing cleanup
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.REASSIGNED)
                elif (cur_registered_state == EntityState.REASSIGNED):
                    # check if reassigned is finished or not
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.ABORTED):
                    # check if aborted is finished or not
                    self.do_active_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.CLEANUP):
                    # check if exceeded has threshold for doing cleanup
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_ACTIVE_ENTITY_FINISHED_THRESHOLD):
                        self.do_cleanup(xchild_entity)
                else:
                    raise Exception("ClusterEntityProtocol: monitor_active_children: invalid state: {}".format(cur_registered_state))

    # TODO: there needs to be a method in between to check if the state change is needed
    # state update time is not useful for doing final state change
    def do_active_child_state_change(self, xchild_entity, cur_state, state_update_time, target_state):
        # define the placeholders for all possible state transitions
        if (cur_state == EntityState.CREATED and target_state == EntityState.ALIVE):
            # CREATED to ALIVE
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
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec())

        # take path
        entity_state_path = ClusterPaths.get_entities_state_by_id(xchild_entity.entity_type, state, xchild_entity.entity_id)

        # update timestamp and the value
        if (self.cluster_handler.dir_exists(entity_state_path) == False):
            self.cluster_handler.create(entity_state_path)
            self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)
        else:
            utils.warn("ClusterEntityProtocol: __do_child_entity_state_change__: {}, get_entities_state_by_id: {} exists.".format(self.get_entity_id(), xchild_entity.entity_id))
            if (self.cluster_handler.is_non_empty_dir(entity_state_path) == True):
                raise Exception("ClusterEntityProtocol: __do_child_entity_state_change__: {}: state transition not allowed as update timestamp already exists: {}".format(self.get_entity_id(), xchild_entity.entity_id))
            else:
                self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

    def do_active_child_state_change_created_to_alive(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.ALIVE)

    def do_active_child_state_change_alive_to_dead(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.DEAD)

    def do_active_child_state_change_dead_to_reassigned(self, xchild_entity):
        self.__do_child_entity_state_change__(xchild_entity, EntityState.REASSIGNED)

    def do_active_child_state_change_created_to_aborted(self, xchild_entity):
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

    def monitor_passive_children(self):
        # take current time
        cur_ts = funclib.get_utctimestamp_sec()

        # at this point, the current state is as per in the cluster
        for xchild_entity_type in cluster_common_v2.EntityPassiveChildrenMap[self.get_entity_type()]:
            # debug
            utils.debug("ClusterEntityProtocol: {} monitor_passive_children: xchild_entity_type: {}".format(self.get_entity_id(), xchild_entity_type))

            # get entity ids
            xchild_entity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_passive_children_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

            # sort the entities
            xchild_entity_ids = sorted(xchild_entity_ids)

            # iterate over each xchild_entity_ids
            for xchild_entity_id in xchild_entity_ids:
                # get the entity
                xchild_entity = cluster_common_v2.deserialize_cluster_entity(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(xchild_entity_type, xchild_entity_id)))

                # get effective state
                xentity_state_protocol = ClusterEntityStateProtocol(xchild_entity_type, xchild_entity_id)
                cur_registered_state = xentity_state_protocol.get_registered_state_resolved()
                cur_state_update_time = xentity_state_protocol.get_state_update_time(cur_registered_state)
                cur_time_diff = cur_ts - cur_state_update_time.ts

                # do a switch case and call the methods for each possible transition
                if (cur_registered_state == EntityState.CREATED):
                    # do verification, clock synchronization and move to ALIVE
                    self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.ALIVE)
                elif (cur_registered_state == EntityState.ALIVE):
                    # do monitoring, record state. Passive children dont have heartbeat
                    utils.info("ClusterEntityProtocol: {} monitor_passive_children: passive child entity {} is ALIVE".format(self.get_entity_id(), xchild_entity_id))
                    self.do_execute_passive_child(xchild_entity)
                elif (cur_registered_state == EntityState.COMPLETED):
                    # after a wait period, move to cleanup
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                        self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.FAILED):
                    # after a wait period, move to cleanup. no retries
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                        self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.ABORTED):
                    # aborted entity move to cleanup
                    if (cur_time_diff >= ClusterEntityProtocol.MAX_PASSIVE_ENTITY_FINISHED_THRESHOLD):
                        self.do_passive_child_state_change(xchild_entity, cur_registered_state, cur_state_update_time, EntityState.CLEANUP)
                elif (cur_registered_state == EntityState.CLEANUP):
                    # check if exceeded has threshold for doing cleanup
                    self.do_cleanup(xchild_entity)
                else:
                    raise Exception("ClusterEntityProtocol: {} monitor_passive_children: invalid state: {}".format(self.get_entity_id(), cur_registered_state))

    def do_passive_child_state_change(self, xchild_entity, cur_state, state_update_time, target_state):
        # define the placeholders for all possible state transitions
        if (cur_state == EntityState.CREATED and target_state == EntityState.ALIVE):
            # CREATED to ALIVE
            self.do_passive_child_state_change_created_to_alive(xchild_entity)
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
        else:
            raise Exception("ClusterEntityProtocol: {}: do_passive_child_state_change: invalid state change: {}, {}, {}".format(self.get_entity_id(), xchild_entity.entity_id, cur_state, target_state))

    def do_passive_child_state_change_created_to_alive(self, xchild_entity):
        # check if there are enough resources - workers, agents to assign. if the resources are there, then 
        # assign and move to alive state
        if (cluster_common_v2.EntityIsActiveMap[xchild_entity.entity_type] == False):
            # check available resources
            if (xchild_entity.entity_type == EntityType.WF):
                # selected agent id
                selected_agent_id = None

                # if it is live, then assign agent else execute each job
                if (xchild_entity.entity_spec.is_live == True):
                    alive_agent_ids = sorted(self.__get_alive_entity_ids__(EntityType.AGENT))
                    available_agent_ids = sorted(self.__get_available_entity_ids__(EntityType.AGENT, alive_agent_ids))

                    # check if there are any available agents
                    if (len(available_agent_ids) > 0):
                        # pick one randomly
                        # random.shuffle(alive_agent_ids)
                        selected_agent_id = available_agent_ids[-1]

                        # assign agent id to the wf
                        self.cluster_handler.create(ClusterPaths.get_entity_assigned_executors_by_id(xchild_entity.entity_type, xchild_entity.entity_id, EntityType.AGENT, selected_agent_id))

                        # create entry into agent id for this wf
                        self.cluster_handler.create(ClusterPaths.get_entity_assigned_execution_tasks_by_id(EntityType.AGENT, selected_agent_id, xchild_entity.entity_type, xchild_entity.entity_id))
               
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

    # TODO: move this to separate class
    # this cleanup is only run by the supervisor which has references in active, passive and dependents. These references need to be deleted
    def do_cleanup(self, xchild_entity):
        # delete from active
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_active_children_by_id(self.get_entity_type(), self.get_entity_id(),
            xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # delete from passive
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_passive_children_by_id(self.get_entity_type(), self.get_entity_id(),
            xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # delete from dependents
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_dependents_by_id(self.get_entity_type(), self.get_entity_id(),
            xchild_entity.entity_type, xchild_entity.entity_id), ignore_if_missing = True)

        # run cleanup that is common for all active entities in failsafe mode
        entity_cleanup_protocol = ClusterEntityCleanupProtocol(xchild_entity.entity_type, xchild_entity.entity_id)
        entity_cleanup_protocol.cleanup()

    def is_alive_cached(self):
        return self.heartbeat_protocol.is_alive_cached()

    def monitor_execution_tasks(self):
        # at this point, the current state is as per in the cluster
        for xchild_entity_type in cluster_common_v2.EntityExecutionTaskTypes:
            # debug
            utils.debug("ClusterEntityProtocol: {} monitor_execution_tasks: xchild_entity_type: {}".format(self.get_entity_id(), xchild_entity_type))

            # get entity ids
            xchild_entity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_assigned_execution_tasks_by_child_type(self.get_entity_type(), self.get_entity_id(), xchild_entity_type))

            # sort the entities
            xchild_entity_ids = sorted(xchild_entity_ids)

            # iterate over each xchild_entity_ids
            for xchild_entity_id in xchild_entity_ids:
                # get the entity
                xchild_entity = cluster_common_v2.deserialize_cluster_entity(self.cluster_handler.read_most_recent_json(ClusterPaths.get_entity(xchild_entity_type, xchild_entity_id)))

                # get effective state
                xentity_state_protocol = ClusterEntityStateProtocol(xchild_entity_type, xchild_entity_id)
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
                elif (cur_registered_state == EntityState.CLEANUP):
                    pass
                else:
                    raise Exception("ClusterEntityProtocol: {} monitor_execution_tasks: invalid state: {}".format(self.get_entity_id(), cur_registered_state))

    def do_execute_passive_child(self, xchild_entity):
        raise Exception("Derived class should implement this")

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
            xentity_state_protocol = ClusterEntityStateProtocol(xentity_type, xentity_id)

            # check if the entity is alive
            if (xentity_state_protocol.is_alive()):
                result.append(xentity_id)

        # return
        return result

    # TODO: this is very inefficient
    def __get_available_entity_ids__(self, xentity_type, xentity_ids):
        # create result
        result = []

        # for each entity, check how many have nothing running
        for xentity_id in xentity_ids:
            # find the capacity of each entity. TODO: make this part of entity class
            total_capacity = cluster_common_v2.EntityCapacityMap[xentity_type]
            used_capacity = 0

            # create flag
            is_available = True 

            # check for each ExectionTask type
            for execution_task_type in cluster_common_v2.EntityExecutionTaskTypes:
                # find all assigned tasks
                assigned_task_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entity_assigned_execution_tasks_by_child_type(xentity_type, xentity_id, execution_task_type))

                # for each assigned task, check if the task is finished
                for assigned_task_id in assigned_task_ids:
                    xentity_state_protocol = ClusterEntityStateProtocol(execution_task_type, assigned_task_id)
                    # check if the task is finished or not
                    if (xentity_state_protocol.is_finished() == False):
                        is_available = False

                # if any task is not finished, return False
                if (is_available == False):
                    used_capacity = used_capacity + 1

            # check if there is free capacity
            if (used_capacity < total_capacity):
                result.append(xentity_id)

        # return
        return result
 
# separate protocol class for cleanup
class ClusterEntityCleanupProtocol:
    def __init__(self, entity_type, entity_id):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def cleanup(self):
        # delete entity heartbeat
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_heartbeat(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete active, passive and dependents
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_active_children(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_passive_children(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_dependents(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete from incoming
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_incoming(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete from assigned
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_supervisor(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_executors(self.entity_type, self.entity_id), ignore_if_missing = True)
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_assigned_execution_tasks(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete data 
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_data(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entity
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity(self.entity_type, self.entity_id), ignore_if_missing = True)

        # delete entities state except CLEANUP
        for state in EntityState.get_all():
            if (state != EntityState.CLEANUP):
                self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entities_state_by_id(self.entity_type, state, self.entity_id), ignore_if_missing = True)

        # delete CLEANUP
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entities_state_by_id(self.entity_type, EntityState.CLEANUP, self.entity_id), ignore_if_missing = True)

        # delete entity_id at the end
        self.cluster_handler.remove_dir_recursive(ClusterPaths.get_entity_id(self.entity_type, self.entity_id), ignore_if_missing = True)

# Master Protocol
class ClusterMasterProtocol(ClusterEntityProtocol):
    MAX_CUR_MASTER_CACHE = 30
    def __init__(self, entity):
        super().__init__(entity)
        self.is_cur_master_cache = False
        self.cur_master_cache_ts = 0
        self._lock = threading.Lock()
        self.election_protocol = ClusterMasterElectionProtocol(self.get_entity_id())

    # initialize
    def initialize(self):
        # call base class initialization first
        super().initialize()

        # Only MASTER move to ALIVE state by default
        if (self.get_entity_type() == EntityType.MASTER):
            utils.info("ClusterMasterProtocol: {} initialize: Master moved to ALIVE as part of initialization".format(self.get_entity_id()))
            self.cluster_handler.create(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.ALIVE, self.get_entity_id()))
            self.cluster_handler.update_dynamic_value(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.ALIVE, self.get_entity_id()),
                cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec()))

    # check if this is the current master. TODO: Why there are 2 methods in Election and here
    def is_current_master(self):
       # use lock for avoiding race condition
       with self._lock:
            # find time difference
            cur_time = funclib.get_utctimestamp_sec()
            time_diff = abs(cur_time - self.cur_master_cache_ts)

            # update cache if needed
            if (self.cur_master_cache_ts == 0 or time_diff > ClusterMasterProtocol.MAX_CUR_MASTER_CACHE):
                self.is_cur_master_cache = self.election_protocol.is_current_master()
                self.cur_master_cache_ts = cur_time

            # return
            return self.is_cur_master_cache

    def refresh_master_cache(self):
       # use lock for avoiding race condition
       with self._lock:
           self.is_cur_master_cache = self.election_protocol.is_current_master()
           self.cur_master_cache_ts = funclib.get_utctimestamp_sec()

    # monitor the incoming entities for assignment to their supervisors
    def monitor_incoming_for_supervisor(self):
        # check if this is current master or not
        if (self.is_current_master() == False):
            utils.info("ClusterMasterProtocol: {}: monitor_incoming_for_supervisor: not the current master. Returning".format(self.get_entity_id()))
            return

        # iterate over all entity types
        for xentity_type in EntityType.get_all():
            # get all incoming entity ids
            xincoming_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_incoming(xentity_type))

            # get all assigned supervisor entity ids
            xassigned_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_assigned_supervisor(xentity_type))

            # find which ones are not assigned yet
            xnon_assigned_ids = list(set(xincoming_ids).difference(set(xassigned_ids)))

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
                if (ClusterEntityStateProtocol(xsupervisor_entity_type, xsupervisor_id).is_alive()):
                    xalive_entity_ids.append(xsupervisor_id)

            # debug
            utils.info("__select_assigned_supervisor_entity__: xentity_type: {}, xsupervisor_ids: {}, xalive_entity_ids: {}".format(xentity_type, xsupervisor_ids, xalive_entity_ids))

            # check if there are any
            if (len(xalive_entity_ids) > 0):
                utils.info("ClusterMasterProtocol: {}: __select_assigned_entity__: xentity_type: {}, supervisor candidates: xalive_entity_ids: {}".format(self.get_entity_id(), xentity_type, xalive_entity_ids))
                # TODO: workaround to not select master if workers are available
                if (len(xalive_entity_ids) > 1):
                    xalive_entity_non_master_node_ids = list(filter(lambda t: self.__is_same_entity_node_as_current_master__(t) == False, xalive_entity_ids))
                    xalive_entity_non_master_node_ids = utils.random_shuffle(xalive_entity_non_master_node_ids, seed = funclib.get_utctimestamp_sec())
                    return cluster_common_v2.ClusterEntityRef.new(xsupervisor_entity_type, xalive_entity_non_master_node_ids[0])
                else:
                    return cluster_common_v2.ClusterEntityRef.new(xsupervisor_entity_type, xalive_entity_ids[0])
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
# Resource Manager Protocol            
class ClusterResourceManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# SWF Manager Protocol
class ClusterSWFManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# WF Manager Protocol
class ClusterWFManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    # Override
    def do_execute_passive_child(self, xchild_entity):
        utils.warn_once("TODO: ClusterWFManagerProtocol: do_execute_passive_child: the semantics are not clear")

        # check the type of the child
        xchild_entity_type = xchild_entity.entity_type

        # switch case for different kinds of passive children
        if (xchild_entity_type == EntityType.WF):
            self.__do_execute_passive_wf__(xchild_entity)

    def __do_execute_passive_wf__(self, wf_entity):
        utils.info("ClusterWFManagerProtocol {}: __do_execute_passive_wf__: {}".format(self.get_entity_id(), wf_entity.entity_id))
        wf_protocol = ClusterWFProtocol(wf_entity)
        wf_protocol.execute()

# Job Manager Protocol
class ClusterJobManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Task Manager Protocol
class ClusterTaskManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# SWF Protocol
class ClusterSWFProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# WF Protocol
class ClusterWFProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    # commands to execute the workflow
    def execute(self):
        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # TODO
        utils.warn_once("ClusterWFProtocol: execute: this is doing state management locally. Need to use ClusterEntityStateProtocol")

        # check if it is remote or not
        if (wf_spec.is_remote == True):
            self.execute_remote()
        else:
            # check if it is live or not
            if (wf_spec.is_live == False):
                self.execute_static()
            else:
                self.execute_live()

    def __update_wf_state__(self, cluster_handler_ref, entity_state):
        # TODO
        utils.warn_once("__update_wf_state__: this should be replaced with ClusterEntityStateProtocol")
 
        # update the state
        cluster_handler_ref.create(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), entity_state, self.get_entity_id()))
        cluster_handler_ref.update_dynamic_value(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), entity_state, self.get_entity_id()),
            cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec()))

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
            # read input
            input_ids = wf_spec.input_ids
            file_index = 0
            
            # validation for multiple inputs or outputs. TODO
            if (len(wf_spec.input_ids) > 1 or len(wf_spec.output_ids) > 1):
                raise Exception("ClusterWFProtocol: {}: multiple inputs and outputs not supported: {}, {}".format(self.get_entity_id(), input_ids, output_ids))

            # read input
            xinput = self.cluster_handler.read_tsv(ClusterPaths.get_entity_data_input_file(wf_entity.entity_type, wf_entity.entity_id, input_ids[0], file_index))
            utils.info("ClusterWFProtocol: {}: execute_static: num rows: {}, num_cols: {}".format(self.get_entity_id(), xinput.num_rows(), xinput.num_cols()))

            # resolve start_ts. TODO: This needs to fall on some boundaries of timestamps
            wf_spec_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else funclib.get_utctimestamp_sec()
            wf_spec_end_ts = wf_spec_start_ts + wf_spec.duration

            # check if entire input is to be used
            wf_spec_use_full_data = wf_spec.use_full_data

            # define some constants
            if (wf_spec.interval > 0):
                raise Exception("ClusterWFProtocol: static workflow dont support interval: {}".format(wf_spec.interval))

            # parse operations from the wf_spec
            operations = self.execute_live_single_round_get_operations(wf_spec)

            # debug
            utils.info("ClusterWFProtocol: {}: execute_static: duration: {}: start_ts: {}, end_ts: {}".format(
                self.get_entity_id(), wf_spec.duration, wf_spec_start_ts, wf_spec_end_ts)) 

            # resolve the input
            xinput_resolved = self.resolve_meta(xinput, wf_spec_start_ts, wf_spec_use_full_data, wf_spec_start_ts, wf_spec_end_ts)

            # check if this is external execution or not
            if (wf_spec.is_external == False):
                # execute and read the output
                xoutput = self.execute_live_single_round(operations, xinput_resolved)

                # write the output. TODO: single output only
                output_id = wf_entity.entity_spec.output_ids[0]
                file_index = 0
                self.cluster_handler.create(ClusterPaths.get_entity_data_outputs(wf_entity.entity_type, wf_entity.entity_id))
                self.cluster_handler.create(ClusterPaths.get_entity_data_output(wf_entity.entity_type, wf_entity.entity_id, output_id))
                self.cluster_handler.write_tsv(ClusterPaths.get_entity_data_output_file(wf_entity.entity_type, wf_entity.entity_id, output_id, file_index), xoutput)
            else:
                # wait for the presence of output. TODO
                output_id = wf_entity.entity_spec.output_ids[0]
                file_index = 0
                total_wait_time = 0
                while (total_wait_time < wf_spec.max_job_execution_time):
                    # check for local cluster state
                    if (self.cluster_handler.file_exists(ClusterPaths.get_entity_data_output_file(self.get_entity_type(), self.get_entity_id(), output_id, file_index))):
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
    def execute_live(self):
        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # debug
        utils.info("ClusterWFProtocol: execute_live: {}".format(self.get_entity_id()))

        # change state to ALIVE
        self.__update_wf_state__(self.cluster_handler, EntityState.ALIVE) 

        # run under try-catch block to handle exceptions and set the final state
        try:
            # read input
            input_ids = wf_spec.input_ids
            file_index = 0
            
            # validation for multiple inputs or outputs. TODO
            if (len(wf_spec.input_ids) > 1 or len(wf_spec.output_ids) > 1):
                raise Exception("ClusterWFProtocol: {}: multiple inputs and outputs not supported: {}, {}".format(self.get_entity_id(), input_ids, output_ids))

            # read input
            xinput = self.cluster_handler.read_tsv(ClusterPaths.get_entity_data_input_file(wf_entity.entity_type, wf_entity.entity_id, input_ids[0], file_index))

            # resolve start_ts. TODO: This needs to fall on some boundaries of timestamps
            wf_spec_start_ts = wf_spec.start_ts if (wf_spec.start_ts is not None and wf_spec.start_ts > 0) else funclib.get_utctimestamp_sec()

            # check if entire input is to be used
            wf_spec_use_full_data = wf_spec.use_full_data

            # define some constants
            num_iter = int(math.ceil(wf_spec.duration / wf_spec.interval))

            # parse operations from the wf_spec
            operations = self.execute_live_single_round_get_operations(wf_spec)

            # initialize start timestamp
            cur_start_ts = wf_spec_start_ts
            cur_end_ts = cur_start_ts + wf_spec.interval 

            # iterate
            for iter_count in range(num_iter):
                # wait until the current timestamp is more than the cur_start_ts
                cur_ts = funclib.get_utctimestamp_sec()

                # debug
                utils.info("ClusterWFProtocol: {}: execute_live: duration: {}, interval: {}, iteration: {} / {}: start_ts: {}, end_ts: {}, cur_ts: {}".format(
                    self.get_entity_id(), wf_spec.duration, wf_spec.interval, iter_count, num_iter, cur_start_ts, cur_end_ts, cur_ts))

                # check if it is time to run the loop
                if (cur_ts <= cur_end_ts):
                    # wait
                    utils.info("ClusterWFProtocol: {}: execute_live: iteration: {} / {}: sleeping for {} seconds".format(self.get_entity_id(), iter_count, num_iter, (cur_end_ts - cur_ts)))
                    time.sleep(cur_end_ts - cur_ts)

                # resolve the input
                xinput_resolved = self.resolve_meta(xinput, wf_spec_start_ts, wf_spec_use_full_data, cur_start_ts, cur_end_ts)

                # call a single run. live means that the files are ETL formatted and not based on batchids
                xoutput = self.execute_live_single_round(operations, xinput_resolved)

                # etl output are laid out based on timestamp. if the event has the timestamp, then use that to determine output filename
                cur_file_start_ts, cur_file_end_ts = self.__resolve_etl_file_timestamp__(xoutput, cur_start_ts, cur_end_ts)

                # write the output. TODO: single output only
                output_id = wf_entity.entity_spec.output_ids[0]
                self.cluster_handler.create(ClusterPaths.get_entity_data_outputs(wf_entity.entity_type, wf_entity.entity_id))
                self.cluster_handler.create(ClusterPaths.get_entity_data_output(wf_entity.entity_type, wf_entity.entity_id, output_id))
                self.cluster_handler.create(ClusterPaths.get_entity_data_output_etl_dt(wf_entity.entity_type, wf_entity.entity_id, output_id, cur_start_ts))
                self.cluster_handler.write_tsv(ClusterPaths.get_entity_data_output_etl_file(wf_entity.entity_type, wf_entity.entity_id, output_id, cur_file_start_ts, cur_file_end_ts), xoutput)

                # update the timestamps
                cur_start_ts = cur_end_ts
                cur_end_ts = cur_end_ts + wf_spec.interval

            # set state to COMPLETED
            self.__update_wf_state__(self.cluster_handler, EntityState.COMPLETED)

            # debug
            utils.info("ClusterWFProtocol: {}: execute_live: wf: {}. finished".format(self.get_entity_id(), wf_entity.entity_id))
        except Exception as e:
            # set the final state
            self.__update_wf_state__(self.cluster_handler, EntityState.FAILED)
            raise e

    # execute as task in remote cluster
    def execute_remote(self):
        # local reference
        wf_entity = self.entity
        wf_spec = wf_entity.entity_spec

        # debug
        utils.info("ClusterWFProtocol: execute_remote: {}".format(self.get_entity_id()))

        # run under try-catch block to handle exceptions and set the final state
        try:
            # read input
            input_ids = wf_spec.input_ids
            file_index = 0
            
            # validation for multiple inputs or outputs. TODO
            if (len(wf_spec.input_ids) > 1 or len(wf_spec.output_ids) > 1):
                raise Exception("ClusterWFProtocol: {}: multiple inputs and outputs not supported: {}, {}".format(self.get_entity_id(), input_ids, output_ids))

            # read input
            xinput = self.cluster_handler.read_tsv(ClusterPaths.get_entity_data_input_file(wf_entity.entity_type, wf_entity.entity_id, input_ids[0], file_index))

            # save input locally
            self.local_cluster_handler.create(ClusterPaths.get_entity_data(wf_entity.entity_type, wf_entity.entity_id))
            self.local_cluster_handler.create(ClusterPaths.get_entity_data_inputs(wf_entity.entity_type, wf_entity.entity_id))

            # iterate over all input tsvs. TODO: multiple inputs
            xinputs = [xinput]
            for i in range(len(wf_spec.input_ids)):
                input_id = wf_spec.input_ids[i]
                file_index = 0
                self.local_cluster_handler.create(ClusterPaths.get_entity_data_input(wf_entity.entity_type, wf_entity.entity_id, input_id))
                self.local_cluster_handler.write_tsv(ClusterPaths.get_entity_data_input_file(wf_entity.entity_type, wf_entity.entity_id, input_id, file_index), xinputs[i])

            # initialize outputs
            self.local_cluster_handler.create(ClusterPaths.get_entity_data_outputs(wf_entity.entity_type, wf_entity.entity_id))

            # iterate for each output id and initialize
            for i in range(len(wf_spec.output_ids)):
                output_id = wf_spec.output_ids[i]
                self.local_cluster_handler.create(ClusterPaths.get_entity_data_output(wf_entity.entity_type, wf_entity.entity_id, output_id))

            # copy the wf spec with new parameters for remote execution
            wf_entity_remote = cluster_common_v2.ClusterEntityWF.from_json(wf_entity.to_json())
            wf_entity_remote.entity_spec.is_remote = False 
            self.local_cluster_handler.create(ClusterPaths.get_entity_id(self.get_entity_type(), self.get_entity_id()))
            self.local_cluster_handler.create(ClusterPaths.get_entity(self.get_entity_type(), self.get_entity_id()))
            self.local_cluster_handler.update_dynamic_value(ClusterPaths.get_entity(self.get_entity_type(), self.get_entity_id()), wf_entity_remote)

            # create entry into incoming
            self.local_cluster_handler.create(ClusterPaths.get_entity_incoming(self.get_entity_type(), self.get_entity_id()))

            # update the current state
            self.__update_wf_state__(self.cluster_handler, EntityState.ALIVE)
            self.__update_wf_state__(self.local_cluster_handler, EntityState.CREATED)

            # wait for the completion state
            final_state = None
            while True:
                # check for local cluster state
                if (self.local_cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.COMPLETED, self.get_entity_id()))):
                    # set final state
                    final_state = EntityState.COMPLETED

                    # check for output to appear first. TODO: need better way to coordinate as this can still time out
                    if (self.local_cluster_handler.file_exists_with_wait(ClusterPaths.get_entity_data_output_file(self.get_entity_type(), self.get_entity_id(), wf_spec.output_ids[0], 0), attempts = 100)):
                        # read output
                        xoutput = self.local_cluster_handler.read_tsv(ClusterPaths.get_entity_data_output_file(self.get_entity_type(), self.get_entity_id(), wf_spec.output_ids[0], 0))
                        xoutput.show_transpose(3, title = "ClusterWFProtocol: execute_remote")

                        # copy the output
                        self.cluster_handler.write_tsv(ClusterPaths.get_entity_data_output_file(self.get_entity_type(), self.get_entity_id(), wf_spec.output_ids[0], 0), xoutput)

                    # break loop
                    break
                elif (self.local_cluster_handler.dir_exists(ClusterPaths.get_entities_state_by_id(self.get_entity_type(), EntityState.FAILED, self.get_entity_id()))):
                    # set final state
                    final_state = EntityState.FAILED

                    # break loop
                    break
                else:
                    # wait for some time
                    utils.info("ClusterWFProtocol: {}: execute_remote: waiting for 10 seconds".format(self.get_entity_id()))
                    time.sleep(10)

            # update the final state
            self.__update_wf_state__(self.cluster_handler, final_state)
    
            # debug
            utils.info("ClusterWFProtocol: {}: execute_remote: wf: {}. finished".format(self.get_entity_id(), wf_entity.entity_id))
        except Exception as e:
            # set the final state. TODO: communicate the failure to remote cluster as ABORTED
            self.__update_wf_state__(self.local_cluster_handler, EntityState.ABORTED)
            self.__update_wf_state__(self.cluster_handler, EntityState.FAILED)
            raise e

    # resolve meta parameters
    def resolve_meta(self, xtsv, wf_start_ts, use_full_data, start_ts, end_ts):
        # resolve params first
        xtsv1 = self.__resolve_meta_params__(xtsv, start_ts, end_ts)

        # resolve data and do etl scan if needed
        xtsv2 = self.__resolve_reference_paths__(xtsv1, wf_start_ts, use_full_data, start_ts, end_ts)

        # resolve meta params again TODO
        xtsv3 = self.__resolve_meta_params__(xtsv2, start_ts, end_ts)

        # return
        return xtsv3

    # TODO: these need to be defined properly
    def __resolve_reference_paths__(self, xtsv, wf_start_ts, use_full_data, start_ts, end_ts):
        # check if TSVReference is defined. TODO
        if (xtsv.has_col(cluster_common_v2.TSVReference.OMIGO_REFERENCE_PATH)):
            return self.__resolve_reference_paths__(cluster_common_v2.TSVReference.read(xtsv), wf_start_ts, use_full_data, start_ts, end_ts)
        
        # check if etl path is defined
        etl_path_col = None 
        for c in xtsv.get_columns():
            if (c.startswith(cluster_arjun.OMIGO_ARJUN_ETL_PATH_PREFIX)):
                utils.info("ClusterWFProtocol: __resolve_reference_paths__: found etl path: {}, {}".format(c, xtsv.get_columns()))
                etl_path_col = c
                break

        # if etl path is defined, resolve it
        if (etl_path_col is not None):
            etsvs = []
            mps = xtsv.to_maps()

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
                effective_etl_start_ts = funclib.utctimestamp_to_datetime_str(wf_start_ts) if (use_full_data == True) else etl_start_ts

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
        return xtsv

    # internal method to resolve meta parameters
    def __resolve_meta_params__(self, xtsv, start_ts, end_ts):
        def __resolve_meta_params_inner__(x):
            # column values
            start_ts_str = funclib.utctimestamp_to_datetime_str(start_ts)
            end_ts_str = funclib.utctimestamp_to_datetime_str(end_ts)

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
        xtsv = xtsv.transform_inline(xtsv.get_columns(), lambda x: __resolve_meta_params_inner__(x), ignore_if_missing = True)

        # return
        return xtsv

    # resolve meta parameters for external task
    def resolve_external_task_meta_params(self, xtsv, input_id, output_id, file_index):
        return self.__resolve_external_task_meta_params__(xtsv, input_id, output_id, file_index)

    # internal method to resolve meta parameters for external task
    def __resolve_external_task_meta_params__(self, xtsv, input_id, output_id, file_index):
        def __resolve_external_task_meta_params_inner__(x):
            # list of columns to replace        
            cols = {
                cluster_arjun.OMIGO_ARJUN_BASE_PATH_TEMPLATE: ClusterPaths.get_base_path(),
                cluster_arjun.OMIGO_ARJUN_INPUT_FILE_TEMPLATE: ClusterPaths.get_entity_data_input_file(self.get_entity_type(), self.get_entity_id(), input_id, file_index),
                cluster_arjun.OMIGO_ARJUN_OUTPUT_FILE_TEMPLATE: ClusterPaths.get_entity_data_output_file(self.get_entity_type(), self.get_entity_id(), output_id, file_index) 
            }

            # resolve
            for k in cols.keys():
                x = x.replace(k, cols[k])

            # return
            return x

        # apply the tranformation on each column
        xtsv = xtsv.transform_inline(xtsv.get_columns(), lambda x: __resolve_external_task_meta_params_inner__(x), ignore_if_missing = True)

        # return
        return xtsv

    # resolve timestamp for etl file
    def __resolve_etl_file_timestamp__(self, xtsv, start_ts, end_ts):
        # check for no data
        if (xtsv.num_rows() == 0):
            return (start_ts, end_ts)

        # check if the special column is defined
        if (xtsv.has_col(cluster_arjun.OMIGO_ARJUN_EVENT_TS)):
            # take the min max values
            col_values = sorted(xtsv.col_as_array_uniq(cluster_arjun.OMIGO_ARJUN_EVENT_TS))
            event_start_ts, event_end_ts = (col_values[0], col_values[-1])

            # resolve the value of the timestamp to numeric seconds
            event_start_ts = funclib.datetime_to_utctimestamp(event_start_ts)
            event_end_ts = funclib.datetime_to_utctimestamp(event_end_ts)

            # return
            return (event_start_ts, event_end_ts)

        # return the default
        return (start_ts, end_ts)

    # execute single round of live wf
    def execute_live_single_round(self, operations, xinput_resolved):
        # if the input is empty, return empty output
        if (xinput_resolved.num_rows() == 0):
            utils.warn_once("execute_live_single_round: empty tsv. continuing. The empty tsv can lead to unpredictable behavior")

        # read the input as the base
        otsv = xinput_resolved

        # iterate over the tasks and call each one of them
        for (operation, extend_class_op) in operations:
            # call the function. and set the job status correctly
            try:
                # if extend_class_op is defined, then instantiate the object
                extend_class_obj = cluster_common_v2.load_extend_class_obj(extend_class_op, otsv.get_header(), otsv.get_data()) if (extend_class_op is not None) else None
                utils.debug("ClusterWFProtocol: {} execute_live_single_round: extend_class_op: {}, extend_class_obj: {}".format(self.get_entity_id(), extend_class_op, extend_class_obj))

                # read function parameters
                args = cluster_data.load_native_objects(operation.args)
                kwargs = cluster_data.load_native_objects(operation.kwargs)

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
                utils.debug("ClusterWFProtocol: {} execute_live_single_round: custom_func: name: {}, args: {}, kwargs: {}, extend_class_obj: {}, func: {}".format(self.get_entity_id(), operation.name, args, kwargs, extend_class_obj, class_func))

                # note that custom_func is not called as the context is already in the function.
                otsv = class_func(*args, **kwargs)
            except Exception as e:
                utils.error("ClusterWFProtocol: {} execute_live_single_round: Found error while running the method: {}".format(e))
                raise e

        # return output
        return otsv

    # get all operations for single round of live wf
    def execute_live_single_round_get_operations(self, wf_spec):
        # create tasks
        operations = []

        # flatten the wf spec into tasks. ignore all partitioning tasks. then execute sequentially
        jobs_specs = wf_spec.jobs_specs
        for job_spec in jobs_specs:
            # check if a custom class is called
            extend_class_op = None 

            # instantiate the class object
            if (job_spec.extend_class_def is not None):
                extend_class_op = job_spec.extend_class_def.extend_class_op

            # add map task
            if (job_spec.map_task is not None):
                # iterate and add all operations
                for op in job_spec.map_task.map_ops:
                    # ignore extend_class def
                    if (op.task_type == ClusterTaskType.EXTEND_CLASS):
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

        # return
        return operations

# Job Protocol
class ClusterJobProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Task Protocol
class ClusterTaskProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Batch Protocol
class ClusterBatchProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Worker Protocol
class ClusterWorkerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Agent Protocol
class ClusterAgentProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Double Agent Protocol
class ClusterDoubleAgentProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Inteli Agent Protocol
class ClusterInteliAgentProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Client Protocol
class ClusterClientProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

# Session Protocol
class ClusterSessionProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    # method for submitting a workflow to the cluster
    def submit_workflow(self, wf_spec, xinputs):
        # create wf entity
        wf_entity = cluster_common_v2.ClusterEntityWF.new(ClusterIds.generate_wf_id(), ClusterIds.get_client_id(), ClusterIds.get_session_id(), wf_spec)

        # persist input for wf
        self.cluster_handler.create(ClusterPaths.get_entity_data(wf_entity.entity_type, wf_entity.entity_id))
        self.cluster_handler.create(ClusterPaths.get_entity_data_inputs(wf_entity.entity_type, wf_entity.entity_id))

        # iterate over all input ids 
        for i in range(len(wf_spec.input_ids)):
            input_id = wf_spec.input_ids[i]
            file_index = 0
            self.cluster_handler.create(ClusterPaths.get_entity_data_input(wf_entity.entity_type, wf_entity.entity_id, input_id))
            self.cluster_handler.write_tsv(ClusterPaths.get_entity_data_input_file(wf_entity.entity_type, wf_entity.entity_id, input_id, file_index), xinputs[i])

        # initialize output directories 
        self.cluster_handler.create(ClusterPaths.get_entity_data_outputs(wf_entity.entity_type, wf_entity.entity_id))

        # iterate over all output ids and initialize 
        for i in range(len(wf_spec.output_ids)):
            output_id = wf_spec.output_ids[i]
            self.cluster_handler.create(ClusterPaths.get_entity_data_output(wf_entity.entity_type, wf_entity.entity_id, output_id))

        # get wf protocol
        wf_protocol = ClusterWFProtocol(wf_entity)

        # initialize wf entity
        wf_protocol.initialize()

        # add as dependent
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
            xentity_state_protocol = ClusterEntityStateProtocol(current_master.entity_type, current_master.entity_id)
           
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
    def run_master_election(self):
        # check if master already exists and is valid
        current_master_id = self.get_elected_master()
        if (current_master_id is not None):
            # check if already the current master
            if (current_master_id == self.entity_id):
                utils.info("ClusterMasterElectionProtocol: {}: run_election: already the current master".format(self.entity_id))
            else:
                utils.info("ClusterMasterElectionProtocol: {}: run_election: another master exists: {}".format(self.entity_id, current_master_id))
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
            xentity_state_protocol = ClusterEntityStateProtocol(candidate.entity_type, candidate.entity_id)
            if (xentity_state_protocol.is_alive() == False):
                utils.info("ClusterMasterElectionProtocol: {}: run_election: found dead candidate: {}. Ignoring.".format(self.entity_id, candidate_id))
                continue

            # find the one with the min
            if (min_ts == -1 or min_ts > candidate.ts):
                min_ts = candidate.ts
                min_ts_candidate = candidate
            elif (min_ts == candidate.ts):
                if (candidate.entity_id < self.get_entity_id()):
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
        self.local_cluster_handler = ClusterPaths.get_local_cluster_handler()

    # create cluster
    def create_cluster(self, create_remote_flag = False):
        # print the cluster base path
        utils.info("create_cluster: Using base path: {}".format(cluster_common_v2.HYDRA_PATH))
        utils.info("create_cluster: Using local base path: {}".format(cluster_common_v2.HYDRA_LOCAL_PATH))

        # create root path
        self.cluster_handler.create("")

        # iterate over base paths
        for base_path in ClusterPaths.get_base_paths():
            self.cluster_handler.create(base_path)

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
            self.local_cluster_handler.create("")

            # # iterate over base paths
            for base_path in ClusterPaths.get_base_paths():
                self.local_cluster_handler.create(base_path)

            # iterate over all entity types
            for xentity_type in EntityType.get_all():
                # create states
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_ids(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_state(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_active_children(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_passive_children(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_dependents(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_heartbeat(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_incoming(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_assigned_supervisor(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_assigned_executors(xentity_type)))
                tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_data(xentity_type)))

                # update for all states
                for xentity_state in EntityState.get_all():
                    tasks.append(utils.ThreadPoolTask(self.local_cluster_handler.create, ClusterPaths.get_entities_state_by_state(xentity_type, xentity_state)))

        # run under thread pool
        utils.run_with_thread_pool(tasks, num_par = 10)

    # remove all entities. TODO: implement logic to revoke any validation tokens for failsafe stopping of entities
    def remove_entities(self, only_dead = False):
        # find the list of entities to remove
        remove_entities = []

        # iterate over all entity types
        for xentity_type in EntityType.get_all():
            # get entity ids
            xentity_ids = self.cluster_handler.list_dirs(ClusterPaths.get_entities_ids(xentity_type))

            # iterate over all entities and add to the list
            for xentity_id in xentity_ids:
                # check if deletion only to be done for dead entites
                if (only_dead == True):
                    xentity_state_protocol = ClusterEntityStateProtocol(xentity_type, xentity_id)
                    if (xentity_state_protocol.is_alive() == False):
                        remove_entities.append((xentity_type, xentity_id))
                        utils.info("ClusterAdmin: remove_entities: {}".format(xentity_id))
                else:
                    remove_entities.append((xentity_type, xentity_id))
                    utils.info("ClusterAdmin: remove_entities: {}".format(xentity_id))
       
        # iterate over all entity types. TODO ignore this for a while
        # for (xentity_type, xentity_id) in remove_entities:
        #     # move the entities to cleanup. TODO: use ABORTED state here first
        #     update_time = cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec())
        #     self.cluster_handler.create(ClusterPaths.get_entities_state_by_id(xentity_type, EntityState.CLEANUP, xentity_id))
        #     self.cluster_handler.update_dynamic_value(ClusterPaths.get_entities_state_by_id(xentity_type, EntityState.CLEANUP, xentity_id), update_time)

        # wait for max heartbeat timeout.
        if (len(remove_entities) > 0):
            utils.info("ClusterAdmin: waiting for {} seconds for stopping any heartbeat threads. enable it again".format(ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT))
            # time.sleep(ClusterHeartbeatProtocol.MAX_HEARTBEAT_WAIT)

        # inner method for threadpool
        def __remove_entities_inner__(xentity_type, xentity_id):
            # remove each entity
            xentity_protocol = ClusterEntityCleanupProtocol(xentity_type, xentity_id)
            xentity_protocol.cleanup()

        # iterate over all entities again and remove
        tasks = []
        for (xentity_type, xentity_id) in remove_entities:
            # add the task
            tasks.append(utils.ThreadPoolTask(__remove_entities_inner__, xentity_type, xentity_id))

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
    def do_state_change(self, xentity_type, xentity_id, target_state):
        utils.info("ClusterAdmin: do_state_change: this is just a tooling method. Dont use. {}, {}, {}".format(xentity_type, xentity_id, target_state))

        # create new update time
        target_state_update_time = cluster_common_v2.ClusterUpdateTime.new(funclib.get_utctimestamp_sec())

        # take path
        entity_state_path = ClusterPaths.get_entities_state_by_id(xentity_type, target_state, xentity_id)
        self.cluster_handler.create(entity_state_path)
        self.cluster_handler.update_dynamic_value(entity_state_path, target_state_update_time)

# Executor context that can be called either from TSV itself for inline processing, or from outside 
# for more asynchronous workflows
# TODO: the is_remote and is_external is for jobs and not workflows
class ClusterExecutorContext:
    def __init__(self, session_protocol, is_live = False, is_remote = False, is_external = False, max_job_execution_time = 600, interval = 60, duration = 300):
        self.cluster_handler = ClusterPaths.get_cluster_handler()
        self.session_protocol = session_protocol
        self.is_live = is_live
        self.is_remote = is_remote
        self.is_external = is_external
        self.max_job_execution_time = max_job_execution_time
        self.interval = interval
        self.duration = duration

    # execute multiple jobs are workflow
    def execute_jobs(self, xtsv, jobs_operations, input_ids, output_ids, start_ts = None, use_full_data = False, num_splits = 10):
        # create workflow semantics from job
        wf_spec = self.__create_wf_spec__(jobs_operations, input_ids, output_ids, start_ts, use_full_data, num_splits)

        # get session protocol
        xinputs = [xtsv]
        wf_id = self.session_protocol.submit_workflow(wf_spec, xinputs)

        # return
        return wf_id

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

        # create job_spec 
        job_spec = cluster_common_v2.ClusterSpecJob.new(map_partitioner, map_task, reduce_partitioner, reduce_task, singleton_task, extend_class_def)

        # return
        return job_spec
    
    # create wf spec
    def __create_wf_spec__(self, jobs_operations, input_ids, output_ids, start_ts, use_full_data, num_splits):
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
        wf_spec = cluster_common_v2.ClusterSpecWF.new(jobs_specs, is_live = self.is_live, is_remote = self.is_remote, is_external = self.is_external, max_job_execution_time = self.max_job_execution_time,
            interval = self.interval, start_ts = start_ts, use_full_data = use_full_data, duration = self.duration, input_ids = input_ids, output_ids = output_ids)

        # return
        return wf_spec

    def get_workflow_input_path(self, workflow_id, input_id):
        return cluster_common_v2.ClusterPaths.get_entity_data_input(EntityType.WF, workflow_id, input_id)

    def get_workflow_output_path(self, workflow_id, output_id):
        return cluster_common_v2.ClusterPaths.get_entity_data_output(EntityType.WF, workflow_id, output_id)
