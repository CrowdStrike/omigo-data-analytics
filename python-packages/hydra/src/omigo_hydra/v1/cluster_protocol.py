import time 
import os
import random
import math
from omigo_core import tsv, utils, funclib
from omigo_hydra import cluster_data
from omigo_hydra import cluster_common
import threading

# time interval to sleep before running cleanup
CLEANUP_SLEEP_SECS = 0
HEARTBEAT_SEC = 15 
MAX_HEARTBEAT_WAIT = 60
MAX_HEARTBEAT_CLEANUP_DEAD_WAIT = 5 * MAX_HEARTBEAT_WAIT
MAX_HEARTBEAT_CACHE = 5
POST_THREAD_POOL_WAIT_SEC = 5

class ClusterEntityProtocol:
    def __init__(self, entity):
        self.entity = entity
        self.cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
        self._lock = threading.Lock()
        self.last_heartbeat_cache = 0

    def is_supervised(self, entity_type):
        raise Exception("Derived class needs to implement this method")

    def get_entity(self):
        with self._lock:
            return self.entity

    def get_entity_id(self):
        return self.get_entity().entity_id

    def get_entity_type(self):
        return self.get_entity().entity_type

    def get_entity_ts(self):
        return self.get_entity().ts

    def update_entity_ts(self, ts):
        with self._lock:
            self.entity.ts = ts

    def get_protocol_entity_type(self):
        raise Exception("Derived class needs to implement this method")

    def is_operation_allowed(self):
        # check the heartbeat
        cur_time = funclib.get_utctimestamp_sec()

        # update cache if needed
        if (self.last_heartbeat_cache == 0 or cur_time - self.last_heartbeat_cache > MAX_HEARTBEAT_CACHE):
            # read the entity heartbeat
            entity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(self.get_entity_type(), self.get_entity_id()))
            self.last_heartbeat_cache = entity_json["ts"]

        time_diff = cur_time - self.last_heartbeat_cache
        if (time_diff >  MAX_HEARTBEAT_WAIT):
            utils.info("ClusterEntityProtocol: is_operation_allowed: heartbeat expired: {}. Time Diff: {} seconds. Returning False".format(self.get_entity_id(), time_diff))
            return False

        return True

    def initialize(self):
        # initialize the self entity
        self.cluster_handler.create(cluster_common.ClusterPaths.get_primary_entity(self.get_entity_type(), self.get_entity_id())) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_primary_entity_details(self.get_entity_type(), self.get_entity_id())) 
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_primary_entity_details(self.get_entity_type(), self.get_entity_id()), self.get_entity())

        # start heart beat thread
        self.start_heartbeat_thread()

    # TODO: json blob is kind of weird
    def __update_heartbeat__(self):
        while True:
            utils.debug("ClusterEntityProtocol: __update_heartbeat__: {}".format(self.get_entity_id()))
            if (self.__update_heartbeat_inner__() == True):
                time.sleep(HEARTBEAT_SEC)
            else:
                # TODO
                utils.debug("ClusterEntityProtocol: __update_heartbeat__ exiting")
                return 

    # inner heartbeat method
    def __update_heartbeat_inner__(self):
        # check if the entity is alive
        if (self.__is_alive__(self.get_entity_type(), self.get_entity_id()) == False):
            utils.debug("ClusterEntityProtocol: __update_heartbeat__: {}, heartbeat too old. Exiting".format(self.get_entity_id()))
            return False

        # check if the entity is part of cleanup already. TODO: consider all possible states
        if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_cleanup_entity(self.get_entity_type(), self.get_entity_id())) == True):
            utils.info("ClusterEntityProtocol: __update_heartbeat__: {}, entity in cleanup. Exiting.".format(self.get_entity_id()))
 
        # update the entity in place. TODO: this should be transactional
        self.update_entity_ts(funclib.get_utctimestamp_sec())

        # update on cluster
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_primary_entity_details(self.get_entity_type(), self.get_entity_id()), self.get_entity(), ignore_logging = True)

        # return
        return True

    def start_heartbeat_thread(self):
        utils.info("ClusterEntityProtocol: start_heartbeat_thread: starting heartbeat thread for: {}".format(self.get_entity_id()))

        # create the thread to update the heartbeat
        thread = threading.Thread(target = self.__update_heartbeat__, daemon = True)
        thread.start()
        
    def __is_alive__(self, xentity_type, xentity_id, wait_sec = 3, attempts = 10):
        # check if the directories exists
        if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_primary_entity(xentity_type, xentity_id)) == False):
            utils.debug("ClusterEntityProtocol: __is_alive__: entity missing: {}".format(xentity_id))
            return False

        # avoid race condition of missing details
        if (self.cluster_handler.dir_exists_with_wait(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id), ignore_missing = True) == False):
            utils.debug("ClusterEntityProtocol: __is_alive__: entity details missing: {}".format(xentity_id))
            return False

        # read the entity value
        xentity = cluster_common.ClusterEntity.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id)))
        xentity_server_timestamp = self.cluster_handler.read_most_recent_server_timestamp(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id))

        # check for None
        if (xentity is None or xentity_server_timestamp is None):
            utils.debug("ClusterEntityProtocol: __is_alive__: entity details missing: {}".format(xentity_id))
            return False

        # check the diff
        clock_skew = xentity.ts - xentity_server_timestamp
        if (abs(clock_skew) > MAX_HEARTBEAT_WAIT):
            raise Exception("ClusterEntityProtocol: __is_alive__: {}, clock skew detected. entity ts: {}, server ts: {}, skew: {}. Raising exception for debugging".format(xentity_id, xentity.ts, xentity_server_timestamp, clock_skew))

        # check if the timestamp has already expired
        cur_time = funclib.get_utctimestamp_sec()
        time_diff = cur_time - xentity_server_timestamp

        # check against max time
        if (time_diff > MAX_HEARTBEAT_WAIT):
            utils.debug("ClusterEntityProtocol: is_alive: {}, heartbeat too old. Time Diff: {} seconds. Exiting".format(xentity_id, time_diff))
            return False
        else:
            return True

    def update_registered(self, xentity_type):
        # validation. TODO: fix this debug
        if (self.is_supervised(xentity_type) == False):
            raise Exception("The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterEntityProtocol: update_registered: self entity_id: {}, self entity_type: {}, monitor entity_type: {}. This operation is not allowed by this entity because of lack of ownership".format(
                self.get_entity_id(), self.get_protocol_entity_type(), xentity_type))
            return

        # read all entity ids
        xentity_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_primary_entities(xentity_type))
        xregistered_entity_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_registered_entities(xentity_type))

        # logging
        utils.info("ClusterEntityProtocol: update_registered: self entity_id: {}, entity_ids: {}, xregistered_entity_ids: {}".format(self.get_entity_id(), xentity_ids, xregistered_entity_ids))

        # cleanup ids. TODO move this to multi threading
        xcleanup_ids = []

        # construct manager object and check for presence in masters. TODO: the variable entity_id is confusing
        for xentity_id in xentity_ids:
            # check if the entity is alive
            if (self.__is_alive__(xentity_type, xentity_id) == False):
                utils.info("ClusterEntityProtocol: update_registered: entity is not alive: {}".format(xentity_id))
                xcleanup_ids.append((xentity_type, xentity_id))
                continue

            # read entity 
            xentity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id))
            xentity = cluster_common.ClusterEntity.from_json(xentity_json)

            # check if it is known in the registered 
            if (xentity_id in xregistered_entity_ids):
                # read master manager
                registered_entity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id))
                registered_entity = cluster_common.ClusterEntity.from_json(registered_entity_json)

                # compare the token. if the tokens mismatch, the old manager has come back. do the cleanup first. Pick this manager in next cycle
                if (xentity.token != registered_entity.token):
                    # dont accept a new entity here. TODO: its confusing if to bundle multiple protocol actions or wait for update_registered cycle
                    utils.info("ClusterEntityProtocol: update_registered: self entity_id: {}, entity_type read: {}, entity_id read: {}, entity.token != registered_entity.token".format(
                        self.get_entity_id(), xentity_type, xentity_id))
                    xcleanup_ids.append((xentity_type, xentity_id))
                else:
                    utils.info("update_registered: entity already known: {} / {}".format(xentity_type, xentity_id))
            else:
                # this entity is not known yet. create a new entry.
                self.register(xentity_type, xentity_id)

        # look for inconsistency
        for xregistered_entity_id in xregistered_entity_ids:
            # check if existing registered entity exist in main entities
            if (xregistered_entity_id not in xentity_ids):
                # registered entity does not exist.
                utils.info("ClusterEntityProtocol: update_registered: removing a registered entity which was not found in primary: {} / {}".format(xentity_type, xregistered_entity_id))
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_registered_entity(xentity_type, xregistered_entity_id))

        # cleanup
        for (xcleanup_entity_type, xcleanup_entity_id) in xcleanup_ids:
            utils.info("ClusterEntityProtocol: update_registered: cleanup id: {} / {}".format(xcleanup_entity_type, xcleanup_entity_id))
            self.__cleanup_inactive__(xcleanup_entity_type, xcleanup_entity_id)
            
    def register(self, xentity_type, xentity_id):
        # validation
        if (self.is_supervised(xentity_type) == False and xentity_type != self.get_protocol_entity_type()):
            raise Exception("The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterEntityProtocol: register: This operation is not allowed by this entity because of lack of ownership")
            return

        utils.info("ClusterEntityProtocol: register: adding a new entity: {} / {}".format(xentity_type, xentity_id))
        xentity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_registered_entity(xentity_type, xentity_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id))
        self.cluster_handler.update_dynamic_value_json(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id), xentity_json)

    # TODO: placeholder. This is transactional
    def __cleanup_inactive__(self, xentity_type, xentity_id):
        # validation
        if (self.is_supervised(xentity_type) == False):
            raise Exception("ClusterEntityProtocol: The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterEntityProtocol: This operation is not allowed by this entity because of lack of ownership")
            return

        utils.info("ClusterEntityProtocol: __cleanup_inactive__: called for {} / {}".format(xentity_type, xentity_id))
        self.cluster_handler.remove_dir(cluster_common.ClusterPaths.get_active_entity(xentity_type, xentity_id), ignore_missing = True)
        self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_registered_entity(xentity_type, xentity_id), ignore_missing = True)

        # create entry into cleanup
        self.cluster_handler.create(cluster_common.ClusterPaths.get_cleanup_entity(xentity_type, xentity_id))

    def update_active(self, xentity_type):
        # validation
        if (self.is_supervised(xentity_type) == False):
            raise Exception("ClusterEntityProtocol: update_active: The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterEntityProtocol: update_active: This operation is not allowed by this entity because of lack of ownership")
            return

        # get the list of managers
        xentity_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_primary_entities(xentity_type))
        xregistered_entity_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_registered_entities(xentity_type))
        xactive_entity_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(xentity_type))

        # debug
        utils.info("ClusterEntityProtocol: update_active: entity_type: {}, entity_ids: {}, xregistered_entity_ids: {}, active_entity_ids: {}".format(
            xentity_type, xentity_ids, xregistered_entity_ids, xactive_entity_ids))

        # updated list of entity ids 
        xupdated_active_entity_ids = []
        xupdated_inactive_entity_ids = []

        # take all managers
        for xentity_id in xentity_ids:
            # check if alive
            if (self.__is_alive__(xentity_type, xentity_id) == False):
                utils.info("ClusterEntityProtocol: update_active: entity is not alive: {}".format(xentity_id))
                xupdated_inactive_entity_ids.append(xentity_id)
                continue

            # read entity details
            xentity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id))
            xentity = cluster_common.ClusterEntity.from_json(xentity_json)

            # check if the entry exists under master
            if (xentity_id in xregistered_entity_ids):
                # if it exists, then the token should match
                xregistered_entity_json = self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id))
                xregistered_entity = cluster_common.ClusterEntity.from_json(xregistered_entity_json)

                # compare the token
                if (xentity.token == xregistered_entity.token):
                    xupdated_active_entity_ids.append(xentity_id)
                else:
                    # this entity is invalid
                    xupdated_inactive_entity_ids.append(xentity_id)
            else:
                # update the registered entities 
                self.cluster_handler.create(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id))
                self.cluster_handler.update_dynamic_value_json(cluster_common.ClusterPaths.get_registered_entity_details(xentity_type, xentity_id), xentity_json)
                xupdated_active_entity_ids.append(xentity_id)

        # check for active entities that dont have original reference
        for xentity_id in xactive_entity_ids:
            # check if active was registered 
            if (xentity_id not in xregistered_entity_ids):
                utils.info("ClusterEntityProtocol: update_active: removing active entity_id as not found in registered: {} / {}".format(xentity_type, xentity_id))
                self.__cleanup_inactive__(xentity_type, xentity_id)

        # put all the active ones. including any new ones
        for xentity_id in set(xupdated_active_entity_ids):
            if (xentity_id not in xactive_entity_ids):
                utils.info("ClusterEntityProtocol: update_active: currently active entity: {} / {}".format(xentity_type, xentity_id))
                self.cluster_handler.create(cluster_common.ClusterPaths.get_active_entity(xentity_type, xentity_id))
            else:
                utils.info("ClusterEntityProtocol: update_active: entity already active: {} / {}".format(xentity_type, xentity_id))

        # remove the inactive entities 
        for xentity_id in set(xupdated_inactive_entity_ids):
            utils.info("ClusterEntityProtocol: update_active: removing inactive entity: {} / {}".format(xentity_type, xentity_id))
            self.cluster_handler.remove_dir(cluster_common.ClusterPaths.get_active_entity(xentity_type, xentity_id), ignore_missing = True)
        
    def recover_dead(self, xentity_type, xentity_id):
        # validation
        if (self.is_supervised(xentity_type) == False):
            raise Exception("ClusterEntityProtocol: recover_dead: The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.info("ClusterEntityProtocol: recover_dead: This operation is not allowed by this entity because of lack of ownership: {}, {}".format(xentity_type, self.get_protocol_entity_type()))
            return

        # debug
        utils.info("ClusterEntityProtocol: recover_dead: {}".format(xentity_id))

        # depending upon the entity type, the failover action can be different
        if (xentity_type == cluster_common.ClusterPaths.MANAGER):
            # get all the job ids
            job_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_manager_jobs(xentity_id))

            # iterate each job id and reassign
            if (job_ids is None or len(job_ids) == 0):
                # create entry into recovery to indicate recovery is done
                self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id)) 
                return

            # get the list of active managers
            active_manager_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(cluster_common.ClusterPaths.MANAGER))
            random.shuffle(active_manager_ids)

            # check if there are any active managers
            if (active_manager_ids is None or len(active_manager_ids) == 0):
                utils.warn("ClusterEntityProtocol: recover_dead: entity: {}, no active managers found for recovery".format(xentity_id))
                return

            # iterate through job ids and reassign
            for i in range(len(job_ids)):
                # job_id
                job_id = job_ids[i]

                # assign a new manager
                assigned_manager_index = i % len(active_manager_ids) 
                assigned_manager_id = active_manager_ids[assigned_manager_index]

                # debug
                utils.info("ClusterEntityProtocol: recover_dead: {}, job_id: {}, new manager_id: {}".format(xentity_id, job_id, assigned_manager_id))
            
                # notify the manager that a new job is created
                self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job(assigned_manager_id, job_id))

                # copy the latest status of the job from old manager to new one. TODO: manager_id is repetetive
                # the status directory is owned by manager and may not exist TODO: this is not clean. even on manager side lot of confusion
                if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_manager_job_status(xentity_id, job_id))):
                    manager_job_status = cluster_common.ClusterManagerJobStatus.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_manager_job_status(xentity_id, job_id)))
                    manager_job_status.manager_id = assigned_manager_id
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job_status(assigned_manager_id, job_id))
                    self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_job_status(assigned_manager_id, job_id), manager_job_status)
                else:
                    utils.info("ClusterEntityProtocol: recover_dead: {}, job_id: {}, status not created yet.".format(xentity_id, job_id))

                # read the job manager reference
                job_ref = cluster_common.ClusterJobRef.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_manager(job_id)))
                job_ref.manager_id = assigned_manager_id

                # update
                self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_job_manager(job_id), job_ref) 

                # delete the entry from the old manager
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_manager_job(xentity_id, job_id), ignore_missing = True)

            # create entry in recovered
            self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id)) 
        elif (xentity_type == cluster_common.ClusterPaths.WORKER):
            # get the list of jobs that are affected
            job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_jobs(xentity_id)))

            # get the list of jobs that are recovered
            recovered_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_recovered_jobs(xentity_id)))

            # jobs that need recovery
            non_recovered_job_ids = job_ids.difference(recovered_job_ids)

            # debug
            utils.info("ClusterEntityProtocol: recover_dead: {}, job_ids: {}, recovered_job_ids: {}, non_recovered_job_ids: {}".format(xentity_id, job_ids, recovered_job_ids, non_recovered_job_ids))

            # check if recovery is complete or not
            if (len(non_recovered_job_ids) > 0): 
                # create entry into the dead-worker list as job manager needs to take care of assigning resources
                for job_id in non_recovered_job_ids:
                    # create entry into dead worker
                    utils.info("ClusterEntityProtocol: recover_dead: {}, job_id: {} updated with dead worker".format(xentity_id, job_id))
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_job_dead_worker(job_id, xentity_id))
            else:
                # all the jobs have been recovered
                utils.info("ClusterEntityProtocol: recover_dead: entity_id: {}, all jobs are recovered.".format(xentity_id))
                self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id))
        elif (xentity_type == cluster_common.ClusterPaths.CLIENT):
            session_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_client_sessions(xentity_id)))
            recovered_session_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_recovered_entities(cluster_common.ClusterPaths.SESSION)))
            non_recovered_session_ids = session_ids.difference(recovered_session_ids)

            # check if recovery is completed
            if (len(non_recovered_session_ids) > 0):
                # add session for cleanup
                for session_id in non_recovered_session_ids:
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_cleanup_entity(cluster_common.ClusterPaths.SESSION, session_id))
            else:
                # recovery complete
                self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id))
        elif (xentity_type == cluster_common.ClusterPaths.SESSION):
            job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_session_jobs(xentity_id)))
            recovered_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_recovered_entities(cluster_common.ClusterPaths.JOB)))
            non_recovered_job_ids = job_ids.difference(recovered_job_ids)

            # check if recovery is completed
            if (len(non_recovered_job_ids) > 0):
                # add job ids for cleanup
                for job_id in non_recovered_job_ids:
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_cleanup_entity(cluster_common.ClusterPaths.JOB, job_id))
            else:
                # recovery complete
                self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id))
        else:
            utils.info("ClusterEntityProtocol: recover_dead: entity_id: {}, no specific recovery needed".format(xentity_id))
            self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xentity_id))

    def cleanup_dead(self, xentity_type):    
        # validation
        if (self.is_supervised(xentity_type) == False):
            raise Exception("ClusterEntityProtocol: cleanup_dead: The entity type is not under supervision in this protocol: {}, {}".format(xentity_type, self.get_protocol_entity_type()))

        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterEntityProtocol: cleanup_dead: This operation is not allowed by this entity because of lack of ownership")
            return

        # list the primary entities
        xcleanup_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_cleanup_entities(xentity_type)))
        xrecovered_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_recovered_entities(xentity_type)))
        xrecovery_pending_ids = xcleanup_ids.difference(xrecovered_ids)
        xcleanup_pending_ids = xrecovered_ids.difference(xcleanup_ids)
        xlong_dead_ids = []

        # debug
        utils.info("ClusterEntityProtocol: cleanup_dead: xcleanup_ids: {}, xrecovered_ids: {}, xrecovery_pending_ids: {}, xcleanup_pending_ids: {}".format(
            xcleanup_ids, xrecovered_ids, xrecovery_pending_ids, xcleanup_pending_ids))

        # do recovery first
        for xrecovery_id in xrecovery_pending_ids:
            utils.info("ClusterEntityProtocol: cleanup_dead: calling recovery: {}".format(xrecovery_id)) 
            self.recover_dead(xentity_type, xrecovery_id)

        # debug
        utils.info("ClusterEntityProtocol: cleanup_dead: entity_type: {}, xcleanup_ids: {}".format(xentity_type, xcleanup_ids))

        # also check for any entities that were left dead for long time
        xentity_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_primary_entities(xentity_type)))
       
        # variables to store ids 
        cur_time = funclib.get_utctimestamp_sec()

        # iterate
        for xentity_id in xentity_ids:
            # check if dead
            if (self.__is_alive__(xentity_type, xentity_id) == False):
                # read entity and check how long
                xentity_json = self.cluster_handler.read_most_recent_json_with_wait(cluster_common.ClusterPaths.get_primary_entity_details(xentity_type, xentity_id), ignore_missing = True)

                # check for None
                if (xentity_json is None):
                    utils.warn("ClusterEntityProtocol: cleanup_dead: entity exists without details: {}. Cleaning".format(xentity_id))
                    xlong_dead_ids.append(xentity_id)
                    continue

                # get time diff 
                xentity = cluster_common.ClusterEntity.from_json(xentity_json)
                time_diff = cur_time - xentity.ts

                # check if time_diff exceeds max 
                if (time_diff > MAX_HEARTBEAT_CLEANUP_DEAD_WAIT):
                    utils.warn("ClusterEntityProtocol: cleanup_dead: entity not cleaned and dead for long time: {}, time_diff: {} seconds".format(xentity_id, time_diff))
                    xlong_dead_ids.append(xentity_id)
                   
        # iterate and remove
        xcleanup_union_long_dead_ids = xcleanup_pending_ids.union(xlong_dead_ids)
        for xcleanup_id in xcleanup_union_long_dead_ids:
            utils.info("ClusterEntityProtocol: cleanup_dead: entity_type: {}, remove entity_id: {}".format(xentity_type, xcleanup_id))

            # delete the root path based entities
            if (xentity_type == cluster_common.ClusterPaths.MASTER):
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_master_candidate(xcleanup_id), ignore_missing = True)
            elif (xentity_type == cluster_common.ClusterPaths.MANAGER):
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_manager(xcleanup_id), ignore_missing = True)
            elif (xentity_type == cluster_common.ClusterPaths.CLIENT):
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_client(xcleanup_id), ignore_missing = True)
            elif (xentity_type == cluster_common.ClusterPaths.SESSION):
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_session(xcleanup_id), ignore_missing = True)
            elif (xentity_type == cluster_common.ClusterPaths.WORKER):
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_worker_jobs_batches(xentity_id), ignore_missing = True)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_worker(xcleanup_id), ignore_missing = True)
            elif (xentity_type == cluster_common.ClusterPaths.JOB):
                # self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job(xentity_id), ignore_missing = True)
                utils.info("ClusterEntityProtocol: cleanup_dead: {}, job is taken care by manager".format(xentity_id))
            else:
                raise Exception("ClusterEntityProtocol: cleanup_dead: unknown xentity_type: {}".format(xentity_type))

            # delete the entity
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_primary_entity(xentity_type, xcleanup_id), ignore_missing = True)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_active_entity(xentity_type, xcleanup_id), ignore_missing = True)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_registered_entity(xentity_type, xcleanup_id), ignore_missing = True)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_cleanup_entity(xentity_type, xcleanup_id), ignore_missing = True)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_recovered_entity(xentity_type, xcleanup_id), ignore_missing = True)

# Master Protocol
class ClusterMasterProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)
        self.cluster_props = cluster_common.ClusterProps.new({"SPLUNK": 4, "S3": 10})
        self.is_master_current = False
        # self.__start__()

    def is_supervised(self, entity_type):
        return entity_type in [cluster_common.ClusterPaths.MASTER, cluster_common.ClusterPaths.MANAGER, cluster_common.ClusterPaths.SESSION, cluster_common.ClusterPaths.CLIENT, cluster_common.ClusterPaths.WORKER, cluster_common.ClusterPaths.JOB]

    # Use get_entity_type(). TODO
    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.MASTER

    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        if (self.is_master_current == False):
            utils.debug("ClusterMasterProtocol: is_operation_allowed: False as not the current master: {}".format(self.is_master_current))
            return False
        else:
            return True 

    def __start__(self):
        # check if cluster is bootstrapped
        # if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_master()) == False):
        #     utils.info("ClusterMasterProtocol: Cluster is not bootstrapped yet")

        #     # create the main directory for the cluster
        #     utils.info("ClusterMasterProtocol: __start__: creating master")
        #     self.cluster_handler.create(cluster_common.ClusterPaths.get_master())
        #     self.cluster_handler.create(cluster_common.ClusterPaths.get_master_current())
        #     self.cluster_handler.create(cluster_common.ClusterPaths.get_master_candidates())
 
        # set the current state
        if (self.__has_master_current__()):
            if (self.__is_current_master__()):
                self.is_master_current = True
            else:
                self.is_master_current = False
        else:
            self.is_master_current = False

        # do initialize
        # if (self.is_master_current):
        #     # initialize if not done yet
        #     if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_managers()) == False):
        #         self.initialize_cluster()

        # initialize candidate
        # self.initialize_candidate()

    def __has_master_current__(self):
        # check if master current exists.
        if (self.cluster_handler.is_non_empty_dir(cluster_common.ClusterPaths.get_master_current())):
            # check for active master entity. TODO. Needs to take care of restarted master and run election again in that case.
            master_current = cluster_common.ClusterMaster.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_master_current()))

            # the heartbeat is in the entity. TODO: this is getting complex
            master_current_entity = cluster_common.ClusterMaster.from_json(self.cluster_handler.read_most_recent_json(
               cluster_common.ClusterPaths.get_primary_entity_details(master_current.entity_type, master_current.entity_id)))

            # compare the heartbeat time
            time_diff = funclib.get_utctimestamp_sec() - master_current_entity.ts 
            if (time_diff >= MAX_HEARTBEAT_WAIT):
                utils.warn("__has_master_current__: current master has expired: {}, time_diff: {} seconds".format(master_current.entity_id, time_diff))
                return False
            else:
                utils.debug("__has_master_current__: elected master: {}".format(master_current.entity_id))
                return True

        utils.info("__has_master_current__: no elected master")
        return False

    def __is_current_master__(self):
        # check if the master is already elected
        if (self.__has_master_current__()):
            # read master current
            master_current = cluster_common.ClusterMaster.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_master_current()))

            # compare the ids
            if (master_current.entity_id == self.get_entity_id()):
                utils.info("__is_current_master__: already the master current: {}".format(master_current.entity_id))
                return True
            else:
                utils.info("__is_current_master__: another master exists: {}".format(master_current.entity_id))
        else:
            # all other scenarios, its false
            utils.info("__is_current_master__: no master current")
            return False

    def initialize_cluster(self):
        utils.info("initialize_cluster: called")

        # create an entry into master candidates
        self.cluster_handler.create(cluster_common.ClusterPaths.get_managers()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_clients()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_sessions()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_workers()) 

        # create other top level paths
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_statuses()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_statuses_incoming()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_statuses_active()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_statuses_completed()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_statuses_cleanup()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_batches()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_jobs_batches_statuses()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_workers_jobs_batches()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data()) 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data_jobs())

        # create base path of entities
        self.cluster_handler.create(cluster_common.ClusterPaths.get_entities())

        # create all entities with life cycle management
        xentity_types = [cluster_common.ClusterPaths.MASTER, cluster_common.ClusterPaths.MANAGER, cluster_common.ClusterPaths.CLIENT, cluster_common.ClusterPaths.SESSION, cluster_common.ClusterPaths.WORKER, cluster_common.ClusterPaths.JOB]
        
        # iterate
        for xentity_type in xentity_types:
            self.cluster_handler.create(cluster_common.ClusterPaths.get_aborted_entities(xentity_type)) 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_active_entities(xentity_type)) 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_cleanup_entities(xentity_type)) 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_primary_entities(xentity_type)) 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_recovered_entities(xentity_type)) 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_registered_entities(xentity_type)) 

        # master jobs
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_jobs_statuses())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_jobs_statuses_incoming())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_jobs_statuses_active())

        # master candidates
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_current())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_candidates())

    def initialize_candidate(self):
        # create master candidates
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_candidates())

        # initialize candidate
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_candidate(self.get_entity_id()))
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_master_candidate(self.get_entity_id()), self.get_entity())

    def cleanup_candidates(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: cleanup_candidates: not allowed because of lack of ownership")
            return

        # check for candidates that dont have active entity
        candidate_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_master_candidates()))

        # create array for cleanup
        cleanup_ids = []

        # validation 
        if (self.get_entity_id() not in candidate_ids):
            raise Exception("ClusterMasterProtocol: cleanup_candidates: self not found in candidate_ids: {}, {}".format(self.get_entity_id(), candidate_ids))

        # remove self from the list of candidate ids
        candidate_ids.remove(self.get_entity_id())

        # for each candidate id, check if it is alive. If not then delete the entry
        cur_time = funclib.get_utctimestamp_sec()
        if (len(candidate_ids) > 0):
            for candidate_id in candidate_ids:
                # check if alive
                if (self.__is_alive__(self.get_entity_type(), candidate_id) == False):
                    # cleanup
                    cleanup_ids.append(candidate_id)

        # iterate through cleanup candidate ids and delete
        for candidate_id in cleanup_ids:
            utils.info("ClusterMasterProtocol: cleanup_candidates: candidate cleanup: {}".format(candidate_id))
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_master_candidate(candidate_id), ignore_missing = True)

    def run_election(self):
        # check if master already exists and is valid
        if (self.__has_master_current__()):
            if (self.__is_current_master__()):
                self.is_master_current = True
            else:
                self.is_master_current = False
            utils.info("run_election: master exists.")
            return

        # get the current master id
        utils.info("run_election: candidate id: {}".format(self.get_entity_id()))

        # no master exists
        candidate_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_master_candidates())

        # need to wait for a while. TODO
        utils.info("run_election: Master: {} sleeping for 1 seconds for eventual consistency".format(self.get_entity_id()))
        time.sleep(1)

        # get all the candidates. pick the one with least timestamp
        min_ts = -1
        min_ts_candidate = None

        # check if there are any candidates
        if (candidate_ids is None or len(candidate_ids) == 0):
            utils.info("run_election: no candidates found.")
            self.is_master_current = False
            return

        # iterate over all candidates
        for candidate_id in candidate_ids:
            # check if the candidate is alive
            if (self.__is_alive__(cluster_common.ClusterPaths.MASTER, candidate_id) == False):
                utils.info("ClusterMasterProtocol: run_election: found dead candidate: {}. Ignoring.".format(candidate_id))
                continue

            # parse the candidate
            candidate = cluster_common.ClusterMaster.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_master_candidate(candidate_id)))

            # find the one with the min
            if (min_ts == -1 or min_ts > candidate.ts):
                min_ts = candidate.ts
                min_ts_candidate = candidate
            elif (min_ts == candidate.ts):
                if (candidate.entity_id < self.get_entity_id()):
                    min_ts_candidate = candidate

        # pick the winner. Check if the winner is the same as the current
        if (min_ts_candidate.entity_id == self.get_entity_id()):
            # elected as winner. Create an entry into the /master/current
            self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_master_current(), min_ts_candidate)
            utils.info("Won election. {}".format(self.get_entity_id()))
            self.is_master_current = True
        else:
            utils.info("Lost election. Found another winner: {}".format(min_ts_candidate.entity_id))
            self.is_master_current = False

    # def create_client(self, client):
    #     # check if this operation is allowed
    #     if (self.is_operation_allowed() == False):
    #         utils.debug("ClusterMasterProtocol: process_jobs: This operation is not allowed by this entity because of lack of ownership")
    #         return

    #     # create under clients
    #     client_protocol = ClusterClientProtocol(client)
    #     client_protocol.initialize()
    #     self.cluster_handler.create(cluster_common.ClusterPaths.get_client_sessions(client.entity_id))

    def process_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: process_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # look at the list jobs
        master_incoming_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_master_jobs_statuses_incoming()))
        master_active_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_master_jobs_statuses_active()))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        master_new_incoming_job_ids = list(master_incoming_job_ids.difference(master_active_job_ids).difference(completed_job_ids))

        # list all active managers
        active_manager_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(cluster_common.ClusterPaths.MANAGER))
        random.shuffle(active_manager_ids)

        # defensive programming
        if (len(master_new_incoming_job_ids) > 0 and len(active_manager_ids) == 0):
            utils.warn("ClusterMasterProtocol: process_jobs: there are new jobs but no active managers: non_active_jobs: {}".format(master_new_incoming_job_ids))
            return

        # debugging
        utils.info("ClusterMasterProtocol: process_jobs: master_incoming_job_ids: {}, master_active_job_ids: {}, completed_job_ids: {}, master_new_incoming_job_ids: {}".format(
            master_incoming_job_ids, master_active_job_ids, completed_job_ids, master_new_incoming_job_ids))

        # if there are jobs to process 
        if (len(master_new_incoming_job_ids) > 0):
            # sleeping for 10 seconds to care of EC
            utils.info("ClusterMasterProtocol: process_jobs: sleeping for {} seconds".format(CLEANUP_SLEEP_SECS))
            time.sleep(CLEANUP_SLEEP_SECS)

        # process which ones are not active yet
        for i in range(len(master_new_incoming_job_ids)):
            # job_id
            job_id = master_new_incoming_job_ids[i]

            # read the job details
            job = cluster_common.ClusterJob.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))

            # check the available cluster resources
            utils.debug("ClusterMasterProtocol: process_jobs: need to check available cluster resources before assigning job")

            # assign a new manager
            assigned_manager_index = i % len(active_manager_ids) 
            assigned_manager_id = active_manager_ids[assigned_manager_index]

            # debug
            utils.info("ClusterMasterProtocol: job_id: {}, assigned_manager_id: {}".format(job_id, assigned_manager_id))

            # TODO: this is flaky design
            job_ref = cluster_common.ClusterJobRef.new(job.job_spec.client_id, job.job_spec.session_id, assigned_manager_id, job_id)

            # update the jobs manager
            self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_job_manager(job_id), job_ref)

            # notify the manager that a new job is created
            self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job(assigned_manager_id, job_id))

            # create entry in master active 
            self.cluster_handler.create(cluster_common.ClusterPaths.get_master_job_statuses_active(job_id))

    def cleanup_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: cleanup_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # read all the cleanup job ids
        cleanup_job_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_cleanup())

        # if there is cleanup
        if (len(cleanup_job_ids) > 0):
            # sleeping for 10 seconds to care of EC
            utils.info("ClusterMasterProtocol: cleanup_jobs: sleeping for {} seconds".format(CLEANUP_SLEEP_SECS))
            time.sleep(CLEANUP_SLEEP_SECS)

        # ignore_missing flag
        ignore_missing = True

        # iterate and delete
        for job_id in cleanup_job_ids:
            # debug
            utils.info("ClusterMasterProtocol: cleanup_jobs: job_id: {}".format(job_id))

            # delete entities
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.JOB, job_id), ignore_missing = ignore_missing)

            # delete from incoming, active and completed
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_statuses_incoming(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_statuses_active(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_statuses_completed(job_id), ignore_missing = ignore_missing)

            # delete from master incoming and active
            self.cluster_handler.remove_dir(cluster_common.ClusterPaths.get_master_job_statuses_incoming(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir(cluster_common.ClusterPaths.get_master_job_statuses_active(job_id), ignore_missing = ignore_missing)

            # finally delete the cleanup flag
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_statuses_cleanup(job_id), ignore_missing = ignore_missing)

    def process_managers(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: process_managers: not allowed because of lack of ownership")
            return

        self.update_registered(cluster_common.ClusterPaths.MANAGER)
        self.update_active(cluster_common.ClusterPaths.MANAGER)
            
    def process_workers(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: process_workers: not allowed because of lack of ownership")
            return

        self.update_registered(cluster_common.ClusterPaths.WORKER)
        self.update_active(cluster_common.ClusterPaths.WORKER)

    # TODO: need a better name
    def process_cleanup(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: process_cleanup: not allowed because of lack of ownership")
            return

        self.cleanup_dead(cluster_common.ClusterPaths.MASTER)
        self.cleanup_dead(cluster_common.ClusterPaths.MANAGER)
        self.cleanup_dead(cluster_common.ClusterPaths.CLIENT)
        self.cleanup_dead(cluster_common.ClusterPaths.SESSION)
        self.cleanup_dead(cluster_common.ClusterPaths.WORKER)
        # TODO: not sure about the job as its ownership is with Manager
        # self.cleanup_dead(cluster_common.ClusterPaths.JOB)
            
    # last resort method as a stop gap to recover failure in job managers
    def recover_jobs_failsafe(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterMasterProtocol: recover_jobs_failsafe: not allowed because of lack of ownership")
            return

        # look for all the jobs. use /entities reference
        job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_primary_entities(cluster_common.ClusterPaths.JOB)))
        cleanup_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_cleanup_entities(cluster_common.ClusterPaths.JOB)))
        non_cleanup_job_ids = list(job_ids.difference(cleanup_job_ids))

        # check active managers
        active_manager_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(cluster_common.ClusterPaths.MANAGER))
        random.shuffle(active_manager_ids)

        # flag for assigning new manager
        assign_new_manager = False

        # check which jobs are assigned to which managers
        for i in range(len(non_cleanup_job_ids)):
            # job_id
            job_id = non_cleanup_job_ids[i]

            # debug
            utils.info("ClusterMasterProtocol: recover_jobs_failsafe: job_id: {}".format(job_id))

            # TODO: JOB entity is confusing. check for /jobs again
            if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_job(job_id)) == False):
                utils.info("ClusterMasterProtocol: recover_jobs_failsafe: {}, entity present but /jobs missing. Ignoring".format(job_id))
                continue

            # check if job has reference to manager
            if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_job_manager(job_id))):
                # read job ref
                job_ref = cluster_common.ClusterJobRef.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_manager(job_id)))
            
                # check if the manager exists and has reference to the job
                if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_primary_entity(cluster_common.ClusterPaths.MANAGER, job_ref.manager_id))):
                    # check if the manager has the job reference
                    if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_manager_job(job_ref.manager_id, job_id)) == False):
                        # create the entry
                        utils.info("ClusterMasterProtocol: recover_jobs_failsafe: job_id: {}, manager_id: {}, missing job_id in manager. creating".format(job_id, job_ref.manager_id))
                        self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job(job_ref.manager_id, job_id))

                        # create entry in master active 
                        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_job_statuses_active(job_id))

                        # update flag
                        assign_new_manager = False
                else:
                    assign_new_manager = True
            else:
                assign_new_manager = True

            # check if a new manager needs to be assigned
            if (assign_new_manager == True):
                # read the job details
                job = cluster_common.ClusterJob.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))

                # the assigned manager is not valid anymore. find a new one
                assigned_manager_index = i % len(active_manager_ids) 
                assigned_manager_id = active_manager_ids[assigned_manager_index]

                # debug
                utils.info("ClusterMasterProtocol: recover_jobs_failsafe: job_id: {}, assigning new manager: {}".format(job_id, assigned_manager_id)) 

                # TODO: this is flaky design
                job_ref = cluster_common.ClusterJobRef.new(job.job_spec.client_id, job.job_spec.session_id, assigned_manager_id, job_id)

                # update the jobs manager
                self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_job_manager(job_id), job_ref)

                # notify the new manager that a new job is created
                self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job(assigned_manager_id, job_id))

                # create entry in master active 
                self.cluster_handler.create(cluster_common.ClusterPaths.get_master_job_statuses_active(job_id))

                # TODO: delete the manager_job_status. this is hacky
                utils.warn("ClusterMasterProtocol: recover_jobs_failsafe: {}, not sure if clean the manager_job status or not".format(job_id))
                # self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_manager_job_status(assigned_manager_id, job_id), ignore_missing = True)

# Manager Protocol
class ClusterManagerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def is_supervised(self, entity_type):
        return entity_type in [cluster_common.ClusterPaths.JOB]

    # TODO: Check the ownership rights for this protocol
    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        return True

    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.MANAGER

    def get_manager_id(self):
        return self.get_entity_id()

    def initialize(self):
        # call base class
        super().initialize()

        # create entry in managers
        self.cluster_handler.create(cluster_common.ClusterPaths.get_manager(self.get_manager_id()))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_details(self.get_manager_id()))
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_details(self.get_manager_id()), self.get_entity())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_jobs(self.get_manager_id()))

    def process_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterManagerProtocol: process_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # manager_id in a variable
        manager_id = self.get_manager_id()

        # get the list of jobs that are assigned
        job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_manager_jobs(manager_id)))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        non_completed_job_ids = job_ids.difference(completed_job_ids)
        utils.info("ClusterManagerProtocol: process_jobs: manager_id: {}, job_ids: {}, completed_job_ids: {}, non_completed_job_ids: {}".format(manager_id, job_ids, completed_job_ids, non_completed_job_ids))

        # if there are jobs to process 
        if (len(non_completed_job_ids) > 0):
            # sleeping for 10 seconds to care of EC
            utils.info("ClusterManagerProtocol: process_jobs: sleeping for {} seconds before checking non_completed_job_ids: ".format(CLEANUP_SLEEP_SECS))
            time.sleep(CLEANUP_SLEEP_SECS)

        # iterate over job_ids
        for job_id in non_completed_job_ids:
            # debug
            utils.info("ClusterManagerProtocol: process_jobs: manager_id: {}, job_id: {}".format(manager_id, job_id))

            # read the job spec
            job = cluster_common.ClusterJob.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))

            # manager_job_status. TODO: short this
            manager_job_status = None

            # create the status as the manager needs to be owner throughout. otherwise timestamp skews across machine can affect latest value
            if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id)) == False):
                # create new manager job status
                manager_job_status = cluster_common.ClusterManagerJobStatus.new(job_id, cluster_common.ClusterManagerJobStatus.INITIAL, funclib.get_utctimestamp_sec())

                # update
                self.cluster_handler.create(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id))

                # create the dynamic value
                self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id), manager_job_status)
            else:
                # read the status
                manager_job_status = cluster_common.ClusterManagerJobStatus.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id)))

                # the manager_job_status can still be null. try next round
                if (manager_job_status is None):
                    raise Exception("ClusterManagerProtocol: process_jobs: manager_id: {}, job_id: {}, manager_job_status is None".format(manager_id, job_id))

            # debug
            utils.info("ClusterManagerProtocol: process_jobs: manager_id: {}, job_id: {}, manager_job_status: {}".format(manager_id, job_id, manager_job_status.to_json()))

            # status = INITIAL
            if (manager_job_status.status == cluster_common.ClusterManagerJobStatus.INITIAL):
                # mark the job as active
                self.cluster_handler.create(cluster_common.ClusterPaths.get_job_statuses_active(job_id))

                # do a force check
                utils.info("ClusterManagerProtocol: manager_id: {}, job_id: {}, active".format(manager_id, job_id, self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_statuses_active(job_id))))

                # create batches
                self.__create_batches__(job_id, job.job_spec)
                
                # update status
                manager_job_status.status = cluster_common.ClusterManagerJobStatus.BATCHES_CREATED
                manager_job_status.ts = funclib.get_utctimestamp_sec()
                self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id), manager_job_status)

            # status = BATCHES_CREATED
            if (manager_job_status.status == cluster_common.ClusterManagerJobStatus.BATCHES_CREATED):
                # assign batches to workers
                assign_batches_status = self.__assign_batches__(job)
                if (assign_batches_status == True):
                    # update status
                    manager_job_status.status = cluster_common.ClusterManagerJobStatus.BATCHES_ASSIGNED
                    manager_job_status.ts = funclib.get_utctimestamp_sec()
                    self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id), manager_job_status)
                else:
                    # raise exception
                    raise Exception("ClusterManagerProtocol: process_jobs: manager_id: {}, job_id: {}, assign_batches_status: {}".format(manager_id, job_id, assign_batches_status))

    def monitor_dead_workers(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterManagerProtocol: monitor_dead_workers: This operation is not allowed by this entity because of lack of ownership")
            return

        # manager_id in a variable
        manager_id = self.get_manager_id()

        # get the list of jobs that are assigned
        manager_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_manager_jobs(manager_id)))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        cleanup_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_cleanup()))

        # cleanup
        manager_cleanup_job_ids = manager_job_ids.intersection(completed_job_ids.difference(cleanup_job_ids))

        # need to do processing
        manager_non_completed_job_ids = manager_job_ids.difference(completed_job_ids.union(cleanup_job_ids))

        # assign to active workers
        active_worker_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(cluster_common.ClusterPaths.WORKER))
        random.shuffle(active_worker_ids)

        # for each non completed job, check for dead workers and look for assigning new resources
        for job_id in manager_non_completed_job_ids:
            # get the dead workers
            assigned_worker_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_workers(job_id)))
            # its possible that the list of dead_workers is old. So always keep track of currently active workers 
            assigned_active_worker_ids = assigned_worker_ids.intersection(set(active_worker_ids))
            dead_worker_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_dead_workers(job_id)))
            recovered_worker_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_recovered_workers(job_id)))
            cur_active_worker_ids = assigned_active_worker_ids.difference(dead_worker_ids)

            # get the list of batch ids that are already with cur active workers
            cur_active_batch_ids = []
            for cur_active_worker_id in cur_active_worker_ids:
                batch_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batches(cur_active_worker_id, job_id))
                # add to the list of currently active batch ids
                for batch_id in batch_ids:
                    cur_active_batch_ids.append(batch_id)

            # get the list of dead workers that are not recoverd yet
            non_recovered_worker_ids = dead_worker_ids.difference(recovered_worker_ids)

            # debug
            utils.info("ClusterManagerProtocol: monitor_dead_workers: manager_id: {}, job_id: {}, non_recovered_worker_ids: {}".format(manager_id, job_id, non_recovered_worker_ids))

            # iterate and do recovery. TODO: This is very transactional and failure here can break batch assignment
            for worker_id in non_recovered_worker_ids:
                # debug
                utils.info("ClusterManagerProtocol: monitor_dead_workers: manager_id: {}, job_id: {}, worker_id: {}".format(manager_id, job_id, worker_id))

                # get the list of batches for the worker 
                worker_batch_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batches(worker_id, job_id)))

                # get the list of completed batches
                completed_batch_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id)))

                # list of non completed batch ids
                non_completed_batch_ids = list(worker_batch_ids.difference(completed_batch_ids))

                def __monitor_dead_workers_assign_batch_id__(batch_id, assigned_worker_id):
                    # debug
                    utils.info("ClusterManagerProtocol: monitor_dead_workers: manager_id: {}, worker_id: {}, job_id: {}, batch_id: {}, new assigned worker_id: {}".format(manager_id, worker_id, job_id, batch_id, assigned_worker_id))

                    # create entry for this worker under job
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_job_worker(job_id, assigned_worker_id))

                    # create entry for job under this new worker
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_job(assigned_worker_id, job_id))

                    # create entry for the batch under this worker
                    self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batch(assigned_worker_id, job_id, batch_id))

                    # return
                    return True

                # assign the non completed batch ids to the available workers
                tasks = []
                for i in range(len(non_completed_batch_ids)):
                    # batch_id
                    batch_id = non_completed_batch_ids[i]

                    # debug
                    utils.info("ClusterManagerProtocol: monitor_dead_workers: manager_id: {}, worker_id: {}, job_id: {}, batch_id: {}".format(manager_id, worker_id, job_id, batch_id))

                    # ignore if the batch id is already part of a currently active worker
                    if (batch_id in cur_active_batch_ids):
                        utils.info("ClusterManagerProtocol: monitor_dead_workers: manager_id: {}, worker_id: {}, job_id: {}, batch_id: {} already in active worker. Ignoring".format(manager_id, worker_id, job_id, batch_id))
                        continue

                    # assign to an available work
                    assigned_worker_id = active_worker_ids[i % len(active_worker_ids)]

                    # add task
                    tasks.append(utils.ThreadPoolTask(__monitor_dead_workers_assign_batch_id__, batch_id, assigned_worker_id))

                # execute tasks
                utils.run_with_thread_pool(tasks, num_par = 10, post_wait_sec = POST_THREAD_POOL_WAIT_SEC)

                # after all this, mark this worker as recovered. TODO: WARN: This is transactional and can create a cycle
                utils.warn("ClusterManagerProtocol: monitor_dead_workers: adding marker for recovered workers can create cycle") 
                self.cluster_handler.create(cluster_common.ClusterPaths.get_job_recovered_worker(job_id, worker_id))
                self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_recovered_job(worker_id, job_id))

    def monitor_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterManagerProtocol: monitor_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # manager_id in a variable
        manager_id = self.get_manager_id()

        # get the list of jobs that are assigned
        manager_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_manager_jobs(manager_id)))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        cleanup_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_cleanup()))

        # cleanup
        manager_cleanup_job_ids = manager_job_ids.intersection(completed_job_ids.difference(cleanup_job_ids))

        # need to do processing
        manager_non_completed_job_ids = manager_job_ids.difference(completed_job_ids.union(cleanup_job_ids))

        # debug
        utils.info("ClusterManagerProtocol: monitor_jobs: manager_id: {}, manager_job_ids: {}, completed_job_ids: {},  cleanup_job_ids: {}, manager_cleanup_job_ids: {}, manager_non_completed_job_ids: {}".format(
            manager_id, manager_job_ids, completed_job_ids, cleanup_job_ids, manager_cleanup_job_ids, manager_non_completed_job_ids))

        # iterate over non_completed_job_ids job_ids
        for job_id in manager_non_completed_job_ids:
            # debug
            utils.info("ClusterManagerProtocol: monitor_jobs: manager_id: {}, non_completed_job_ids job_id: {}".format(manager_id, job_id))

            # take the list of batches
            batch_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches(job_id))

            # check the status for all the batches
            active_batch_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_active(job_id)) 
            completed_batch_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id)) 

            # check if all are completed 
            if (len(batch_ids) > 0 and len(batch_ids) == len(set(completed_batch_ids))):
                utils.info("ClusterManagerProtocol: monitor_jobs: manager_id: {}, job_id: {} is completed".format(manager_id, job_id))

                # update manager job status
                self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_manager_job_status(manager_id, job_id),
                    cluster_common.ClusterManagerJobStatus.new(job_id, cluster_common.ClusterManagerJobStatus.COMPLETED, funclib.get_utctimestamp_sec()))

                # need to create the output reference file in job output location
                job = cluster_common.ClusterJob.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))
                self.__create_job_output__(job_id, completed_batch_ids, job.job_spec.output_path)

                # create entry in completed
                self.cluster_handler.create(cluster_common.ClusterPaths.get_job_statuses_completed(job_id))
            else:
                utils.info("ClusterManagerProtocol: monitor_jobs: manager_id: {}, job_id: {} is active. num batches: {}, completed batches: {}".format(
                    manager_id, job_id, len(set(batch_ids)), len(set(completed_batch_ids))))
               
    def cleanup_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterManagerProtocol: cleanup_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # manager_id in a variable
        manager_id = self.get_manager_id()

        # get differenct job ids
        manager_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_manager_jobs(manager_id)))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        cleanup_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_cleanup()))

        # cleanup
        manager_cleanup_job_ids = manager_job_ids.intersection(completed_job_ids.difference(cleanup_job_ids))

        # debug
        utils.info("ClusterManagerProtocol: cleanup_job_ids: manager_id: {}, manager_job_ids: {}, completed_job_ids: {},  cleanup_job_ids: {}, manager_cleanup_job_ids: {}".format(
            manager_id, manager_job_ids, completed_job_ids, cleanup_job_ids, manager_cleanup_job_ids))

        # if there is cleanup
        if (len(manager_cleanup_job_ids) > 0):
            # sleeping for 10 seconds to care of EC
            utils.info("ClusterManagerProtocol: cleanup_jobs: sleeping for {} seconds".format(CLEANUP_SLEEP_SECS))
            time.sleep(CLEANUP_SLEEP_SECS)

        # ignore missing 
        ignore_missing = True

        # iterate over cleanup
        for job_id in manager_cleanup_job_ids:
            # debug
            utils.info("ClusterManagerProtocol: monitor_jobs: manager_id: {}, cleanup job_id: {}".format(manager_id, job_id))

            # read job spec
            job = cluster_common.ClusterJob.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_details(job_id)))

            # delete the entry from all workers
            for worker_id in self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_workers(job_id)):
                utils.info("ClusterManagerProtocol: monitor_jobs: removing entries from workers: worker_id: {}, job_id: {}".format(worker_id, job_id))
                utils.warn("ClusterManagerProtocol: monitor_jobs: TODO: this is affecting recovery. Need to fix the logic")
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_worker_jobs_batches_job(worker_id, job_id), ignore_missing = ignore_missing)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_worker_job(worker_id, job_id), ignore_missing = ignore_missing)

            # method for faster cleanup
            def __monitor_jobs_batch_delete_task__(batch_id):
                # remove individually
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batch(job_id, batch_id), ignore_missing = ignore_missing)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batch_statuses_incoming(job_id, batch_id), ignore_missing = ignore_missing)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batch_statuses_active(job_id, batch_id), ignore_missing = ignore_missing)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batch_statuses_completed(job_id, batch_id), ignore_missing = ignore_missing)
                self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_data_job_batch(job_id, batch_id), ignore_missing = ignore_missing)

                # return
                return True
               
            # iterate through all batches separately for performance. TODO
            tasks = []
            for i in range(job.job_spec.num_batches):
                # create batch_id
                batch_id = "batch-{:04d}".format(i + 1)

                # add task
                tasks.append(utils.ThreadPoolTask(__monitor_jobs_batch_delete_task__, batch_id))

            # execute tasks
            utils.run_with_thread_pool(tasks, num_par = 10, post_wait_sec = POST_THREAD_POOL_WAIT_SEC)

            # delete all job batches
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batches(job_id), ignore_missing = ignore_missing)

            # delete all job batches statuses
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batches_statuses_incoming(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batches_statuses_active(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id), ignore_missing = ignore_missing)
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job_batches_statuses(job_id), ignore_missing = ignore_missing)

            # delete session job.
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_session_job(job.job_spec.session_id, job_id), ignore_missing = ignore_missing)

            # delete entry from the current manager. TODO: this wont work for job failures.
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_manager_job(manager_id, job_id), ignore_missing = ignore_missing)

            # TODO. Delete the entry from jobs here for the time being
            # utils.debug("ClusterManagerProtocol: monitor_jobs: cleanup of jobs. This should be part of JobProtocol")
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_job(job_id), ignore_missing = ignore_missing)

            # delete data
            self.cluster_handler.remove_dir_recursive(cluster_common.ClusterPaths.get_data_job(job_id), ignore_missing = ignore_missing)

            # TODO: the completed status is to be removed by master. Create entry under cleanup
            self.cluster_handler.create(cluster_common.ClusterPaths.get_job_statuses_cleanup(job_id))

    def __create_job_output__(self, job_id, batch_ids, output_path):
        # for each batch, the output data is in standard location
        paths = []
        for batch_id in batch_ids:
            paths.append(self.cluster_handler.get_full_path(cluster_common.ClusterPaths.get_data_job_batch_output(job_id, batch_id)))

        # create tsv
        xtsv = tsv.new_with_cols([cluster_common.TSVReference.OMIGO_REFERENCE_PATH], paths)

        # write the tsv
        self.cluster_handler.write_tsv(output_path, xtsv)

    def __assign_batches__(self, job):
        # job_id
        job_id = job.entity_id

        # read batch ids
        batch_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches(job_id))

        # assign to active workers
        active_worker_ids = self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_active_entities(cluster_common.ClusterPaths.WORKER))
        random.shuffle(active_worker_ids)

        # check if there are any workers
        if (len(active_worker_ids) == 0):
            utils.info("ClusterManagerProtocol: manager_id: {}, No active workers found.".format(self.get_manager_id()))
            return False

        # check how many workers are allowed. TODO: move this to better place
        max_allowed_active_worker_ids = None
        if (len(active_worker_ids) > job.job_spec.max_workers):
            utils.info("ClusterManagerProtocol: __assign_batches__: available workers more than max allowed. taking subset: {}, {}".format(len(active_worker_ids), job.job_spec.max_workers))
            max_allowed_active_worker_ids = active_worker_ids[0:job.job_spec.max_workers]
        else:
            max_allowed_active_worker_ids = active_worker_ids

        # debug
        utils.info("ClusterManagerProtocol: __assign_batches__: manager_id: {}, batch_ids: {}, active_worker_ids: {}, max_allowed_active_worker_ids: {}".format(
            self.get_manager_id(), batch_ids, active_worker_ids, max_allowed_active_worker_ids))

        # task method
        def __assign_batches_task__(batch_id, assigned_worker_id):
            # debug
            utils.info("ClusterManagerProtocol: __assign_batches__: manager_id: {}, job_id: {}, batch_id: {}, assigned_worker_id: {}".format(self.get_manager_id(), job_id, batch_id, assigned_worker_id))

            # create an entry for the worker_id in job
            self.cluster_handler.create(cluster_common.ClusterPaths.get_job_worker(job_id, assigned_worker_id))
            
            # create an entry for the job_id
            self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_job(assigned_worker_id, job_id))

            # create entry for worker job batches
            self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs_batches_jobs(assigned_worker_id))
            self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs_batches_job(assigned_worker_id, job_id))

            # create entry for batch
            self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batches(assigned_worker_id, job_id))
            self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batch(assigned_worker_id, job_id, batch_id))

            # return
            return True

        # iterate and assign
        tasks = []
        for i in range(len(batch_ids)):
            # pick a worker id
            batch_id = batch_ids[i]
            assigned_worker_id = max_allowed_active_worker_ids[i % len(max_allowed_active_worker_ids)]

            # create task
            tasks.append(utils.ThreadPoolTask(__assign_batches_task__, batch_id, assigned_worker_id))

        # execute tasks
        utils.run_with_thread_pool(tasks, num_par = 10, post_wait_sec = POST_THREAD_POOL_WAIT_SEC)

        # success
        return True

    def __create_batch_task__(self, job_id, job_spec, batch_id, input_split):
        # create paths
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data_job(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data_job_batches(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data_job_batch(job_id, batch_id))

        # create batch input
        batch_input_path = cluster_common.ClusterPaths.get_data_job_batch_input(job_id, batch_id)
        batch_output_path = cluster_common.ClusterPaths.get_data_job_batch_output(job_id, batch_id)

        # write input data
        self.cluster_handler.write_tsv(batch_input_path, input_split)

        # create batch spec
        batch_spec = cluster_common.ClusterBatchSpec.new(job_id, job_spec, batch_input_path, batch_output_path)
        batch = cluster_common.ClusterBatch.new(batch_id, batch_spec)

        # create entry under job
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batch(job_id, batch_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batch_details(job_id, batch_id))
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_job_batch_details(job_id, batch_id), batch)
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batch_statuses_incoming(job_id, batch_id))

        # return
        return True

    def __create_batches__(self, job_id, job_spec):
        # read input data
        input_data = self.cluster_handler.read_tsv(job_spec.input_path)

        # generate split batches
        input_splits = input_data.split_batches(job_spec.num_batches, preserve_order = True)
        tasks = []
        results = []

        # iterate and create tasks
        for i in range(len(input_splits)):
            # construct batch
            input_hash = utils.compute_hash("{}-{}".format(job_id, i))
            batch_id = "batch-{:04d}".format(i + 1)
            tasks.append(utils.ThreadPoolTask(self.__create_batch_task__, job_id, job_spec, batch_id, input_splits[i]))
  
        # execute tasks in thread pool
        utils.run_with_thread_pool(tasks, num_par = 10, post_wait_sec = POST_THREAD_POOL_WAIT_SEC)

# Client Protocol
class ClusterClientProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def is_supervised(self, entity_type):
        return False

    # TODO: Check the ownership rights for this protocol
    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        return True

    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.CLIENT

    def get_client_id(self):
        return self.get_entity_id()

    def initialize(self):
        # call master
        super().initialize()

        # create sessions
        self.cluster_handler.create(cluster_common.ClusterPaths.get_client_sessions(self.get_client_id()))
        
# Session Protocol
class ClusterSessionProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def is_supervised(self, entity_type):
        return False

    # TODO: Check the ownership rights for this protocol
    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        return True

    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.SESSION

    def get_session_id(self):
        return self.get_entity_id()

    def initialize(self):
        # call master
        super().initialize()

        # create under sessions
        session_id = self.get_session_id()
        self.cluster_handler.create(cluster_common.ClusterPaths.get_session(session_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_session_details(session_id))
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_session_details(session_id), self.get_entity())
        self.cluster_handler.create(cluster_common.ClusterPaths.get_session_jobs(session_id))

    def create_job(self, job):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterSessionProtocol: create_job: This operation is not allowed by this entity because of lack of ownership")
            return

        # create under sessions
        session_id = self.get_entity_id()
        self.cluster_handler.create(cluster_common.ClusterPaths.get_session_job(session_id, job.entity_id))

        # call job protocol
        job_protocol = ClusterJobProtocol(job)
        job_protocol.initialize()

        # submit to master incoming. TODO: where this should go. JobProtocol or SessionProtocol
        self.cluster_handler.create(cluster_common.ClusterPaths.get_master_job_statuses_incoming(job.entity_id))

# Worker Protocol
class ClusterWorkerProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def is_supervised(self, entity_type):
        return False

    # TODO: Check the ownership rights for this protocol
    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        return True

    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.WORKER

    def get_worker_id(self):
        return self.get_entity_id()

    def initialize(self):
        # call base class
        super().initialize()

        # create entry in workers 
        self.cluster_handler.create(cluster_common.ClusterPaths.get_worker(self.get_worker_id()))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_jobs(self.get_worker_id()))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_details(self.get_worker_id()))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_worker_recovered_jobs(self.get_worker_id()))
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_worker_details(self.get_worker_id()), self.get_entity())

    def process_jobs(self):
        # check if this operation is allowed
        if (self.is_operation_allowed() == False):
            utils.debug("ClusterWorkerProtocol: process_jobs: This operation is not allowed by this entity because of lack of ownership")
            return

        # check the worker jobs
        worker_id = self.get_worker_id()

        # get the different job ids
        job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_jobs(worker_id)))
        completed_job_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_jobs_statuses_completed()))
        non_completed_job_ids = job_ids.difference(completed_job_ids)

        # debug
        utils.info("ClusterWorkerProtocol: process_jobs: worker_id: {}, non_completed_job_ids: {}".format(worker_id, non_completed_job_ids))

        # iterate through each job. for new jobs
        for job_id in non_completed_job_ids:
            # take different batch ids
            completed_batch_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id)))

            # debug
            utils.info("ClusterWorkerProtocol: process_jobs: job_id: {}, completed_batch_ids: {}".format(job_id, len(completed_batch_ids)))

            # get the list of batches
            batch_ids = set(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_worker_jobs_batches_job_batches(worker_id, job_id)))

            # get new batch ids
            new_batch_ids = batch_ids.difference(completed_batch_ids)
 
            # create tasks
            tasks = []

            # method for task
            def __process_jobs_task__(batch_id):
                # create entry in job batch statuses active
                self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batch_statuses_active(job_id, batch_id))

                # read batch
                batch = cluster_common.ClusterBatch.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_batch_details(job_id, batch_id)))

                # process the batch. TODO
                ctsv = cluster_common.ClusterTSV(batch.batch_spec.input_path, batch.batch_spec.output_path, batch.batch_spec.job_spec.operations)
                ctsv.call()

                # create entry in completed
                self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batch_statuses_completed(job_id, batch_id))

                # return
                return True

            # process batches
            for batch_id in new_batch_ids:
                # debug
                utils.info("ClusterWorkerProtocol: process_jobs: processing batch_id: {}".format(batch_id))
 
                # add task
                tasks.append(utils.ThreadPoolTask(__process_jobs_task__, batch_id))

            # execute tasks
            utils.run_with_thread_pool(tasks, num_par = 0, post_wait_sec = POST_THREAD_POOL_WAIT_SEC)

# Job Protocol
class ClusterJobProtocol(ClusterEntityProtocol):
    def __init__(self, entity):
        super().__init__(entity)

    def is_supervised(self, entity_type):
        return False

    # TODO: Check the ownership rights for this protocol
    def is_operation_allowed(self):
        # check base class checks
        if (super().is_operation_allowed() == False):
           return False
 
        return True

    def get_protocol_entity_type(self):
        return cluster_common.ClusterPaths.JOB

    def get_job_id(self):
        return self.get_entity_id()

    def initialize(self):
        # call base class
        super().initialize()

        # create the entries
        job_id = self.get_job_id()
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_details(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_manager(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_statuses_incoming(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_workers(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_dead_workers(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_recovered_workers(job_id))

        # initialize the values
        self.cluster_handler.update_dynamic_value(cluster_common.ClusterPaths.get_job_details(job_id), self.get_entity())

        # create the directory to hold batches and statuses
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches_statuses(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches_statuses_incoming(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches_statuses_active(job_id))
        self.cluster_handler.create(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id))

