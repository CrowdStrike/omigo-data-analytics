import datetime
from datetime import timezone
import json
import os
import time
from omigo_core import tsv, utils, tsvutils, funclib, s3io_wrapper
from omigo_hydra import cluster_data
from omigo_hydra import cluster_class_reflection

# class that takes the base path in S3, and implement all distributed communication under that.
# takes care of protocol level things for future

# global constants. TODO
if ("HYDRA_PATH" in os.environ.keys()):
    HYDRA_PATH = os.environ["HYDRA_PATH"]
else:
    utils.warn_once("Use HYDRA_PATH env variable")

# global constants. TODO
if ("HYDRA_LOCAL_PATH" in os.environ.keys()):
    HYDRA_LOCAL_PATH = os.environ["HYDRA_LOCAL_PATH"]
else:
    utils.warn_once("Use HYDRA_LOCAL_PATH env variable")

# global clients
HYDRA_CLIENT_ID = None 
HYDRA_SESSION_ID = None 
HYDRA_WORKER_ID = None 
HYDRA_MANAGER_ID = None 
HYDRA_MASTER_ID = None 
HYDRA_CLUSTER_HANDLER = None

# this is repeated in s3io_wrapper. TODO
DEFAULT_WAIT_SEC = 3
DEFAULT_ATTEMPTS = 3

def create_token(entity_id, ts):
    return "{}".format(utils.compute_hash("{}-{}".format(entity_id, ts)) % 100000) 
                
def construct_dynamic_value_json():
    return "{}.{}.json".format(ClusterPaths.DYNAMIC_VALUE, funclib.get_utctimestamp_sec()) 

# class that defines all the paths
class ClusterPaths:
    DYNAMIC_VALUE = "value"

    # Entity Constants
    MASTER = "master"
    MANAGER = "manager"
    WORKER = "worker"
    CLIENT = "client"
    SESSION = "session"
    JOB = "job"

    # global constants
    def get_base_path():
        global HYDRA_PATH
        if (HYDRA_PATH is None):
            raise Exception("HYDRA_PATH is None")

        return HYDRA_PATH

    def set_base_path(path):
        global HYDRA_PATH
        HYDRA_PATH = path

    def get_client_id():
        global HYDRA_CLIENT_ID
        if (HYDRA_CLIENT_ID is None):
            raise Exception("HYDRA_CLIENT_ID is None")

        return HYDRA_CLIENT_ID

    def set_client_id(client_id):
        global HYDRA_CLIENT_ID
        HYDRA_CLIENT_ID = client_id

    def get_session_id():
        global HYDRA_SESSION_ID
        if (HYDRA_SESSION_ID is None):
            raise Exception("HYDRA_SESSION_IDis None")

        return HYDRA_SESSION_ID

    def set_session_id(session_id):
        global HYDRA_SESSION_ID
        HYDRA_SESSION_ID = session_id

    def get_worker_id():
        global HYDRA_WORKER_ID
        if (HYDRA_WORKER_ID is None):
            raise Exception("HYDRA_WORKER_ID is None")

        return HYDRA_WORKER_ID

    def set_worker_id(worker_id):
        global HYDRA_WORKER_ID
        HYDRA_WORKER_ID = worker_id

    def get_manager_id():
        global HYDRA_MANAGER_ID
        if (HYDRA_MANAGER_ID is None):
            raise Exception("HYDRA_MANAGER_ID is None")

        return HYDRA_MANAGER_ID

    def set_manager_id(manager_id):
        global HYDRA_MANAGER_ID
        HYDRA_MANAGER_ID = manager_id 

    def get_master_id():
        global HYDRA_MASTER_ID
        if (HYDRA_MASTER_ID is None):
            raise Exception("HYDRA_MASTER_ID is None")

        return HYDRA_MASTER_ID

    def set_master_id(master_id):
        global HYDRA_MASTER_ID
        HYDRA_MASTER_ID = master_id

    def get_cluster_handler():
        global HYDRA_CLUSTER_HANDLER
        if (HYDRA_CLUSTER_HANDLER is None):
            HYDRA_CLUSTER_HANDLER = ClusterFileHandler.new(ClusterPaths.get_base_path())

        return HYDRA_CLUSTER_HANDLER

    #########################################################################################
    # all entities that need life cycle management
    # MASTER, MANAGER, CLIENT, SESSION, WORKER, JOB
    #########################################################################################
    def get_entities():
        return "/entities"

    def get_primary_entities(entity_type):
        return "{}/primary/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_primary_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_primary_entities(entity_type), entity_id)

    def get_primary_entity_details(entity_type, entity_id):
        return "{}/details".format(ClusterPaths.get_primary_entity(entity_type, entity_id))

    def get_registered_entities(entity_type):
        return "{}/registered/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_registered_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_registered_entities(entity_type), entity_id)

    def get_registered_entity_details(entity_type, entity_id):
        return "{}/details".format(ClusterPaths.get_registered_entity(entity_type, entity_id))

    def get_active_entities(entity_type):
        return "{}/active/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_active_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_active_entities(entity_type), entity_id)

    def get_recovered_entities(entity_type):
        return "{}/recovered/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_recovered_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_recovered_entities(entity_type), entity_id)

    def get_aborted_entities(entity_type):
        return "{}/aborted/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_aborted_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_aborted_entities(entity_type), entity_id)

    def get_cleanup_entities(entity_type):
        return "{}/cleanup/{}s".format(ClusterPaths.get_entities(), entity_type)

    def get_cleanup_entity(entity_type, entity_id):
        return "{}/{}".format(ClusterPaths.get_cleanup_entities(entity_type), entity_id)

    #########################################################################################
    # /master
    #########################################################################################
    def get_master():
        return "/master"

    def get_master_candidates():
        return "{}/candidates".format(ClusterPaths.get_master())

    def get_master_candidate(candidate_id):
        return "{}/{}".format(ClusterPaths.get_master_candidates(), candidate_id)

    def get_master_current():
        return "{}/current".format(ClusterPaths.get_master())

    def get_master_jobs_statuses():
        return "{}/jobs-statuses".format(ClusterPaths.get_master())

    def get_master_jobs_statuses_incoming():
        return "{}/incoming".format(ClusterPaths.get_master_jobs_statuses())

    def get_master_job_statuses_incoming(job_id):
        return "{}/{}".format(ClusterPaths.get_master_jobs_statuses_incoming(), job_id)

    def get_master_jobs_statuses_active():
        return "{}/active".format(ClusterPaths.get_master_jobs_statuses())

    def get_master_job_statuses_active(job_id):
        return "{}/{}".format(ClusterPaths.get_master_jobs_statuses_active(), job_id)

    #########################################################################################
    # /clients
    #########################################################################################
    def get_clients():
        return "/clients"

    def get_client(client_id):
        return "{}/{}".format(ClusterPaths.get_clients(), client_id)

    def get_client_details(client_id):
        return "{}/details".format(ClusterPaths.get_client(client_id))

    def get_client_sessions(client_id):
        return "{}/sessions".format(ClusterPaths.get_client(client_id))

    def get_client_session(client_id, session_id):
        return "{}/{}".format(ClusterPaths.get_client_sessions(client_id), session_id)

    #########################################################################################
    # /sessions
    #########################################################################################
    def get_sessions():
        return "/sessions"

    def get_session(session_id):
        return "{}/{}".format(ClusterPaths.get_sessions(), session_id)

    def get_session_details(session_id):
        return "{}/details".format(ClusterPaths.get_session(session_id))

    def get_session_jobs(session_id):
        return "{}/jobs".format(ClusterPaths.get_session(session_id))

    def get_session_job(session_id, job_id):
        return "{}/{}".format(ClusterPaths.get_session_jobs(session_id), job_id)

    #########################################################################################
    # /jobs
    #########################################################################################
    def get_jobs():
        return "/jobs" 

    def get_job(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs(), job_id)

    def get_job_details(job_id):
        return "{}/details".format(ClusterPaths.get_job(job_id))

    def get_job_manager(job_id):
        return "{}/manager".format(ClusterPaths.get_job(job_id))

    def get_jobs_statuses():
        return "/jobs-statuses"

    def get_jobs_statuses_incoming():
        return "{}/incoming".format(ClusterPaths.get_jobs_statuses())

    def get_job_statuses_incoming(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_statuses_incoming(), job_id)

    def get_jobs_statuses_active():
        return "{}/active".format(ClusterPaths.get_jobs_statuses())

    def get_job_statuses_active(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_statuses_active(), job_id)

    def get_jobs_statuses_completed():
        return "{}/completed".format(ClusterPaths.get_jobs_statuses())

    def get_job_statuses_completed(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_statuses_completed(), job_id)

    def get_jobs_statuses_cleanup():
        return "{}/cleanup".format(ClusterPaths.get_jobs_statuses())

    def get_job_statuses_cleanup(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_statuses_cleanup(), job_id)

    def get_jobs_batches():
        return "/jobs-batches"

    def get_job_batches(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_batches(), job_id)

    def get_job_batch(job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_job_batches(job_id), batch_id)

    def get_job_batch_details(job_id, batch_id):
        return "{}/details".format(ClusterPaths.get_job_batch(job_id, batch_id))

    def get_jobs_batches_statuses():
        return "/jobs-batches-statuses"

    def get_job_batches_statuses(job_id):
        return "{}/{}".format(ClusterPaths.get_jobs_batches_statuses(), job_id)

    def get_job_batches_statuses_incoming(job_id):
        return "{}/incoming".format(ClusterPaths.get_job_batches_statuses(job_id))

    def get_job_batch_statuses_incoming(job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_job_batches_statuses_incoming(job_id), batch_id)

    def get_job_batches_statuses_active(job_id):
        return "{}/active".format(ClusterPaths.get_job_batches_statuses(job_id))

    def get_job_batch_statuses_active(job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_job_batches_statuses_active(job_id), batch_id)

    def get_job_batches_statuses_completed(job_id):
        return "{}/completed".format(ClusterPaths.get_job_batches_statuses(job_id))

    def get_job_batch_statuses_completed(job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_job_batches_statuses_completed(job_id), batch_id)

    def get_job_assigned_workers(job_id):
        return "{}/assigned-workers".format(ClusterPaths.get_job(job_id))

    def get_job_assigned_resources(job_id):
        return "{}/assigned-resources".format(ClusterPaths.get_job(job_id))

    def get_job_workers(job_id):
        return "{}/workers".format(ClusterPaths.get_job(job_id))

    def get_job_worker(job_id, worker_id):
        return "{}/{}".format(ClusterPaths.get_job_workers(job_id), worker_id)

    def get_job_dead_workers(job_id):
        return "{}/dead-workers".format(ClusterPaths.get_job(job_id))

    def get_job_dead_worker(job_id, worker_id):
        return "{}/{}".format(ClusterPaths.get_job_dead_workers(job_id), worker_id)

    def get_job_recovered_workers(job_id):
        return "{}/recovered-workers".format(ClusterPaths.get_job(job_id))

    def get_job_recovered_worker(job_id, worker_id):
        return "{}/{}".format(ClusterPaths.get_job_recovered_workers(job_id), worker_id)

    #########################################################################################
    # /managers
    #########################################################################################
    def get_managers():
        return "/managers"

    def get_manager(manager_id):
        return "{}/{}".format(ClusterPaths.get_managers(), manager_id)

    def get_manager_details(manager_id):
        return "{}/details".format(ClusterPaths.get_manager(manager_id))

    def get_manager_jobs(manager_id):
        return "{}/jobs".format(ClusterPaths.get_manager(manager_id))

    def get_manager_job(manager_id, job_id):
        return "{}/{}".format(ClusterPaths.get_manager_jobs(manager_id), job_id)

    def get_manager_job_status(manager_id, job_id):
        return "{}/status".format(ClusterPaths.get_manager_job(manager_id, job_id))

    #########################################################################################
    # /workers
    #########################################################################################
    def get_workers():
        return "/workers"

    def get_worker(worker_id):
        return "{}/{}".format(ClusterPaths.get_workers(), worker_id)

    def get_worker_details(worker_id):
        return "{}/details".format(ClusterPaths.get_worker(worker_id))

    def get_worker_jobs(worker_id):
        return "{}/jobs".format(ClusterPaths.get_worker(worker_id))

    def get_worker_job(worker_id, job_id):
        return "{}/{}".format(ClusterPaths.get_worker_jobs(worker_id), job_id)

    def get_workers_jobs_batches():
        return "/workers-jobs-batches"

    def get_worker_jobs_batches(worker_id):
        return "{}/{}".format(ClusterPaths.get_workers_jobs_batches(), worker_id)

    def get_worker_jobs_batches_jobs(worker_id):
        return "{}/jobs".format(ClusterPaths.get_worker_jobs_batches(worker_id))

    def get_worker_jobs_batches_job(worker_id, job_id):
        return "{}/{}".format(ClusterPaths.get_worker_jobs_batches_jobs(worker_id), job_id)

    def get_worker_jobs_batches_job_batches(worker_id, job_id):
        return "{}/batches".format(ClusterPaths.get_worker_jobs_batches_job(worker_id, job_id))

    def get_worker_jobs_batches_job_batch(worker_id, job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_worker_jobs_batches_job_batches(worker_id, job_id), batch_id)

    def get_worker_recovered_jobs(worker_id):
        return "{}/recovered-jobs".format(ClusterPaths.get_worker(worker_id))

    def get_worker_recovered_job(worker_id, job_id):
        return "{}/{}".format(ClusterPaths.get_worker_recovered_jobs(worker_id), job_id)

    #########################################################################################
    # /data 
    #########################################################################################
    def get_data():
        return "/data"

    def get_data_jobs():
        return "{}/jobs".format(ClusterPaths.get_data())

    def get_data_job(job_id):
        return "{}/{}".format(ClusterPaths.get_data_jobs(), job_id)

    def get_data_job_input(job_id):
        return "{}/input.tsv.gz".format(ClusterPaths.get_data_job(job_id))

    def get_data_job_output(job_id):
        return "{}/output.tsv.gz".format(ClusterPaths.get_data_job(job_id))

    def get_data_job_batches(job_id):
        return "{}/batches".format(ClusterPaths.get_data_job(job_id))

    def get_data_job_batch(job_id, batch_id):
        return "{}/{}".format(ClusterPaths.get_data_job_batches(job_id), batch_id)

    def get_data_job_batch_input(job_id, batch_id):
        return "{}/input.tsv.gz".format(ClusterPaths.get_data_job_batch(job_id, batch_id))

    def get_data_job_batch_output(job_id, batch_id):
        return "{}/output.tsv.gz".format(ClusterPaths.get_data_job_batch(job_id, batch_id))

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

    def file_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, fail_if_missing = True):
        # add fail_if_missing logic
        try:
            return self.fs.file_exists_with_wait(self.__makepath__(path), wait_sec = wait_sec, attempts = attempts)
        except Exception as e:
            # raise exception if fail_if_missing is True
            if (fail_if_missing == True):
                raise e
            else:
                return False

    def dir_exists(self, path):
        return self.fs.dir_exists(self.__makepath__(path))

    def dir_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, fail_if_missing = True):
        # add fail_if_missing logic
        try:
            return self.fs.dir_exists_with_wait(self.__makepath__(path), wait_sec = wait_sec, attempts = attempts)
        except Exception as e:
            # raise exception if fail_if_missing is True
            if (fail_if_missing == True):
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

    def read_most_recent_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, fail_if_missing = True):
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
            if (fail_if_missing == True):
                raise Exception("read_most_recent_with_wait: path: {}, data not found. attempts: over".format(path))
            else:
                return None

    def read_most_recent_json_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, fail_if_missing = True):
        content = self.read_most_recent_with_wait(path, wait_sec = wait_sec, attempts = attempts, fail_if_missing = fail_if_missing)
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

class ClusterProps(cluster_data.JsonSer):
    def __init__(self, props):
        self.props = props

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterProps.new(json_obj["props"])

    def new(props):
        return ClusterProps(props)

# Context class for cluster
class ClusterJobContext(cluster_data.JsonSer):
    def __init__(self, operations):
        self.operations = operations

    def add_operation(self, operation):
        self.operations.append(operation)

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        # deserialize operations
        operations = []
        for xobj in json_obj["operations"]:
            operations.append(ClusterOperation.from_json(xobj))
    
        # create object
        return ClusterJobContext(operations)

    def new_create_operations():
        return ClusterJobContext([])

    def new(operations):
        return ClusterJobContext(operations)
        
class ClusterOperation(cluster_data.JsonSer):
    def __init__(self, name, *args, **kwargs):
        # name of the operation
        self.name = name

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
        return ClusterOperation(name, *args, **kwargs)
                   
class ClusterJobStatus(cluster_data.JsonSer):
    # different job status
    INITIAL = "INITIAL"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"

    def __init__(self, status):
        self.status = status

    def new(status):
        return ClusterJobStatus(status)

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        return ClusterJobStatus(
            json_obj["status"])

class ClusterJobSpec(cluster_data.JsonSer):
    def __init__(self, client_id, session_id, name, input_path, output_path, job_type, operations, num_batches, max_workers, partitioning_strategy, rate_limiter):
        self.client_id = client_id
        self.session_id = session_id
        self.name = name
        self.input_path = input_path
        self.output_path = output_path
        self.job_type = job_type
        self.operations = operations 
        self.num_batches = num_batches
        self.max_workers = max_workers
        self.partitioning_strategy = partitioning_strategy
        self.rate_limiter = rate_limiter

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        # operations is a set of ClusterOperation
        operations = []
        for op in json_obj["operations"]:
            operations.append(ClusterOperation.from_json(op))

        # create job spec
        return ClusterJobSpec(
            json_obj["client_id"],
            json_obj["session_id"],
            json_obj["name"],
            json_obj["input_path"],
            json_obj["output_path"],
            json_obj["job_type"], 
            operations,
            json_obj["num_batches"],
            json_obj["max_workers"],
            json_obj["partitioning_strategy"],
            json_obj["rate_limiter"])

    def new(client_id, session_id, name, input_path, output_path, job_type, operations, num_batches, max_workers, partitioning_strategy, rate_limiter):
        return ClusterJobSpec(client_id, session_id, name, input_path, output_path, job_type, operations, num_batches, max_workers, partitioning_strategy, rate_limiter)

class ClusterEntity(cluster_data.JsonSer):
    def __init__(self, entity_type, entity_id, ts, token):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.ts = ts
        self.token = token

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterEntity.new(
            json_obj["entity_type"],
            json_obj["entity_id"],
            json_obj["ts"],
            json_obj["token"]
        )

    def new_create_token(entity_type, entity_id):
        ts = funclib.get_utctimestamp_sec()
        token = create_token(entity_id, ts)
        return ClusterEntity.new(entity_type, entity_id, ts, token)

    def new(entity_type, entity_id, ts, token): 
        return ClusterEntity(entity_type, entity_id, ts, token)

class ClusterMaster(ClusterEntity):
    def __init__(self, entity_id, ts, token):
        super().__init__(ClusterPaths.MASTER, entity_id, ts, token)

class ClusterManager(ClusterEntity):
    def __init__(self, entity_id, ts, token):
        super().__init__(ClusterPaths.MANAGER, entity_id, ts, token)

class ClusterClient(ClusterEntity):
    def __init__(self, entity_id, ts, token):
        super().__init__(ClusterPaths.CLIENT, entity_id, ts, token)

class ClusterSession(ClusterEntity):
    def __init__(self, entity_id, ts, token):
        super().__init__(ClusterPaths.SESSION, entity_id, ts, token)

class ClusterWorker(ClusterEntity):
    def __init__(self, entity_id, ts, token):
        super().__init__(ClusterPaths.WORKER, entity_id, ts, token)

class ClusterJob(ClusterEntity):
    def __init__(self, entity_id, ts, token, job_spec):
        super().__init__(ClusterPaths.JOB, entity_id, ts, token)
        self.job_spec = job_spec

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterJob.new(
            json_obj["entity_id"],
            json_obj["ts"],
            json_obj["token"],
            ClusterJobSpec.from_json(json_obj["job_spec"])
        )

    def new_create_token(entity_id, job_spec):
        ts = funclib.get_utctimestamp_sec()
        token = create_token(entity_id, ts)
        return ClusterJob(entity_id, ts, token, job_spec)

    def new(entity_id, ts, token, job_spec):
        return ClusterJob(entity_id, ts, token, job_spec)

class ClusterJobRef(cluster_data.JsonSer):
    def __init__(self, client_id, session_id, manager_id, job_id):
        self.client_id = client_id
        self.session_id = session_id
        self.manager_id = manager_id
        self.job_id = job_id

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        return ClusterJobRef.new(
            json_obj["client_id"],
            json_obj["session_id"],
            json_obj["manager_id"],
            json_obj["job_id"]
        )

    def new(client_id, session_id, manager_id, job_id):
        return ClusterJobRef(client_id, session_id, manager_id, job_id)

class ClusterBatch(cluster_data.JsonSer):
    def __init__(self, batch_id, batch_spec):
        self.batch_id = batch_id
        self.batch_spec = batch_spec

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        return ClusterBatch.new(
            json_obj["batch_id"],
            ClusterBatchSpec.from_json(json_obj["batch_spec"])
        )

    def new(batch_id, batch_spec):
        return ClusterBatch(batch_id, batch_spec)

class ClusterBatchSpec(cluster_data.JsonSer):
    def __init__(self, job_id, job_spec, input_path, output_path):
        self.job_id = job_id
        self.job_spec = job_spec
        self.input_path = input_path
        self.output_path = output_path

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        return ClusterBatchSpec.new(
            json_obj["job_id"],
            ClusterJobSpec.from_json(json_obj["job_spec"]),
            json_obj["input_path"],
            json_obj["output_path"]
        )

    def new(job_id, job_spec, input_path, output_path):
        return ClusterBatchSpec(job_id, job_spec, input_path, output_path)

class ClusterManagerJobStatus(cluster_data.JsonSer):
    INITIAL = "INITIAL"
    BATCHES_CREATED = "BATCHES_CREATED"
    BATCHES_ASSIGNED = "BATCHES_ASSIGNED"
    COMPLETED = "COMPLETED"

    def __init__(self, job_id, status, ts):
        self.job_id = job_id
        self.status = status
        self.ts = ts

    def from_json(json_obj):
        # check for None 
        if (json_obj is None):
            return None

        return ClusterManagerJobStatus.new(
            json_obj["job_id"],
            json_obj["status"],
            json_obj["ts"]
        )

    def new(job_id, status, ts):
        return ClusterManagerJobStatus(job_id, status, ts)

class TSVReference:
    def __init__(self, xtsv):
        raise Exception("This class has been moved to cluster_common_v2")

class ClusterTSV:
    def __init__(self, input_path, output_path, operations):
        self.input_path = input_path
        self.output_path = output_path
        self.operations = operations
        self.status = ClusterJobStatus.INITIAL
        self.cluster_handler = ClusterPaths.get_cluster_handler()

    def get_status(self):
        return self.status

    def call(self):
        # check if the output already exists. If yes, then return
        if (self.cluster_handler.file_exists(self.output_path)):
            utils.info("ClusterTSV: output already exists: {}".format(self.output_path))
            self.status = ClusterJobStatus.COMPLETED
            return

        # read data 
        xtsv = self.cluster_handler.read_tsv(self.input_path)
        utils.debug("ClusterTSV: call: xtsv: {}".format(xtsv.get_columns()))

        # move the status to running
        self.status = ClusterJobStatus.RUNNING

        # intiialize
        otsvs = []

        # TODO: wrap this around in a fake reference file as short term solution
        if (TSVReference.OMIGO_REFERENCE_PATH not in xtsv.get_columns()):
            utils.warn("ClusterTSV: temporary work around to read non reference file. Fix this. TODO")
            xtsv = tsv.new_with_cols([TSVReference.OMIGO_REFERENCE_PATH], data = [self.cluster_handler.get_full_path(self.input_path)])

        # get all paths
        for path in xtsv.col_as_array_uniq(TSVReference.OMIGO_REFERENCE_PATH):
            # run through all the operations. the output is initialized with input itself
            xtsv_output = tsv.read(path)
            utils.debug("ClusterTSV: call: initial input path: {}, num_rows: {}, num_cols: {}".format(path, xtsv_output.num_rows(), xtsv_output.num_cols()))

            # iterate through operations
            for operation in self.operations:
                # lookup the function to call
                func = cluster_class_reflection.load_fully_qualified_func(operation.name)
                class_reference = cluster_class_reflection.get_class_that_defined_method(func)
                args = cluster_data.load_native_objects(operation.args)
                kwargs = cluster_data.load_native_objects(operation.kwargs) 
                utils.debug("ClusterTSV: call: custom_func: name: {}, args: {}, kwargs: {}, func: {}".format(operation.name, args, kwargs, func))

                # call the function. and set the job status correctly
                try:
                    # note that custom_func is not called as the context is already in the function.
                    utils.debug("ClusterTSV: call: function name: {}, args: {}, kwargs: {}".format(func.__name__, args, kwargs))
                    class_func = getattr(xtsv_output.extend_class(class_reference), func.__name__)
                    xtsv_output = class_func(*args, **kwargs)
                except Exception as e:
                    utils.error("ClusterTSV: call: Found error while running the method: {}".format(e))
                    self.status = ClusterJobStatus.FAILED
                    raise e

            # append to the list of outputs
            otsvs.append(xtsv_output)
            utils.info("ClusterTSV: call: xtsv_output path: {}, num_rows: {}, num_cols: {}".format(path, xtsv_output.num_rows(), xtsv_output.num_cols()))

        # persist the final output. TODO: Dont know what to do here. Need error handling
        otsv = tsv.new_with_cols([".omigo.empty"]) if (len(otsvs) == 0) else tsvutils.merge(otsvs, def_val_map = {})
        self.cluster_handler.write_tsv(self.output_path, otsv)
        self.status = ClusterJobStatus.COMPLETED
        utils.info("ClusterTSV: call: Output written to: {}".format(self.output_path))

