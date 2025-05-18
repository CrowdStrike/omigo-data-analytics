from omigo_core import dataframe, utils, timefuncs
from omigo_hydra import s3io_wrapper
import json
import time
import base64

# Increment this version to rerun analysis
RUN_VERSION = "version:v01"
DEFAULT_WAIT_10SEC = 10
DEFAULT_WAIT_30SEC = 30
DEFAULT_WAIT_60SEC = 60

# Class RunnerBase
class RunnerBase:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.fs = s3io_wrapper.S3FSWrapper()

        # debug
        utils.info("RunnerBase: init: base_dir: {}".format(self.base_dir))

    def get_base_dir(self):
        return self.base_dir

    def get_output_dir(self):
        return "{}/output".format(self.base_dir)

    def get_job_status_dir(self):
        return "{}/job-status".format(self.base_dir)

    def get_job_tags_dir(self):
        return "{}/job-tags".format(self.base_dir)

    def get_job_status_created(self):
        return "{}/created".format(self.get_job_status_dir())

    def get_job_status_running(self):
        return "{}/running".format(self.get_job_status_dir())

    def get_job_status_completed(self):
        return "{}/completed".format(self.get_job_status_dir())

    def get_job_status_created_job_id_file(self, job_id):
        return "{}/{}".format(self.get_job_status_created(), job_id)

    def get_job_status_running_job_id_file(self, job_id):
        return "{}/{}".format(self.get_job_status_running(), job_id)

    def get_job_status_completed_job_id_file(self, job_id):
        return "{}/{}".format(self.get_job_status_completed(), job_id)

    def get_workers_dir(self):
        return "{}/workers".format(self.base_dir)

    def get_worker_dir(self, worker_id):
        return "{}/{}".format(self.get_workers_dir(), worker_id)

    def get_worker_jobs_dir(self, worker_id):
        return "{}/jobs".format(self.get_worker_dir(worker_id))

    def get_worker_job_id_file(self, worker_id, job_id):
        return "{}/{}".format(self.get_worker_jobs_dir(worker_id), job_id)

    def get_job_output_dir(self, job_id):
        return "{}/{}".format(self.get_output_dir(), job_id)

    def get_job_graph_analysis_file(self, job_id):
        return "{}/graph_analysis.tsv.gz".format(self.get_job_output_dir(job_id))

    def get_job_progressive_output_file(self, job_id):
        return "{}/progressive_output.txt".format(self.get_job_output_dir(job_id))

    def get_job_final_output_file(self, job_id):
        return "{}/final_output.txt".format(self.get_job_output_dir(job_id))

    def get_job_tag_empty_dir(self, job_id):
        return "{}/{}".format(self.get_job_tags_dir(), "empty")

    def get_job_tag_empty_dir_by_job_id(self, job_id):
        return "{}/{}".format(self.get_job_tag_empty_dir(), job_id)

    def get_job_tag_combined_dir(self):
        return "{}/{}".format(self.get_job_tags_dir(), "combined")

    def get_job_tag_empty_dir_by_job_id(self, job_id):
        return "{}/{}".format(self.get_job_tag_empty_dir(), job_id)

    def get_job_tag_combined_dir_by_job_id(self, job_id, key, value):
        value_b16 = base64.b16encode(bytes(str(value), "utf-8")).decode("utf-8")
        return "{}/{}-{}={}".format(self.get_job_tag_combined_dir(), job_id, key, value_b16)

    def get_shutdown_file(self):
        return "{}/shutdown".format(self.base_dir)

    def get_halt_file(self):
        return "{}/halt".format(self.base_dir)

    def is_shutdown_mode(self):
        return self.fs.file_exists(self.get_shutdown_file())

# class ClusterAdmin
class ClusterAdmin:
    def __init__(self, base_dir, wait_sec = DEFAULT_WAIT_60SEC):
        self.runner_base = RunnerBase(base_dir)
        self.wait_sec = wait_sec

    # create cluster
    def create_cluster(self, num_workers = 3):
        # print the cluster base path
        utils.info("create_cluster: Using base path: {}".format(self.runner_base.get_base_dir()))

        # create all paths
        self.runner_base.fs.create_dir(self.runner_base.get_base_dir())
        self.runner_base.fs.create_dir(self.runner_base.get_output_dir())
        self.runner_base.fs.create_dir(self.runner_base.get_job_status_dir())
        self.runner_base.fs.create_dir(self.runner_base.get_job_tags_dir())
        self.runner_base.fs.create_dir(self.runner_base.get_job_status_created())
        self.runner_base.fs.create_dir(self.runner_base.get_job_status_running())
        self.runner_base.fs.create_dir(self.runner_base.get_job_status_completed())
        self.runner_base.fs.create_dir(self.runner_base.get_workers_dir())
        self.runner_base.fs.create_dir(self.runner_base.get_job_tag_combined_dir())

        # create workers
        self.create_workers(num_workers)

    # create workers
    def create_workers(self, num_workers):
        # guards
        if (num_workers >= 100 or num_workers < 0):
            raise Exception("create_workers: number of workers need to be below 100: {}".format(num_workers))

        # run a loop
        for i in range(num_workers):
            worker_id = "worker{:02d}".format(i + 1)
            self.runner_base.fs.create_dir(self.runner_base.get_worker_dir(worker_id))
            self.runner_base.fs.create_dir(self.runner_base.get_worker_jobs_dir(worker_id))

    # delete job
    def delete_job(self, job_id, ignore_if_missing = True):
        # delete job tags
        for filename in self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_tag_combined_dir()):
            if (filename.startswith("{}-".format(job_id)) == True):
                self.runner_base.fs.delete_file("{}/{}".format(self.runner_base.get_job_tag_combined_dir(), filename), ignore_if_missing = ignore_if_missing)

        # delete job output dir
        if (self.runner_base.fs.dir_exists(self.runner_base.get_job_output_dir(job_id))):
            for filename in self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_output_dir(job_id)):
                self.runner_base.fs.delete_file("{}/{}".format(self.runner_base.get_job_output_dir(job_id), filename), ignore_if_missing = ignore_if_missing)

            # delete directory
            self.runner_base.fs.delete_dir_with_wait(self.runner_base.get_job_output_dir(job_id), ignore_if_missing = ignore_if_missing)

        # delete job-status
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_completed_job_id_file(job_id), ignore_if_missing = ignore_if_missing)
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_running_job_id_file(job_id), ignore_if_missing = ignore_if_missing)
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_created_job_id_file(job_id), ignore_if_missing = ignore_if_missing)

        # goto each worker and delete the entry for job if exists
        for worker_id in self.runner_base.fs.list_dirs(self.runner_base.get_workers_dir()):
            if (self.runner_base.fs.file_exists(self.runner_base.get_worker_job_id_file(worker_id, job_id)) == True):
                utils.warn("Found job meant for deletion: {} running under worker: {}".format(job_id, worker_id))
                self.runner_base.fs.delete_file(self.runner_base.get_worker_job_id_file(worker_id, job_id), ignore_if_missing = ignore_if_missing)

    def resubmit(self, job_id, ignore_if_missing = True):
        # delete job-status
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_completed_job_id_file(job_id), ignore_if_missing = ignore_if_missing)
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_running_job_id_file(job_id), ignore_if_missing = ignore_if_missing)
        self.runner_base.fs.delete_file(self.runner_base.get_job_status_created_job_id_file(job_id), ignore_if_missing = ignore_if_missing)

# Class BaseJobGenerator
class BaseJobGenerator:
    def __init__(self, base_dir, wait_sec = DEFAULT_WAIT_60SEC, num_iter = 10):
        self.runner_base = RunnerBase(base_dir)
        self.wait_sec = wait_sec
        self.num_iter = num_iter

        # debug
        utils.info("BaseJobGenerator: base_dir: {}, wait_sec: {}, num_iter: {}".format(base_dir, wait_sec, num_iter))

    # run the jobs in a loop
    def run_loop(self):
        try:
            for i in range(self.num_iter):
                # check for shutdown mode
                if (self.runner_base.is_shutdown_mode() == True):
                    utils.warn("BaseJobGenerator: shutdown mode detected. Stopping")
                    return

                # run
                self.run()

                # sleep
                utils.info("BaseJobGenerator: Iteration: {}, Sleeping for {} seconds".format(i, self.wait_sec))
                time.sleep(self.wait_sec)
        except Exception as e:
            utils.error("BaseJobGenerator: run_loop: caught exception: {}. Returning".format(e))
            return
        except:
            utils.error("BaseJobGenerator: run_loop: caught interrupt. Returning")
            return

    # implement this method
    def run(self):
        raise Exception("Not implemented in base class")

# Class JobManager
class JobManager:
    def __init__(self, base_dir, wait_sec = DEFAULT_WAIT_30SEC):
        self.runner_base = RunnerBase(base_dir)
        self.wait_sec = wait_sec
        self.max_attempts = 3

        # debug
        utils.info("JobManager: base_dir: {}, wait_sec: {}".format(base_dir, wait_sec))

    def run_loop(self):
        num_attempts = 0
        # run for given number of attempts
        while (num_attempts < self.max_attempts):
            try:
              while True:
                  # check for shutdown mode
                  if (self.runner_base.is_shutdown_mode() == True):
                      utils.warn("JobManager: shutdown mode detected. Stopping")
                      return

                  # run
                  self.run()

                  # sleep
                  utils.info("JobManager: Sleeping for {} seconds".format(self.wait_sec))
                  time.sleep(self.wait_sec)
            except Exception as e:
                utils.error("JobManager: run_loop: caught exception: {}. attemps: {} / {}".format(e, num_attempts, self.max_attempts))
                # increase
                num_attempts = num_attempts + 1
            except:
                utils.error("JobManager: run_loop: caught Interrupt: returning")
                return

    def run(self):
        # check the list of created, running, completed
        all_created = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_created())
        all_running = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_running())
        all_completed = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed())

        # dedup sets to find the actual jobs that need to be run
        new_job_ids_created = list(filter(lambda t: t not in all_running and t not in all_completed, all_created))

        # iterate
        job_xdfs = [dataframe.new_with_cols(["job_id", "submit_ts"])]
        for new_job_id in new_job_ids_created:
            job_spec_json = json.loads(self.runner_base.fs.read_file_contents_as_text(self.runner_base.get_job_status_created_job_id_file(new_job_id)))
            job_xdfs.append(dataframe.from_maps([{"job_id": job_spec_json["job_id"], "submit_ts": job_spec_json["submit_ts"]}]))

        # create single tsv
        job_xdf = dataframe.merge_union(job_xdfs)

        # iterate
        for new_job_id in job_xdf.sort("submit_ts").col_as_array("job_id"):
            # debug
            utils.info("Looking for worker for job_id: {}".format(new_job_id))

            # read job spec
            job_spec_json = json.loads(self.runner_base.fs.read_file_contents_as_text(self.runner_base.get_job_status_created_job_id_file(new_job_id)))

            # find the worker that is not running anything
            worker_ids = self.runner_base.fs.list_leaf_dir(self.runner_base.get_workers_dir())

            # iterate and find idle worker
            idle_workers = []
            for worker_id in worker_ids:
                # get the list of current jobs taken by the worker
                worker_pending_jobs = self.runner_base.fs.list_leaf_dir(self.runner_base.get_worker_jobs_dir(worker_id))

                # check if the worker is idle
                if (len(worker_pending_jobs) == 0):
                    idle_workers.append(worker_id)

            # debug
            utils.info("Idle workers: {}".format(idle_workers))

            # put the new job id in the worker queue
            if (len(idle_workers) > 0):
                worker_id = idle_workers[0]
                job_spec_json["start_ts"] = timefuncs.utctimestamp_to_datetime_str(timefuncs.get_utctimestamp_sec())
                self.runner_base.fs.write_text_file(self.runner_base.get_worker_job_id_file(worker_id, new_job_id), "")
                self.runner_base.fs.write_text_file(self.runner_base.get_job_status_running_job_id_file(new_job_id), json.dumps(job_spec_json))
                utils.info("Job Assigned: job_id: {}: worker: {}".format(new_job_id, worker_id))

# Class ReprocessBatch
class ReprocessBatchWorker:
    def __init__(self, base_dir):
        self.runner_base = RunnerBase(base_dir)

        # debug
        utils.info("ReprocessBatchWorker: base_dir: {}".format(base_dir))

    def resubmit_analysis(self, last_n, version_tag = "version:v0"):
        # get job ids
        job_ids = sorted(self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed()), reverse = True)

        # check for last_n
        job_ids = job_ids[0:last_n]

        # call rerun_specific_jobs
        for job_id in job_ids:
            self.resubmit_specific_analysis(job_id, version_tag)

    def resubmit_specific_analysis(self, job_id, version_tag):
        utils.info("ReprocessBatchWorker: resubmit_specific_analysis: {}".format(job_id))
        analysis_file = self.runner_base.get_job_graph_analysis_file(job_id)
        if (tsv.exists(analysis_file)):
            # read old file, drop previous version
            tsv.read(analysis_file) \
                .drop_cols_with_prefix("version", ignore_if_missing = True) \
                .drop_cols_if_exists("v0") \
                .add_const(version_tag, "1") \
                .write(analysis_file)

    def resubmit_completed(self, last_n, use_checkpoint = True):
        # add some guards
        if (last_n > 10):
            raise Exception("resubmit_completed: this moves the job status to CREATED. Not designed to be used for lot of requests")

        # get job ids
        job_ids = sorted(self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed()), reverse = True)

        # check for last_n
        job_ids = job_ids[0:last_n]

        # call rerun_specific_jobs
        self.resubmit_specific_jobs(job_ids, use_checkpoint = use_checkpoint)

    def resubmit_created(self, last_n, use_checkpoint = True):
        # check for shutdown mode
        if (self.runner_base.is_shutdown_mode() == False):
            raise Exception("resubmit_created: This can only be called during shutdown mode")

        # add some guards
        if (last_n > 100):
            raise Exception("resubmit_created: this moves the job status to CREATED. Not designed to be used for lot of requests")

        # get job ids
        created_job_ids = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_created())
        running_job_ids = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_running())
        completed_job_ids = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed())
        job_ids = sorted(set(created_job_ids).difference(set(running_job_ids + completed_job_ids)), reverse = True)

        # check for last_n
        job_ids = job_ids[0:last_n]

        # call rerun_specific_jobs
        self.resubmit_specific_jobs(job_ids, use_checkpoint = use_checkpoint)

    def resubmit_failed(self, last_n, use_checkpoint = True):
        job_ids = sorted(self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed()), reverse = True)

        # take only last n
        job_ids = job_ids[0:last_n]

        for job_id in job_ids:
            # read job spec
            job_spec_json = json.loads(self.runner_base.fs.read_file_contents_as_text(self.runner_base.get_job_status_completed_job_id_file(job_id)))

            # check if the status=FAILED
            if (job_spec_json["status"] == "FAILED"):
                utils.info("ReprocessBatchWorker: rerun_failed: failed job: {}".format(job_id))
                self.resubmit_specific_jobs([job_id], use_checkpoint = use_checkpoint)

    def resubmit_specific_jobs(self, job_ids, use_checkpoint = True):
        # iterate
        for job_id in job_ids:
            # read job spec
            job_spec_json = json.loads(self.runner_base.fs.read_file_contents_as_text(self.runner_base.get_job_status_created_job_id_file(job_id)))

            # change the status
            utils.info("ReprocessBatchWorker: resubmit_specific_jobs: job_id: {}".format(job_id))
            self.runner_base.fs.delete_file(self.runner_base.get_job_status_completed_job_id_file(job_id), ignore_if_missing = True)
            self.runner_base.fs.delete_file(self.runner_base.get_job_status_running_job_id_file(job_id), ignore_if_missing = True)

            # delete existing files if use_checkpoint = False
            if (use_checkpoint == False):
                # check if the directory exists
                if (self.runner_base.fs.dir_exists(self.runner_base.get_job_output_dir(job_id)) == True):
                    utils.warn("ReprocessBatchWorker: resubmit_specific_jobs: deleting files in old directory: job_id: {}, use_checkpoint: {}".format(job_id, use_checkpoint))

                    # delete files
                    for file in self.runner_base.fs.list_files(self.runner_base.get_job_output_dir(job_id)):
                        utils.warn("ReprocessBatchWorker: resubmit_specific_jobs: deleting file: {}".format(file))
                        self.runner_base.fs.delete_file("{}/{}".format(self.runner_base.get_job_output_dir(job_id), file))

            # debug
            utils.info("ReprocessBatchWorker: resubmit_specific_jobs: Job Submitted: {}".format(job_id))

# Class Worker
class Worker:
    def __init__(self, base_dir, worker_id, run_job_func = None, wait_sec = DEFAULT_WAIT_30SEC, num_iter = 1000):
        self.runner_base = RunnerBase(base_dir)
        self.worker_id = worker_id
        self.run_job_func = run_job_func
        self.wait_sec = wait_sec
        self.num_iter = num_iter

        # debug
        utils.info("Worker: base_dir: {}, worker_id: {}, wait_sec: {}, num_iter: {}".format(base_dir, worker_id, wait_sec, num_iter))

    def run_loop(self):
        # check worker exists
        if (self.runner_base.fs.dir_exists(self.runner_base.get_worker_dir(self.worker_id)) == False):
            utils.error("Worker is not initialized: {}: {}".format(self.worker_id, self.runner_base.get_worker_dir(self.worker_id)))
            return

        # retry
        retry_attempts = 3

        # loop
        while (retry_attempts > 0):
            # decrement counter
            retry_attempts = retry_attempts - 1

            # look into assigned jobs directory
            try:
                for i in range(self.num_iter):
                    # check for shutdown mode
                    if (self.runner_base.is_shutdown_mode() == True):
                        utils.warn("Worker: shutdown mode detected. Stopping")
                        return

                    # run
                    self.run()

                    # sleep
                    utils.info("Worker: iteration: {}, Sleeping for {} seconds".format(i, self.wait_sec))
                    time.sleep(self.wait_sec)
            except Exception as e:
                utils.error("Worker: run_loop: caught exception or interrupt: {}, retry_attempts: {}".format(e, retry_attempts))
                if (retry_attempts > 0):
                    continue
                else:
                    return
            except:
                utils.error("Worker: run_loop: caught interrupt. retry_attempts left: {}".format(retry_attempts))
                return

    def run(self):
        # get job ids
        job_ids = self.runner_base.fs.list_leaf_dir(self.runner_base.get_worker_jobs_dir(self.worker_id))
        utils.info("Worker: worker_id: {}, job_ids: {}".format(self.worker_id, job_ids))

        # run the job
        for job_id in job_ids:
            # read job spec
            created_job_spec_file = self.runner_base.get_job_status_created_job_id_file(job_id)
            running_job_spec_file = self.runner_base.get_job_status_running_job_id_file(job_id)
            completed_job_spec_file = self.runner_base.get_job_status_completed_job_id_file(job_id)
            job_spec = self.runner_base.fs.read_file_contents_as_text(running_job_spec_file)

            # empty file
            if (len(job_spec) == 0):
                job_spec = self.runner_base.fs.read_file_contents_as_text(created_job_spec_file)

            # read json
            job_spec_json = json.loads(job_spec)

            # create the output directory first
            self.runner_base.fs.create_dir(self.runner_base.get_job_output_dir(job_id))

            # run the job
            if (self.run_job_func is not None):
                status = self.run_job_func(job_spec_json)
            else:
                utils.warn("Worker: job_id: {}, run_job_func is None".format(job_id))

            # update the status
            job_spec_json["status"] = status
            job_spec_json["completion_ts"] = timefuncs.utctimestamp_to_datetime_str(timefuncs.get_utctimestamp_sec())

            # write to completed
            self.runner_base.fs.write_text_file(completed_job_spec_file, json.dumps(job_spec_json))

            # debug
            utils.info("Worker: {}, Job Finished: {}, details: {}".format(self.worker_id, job_id, job_spec_json))

            # remove from the local jobs directory
            self.runner_base.fs.delete_file(self.runner_base.get_worker_job_id_file(self.worker_id, job_id))

# class RunAnalysis
class RunAnalysis:
    def __init__(self, base_dir, run_job_func = None, last_n = 200, wait_sec = DEFAULT_WAIT_60SEC):
        self.runner_base = RunnerBase(base_dir)
        self.run_job_func = run_job_func
        self.last_n = last_n
        self.wait_sec = wait_sec

    def run_loop(self):
        # look into assigned jobs directory
        try:
            while True:
                # check for shutdown mode
                if (self.runner_base.is_shutdown_mode() == True):
                    utils.warn("RunAnalysis: : shutdown mode detected. Stopping")
                    return

                # run
                self.run()

                # sleep
                utils.info("RunAnalysis: Sleeping for {} seconds".format(self.wait_sec))
                time.sleep(self.wait_sec)
        except Exception as e:
            utils.error("RunAnalysis: run_loop: caught exception or interrupt: {}".format(e))
            return
        except:
            utils.error("RunAnalysis: run_loop: caught interrupt. Returning")
            return

    def run(self):
        # check the list of running, completed
        all_completed = self.runner_base.fs.list_leaf_dir(self.runner_base.get_job_status_completed())

        # get job ids
        job_ids = sorted(all_completed, reverse = True)[0:self.last_n]

        # iterate
        for job_id in job_ids:
            # check if it exists
            analysis_file = self.runner_base.get_job_graph_analysis_file(job_id)
            if (tsv.exists(analysis_file) == True):
                # check for new columns
                if (RUN_VERSION in tsv.read(analysis_file).get_columns()):
                    utils.debug("RunAnalysis: job_id: {} already exists".format(job_id))
                    continue

            # debug
            utils.info("RunAnalysis: processing job_id: {}: {}".format(job_id, analysis_file))

            # run
            running_job_spec_file = self.runner_base.get_job_status_running_job_id_file(job_id)
            job_spec = self.runner_base.fs.read_file_contents_as_text(running_job_spec_file)

            # failsafe
            if (self.run_job_func is not None):
                self.run_job_func(job_spec)
            else:
                utils.warn("RunAnalysis: run_job_func is None, job_id: {}".format(job_id))


