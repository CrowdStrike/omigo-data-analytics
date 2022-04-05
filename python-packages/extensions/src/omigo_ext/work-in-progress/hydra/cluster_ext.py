from omigo_core import tsv
from omigo_core import utils 
from omigo_ajaiswal_ext.hydra import cluster_common
from omigo_ajaiswal_ext.hydra import cluster_data
from omigo_ajaiswal_ext.hydra import cluster_protocol
from omigo_ajaiswal_ext.hydra import cluster_services
from omigo_ajaiswal_ext.hydra import cluster_class_reflection
import json
import time
import math

# global constants
CLUSTER_TS = cluster_common.get_utctimestamp_sec()
JOB_COUNTER = 0
JOB_WAIT_SEC = 10

# executor class for job
class ClusterExecutor(cluster_data.JsonSer):
    def __init__(self, header, data, batch_size = 5, max_workers = 10, ctx = None):
        self.xtsv = tsv.TSV(header, data)
        self.client_id = cluster_common.ClusterPaths.get_client_id()
        self.session_id = cluster_common.ClusterPaths.get_session_id()
        self.cluster_handler = cluster_common.ClusterPaths.get_cluster_handler()
        self.batch_size = batch_size 
        self.max_workers = max_workers
        self.ctx = cluster_common.ClusterJobContext.new_create_operations() if (ctx is None) else ctx
        
    def custom_func(self, func, *args, **kwargs):
        # get the function name
        func_name = cluster_class_reflection.get_fully_qualified_name(func)

        # check that the function exists in supported modules
        if (cluster_class_reflection.verify_tsv_subclass_func(func_name) == False):
            raise Exception("The function is not supported: {}".format(func_name))

        # call 
        self.ctx.add_operation(cluster_common.ClusterOperation(func_name, *args, **kwargs))
        return ClusterExecutor(self.xtsv.header, self.xtsv.data, batch_size = self.batch_size, max_workers = self.max_workers, ctx = self.ctx)

    def to_json(self, transient_keys = []):
        transient_keys2 = ["xtsv", "cluster_handler"]
        for x in transient_keys:
            transient_keys2.append(x)
       
        return super().to_json(transient_keys = transient_keys2) 

    def __create_job_name__(self, operations):    
        return ":".join([x.name for x in operations])
    
    # persist method that keeps the output in external location
    def submit_job(self):
        # use global constants
        global CLUSTER_TS
        global JOB_COUNTER
        JOB_COUNTER = JOB_COUNTER + 1

        # check the hash of input and all the jobs. if there is already a job existing for that, then reuse.
        input_hash = utils.compute_hash(self.client_id + self.session_id + self.xtsv.get_hash() + ",".join(["{}".format(utils.compute_hash(x.to_json())) for x in self.ctx.operations]))

        # generate the jobid
        job_id = "job-{}-{}-{:04d}".format(cluster_services.ID_SUFFIX, CLUSTER_TS, JOB_COUNTER)

        # copy xtsv into data job input
        self.cluster_handler.create(cluster_common.ClusterPaths.get_data_job(job_id))
        input_path = cluster_common.ClusterPaths.get_data_job_input(job_id)
        output_path = cluster_common.ClusterPaths.get_data_job_output(job_id)

        # write the input data
        self.cluster_handler.write_tsv(input_path, self.xtsv)

        # determine num_batches
        num_batches = int(math.ceil(self.xtsv.num_rows() / self.batch_size))

        # create a job spec and write to the jobs directory
        job_spec = cluster_common.ClusterJobSpec.new(
            self.client_id,
            self.session_id,
            self.__create_job_name__(self.ctx.operations),
            input_path,
            output_path,
            "map",
            self.ctx.operations,
            num_batches,
            self.max_workers,
            "simple",
            1)

        # create a new job_id
        job = cluster_common.ClusterJob.new_create_token(job_id, job_spec)
        session_protocol = cluster_services.get_session_protocol()
        session_protocol.create_job(job)

        # TODO: run a loop to monitor the progress
        job_completed = False
        while (job_completed == False):
            # check the status
            if (self.cluster_handler.dir_exists(cluster_common.ClusterPaths.get_job_statuses_completed(job_id))):
                # job is completed. Return the output
                job_completed = True
                utils.info("submit_job: job: {} is completed.".format(job_id))
            else:
                # master
                master_current = cluster_common.ClusterMaster.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_master_current()))

                # count the number of batches that are completed
                num_batches = len(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_incoming(job_id)))
                num_completed_batches = len(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_batches_statuses_completed(job_id)))

                # get assigned manager
                assigned_manager_id = "Unassigned" 
                job_ref = cluster_common.ClusterJobRef.from_json(self.cluster_handler.read_most_recent_json(cluster_common.ClusterPaths.get_job_manager(job_id)))
                if (job_ref is not None):
                    assigned_manager_id = job_ref.manager_id

                # get the number of workers
                num_workers = len(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_workers(job_id)))
                num_dead_workers = len(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_dead_workers(job_id)))
                num_recovered_workers = len(self.cluster_handler.list_dirs(cluster_common.ClusterPaths.get_job_recovered_workers(job_id)))
                
                # debug
                utils.info("submit_job: job_id: {}: Master: {}, Manager: {}, Workers: {}, Dead Workers: {}, Recovered: {}, Batches: {} / {}. Sleeping for {} seconds.".format(
                    job_id, master_current.entity_id, assigned_manager_id, num_workers, num_dead_workers, num_recovered_workers, num_completed_batches, num_batches, JOB_WAIT_SEC))
                time.sleep(JOB_WAIT_SEC)
        
        # return        
        return cluster_common.TSVReference(tsv.read(self.cluster_handler.get_full_path(cluster_common.ClusterPaths.get_data_job_output(job_id)))).read(num_par = 4)

    def persist(self, output_path):
        xtsv = self.submit_job()
        tsv.write(xtsv, output_path)
        return self

    def collect(self):
        xtsv = self.submit_job()
        return xtsv

class ClusterSampleTSV(tsv.TSV):
    def __init__(self, header, data):
        super().__init__(header, data)

    def myfunc(self):
        return self.take(10)

