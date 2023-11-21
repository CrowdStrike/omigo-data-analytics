from omigo_hydra import cluster_services_v2
import sys

from omigo_core import utils
utils.disable_info_mode()

capabilities = sys.argv[1].split(",")
cluster_services_v2.print_local_alive_wf_ids(capabilities)
