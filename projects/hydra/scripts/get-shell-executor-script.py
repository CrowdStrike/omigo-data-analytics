from omigo_hydra import cluster_services_v2
import sys

from omigo_core import utils
utils.disable_info_mode()

wf_id = sys.argv[1]
cluster_services_v2.print_shell_executor_scripts(wf_id)
