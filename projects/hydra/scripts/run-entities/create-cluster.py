from omigo_hydra import cluster_services_v2

cluster_services_v2.init(1)
cluster_services_v2.CLUSTER_ADMIN.create_cluster()
