from omigo_core import tsv, utils, funclib
import queue

# This is WIP. This detects cycles and ignore assigning them levels
# TODO: there is this reverse_first flag which can be confusing. This api needs to be broken down into
# forward or reverse only
def get_bfs_levels(etsv, vertex_ids, src_col, dest_col, reverse_first = True):
    # create map of levels
    fwd_levels = {}
    found_ids = []
    for v in vertex_ids:
        fwd_levels[v] = 0
        found_ids.append(v)

    # list of ids found so far in crawl
    # found_ids = [vertex_id]
    flag = True

    # run loop
    current_level = 0
    while (flag == True):
        new_flag = False
        for src, dest in etsv.to_tuples([src_col, dest_col]):
            # check if it is a new id
            if (src in found_ids and dest not in found_ids and fwd_levels[src] <= current_level):
                fwd_levels[dest] = fwd_levels[src] + 1
                found_ids.append(dest)
                new_flag = True

        # set flag if next loop is to be run
        if (new_flag == False):
            flag = False

        # increase level
        current_level = current_level + 1

    # run second round for reverse
    rev_levels = {}
    found_ids = []
    for v in vertex_ids:
        rev_levels[v] = 0
        found_ids.append(v)

    flag = True

    # run loop
    current_level = 0
    while (flag == True):
        new_flag = False
        for src, dest in etsv.to_tuples([dest_col, src_col]):
            # check if it is a new id
            if (src in found_ids and dest not in found_ids and rev_levels[src] >= current_level):
                # if level is not mapped already
                rev_levels[dest] = rev_levels[src] - 1
                found_ids.append(dest)
                new_flag = True

        # set flag if next loop is to be run
        if (new_flag == False):
            flag = False

        # decrease level
        current_level = current_level - 1

    # check in which order the levels are to be picked
    levels = {}
    combined_levels = [rev_levels, fwd_levels] if (reverse_first == True) else [fwd_levels, rev_levels]

    # assign if not assigned already
    for xlevels in combined_levels:
        for k in xlevels.keys():
            if (k not in levels.keys()):
                levels[str(k)] = xlevels[k]

    # return
    return levels

def get_forward_edges_only(etsv, src_col, dest_col, prefix, sep = ","):
    # list of ids found so far in crawl
    paths = {}
    all_paths = {}

    # initialize
    for node in set(etsv.col_as_array_uniq(src_col) + etsv.col_as_array_uniq(dest_col)):
        paths[node] = []
        all_paths[node] = []

    # create a map of children
    children_map = {}
    for (parent, cstr) in etsv.aggregate(src_col, [dest_col], [funclib.uniq_mkstr]).to_tuples([src_col, "{}:uniq_mkstr".format(dest_col)]):
        children = cstr.split(sep)
        children_map[parent] = children

    # create a map of parents
    parents_map = {}
    for (child, pstr) in etsv.aggregate(dest_col, [src_col], [funclib.uniq_mkstr]).to_tuples([dest_col, "{}:uniq_mkstr".format(src_col)]):
        parents = pstr.split(sep)
        parents_map[child] = parents

    # root nodes
    root_nodes = set(etsv.col_as_array_uniq(src_col)).difference(set(parents_map.keys()))

    # do a forward crawl to fill paths and all_paths
    forward_queue = queue.Queue()
    visited_parents = {}

    # add all roots
    for root in root_nodes:
        forward_queue.put(root)

    # while loop until queue is empty
    while (forward_queue.empty() == False):
        # remove an item
        node = forward_queue.get()

        # mark as visited
        visited_parents[node] = 1

        # get children
        children = children_map[node] if (node in children_map.keys()) else []

        # iterate and add
        for child in children:
            src = node
            dest = child

            all_paths[dest] = list(sorted(set(all_paths[dest] + all_paths[src] + [src])))
            if (dest not in paths[dest] and dest not in paths[src]):
                paths[dest] = list(sorted(set(paths[dest] + paths[src] + [src])))

            # append child to queue
            if (child not in visited_parents.keys()):
                forward_queue.put(child)

    # now do a backward run to include all paths to the parents
    for child in parents_map.keys():
        for parent in parents_map[child]:
            src = parent
            dest = child

            # get a combined list of all paths
            all_paths[dest] = list(sorted(set(all_paths[dest] + all_paths[src] + [src])))

    def __get_ancestor_paths__(x):
        results = []
        if (x in parents_map.keys()):
            for parent in parents_map[x]:
                if (x in paths.keys() and parent in paths[x]):
                    for ancestors in __get_ancestor_paths__(parent):
                        results.append([x] + ancestors)
        else:
           results.append([x])

        # return
        return results

    # find all unique paths to all the target nodes
    ancestors_map = {}
    for dest in etsv.col_as_array_uniq(dest_col):
        # do a backtrack and create unique paths
        ancestors = __get_ancestor_paths__(dest)
        ancestors_map[dest] = list([",".join(vs) for vs in ancestors])

    # return
    return etsv \
        .transform(dest_col, lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform(dest_col, lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix)) \
        .transform(dest_col, lambda t: "|".join(ancestors_map[t]) if (t in ancestors_map.keys()) else "", "{}:ancestors".format(prefix))

def get_time_based_forward_edges_only(etsv, src_col, dest_col, ts_col, prefix):
    utils.warn_once("get_time_based_forward_edges_only: this is hard to understand and time ordering is tricky. Use get_forward_edges_only")

    # list of ids found so far in crawl
    paths = {}
    all_paths = {} 

    # run loop
    sorted_edges = etsv \
        .filter([src_col, dest_col], lambda t1, t2: t1 != t2) \
        .numerical_sort([ts_col]) \
        .to_tuples([src_col, dest_col])

    # iterate over sorted edges
    for src, dest in sorted_edges:
        if (src not in all_paths.keys()):
            all_paths[src] = []

        if (dest not in all_paths.keys()):
            all_paths[dest] = []

        if (src not in paths.keys()):
            paths[src] = []

        if (dest not in paths.keys()):
            paths[dest] = []

        all_paths[dest] = list(sorted(set(all_paths[dest] + all_paths[src] + [src])))
        if (dest not in paths[dest] and dest not in paths[src]):
            paths[dest] = list(sorted(set(paths[dest] + paths[src] + [src])))

    # TODO: do a second pass to incorporate missing paths from first pass. this is not well understood.
    for src, dest in sorted_edges:
        all_paths[dest] = list(sorted(set(all_paths[dest] + all_paths[src] + [src])))

    # return
    return etsv \
        .transform(dest_col, lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform(dest_col, lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix))


def remove_dangling_edges(vtsv, etsv, max_iter = 5, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "remove_dangling_edges")
    utils.warn_once("{}: this can remove event detect_keys if there is no incoming or outgoing edge".format(dmsg))

    # check for column names
    if (vtsv.has_col("node_id") == False or etsv.has_col("src") == False or etsv.has_col("target") == False):
        raise Exception("{}: predefined column names not found".format(dmsg))

    # flag to maintain current state
    dangling_edges_pruned = False
    count = 0

    # initialize results
    vtsv_result = vtsv
    etsv_result = etsv

    # loop
    while (dangling_edges_pruned == False and count <= max_iter):
        # increase counter
        count = count + 1
    
        # count edges
        etsv_edge_count = etsv_result \
            .aggregate(["target"], ["src"], [funclib.uniq_len], collapse = False) \
            .rename("src:uniq_len", "incoming_target") \
            .aggregate(["src"], ["target"], [funclib.uniq_len], collapse = False) \
            .rename("target:uniq_len", "outgoing_target")
    
        # get outgoing edges
        etsv_num_outgoing_target = etsv_edge_count \
            .select(["src", "outgoing_target"]) \
            .rename("src", "right:src") \
            .distinct()
        
        # ege count flag
        etsv2_edge_flags = etsv_edge_count \
            .print_stats(msg = "etsv_edge_count") \
            .drop_cols(["outgoing_target"]) \
            .left_map_join(etsv_num_outgoing_target, ["target"], rkeys = ["right:src"], def_val_map = {"outgoing_target": "0"}) \
            .drop_cols_with_prefix("right") \
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (self.is_spl_node(s) and int(o) == 0) else 0, "outgoing_target_zero_flag") \
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (self.is_spl_node(s) and int(i) > 1) else 0, "incoming_target_mult_flag") \
            .sort(["outgoing_target_zero_flag", "incoming_target_mult_flag", "src", "target"]) \
            .noop(1000, "etsv2_edge_flags", tsv.TSV.select, ["src", "target", "incoming_target", "outgoing_target", "outgoing_target_zero_flag", "incoming_target_mult_flag"])

        etsv2_sync = etsv2_edge_flags \
            .not_eq_str("outgoing_target_zero_flag", "1") \
            .not_eq_str("incoming_target_mult_flag", "1") \
            .noop(10000, "etsv2_sync", tsv.TSV.select, ["src", "target", "incoming_target", "outgoing_target", "data_source", "outgoing_target_zero_flag", "incoming_target_mult_flag"]) \
            .drop_cols(["incoming_target", "outgoing_target", "outgoing_target_zero_flag", "incoming_target_mult_flag"])

        # check for running the flag
        if (etsv2_edge_flags.num_rows() == etsv2_sync.num_rows()):
            dangling_edges_pruned = True
            utils.debug("etsv: no more dangling edges found")
        else:
            utils.info("etsv: dangling edges found. running the loop again : {} / {}".format(count, max_iter))

        # update the core data structure 
        etsv_result = etsv2_sync

        # only keep vertices that had edges
        vtsv_result = vtsv_result \
            .values_in("node_id", etsv_result.col_as_array_uniq("src") + etsv_result.col_as_array_uniq("target"))

    # return
    return vtsv_result, etsv_result

