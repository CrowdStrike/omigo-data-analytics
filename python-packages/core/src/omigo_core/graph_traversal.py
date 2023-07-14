from omigo_core import tsv, utils, funclib
import queue

# This is WIP. This detects cycles and ignore assigning them levels
# TODO: there is this reverse_first flag which can be confusing. This api needs to be broken down into
# forward or reverse only
def get_bfs_levels(etsv, vertex_ids, reverse_first = True):
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
        for src, dest in etsv.to_tuples(["src", "target"]):
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
        for src, dest in etsv.to_tuples(["target", "src"]):
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

def get_forward_edges_only(etsv, prefix, sep = ","):
    # list of ids found so far in crawl
    paths = {}
    all_paths = {}

    # initialize
    for node in set(etsv.col_as_array_uniq("src") + etsv.col_as_array_uniq("target")):
        paths[node] = []
        all_paths[node] = []

    # create a map of children
    children_map = {}
    for (parent, cstr) in etsv.aggregate("src", ["target"], [funclib.uniq_mkstr]).to_tuples(["src", "target:uniq_mkstr"]):
        children = cstr.split(sep)
        children_map[parent] = children

    # create a map of parents
    parents_map = {}
    for (child, pstr) in etsv.aggregate("target", ["src"], [funclib.uniq_mkstr]).to_tuples(["target", "src:uniq_mkstr"]):
        parents = pstr.split(sep)
        parents_map[child] = parents

    # root nodes
    root_nodes = set(etsv.col_as_array_uniq("src")).difference(set(parents_map.keys()))

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
    for dest in etsv.col_as_array_uniq("target"):
        # do a backtrack and create unique paths
        ancestors = __get_ancestor_paths__(dest)
        ancestors_map[dest] = list([",".join(vs) for vs in ancestors])

    # return
    return etsv \
        .transform("target", lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform("target", lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix)) \
        .transform("target", lambda t: "|".join(ancestors_map[t]) if (t in ancestors_map.keys()) else "", "{}:ancestors".format(prefix))

def get_time_based_forward_edges_only(etsv, ts_col, prefix):
    utils.warn_once("get_time_based_forward_edges_only: this is hard to understand and time ordering is tricky. Use get_forward_edges_only")

    # list of ids found so far in crawl
    paths = {}
    all_paths = {} 

    # run loop
    sorted_edges = etsv \
        .filter(["src", "target"], lambda t1, t2: t1 != t2) \
        .numerical_sort([ts_col]) \
        .to_tuples(["src", "target"])

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
        .transform("target", lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform("target", lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix))


def remove_dangling_edges(etsv, retain_node_filter_func = None, max_iter = 5, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "remove_dangling_edges")
    utils.warn_once("{}: this can remove event retain keys if there is no incoming or outgoing edge".format(dmsg))

    # check for column names
    if (etsv.has_col("src") == False or etsv.has_col("target") == False):
        raise Exception("{}: predefined column names not found".format(dmsg))

    # flag to maintain current state
    dangling_edges_pruned = False
    count = 0

    # initialize results
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
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (retain_node_filter_func(s) and int(o) == 0) else 0, "outgoing_target_zero_flag") \
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (retain_node_filter_func(s) and int(i) > 1) else 0, "incoming_target_mult_flag") \
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

    # return
    return etsv_result

# TODO: This method is a reference implementation
def remove_cycles(vtsv, etsv, ts_col, retain_node_filter_func = None, dmsg = ""):
    utils.extend_inherit_message(dmsg, "remove_cycles")
    utils.warn_once("remove_cycles: This logic needs to be corrected for same edges from multiple data_sources to not confuse each other")
    utils.warn_once("remove_cycles: there is a weird check for single edge. the t1 in t2.split is not clear")

    # validation
    if (vtsv.has_col("node_id") == False or etsv.has_col("src") == False or etsv.has_col("target") == False or etsv.has_col("data_source") == False or etsv.has_col(ts_col) == False):
        raise Exception("{}: predefined column names not found".format(dmsg))

    # edge count for the edges originating from the same data source 
    etsv_spl = etsv \
        .filter("src", retain_node_filter_func) \
        .aggregate(["src", "target", ts_col], ["data_source"], [funclib.uniq_mkstr]) \
        .distinct() \
        .noop(1000, title = "etsv_spl")

    # edge count for edges that are associated with vertices that need to be retained
    etsv_non_spl = etsv \
        .exclude_filter("src", retain_node_filter_func) \
        .aggregate(["src", "target", ts_col], ["data_source"], [funclib.uniq_mkstr]) \
        .distinct() \
        .noop(1000, title = "etsv_non_spl")
 
    # get forward edges and do some dedup based on ts_col 
    etsv_non_spl2 = get_time_based_forward_edges_only(etsv_non_spl, ts_col, "graph") \
        .sort([ts_col, "target", "src"]) \
        .transform("graph:src_paths", lambda t: ",".join([t1[0:4] for t1 in t.split(",")]) if (t != "") else "", "graph:src_paths2") \
        .transform("graph:all_paths", lambda t: ",".join([t1[0:4] for t1 in t.split(",")]) if (t != "") else "", "graph:all_paths2") \
        .noop(ts_col, funclib.utctimestamp_to_datetime_str, "graph:ts_min2") \
        .transform(["src", "graph:src_paths"], lambda t1, t2: 1 if (t1 in t2.split(",")) else 0, "graph:flag") \
        .noop(1000, "etsv_non_spl2 1", tsv.TSV.select, ["src", "target", ts_col, "graph:src_paths2", "graph:all_paths2", "graph:flag", "data_source:uniq_mkstr"]) \
        .eq_int("graph:flag", 1) \
        .drop_cols_with_prefix("graph") \
        .noop(1000, "etsv_non_spl2 2", tsv.TSV.select, ["src", "target", ts_col, "data_source:uniq_mkstr"])

    etsv2_included = tsv.merge_union([etsv_spl, etsv_non_spl2]) \
        .select(["src", "target", ts_col]) \
        .distinct() \
        .noop(1000, title = "etsv2_included") \
        .to_tuples(["src", "target", ts_col])

    # take only edges that have been picked to be included
    etsv2 = etsv \
        .filter(["src", "target", ts_col], lambda t1, t2, t3: (t1, t2, t3) in etsv2_included) \
        .sort(["src", "target"]) \
        .noop(1000, title = "etsv2", max_col_width = 20)

    # take only vertices that have edges
    vtsv2 = vtsv \
        .filter("node_id", lambda t: t in etsv2.col_as_array_uniq("src") + etsv2.col_as_array_uniq("target"))

    # return
    return vtsv2, etsv2

def merge_similar_nodes_reference(self, vtsv, etsv, retain_vertex_ids, ts_col, retain_node_filter_func = None, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "merge_similar_nodes")

    # check for cycles
    vtsv_nocycle, etsv_nocycle = remove_cycles(vtsv, etsv, ts_col, retain_node_filter_func = retain_node_filter_func)
        
    # warn if cycles were present, and then remove them
    if (etsv.num_rows() != etsv_nocycle.num_rows()):
        utils.warn_once("merge_similar_nodes: this api is unpredictable in presence of cycles. removing them for robustness")
        # debug
        etsv \
            .filter(["src", "target"], lambda src, tgt: (src, tgt) not in etsv_nocycle.to_tuples(["src", "target"])) \
            .show_transpose(3, title = "{}: edges removed because of cycle".format(dmsg), max_col_width = 20)
        
        # assign to original variables
        vtsv, etsv = vtsv_nocycle, etsv_nocycle
        
    # remove columns that are created again
    etsv_sel = etsv

    # find the number of outgoing edges to mark leaves
    etsv_edge_count = etsv_sel \
        .aggregate(["target"], ["src"], [funclib.uniq_len], collapse = False) \
        .rename("src:uniq_len", "incoming_target") \
        .transform("target", lambda t: 1 if (t in retain_vertex_ids) else 0, "target_is_retain_vertex") \
        .transform(prop_col, lambda arr: ",".join(arr), "target_level2", use_array_notation = True) \
        .aggregate("src", ["target", "target_level2", "count"], [funclib.uniq_len, funclib.uniq_mkstr, funclib.sumint], collapse = False) \
        .rename("target:uniq_len", "outgoing_target") \
        .rename("target_level2:uniq_mkstr", "edge_target_level2") \
        .rename("count:sumint", "count_target_level2") \
        .drop_cols("target_level2")

    etsv_num_outgoing_target = etsv_edge_count \
        .select(["src", "outgoing_target", "edge_target_level2", "count_target_level2"]) \
        .rename("src", "right:src") \
        .distinct() \
        .noop("merge_similar_nodes: etsv_num_outgoing_target")

    etsv_target_edge_count = etsv_edge_count \
        .drop_cols(["outgoing_target", "edge_target_level2", "count_target_level2"]) \
        .left_map_join(etsv_num_outgoing_target, ["target"], rkeys = ["right:src"], def_val_map = {"outgoing_target": "0", "edge_target_level2": "", "count_target_level2": "0"}) \
        .drop_cols_with_prefix("right") \
        .reorder(["src", "target"])

    etsv_flags = etsv_target_edge_count \
        .transform(["src", "incoming_target", "outgoing_target"], lambda t1, t2, t3: 1 if (self.is_spl_node(t1) == False and (int(t2) <= 1 and int(t3) == 0)) else 0, "is_leaf_target") \
        .transform(["src", "incoming_target", "outgoing_target"], lambda t1, t2, t3: 1 if (int(t2) <= 1 and int(t3) == 1) else 0, "is_leaf_edge_target")

    etsv_flags \
        .exclude_filter(["is_leaf_target", "is_leaf_edge_target"], lambda t1, t2: t1 == "0" and t2 == "0")

    # group the leaf nodes based on collapse flag
    etsv_grouped_leaf0 = etsv_flags \
        .exclude_filter(["is_leaf_target", "is_leaf_edge_target"], lambda t1, t2: t1 == "1" or t2 == "1") \
        .add_const("num_nodes", "1") \
        .add_const("etsv_grouped_source", "leaf0")

    etsv_grouped_leaf1 = etsv_flags \
        .eq_str("is_leaf_target", "1") \
        .aggregate(["src", "target_is_retain_vertex", "is_leaf_target"], ["target", "target"], [funclib.uniq_mkstr, funclib.get_len]) \
        .rename("target:uniq_mkstr", "target") \
        .rename("target:get_len", "num_nodes") \
        .add_const("etsv_grouped_source", "leaf1")

    etsv_grouped_leaf2 = etsv_flags \
        .eq_str("is_leaf_edge_target", "1") \
        .aggregate(["src", "edge_target_level2", "is_leaf_edge_target"], ["target", "target"], [funclib.uniq_mkstr, funclib.get_len]) \
        .rename("target:uniq_mkstr", "target") \
        .rename("target:get_len", "num_nodes") \
        .add_const("etsv_grouped_source", "leaf2")

    etsv_grouped = tsv.merge_union([etsv_grouped_leaf0, etsv_grouped_leaf1, etsv_grouped_leaf2]) \
        .drop_cols(["incoming_target", "outgoing_target", "edge_target_level2", "count_target_level2", "is_leaf_target", "is_leaf_edge_target"]) \
        .reorder(["src", "target", "num_nodes"]) \
        .reverse_sort(["num_nodes"])

    # map of node ids for collapsed nodes
    vtsv_target_map = {}
    for t in etsv_grouped.col_as_array_uniq("target"):
        for t1 in t.split(","):
            vtsv_target_map[str(t1)] = str(t)

    # map for count of collapsed nodes         
    vtsv_node_count_map = {}
    for t1, t2 in etsv_grouped.to_tuples(["target", "num_nodes"]):
        vtsv_node_count_map[str(t1)] = str(t2)

    # create summarized columns. TODO: this reference is confusing
    etsv_grouped2 = etsv_grouped \
        .transform_inline("src", lambda t: vtsv_target_map[t] if (t in vtsv_target_map.keys()) else t)

    # create new ids. TODO: poor reference
    vtsv_grouped = vtsv \
        .transform_inline("node_id", lambda t: vtsv_target_map[t] if (t in vtsv_target_map.keys()) else t) \
        .aggregate(["node_id"], ["__is_root__", "__is_retain_vertex__"],
            [funclib.uniq_mkstr, funclib.uniq_mkstr]) \
        .remove_suffix("uniq_mkstr", dmsg = dmsg) \
        .transform("node_id", lambda t: vtsv_node_count_map[t] if (t in vtsv_node_count_map.keys()) else "0", "num_nodes")

    # return 
    return vtsv_grouped, etsv_grouped2
