from omigo_core import dataframe, utils, timefuncs, udfs
import queue

# This is WIP. This detects cycles and ignore assigning them levels
# TODO: there is this reverse_first flag which can be confusing. This api needs to be broken down into
# forward or reverse only
def get_bfs_levels(edf, vertex_ids, reverse_first = True, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "get_bfs_levels")

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
        for src, dest in edf.to_tuples(["src", "target"]):
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
        for src, dest in edf.to_tuples(["target", "src"]):
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

def get_forward_edges_only(edf, prefix, sep = ",", dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "get_forward_edges_only")
    # list of ids found so far in crawl
    paths = {}
    all_paths = {}

    # initialize
    for node in set(edf.col_as_array_uniq("src") + edf.col_as_array_uniq("target")):
        paths[node] = []
        all_paths[node] = []

    # create a map of children
    children_map = {}
    for (parent, cstr) in edf.aggregate("src", ["target"], [udfs.uniq_mkstr]).to_tuples(["src", "target:uniq_mkstr"]):
        children = cstr.split(sep)
        children_map[parent] = children

    # create a map of parents
    parents_map = {}
    for (child, pstr) in edf.aggregate("target", ["src"], [udfs.uniq_mkstr]).to_tuples(["target", "src:uniq_mkstr"]):
        parents = pstr.split(sep)
        parents_map[child] = parents

    # root nodes
    root_nodes = set(edf.col_as_array_uniq("src")).difference(set(parents_map.keys()))

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
    for dest in edf.col_as_array_uniq("target"):
        # do a backtrack and create unique paths
        ancestors = __get_ancestor_paths__(dest)
        ancestors_map[dest] = list([",".join(vs) for vs in ancestors])

    # return
    return edf \
        .transform("target", lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform("target", lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix)) \
        .transform("target", lambda t: "|".join(ancestors_map[t]) if (t in ancestors_map.keys()) else "", "{}:ancestors".format(prefix))

def get_time_based_forward_edges_only(edf, ts_col, prefix, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "get_time_based_forward_edges_only")
    utils.warn_once("get_time_based_forward_edges_only: this is hard to understand and time ordering is tricky. Use get_forward_edges_only")

    # list of ids found so far in crawl
    paths = {}
    all_paths = {}

    # run loop
    sorted_edges = edf \
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
    return edf \
        .transform("target", lambda t: ",".join(paths[t]) if (t in paths.keys()) else "", "{}:src_paths".format(prefix)) \
        .transform("target", lambda t: ",".join(all_paths[t]) if (t in all_paths.keys()) else "", "{}:all_paths".format(prefix))


def remove_dangling_edges(edf, retain_vertex_ids, retain_node_filter_func, max_iter = 5, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "remove_dangling_edges")
    utils.warn_once("{}: this can remove event retain keys if there is no incoming or outgoing edge".format(dmsg))

    # check for column names
    if (edf.has_col("src") == False or edf.has_col("target") == False):
        raise Exception("{}: predefined column names not found".format(dmsg))

    # flag to maintain current state
    dangling_edges_pruned = False
    count = 0

    # initialize results
    edf_result = edf

    # loop
    while (dangling_edges_pruned == False and count <= max_iter):
        # increase counter
        count = count + 1

        # count edges
        edf_edge_count = edf_result \
            .noop(["src", "target", "evports", "users", "ts_min", "ts_max", "count"], n = 1000, title = "edf_result") \
            .aggregate(["target"], ["src"], [udfs.uniq_len], collapse = False) \
            .rename("src:uniq_len", "incoming_target") \
            .noop(["src", "target", "incoming_target"], n = 1000, title = "edf_result incoming_target") \
            .aggregate(["src"], ["target"], [udfs.uniq_len], collapse = False) \
            .rename("target:uniq_len", "outgoing_target") \
            .noop(["src", "target", "outgoing_target"], n = 1000, title = "edf_result outgoing_target") \
            .noop(["src", "target", "evports", "incoming_target", "outgoing_target"], n = 1000, title = "edf_edge_count")

        # get outgoing edges
        edf_num_outgoing_target = edf_edge_count \
            .select(["src", "outgoing_target"]) \
            .rename("src", "right:src") \
            .distinct()

        # ege count flag
        edf2_edge_flags = edf_edge_count \
            .print_stats("edf_edge_count", dmsg = dmsg) \
            .drop_cols(["outgoing_target"]) \
            .left_map_join(edf_num_outgoing_target, ["target"], rkeys = ["right:src"], def_val_map = {"outgoing_target": "0"}) \
            .noop(["src", "target", "incoming_target", "outgoing_target", "right:.*"], n = 1000, title = "edf_edge_count join") \
            .drop_cols_with_prefix("right") \
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (retain_node_filter_func(s) and int(o) == 0) else 0, "outgoing_target_zero_flag") \
            .transform(["src", "target", "incoming_target", "outgoing_target"], lambda s,t,i,o: 1 if (retain_node_filter_func(s) and int(i) > 1) else 0, "incoming_target_mult_flag") \
            .sort(["outgoing_target_zero_flag", "incoming_target_mult_flag", "src", "target"]) \
            .noop(["src", "target", "incoming_target", "outgoing_target", "outgoing_target_zero_flag", "incoming_target_mult_flag"], n = 1000, title = "edf2_edge_flags")

        edf2_sync = edf2_edge_flags \
            .noop(["src", "target", "outgoing_target_zero_flag", "incoming_target_mult_flag"], n = 1000, title = "edf2_edge_flags 1") \
            .exclude_filter(["target", "outgoing_target_zero_flag"], lambda tgt, t: tgt not in retain_vertex_ids and t == "1") \
            .exclude_filter(["target", "incoming_target_mult_flag"], lambda tgt, t: tgt not in retain_vertex_ids and t == "1") \
            .exclude_filter(["target", "incoming_target_mult_flag", "outgoing_target_zero_flag"], lambda tgt, imulti, ozero: tgt in retain_vertex_ids and imulti == "1") \
            .noop(["src", "target", "outgoing_target_zero_flag", "incoming_target_mult_flag"], n = 1000, title = "edf2_edge_flags 2") \
            .noop(["outgoing_target_zero_flag", "incoming_target_mult_flag"], lambda t1, t2: t1 == "1" or t2 == "1") \
            .noop(10000, "edf2_sync", dataframe.DataFrame.select, ["src", "target", "incoming_target", "outgoing_target", "data_source", "outgoing_target_zero_flag", "incoming_target_mult_flag"]) \
            .drop_cols(["incoming_target", "outgoing_target", "outgoing_target_zero_flag", "incoming_target_mult_flag"])

        # check for running the flag
        if (edf2_edge_flags.num_rows() == edf2_sync.num_rows()):
            dangling_edges_pruned = True
            utils.debug("{}: edf: no more dangling edges found".format(dmsg))
        else:
            utils.info("{}: edf: dangling edges found. running the loop again : {} / {}".format(dmsg, count, max_iter))

        # update the core data structure
        edf_result = edf2_sync

    # return
    return edf_result

# TODO: This method is a reference implementation
def remove_cycles(vdf, edf, ts_col, retain_node_filter_func = None, dmsg = ""):
    utils.extend_inherit_message(dmsg, "remove_cycles")

    # warn
    utils.warn_once("remove_cycles: This logic needs to be corrected for same edges from multiple data_sources to not confuse each other")
    utils.warn_once("remove_cycles: there is a weird check for single edge. the t1 in t2.split is not clear")

    # validation
    if (vdf.has_col("node_id") == False or edf.has_col("src") == False or edf.has_col("target") == False or edf.has_col("data_source") == False or edf.has_col(ts_col) == False):
        raise Exception("{}: predefined column names not found".format(dmsg))

    # edge count for the edges originating from the same data source
    edf_spl = edf \
        .filter("src", retain_node_filter_func) \
        .aggregate(["src", "target", ts_col], ["data_source"], [udfs.uniq_mkstr]) \
        .distinct() \
        .noop(1000, title = "edf_spl")

    # edge count for edges that are associated with vertices that need to be retained
    edf_non_spl = edf \
        .exclude_filter("src", retain_node_filter_func) \
        .aggregate(["src", "target", ts_col], ["data_source"], [udfs.uniq_mkstr]) \
        .distinct() \
        .noop(1000, title = "edf_non_spl")

    # get forward edges and do some dedup based on ts_col
    edf_non_spl2 = get_time_based_forward_edges_only(edf_non_spl, ts_col, "graph") \
        .sort([ts_col, "target", "src"]) \
        .transform("graph:src_paths", lambda t: ",".join([t1[0:4] for t1 in t.split(",")]) if (t != "") else "", "graph:src_paths2") \
        .transform("graph:all_paths", lambda t: ",".join([t1[0:4] for t1 in t.split(",")]) if (t != "") else "", "graph:all_paths2") \
        .noop(ts_col, timefuncs.utctimestamp_to_datetime_str, "graph:ts_min2") \
        .transform(["src", "graph:src_paths"], lambda t1, t2: 1 if (t1 in t2.split(",")) else 0, "graph:flag") \
        .noop(1000, "edf_non_spl2 1", dataframe.DataFrame.select, ["src", "target", ts_col, "graph:src_paths2", "graph:all_paths2", "graph:flag", "data_source:uniq_mkstr"]) \
        .eq_int("graph:flag", 1) \
        .drop_cols_with_prefix("graph") \
        .noop(1000, "edf_non_spl2 2", dataframe.DataFrame.select, ["src", "target", ts_col, "data_source:uniq_mkstr"])

    edf2_included = dataframe.merge_union([edf_spl, edf_non_spl2], dmsg = dmsg) \
        .select(["src", "target", ts_col]) \
        .distinct() \
        .noop(1000, title = "edf2_included") \
        .to_tuples(["src", "target", ts_col])

    # take only edges that have been picked to be included
    edf2 = edf \
        .filter(["src", "target", ts_col], lambda t1, t2, t3: (t1, t2, t3) in edf2_included) \
        .sort(["src", "target"]) \
        .noop(1000, title = "edf2", max_col_width = 20)

    # take only vertices that have edges
    vdf2 = vdf \
        .filter("node_id", lambda t: t in edf2.col_as_array_uniq("src") + edf2.col_as_array_uniq("target"))

    # return
    return vdf2, edf2

def merge_similar_nodes_reference(vdf, edf, retain_vertex_ids, ts_col, retain_node_filter_func, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "merge_similar_nodes")

    # check for cycles
    vdf_nocycle, edf_nocycle = remove_cycles(vdf, edf, ts_col, retain_node_filter_func = retain_node_filter_func)

    # warn if cycles were present, and then remove them
    if (edf.num_rows() != edf_nocycle.num_rows()):
        utils.warn_once("{}: this api is unpredictable in presence of cycles. removing them for robustness".format(dmsg))
        # debug
        edf \
            .filter(["src", "target"], lambda src, tgt: (src, tgt) not in edf_nocycle.to_tuples(["src", "target"])) \
            .show_transpose(3, title = "{}: edges removed because of cycle".format(dmsg), max_col_width = 20)

        # assign to original variables
        vdf, edf = vdf_nocycle, edf_nocycle

    # remove columns that are created again
    edf_sel = edf

    # find the number of outgoing edges to mark leaves
    edf_edge_count = edf_sel \
        .aggregate(["target"], ["src"], [udfs.uniq_len], collapse = False) \
        .rename("src:uniq_len", "incoming_target") \
        .transform("target", lambda t: 1 if (t in retain_vertex_ids) else 0, "target_is_retain_vertex") \
        .transform(prop_col, lambda arr: ",".join(arr), "target_level2", use_array_notation = True) \
        .aggregate("src", ["target", "target_level2", "count"], [udfs.uniq_len, udfs.uniq_mkstr, udfs.sumint], collapse = False) \
        .rename("target:uniq_len", "outgoing_target") \
        .rename("target_level2:uniq_mkstr", "edge_target_level2") \
        .rename("count:sumint", "count_target_level2") \
        .drop_cols("target_level2")

    edf_num_outgoing_target = edf_edge_count \
        .select(["src", "outgoing_target", "edge_target_level2", "count_target_level2"]) \
        .rename("src", "right:src") \
        .distinct() \
        .noop("merge_similar_nodes: edf_num_outgoing_target")

    edf_target_edge_count = edf_edge_count \
        .drop_cols(["outgoing_target", "edge_target_level2", "count_target_level2"]) \
        .left_map_join(edf_num_outgoing_target, ["target"], rkeys = ["right:src"], def_val_map = {"outgoing_target": "0", "edge_target_level2": "", "count_target_level2": "0"}) \
        .drop_cols_with_prefix("right") \
        .reorder(["src", "target"])

    edf_flags = edf_target_edge_count \
        .transform(["src", "incoming_target", "outgoing_target"], lambda t1, t2, t3: 1 if (self.is_spl_node(t1) == False and (int(t2) <= 1 and int(t3) == 0)) else 0, "is_leaf_target") \
        .transform(["src", "incoming_target", "outgoing_target"], lambda t1, t2, t3: 1 if (int(t2) <= 1 and int(t3) == 1) else 0, "is_leaf_edge_target")

    edf_flags \
        .exclude_filter(["is_leaf_target", "is_leaf_edge_target"], lambda t1, t2: t1 == "0" and t2 == "0")

    # group the leaf nodes based on collapse flag
    edf_grouped_leaf0 = edf_flags \
        .exclude_filter(["is_leaf_target", "is_leaf_edge_target"], lambda t1, t2: t1 == "1" or t2 == "1") \
        .add_const("num_nodes", "1") \
        .add_const("edf_grouped_source", "leaf0")

    edf_grouped_leaf1 = edf_flags \
        .eq_str("is_leaf_target", "1") \
        .aggregate(["src", "target_is_retain_vertex", "is_leaf_target"], ["target", "target"], [udfs.uniq_mkstr, udfs.get_len]) \
        .rename("target:uniq_mkstr", "target") \
        .rename("target:get_len", "num_nodes") \
        .add_const("edf_grouped_source", "leaf1")

    edf_grouped_leaf2 = edf_flags \
        .eq_str("is_leaf_edge_target", "1") \
        .aggregate(["src", "edge_target_level2", "is_leaf_edge_target"], ["target", "target"], [udfs.uniq_mkstr, udfs.get_len]) \
        .rename("target:uniq_mkstr", "target") \
        .rename("target:get_len", "num_nodes") \
        .add_const("edf_grouped_source", "leaf2")

    edf_grouped = dataframe.merge_union([edf_grouped_leaf0, edf_grouped_leaf1, edf_grouped_leaf2], dmsg = dmsg) \
        .drop_cols(["incoming_target", "outgoing_target", "edge_target_level2", "count_target_level2", "is_leaf_target", "is_leaf_edge_target"]) \
        .reorder(["src", "target", "num_nodes"]) \
        .reverse_sort(["num_nodes"])

    # map of node ids for collapsed nodes
    vdf_target_map = {}
    for t in edf_grouped.col_as_array_uniq("target"):
        for t1 in t.split(","):
            vdf_target_map[str(t1)] = str(t)

    # map for count of collapsed nodes
    vdf_node_count_map = {}
    for t1, t2 in edf_grouped.to_tuples(["target", "num_nodes"]):
        vdf_node_count_map[str(t1)] = str(t2)

    # create summarized columns. TODO: this reference is confusing
    edf_grouped2 = edf_grouped \
        .transform_inline("src", lambda t: vdf_target_map[t] if (t in vdf_target_map.keys()) else t)

    # create new ids. TODO: poor reference
    vdf_grouped = vdf \
        .transform_inline("node_id", lambda t: vdf_target_map[t] if (t in vdf_target_map.keys()) else t) \
        .aggregate(["node_id"], ["__is_root__", "__is_retain_vertex__"],
            [udfs.uniq_mkstr, udfs.uniq_mkstr]) \
        .remove_suffix("uniq_mkstr", dmsg = dmsg) \
        .transform("node_id", lambda t: vdf_node_count_map[t] if (t in vdf_node_count_map.keys()) else "0", "num_nodes")

    # return
    return vdf_grouped, edf_grouped2

# retain_vertex_annotations are the start and end timestamps for the retained vertex ids
def split_graph_filter_func(src, tgt, ts, retain_vertex_ids, retain_vertex_annotations, retain_node_filter_func, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "split_graph_filter_func")

    # return True for special nodes
    if (retain_node_filter_func(src) or retain_node_filter_func(tgt)):
        return True

    # return True if neither are detections
    if (src not in retain_vertex_ids and tgt not in retain_vertex_ids):
        return True

    # check before and after flag
    before_flag = True
    after_flag = True

    # important, for both nodes as detection, keep the edge
    if (src in retain_vertex_ids and tgt in retain_vertex_ids):
        return True

    # before detection
    if (tgt in retain_vertex_ids):
        if (tgt in retain_vertex_annotations.keys()):
            (retain_vertex_ts_min, retain_vertex_ts_max) = retain_vertex_annotations[tgt]
            if (int(ts) > int(retain_vertex_ts_max)):
                before_flag = False

    # after detection
    if (src in retain_vertex_ids):
        if (src in retain_vertex_annotations.keys()):
            (retain_vertex_ts_min, retain_vertex_ts_max) = retain_vertex_annotations[src]
            if (int(retain_vertex_ts_min) > int(ts)):
                after_flag = False

    # default
    if (before_flag == False or after_flag == False):
        return False
    else:
        return True

def apply_time_order_based_filter(vdf, edf, retain_vertex_ids, retain_node_filter_func, strict_ordering_flag, dmsg = ""):
    utils.extend_inherit_message(dmsg, "apply_time_order_based_filter")
    # find the min and max timestamps
    edf_min_max = edf \
        .aggregate(["src", "target"], ["ts", "ts"], [udfs.minint, udfs.maxint]) \
        .rename("ts:minint", "ts_min") \
        .rename("ts:maxint", "ts_max")

    # create left and right sides. apply timestamp ordering logic to the subgraph to further prune edges
    edf_left = edf_min_max.select(["src", "target", "ts_min"]).exclude_filter("src", retain_node_filter_func).add_prefix("left").distinct()
    edf_right = edf_min_max.select(["src", "target", "ts_max"]).exclude_filter("src", retain_node_filter_func).add_prefix("right").distinct()

    def __apply_time_order_based_filter_template__(lflag, rflag, ldetect, rdetect):
        # if strict time ordering is asked, remove the forward edge
        if (strict_ordering_flag == True):
            return "right"

        # check for detect flags
        if (ldetect == 1 and rdetect == 1):
            return ""
        elif (ldetect == 1):
            return "right"
        elif (rdetect == 1):
            return "left"
        elif (lflag == "0"):
            return "left"
        elif (rflag == "0"):
            return "right"
        else:
            raise Exception("Invalid parameters: lflag: {}, rflag: {}, ldetect: {}, rdetect: {}".format(lflag, rflag, ldetect, rdetect))

    # excluded edges
    excluded_edges = edf_left \
        .inner_map_join(edf_right, ["left:target"], rkeys = ["right:src"]) \
        .transform(["left:ts_min", "right:ts_max"], lambda t1,t2: 1 if (int(t1) <= int(t2)) else 0, "ts:flag") \
        .transform_inline(["left:ts_min", "right:ts_max"], timefuncs.utctimestamp_to_datetime_str) \
        .reorder(["left:src", "left:target", "right:src", "right:target"], use_existing_order = False) \
        .aggregate(["left:src", "left:target"], ["ts:flag"], [udfs.uniq_mkstr], collapse = False) \
        .rename("ts:flag:uniq_mkstr", "left:ts:flag:uniq_mkstr") \
        .aggregate(["right:src", "right:target"], ["ts:flag"], [udfs.uniq_mkstr], collapse = False) \
        .rename("ts:flag:uniq_mkstr", "right:ts:flag:uniq_mkstr") \
        .filter(["left:ts:flag:uniq_mkstr", "right:ts:flag:uniq_mkstr"], lambda t1, t2: t1 == "0" or t2 == "0") \
        .select(["left:src", "left:target", "right:src", "right:target", "left:ts:flag:uniq_mkstr", "right:ts:flag:uniq_mkstr"]) \
        .distinct() \
        .transform(["left:src", "left:target"], lambda t1, t2: 1 if (t1 in retain_vertex_ids or t2 in retain_vertex_ids) else 0, "left:is_detect") \
        .transform(["right:src", "right:target"], lambda t1, t2: 1 if (t1 in retain_vertex_ids or t2 in retain_vertex_ids) else 0, "right:is_detect") \
        .transform(["left:ts:flag:uniq_mkstr", "right:ts:flag:uniq_mkstr", "left:is_detect", "right:is_detect"], lambda lflag, rflag, ldetect, rdetect:
            __apply_time_order_based_filter_determine_side__(strict_time_ordering_flag, lflag, rflag, int(ldetect), int(rdetect)), "exclude_side") \
        .is_nonempty_str("exclude_side") \
        .transform(["left:src", "left:target", "right:src", "right:target", "exclude_side"], lambda lsrc, ltgt, rsrc, rtgt, eside:
            (lsrc, ltgt) if (eside == "left") else (rsrc, rtgt), ["excluded:src", "excluded:target"]) \
        .select(["excluded:src", "excluded:target"]) \
        .distinct()

    # apply exclusion
    edf_result = edf \
        .exclude_filter(["src", "target"], lambda src, tgt: (src, tgt) in excluded_edges.to_tuples(["excluded:src", "excluded:target"]))
    vdf_result = vdf \
        .values_in("node_id", lambda t: t in edf_result.col_as_array_uniq("src") + edf_result.col_as_array_uniq("target"))

    # return
    return vdf_result, edf_result

