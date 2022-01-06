from omigo_core import tsv
from omigo_core import utils 
import graphviz

# Note: graphviz libraries need to be in systems path
# brew install graphviz
# apt-get install graphviz

# default styling function. This can return differnt style options according to the content of the data
# style and shape are reserved keywords in graphviz. This will throw error if those keys are part of the data
def __dot_func_default__(mp):
    # create a new props
    props = {}
    
    # according to specific values in mp, set the style and shape and anything else related to graphviz
    props["style"] = "filled"
    props["shape"] = "rectangle"
    
    # return
    return props

def __plot_graph__(vertex_map, edges_maps, node_props, edge_props, vertex_id_col, edge_src_col, edge_dest_col, vertex_display_id_col, display_func, max_len):
    # check for custom display
    if (display_func is None):
        display_func = __dot_func_default__
        
    # initialize the digraph structure
    digraph_arr = []
    digraph_arr.append("digraph G {")

    # generate rows for all vertices
    for k in vertex_map.keys():
        mp = vertex_map[k]
        mp_props = []
        
        # set the display id
        vertex_id_val = str(mp[vertex_id_col])
        vertex_display_id_val = str(mp[vertex_display_id_col])
        
        # use a fallback as sometimes the display column is not present
        if (vertex_display_id_val != ""):
            mp_props.append("{}".format(vertex_display_id_val))
        else:
            mp_props.append("{}".format(vertex_id_val))
        
        # iterate over all properties of the vertex
        for k1 in node_props:
            # get the value and generate key-value string
            v1 = str(mp[k1])
            kv_str = "[{} = {}]".format(k1, v1)
            
            # truncate the value if it exceeds a specific threshold
            if (max_len is not None and len(kv_str) > max_len):
                if (max_len > 3):
                    kv_str = kv_str[0:(max_len - 3)] + "..."
                else:
                    kv_str = kv_str[0:max_len]

            # append to the list of attributes
            if (v1 != "" and k1 != vertex_id_col):
                mp_props.append(kv_str)

        # add style information
        dot_props = display_func(mp)
        dot_props_str = ", ".join(["\"{}\" = \"{}\"".format(k2, dot_props[k2]) for k2 in dot_props.keys()])
        vertex_str = "    \"{}\" [\"label\" = \"{}\", {} ]".format(k, "\n".join(mp_props), dot_props_str)
        
        # add the vertex string to the digraph
        digraph_arr.append(vertex_str)

    # generate rows for all edges
    digraph_arr.append("")
    for k in edges_maps.keys():
        mp = edges_maps[k]
        src = str(mp[edge_src_col])
        dest = str(mp[edge_dest_col])

        # check if it is a valid edge
        if (src == "" or dest == ""):
            continue

        # create edge str
        edge_str = "    \"{}\" -> \"{}\"".format(src, dest)

        # generate edge props
        if (edge_props is not None and len(edge_props) > 0):
            ed_props = []

            # iterate over all edge properties
            for k1 in edge_props:
                # get the value and generate key-value string
                v1 = str(mp[k1])
                kv_str = "{}".format(v1)
                
                # truncate the value if it exceeds a specific threshold
                if (max_len is not None and len(kv_str) > max_len):
                    if (max_len > 3):
                        kv_str = kv_str[0:(max_len - 3)] + "..."
                    else:
                        kv_str = kv_str[0:max_len]

                # append to the list of attributes
                ed_props.append(kv_str)

            # add style information
            edge_props_str = ", ".join([k2 for k2 in ed_props])
            edge_str = "{} [ label = \"{}\" ]".format(edge_str, edge_props_str)
              
        # append
        digraph_arr.append(edge_str)

    # generate footer
    digraph_arr.append("}")
    digraph_str = "\n".join(digraph_arr)
    
    # debug
    utils.debug(digraph_str)

    # return
    return graphviz.Source(digraph_str)

def plot_graph(vtsv, etsv, vertex_id_col, src_edge_col, dest_edge_col, vertex_display_id_col = None, node_props = None, edge_props = None, custom_display_func = None,
    max_len = None, create_missing_vertices = False):
    
    # do some validation on vertices and edges
    vertex_ids = set(vtsv.col_as_array_uniq(vertex_id_col))
    src_edge_ids = set(etsv.col_as_array_uniq(src_edge_col))
    dest_edge_ids = set(etsv.col_as_array_uniq(dest_edge_col))
    edge_ids = src_edge_ids.union(dest_edge_ids)
    
    # the vertex tsv must be distinct
    if (len(vertex_ids) != vtsv.num_rows()):
        utils.warn("Vertex TSV is not unique")
        vtsv.group_count(vertex_id_col, "group").gt_int("group:count", 1).show()
        
    # ideally all edge ids must be present in the vertices. fallback to create missing vertices
    missing_edge_ids = edge_ids.difference(vertex_ids)
    no_edge_vertex_ids = vertex_ids.difference(edge_ids)
    
    # display warning and check if there needs to be fallback
    if (len(missing_edge_ids) > 0):
        utils.warn("There are edges that dont have vertex information: {}".format(missing_edge_ids))
        
    # display warning for vertices that dont have edges
    if (len(no_edge_vertex_ids) > 0):
        utils.warn("There are vertices that dont have edges: {}".format(no_edge_vertex_ids))
        
    # check if need to create proxy vertices for which there are edges but no vertex properties
    if (create_missing_vertices == True and len(missing_edge_ids) > 0):
        mtsv = tsv.TSV(vertex_id_col, [str(t) for t in missing_edge_ids])
        utils.info("Creating a fallback vertex map with the vertex id")
        vtsv = tsv.merge([vtsv, mtsv], def_val_map = {})

    # vertex display can be different
    if (vertex_display_id_col is None):
        vertex_display_id_col = vertex_id_col
        
    # node_props default to all columns in vertex tsv
    if (node_props is None):
        node_props = vtsv.drop([vertex_id_col]).columns()
        
    # edge_props default to all columns in edges tsv
    if (edge_props is None):
        edge_props = etsv.drop([src_edge_col, dest_edge_col]).columns()
        
    # create map holding vertex_id and its properties
    vertex_map = {}
    for mp in vtsv.to_maps():
        vertex_map[mp[vertex_id_col]] = mp
        
    # create edges map
    edges_maps = {}
    for mp in etsv.to_maps():
        edges_maps[(mp[src_edge_col], mp[dest_edge_col])] = mp

    # get the graphviz output
    return __plot_graph__(vertex_map, edges_maps, node_props, edge_props, vertex_id_col, src_edge_col, dest_edge_col, vertex_display_id_col, custom_display_func, max_len)
