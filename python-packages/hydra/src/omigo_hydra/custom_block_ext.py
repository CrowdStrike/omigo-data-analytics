from omigo_core import tsv, utils

# ...
# .select(["k1", "k2", "k3"]) \
# .topk("args1", "args2", kwarg1 = 100) \
# .randn(100) \
# .select_group_by_topk(["k1"], ["k2"], 13)
#
# .select(["k1", "k2", "k3"]) \
# .extend_class(CustomBlockTSV).custom_block("block_id1")

class CustomBlockTSV(tsv.TSV):
    OMIGO_CONFIG_RESOLUTION_PROPS = "OMIGO_CONFIG_RESOLUTION_PROPS"

    def __init__(self, header, data, base_config = None, config = None):
        super().__init__(header, data)
        self.base_config = base_config

    def apply_block(self, expected_cols = None, prefix = None, block_id = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "apply_block")

        # base condition
        base_config_resolved = os.environ.get(OMIGO_CONFIG_RESOLUTION_PROPS, None) if (self.base_config is None) else self.base_config
        if (base_config_resolved is None):
            raise Exception("{}: base_config_resolved is None".format(dmsg))

        # check for config
        if (block_id is None):
            utils.warn("{}: base_config_resolved: {}, block_id is None".format(dmsg, base_config_resolved))
            if (expected_cols is not None and len(expected_cols) > 0):
                return self.add_empty_cols_if_missing(expected_cols, prefix = prefix, dmsg = dmsg)
            else:
                return self

        # read the config resolution properties
        json_obj = json.loads(tsvutils.read_file_contents_as_text(base_config))

        # read the config resolution class. TODO: reflection
        config_resolver = self.load_custom_config_resolver(json_obj)

        # read the block
        block_config = config_resolver.get_block_config(block_id)

        # read operations
        operations = block_config["operations"]
        func_ops = block_config.__deserialize_operations__(operations)

        # iterate and apply
        for func_op in func_ops:
            print("noop. FIXME")

    def __load_custom_config_resolver__(self, json_obj):
        return CustomConfigResolution(json_obj)

class CustomConfigResolver:
    def __init__(self, json_obj):
        self.json_obj = json_obj

    def get_version(self):
        return json_obj["version"]

    def get_block_config(self, block_id, dmsg = ""):
        dmsg = utils.extends_inherit_message(dmsg, "CustomConfigResolver: get_block_config")

        # get all blocks
        blocks = self.json_obj["blocks"]

        # find and return
        if (block_id in blocks.keys()):
            return blocks[block_id]
        else:
            raise Exception("get_block_config: block_id: {} not found: {}".format(block_id, sorted(blocks.keys())))

    # op_list is a list of tuples - (name, args, kwargs)
    def __serialize_operations__(self, op_func_list, dmsg = ""):
        dmsg = utils.extends_inherit_message(dmsg, "CustomConfigResolver: __serialize_operations__")

        # use hydra to serialize
        ops = list([cluster_common_v2.ClusterInmemoryOperation(op).to_json() for op in op_func_list])
        return ops


    def __deserialize_operations__(self, op_json_list, dmsg = ""):
        dmsg = utils.extends_inherit_message(dmsg, "CustomConfigResolver: __deserialize_operations__")

        # use hydra to deserialize
        ops = list([cluster_common_v2.ClusterOperation.from_json(op) for op in op_json_list])
        return ops
