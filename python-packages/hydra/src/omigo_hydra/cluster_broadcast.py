"""Serializable broadcast variables for SWF V2.

ClusterBroadcastVar wraps a named value as a ClusterOperand.
ClusterBroadcastContext is an ordered dict of name -> ClusterBroadcastVar.
"""

from omigo_hydra.cluster_data import (
    JsonSer, ClusterOperand, cluster_operand_serializer, cluster_operand_deserializer
)


class ClusterBroadcastVar(JsonSer):
    """A named variable wrapping any ClusterOperand."""
    def __init__(self, name, value):
        self.name = name          # str: variable name
        self.value = value        # ClusterOperand: typed value
        self.class_name = "ClusterBroadcastVar"

    def validate(self):
        if (not isinstance(self.name, str)):
            raise Exception("ClusterBroadcastVar: name must be str")
        if (not isinstance(self.value, ClusterOperand)):
            raise Exception("ClusterBroadcastVar: value must be ClusterOperand")

    @staticmethod
    def from_json(obj):
        name = obj["name"]
        value = cluster_operand_deserializer(obj["value"])
        return ClusterBroadcastVar(name, value)


class ClusterBroadcastContext(JsonSer):
    """Shared context of named broadcast variables for an SWF."""
    def __init__(self, variables=None):
        self.variables = variables or {}  # dict: name -> ClusterBroadcastVar
        self.class_name = "ClusterBroadcastContext"

    def get(self, name):
        if (name not in self.variables):
            raise Exception(f"ClusterBroadcastContext: variable '{name}' not found")
        return self.variables[name]

    def set(self, name, value):
        if (isinstance(value, ClusterOperand)):
            self.variables[name] = ClusterBroadcastVar(name, value)
        elif (isinstance(value, ClusterBroadcastVar)):
            self.variables[name] = value
        else:
            self.variables[name] = ClusterBroadcastVar(name, cluster_operand_serializer(value))

    def has(self, name):
        return name in self.variables

    def keys(self):
        return list(self.variables.keys())

    @staticmethod
    def from_json(obj):
        variables = {}
        for name, var_obj in obj["variables"].items():
            variables[name] = ClusterBroadcastVar.from_json(var_obj)
        return ClusterBroadcastContext(variables)
