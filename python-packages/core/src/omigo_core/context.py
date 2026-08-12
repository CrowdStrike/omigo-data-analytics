"""Executor and broadcast context specs + local implementations for omigo_core.

Provides:
- ExecutorContext: abstract spec (all methods raise NotImplementedError)
- BroadcastContext: abstract spec with named API methods (no __setattr__/__getattr__ hacks)
- LocalBVar: thin wrapper so bctx.name.value works in local mode
- LocalBroadcastContext: naive in-memory broadcast context impl
- LocalExecutorContext: in-memory eager execution context impl (for notebooks)
"""

from omigo_core import dataframe


# ---- ExecutorContext spec ----

class ExecutorContext:
    """Abstract spec for execution contexts. All methods raise NotImplementedError."""

    def get_broadcast_context(self):
        """Return the BroadcastContext for this execution environment."""
        raise NotImplementedError("ExecutorContext.get_broadcast_context: subclass must implement")

    def read_df(self, input_id):
        """Read a DataFrame by named input_id."""
        raise NotImplementedError("ExecutorContext.read_df: subclass must implement")

    def from_maps(self, mps, accepted_cols = None, excluded_cols = None, url_encoded_cols = None):
        """Create a DataFrame from a list of dicts."""
        raise NotImplementedError("ExecutorContext.from_maps: subclass must implement")

    def new_with_cols(self, header_fields, data_fields = None):
        """Create a DataFrame with explicit columns."""
        raise NotImplementedError("ExecutorContext.new_with_cols: subclass must implement")

    def create_empty(self):
        """Create an empty DataFrame."""
        raise NotImplementedError("ExecutorContext.create_empty: subclass must implement")

    def store_output(self, df, output_ids):
        """Store a materialized DataFrame under given output_ids."""
        raise NotImplementedError("ExecutorContext.store_output: subclass must implement")

    def read_output(self, output_id):
        """Read a previously stored output DataFrame by output_id."""
        raise NotImplementedError("ExecutorContext.read_output: subclass must implement")

    def get_output_ids(self):
        """Return list of all stored output_ids."""
        raise NotImplementedError("ExecutorContext.get_output_ids: subclass must implement")

    def has_output(self, output_id):
        """Check if an output_id has been stored."""
        raise NotImplementedError("ExecutorContext.has_output: subclass must implement")

    def resolve_input(self, namespace, entity_type, entity_id, input_id):
        """Resolve an input_id to its DataFrame.
        Returns either actual seed data or a proxy marker DataFrame.
        Called once at execution start, not per iteration."""
        raise NotImplementedError("ExecutorContext.resolve_input: subclass must implement")


# ---- BroadcastContext spec ----

class BroadcastContext:
    """Abstract spec for broadcast contexts.

    Defines the API contract. Subclasses implement these methods.
    Attribute-style access (bctx.name = value) is an implementation detail
    of concrete classes — NOT enforced here.
    """

    def asInt(self, value):
        raise NotImplementedError("BroadcastContext.asInt: subclass must implement")

    def asFloat(self, value):
        raise NotImplementedError("BroadcastContext.asFloat: subclass must implement")

    def asStr(self, value):
        raise NotImplementedError("BroadcastContext.asStr: subclass must implement")

    def asBool(self, value):
        raise NotImplementedError("BroadcastContext.asBool: subclass must implement")

    def asDict(self, entries):
        raise NotImplementedError("BroadcastContext.asDict: subclass must implement")

    def asList(self, elements):
        raise NotImplementedError("BroadcastContext.asList: subclass must implement")

    def asTemplate(self, template, valueMap):
        raise NotImplementedError("BroadcastContext.asTemplate: subclass must implement")

    def asFunctionCall(self, func, *args):
        raise NotImplementedError("BroadcastContext.asFunctionCall: subclass must implement")


# ---- LocalBVar: thin wrapper so bctx.name.value works in local mode ----

class LocalBVar:
    """Thin wrapper so bctx.name.value works in local mode (mirrors BVar API)."""
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    # arithmetic -- returns new LocalBVar with computed value
    def __add__(self, other):
        other_val = other._value if (isinstance(other, LocalBVar)) else other
        return LocalBVar(self._value + other_val)

    def __sub__(self, other):
        other_val = other._value if (isinstance(other, LocalBVar)) else other
        return LocalBVar(self._value - other_val)

    def __mul__(self, other):
        other_val = other._value if (isinstance(other, LocalBVar)) else other
        return LocalBVar(self._value * other_val)

    def __truediv__(self, other):
        other_val = other._value if (isinstance(other, LocalBVar)) else other
        return LocalBVar(self._value / other_val)

    def __radd__(self, other):
        return LocalBVar(other).__add__(self)

    def __rsub__(self, other):
        return LocalBVar(other).__sub__(self)

    def __rmul__(self, other):
        return LocalBVar(other).__mul__(self)

    def __repr__(self):
        return "LocalBVar({})".format(repr(self._value))


# ---- LocalBroadcastContext ----

class LocalBroadcastContext(BroadcastContext):
    """Local broadcast context: stores named values as LocalBVar for API compatibility."""
    def __init__(self):
        object.__setattr__(self, '_order', [])

    def __setattr__(self, name, value):
        if (name == '_order'):
            object.__setattr__(self, name, value)
            return
        if (not isinstance(value, LocalBVar)):
            value = LocalBVar(value)
        self.__dict__[name] = value
        order = object.__getattribute__(self, '_order')
        if (name not in order):
            order.append(name)

    def __getattr__(self, name):
        if (name in self.__dict__):
            return self.__dict__[name]
        raise AttributeError("LocalBroadcastContext has no variable '{}'".format(name))

    def asInt(self, value):
        return LocalBVar(int(value))

    def asFloat(self, value):
        return LocalBVar(float(value))

    def asStr(self, value):
        return LocalBVar(str(value))

    def asBool(self, value):
        return LocalBVar(bool(value))

    def asDict(self, entries):
        resolved = {k: v.value if (isinstance(v, LocalBVar)) else v for k, v in entries.items()}
        return LocalBVar(resolved)

    def asList(self, elements):
        resolved = [e.value if (isinstance(e, LocalBVar)) else e for e in elements]
        return LocalBVar(resolved)

    def asTemplate(self, template, valueMap):
        resolved = {k: v.value if (isinstance(v, LocalBVar)) else v for k, v in valueMap.items()}
        return LocalBVar(template.format(**resolved))

    def asFunctionCall(self, func, *args):
        resolved = [a.value if (isinstance(a, LocalBVar)) else a for a in args]
        return LocalBVar(func(*resolved))


# ---- LocalExecutorContext ----

class LocalExecutorContext(ExecutorContext):
    """In-memory execution context for notebooks. Eager evaluation. Dict-backed store.

    - from_maps() / new_with_cols() / create_empty() return plain DataFrame (eager)
    - read_df(input_id) reads from the in-memory __store__ dict
    - store_output(df, output_ids) stores a DataFrame under each output_id in __store__
    - No hydra dependency. No SWF/WF concepts.
    """
    def __init__(self, input_data = None):
        self.__store__ = input_data if (input_data is not None) else {}
        self.broadcast_context = LocalBroadcastContext()

    def get_broadcast_context(self):
        return self.broadcast_context

    def read_df(self, input_id):
        """Read from in-memory store by input_id."""
        if (isinstance(input_id, list)):
            raise Exception("read_df: multiple input_ids are not supported yet. Got: {}".format(input_id))
        if (input_id not in self.__store__):
            raise Exception("LocalExecutorContext: read_df: '{}' not found. Available: {}".format(
                input_id, list(self.__store__.keys())))
        return self.__store__[input_id]

    def from_maps(self, mps, accepted_cols = None, excluded_cols = None, url_encoded_cols = None):
        """Create DataFrame eagerly from maps."""
        return dataframe.from_maps(mps, accepted_cols = accepted_cols,
            excluded_cols = excluded_cols, url_encoded_cols = url_encoded_cols)

    def new_with_cols(self, header_fields, data_fields = None):
        if (data_fields is None):
            data_fields = []
        return dataframe.new_with_cols(header_fields, data_fields)

    def create_empty(self):
        return dataframe.create_empty()

    def store_output(self, df, output_ids):
        """Store DataFrame in memory under each output_id."""
        for output_id in output_ids:
            self.__store__[output_id] = df

    def read_output(self, output_id):
        """Read a previously stored output DataFrame by output_id."""
        if (output_id not in self.__store__):
            raise Exception("LocalExecutorContext.read_output: '{}' not found. Available: {}".format(
                output_id, list(self.__store__.keys())))
        return self.__store__[output_id]

    def get_output_ids(self):
        """Return list of all stored output_ids."""
        return list(self.__store__.keys())

    def has_output(self, output_id):
        """Check if an output_id has been stored."""
        return output_id in self.__store__

    def resolve_input(self, namespace, entity_type, entity_id, input_id):
        """In local/inmemory mode, read from the in-memory store.
        namespace/entity_type/entity_id are ignored — data is keyed by input_id only."""
        return self.read_output(input_id)
