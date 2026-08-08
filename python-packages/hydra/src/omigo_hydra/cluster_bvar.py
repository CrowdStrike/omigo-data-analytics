"""BVar SDK: dual-mode broadcast variables for Hydra SWF V2.

BVar wraps a real Python value (for notebook testing) and records operations
for Hydra serialization. BroadcastContext provides attribute-based naming.
hydra_udf is a decorator for complex UDFs.
"""

from omigo_hydra import cluster_class_reflection
from omigo_hydra.cluster_data import cluster_operand_serializer
from omigo_hydra.cluster_broadcast import ClusterBroadcastContext
from omigo_core.context import BroadcastContext as BaseBroadcastContext


def _ensure_bvar(val):
    """Wrap a raw Python value as a BVar if it isn't already one."""
    if (isinstance(val, BVar)):
        return val
    return BVar(None, val)


def _serialize_op_tree(bvar):
    """Walk a BVar's _op/_op_args tree and produce a JSON-serializable dict.

    Named BVars (with a non-None name and no recorded op) become {"ref": name}.
    Literals become ClusterOperand JSON via cluster_operand_serializer.
    Operations become {"op": ..., "args": ..., ...} dicts.
    """
    # leaf: named variable reference (assigned in context, no local op)
    if (bvar.name is not None and bvar._op is None):
        return {"ref": bvar.name}

    # leaf: literal value (no name, no op)
    if (bvar._op is None):
        return cluster_operand_serializer(bvar._value).to_json()

    op = bvar._op

    # arithmetic / cast ops with tuple args
    if (op in ("add", "sub", "mul", "div", "cast_int", "cast_str", "cast_float")):
        args = [_serialize_op_tree(_ensure_bvar(a)) for a in bvar._op_args]
        return {"op": op, "args": args}

    # make_dict: _op_args is a dict of str -> BVar|raw
    if (op == "make_dict"):
        entries = {}
        for k, v in bvar._op_args.items():
            entries[k] = _serialize_op_tree(_ensure_bvar(v))
        return {"op": "make_dict", "entries": entries}

    # make_list: _op_args is a list of BVar|raw
    if (op == "make_list"):
        args = [_serialize_op_tree(_ensure_bvar(e)) for e in bvar._op_args]
        return {"op": "make_list", "args": args}

    # format_str: _op_args is (template, kwargs_dict)
    if (op == "format_str"):
        template, kwargs = bvar._op_args
        serialized_kwargs = {}
        for k, v in kwargs.items():
            serialized_kwargs[k] = _serialize_op_tree(_ensure_bvar(v))
        return {"op": "format_str", "template": template, "kwargs": serialized_kwargs}

    # func_call: _op_args is (func, tuple_of_args)
    if (op == "func_call"):
        func, call_args = bvar._op_args
        func_name = cluster_class_reflection.get_fully_qualified_name(func)
        serialized_args = [_serialize_op_tree(_ensure_bvar(a)) for a in call_args]
        return {"op": "func_call", "func_name": func_name, "args": serialized_args}

    raise Exception(f"_serialize_op_tree: unknown op: {op}")


class BVar:
    """Dual-mode broadcast variable: executes AND records."""
    def __init__(self, name=None, value=None):
        self.name = name
        self._value = value
        self._op = None       # recorded operation (internal, for serialization)
        self._op_args = None  # recorded args

    @property
    def value(self):
        return self._value

    # --- typed factories (create literal BVars) ---
    @staticmethod
    def int(value):
        """Create a BVar wrapping an int value."""
        if (not isinstance(value, (int, float))):
            raise Exception(f"BVar.int: expected numeric, got {type(value).__name__}")
        return BVar(None, int(value))

    @staticmethod
    def float(value):
        """Create a BVar wrapping a float value."""
        if (not isinstance(value, (int, float))):
            raise Exception(f"BVar.float: expected numeric, got {type(value).__name__}")
        return BVar(None, float(value))

    @staticmethod
    def str(value):
        """Create a BVar wrapping a str value."""
        return BVar(None, str(value))

    @staticmethod
    def bool(value):
        """Create a BVar wrapping a bool value."""
        return BVar(None, bool(value))

    # --- arithmetic ---
    def __add__(self, other):
        other = _ensure_bvar(other)
        result = BVar()
        result._value = self._value + other._value
        result._op = "add"
        result._op_args = (self, other)
        return result

    def __sub__(self, other):
        other = _ensure_bvar(other)
        result = BVar()
        result._value = self._value - other._value
        result._op = "sub"
        result._op_args = (self, other)
        return result

    def __mul__(self, other):
        other = _ensure_bvar(other)
        result = BVar()
        result._value = self._value * other._value
        result._op = "mul"
        result._op_args = (self, other)
        return result

    def __truediv__(self, other):
        other = _ensure_bvar(other)
        result = BVar()
        result._value = self._value / other._value
        result._op = "div"
        result._op_args = (self, other)
        return result

    # --- right-hand operators (for: 60 + bvar, 3600 * bvar) ---
    def __radd__(self, other):
        return _ensure_bvar(other).__add__(self)

    def __rsub__(self, other):
        return _ensure_bvar(other).__sub__(self)

    def __rmul__(self, other):
        return _ensure_bvar(other).__mul__(self)

    # --- type coercion (instance methods, scala style) ---
    def to_int(self):
        result = BVar()
        result._value = int(self._value)
        result._op = "cast_int"
        result._op_args = (self,)
        return result

    def to_str(self):
        result = BVar()
        result._value = str(self._value)
        result._op = "cast_str"
        result._op_args = (self,)
        return result

    def to_float(self):
        result = BVar()
        result._value = float(self._value)
        result._op = "cast_float"
        result._op_args = (self,)
        return result

    # --- data construction ---
    @staticmethod
    def dict(entries):
        """Construct a dict from str->BVar entries."""
        result = BVar()
        result._value = {k: v._value if (isinstance(v, BVar)) else v
                         for k, v in entries.items()}
        result._op = "make_dict"
        result._op_args = entries
        return result

    @staticmethod
    def list(elements):
        """Construct a list from BVar elements."""
        result = BVar()
        result._value = [e._value if (isinstance(e, BVar)) else e for e in elements]
        result._op = "make_list"
        result._op_args = elements
        return result

    # --- string formatting ---
    @staticmethod
    def format(template, **kwargs):
        """String template with broadcast variable substitution."""
        result = BVar()
        values = {k: v._value if (isinstance(v, BVar)) else v
                  for k, v in kwargs.items()}
        result._value = template.format(**values)
        result._op = "format_str"
        result._op_args = (template, kwargs)
        return result

    # --- library function call ---
    @staticmethod
    def call(func, *args):
        """Call a library function, recording it for Hydra serialization."""
        real_args = [a._value if (isinstance(a, BVar)) else a for a in args]
        result = BVar()
        result._value = func(*real_args)
        result._op = "func_call"
        result._op_args = (func, args)
        return result


class BroadcastContext(BaseBroadcastContext):
    """User-facing context: attribute name = broadcast variable name.

    Tracks assignment order via _order list for ordered serialization.
    """
    def __init__(self):
        # Use object.__setattr__ to bypass our custom __setattr__ for internals
        object.__setattr__(self, '_order', [])

    def __setattr__(self, name, value):
        if (name == '_order'):
            object.__setattr__(self, name, value)
            return
        if (isinstance(value, BVar)):
            # If this BVar already has a name (aliasing another ctx var),
            # create an alias BVar that references the original by name.
            if (value.name is not None and value.name != name):
                alias = BVar(name, value._value)
                alias._alias_of = value.name
                self.__dict__[name] = alias
            else:
                value.name = name
                self.__dict__[name] = value
        else:
            self.__dict__[name] = BVar(name, value)
        # track assignment order
        order = object.__getattribute__(self, '_order')
        if (name not in order):
            order.append(name)

    def __getattr__(self, name):
        if (name in self.__dict__):
            return self.__dict__[name]
        raise AttributeError(f"BroadcastContext has no variable '{name}'")

    # --- typed BVar factories (convenience wrappers around BVar static methods) ---
    def asInt(self, value):
        """Create a BVar wrapping an int value."""
        return BVar.int(value)

    def asFloat(self, value):
        """Create a BVar wrapping a float value."""
        return BVar.float(value)

    def asStr(self, value):
        """Create a BVar wrapping a str value."""
        return BVar.str(value)

    def asBool(self, value):
        """Create a BVar wrapping a bool value."""
        return BVar.bool(value)

    def asDict(self, entries):
        """Create a BVar wrapping a dict from str->BVar entries."""
        return BVar.dict(entries)

    def asList(self, elements):
        """Create a BVar wrapping a list from BVar elements."""
        return BVar.list(elements)

    def asTemplate(self, template, valueMap):
        """Create a BVar from a string template. Variables in {curly braces} are replaced with values from the map at runtime."""
        return BVar.format(template, **valueMap)

    def asFunctionCall(self, func, *args):
        """Create a BVar by calling a library function, recording it for Hydra serialization."""
        return BVar.call(func, *args)

    def to_broadcast_context(self):
        """Export as ClusterBroadcastContext for serialization (values only)."""
        bc = ClusterBroadcastContext()
        order = object.__getattribute__(self, '_order')
        for name in order:
            bvar = self.__dict__[name]
            bc.set(name, bvar.value)
        return bc

    def to_json(self):
        """Serialize as ordered steps list preserving operation trees."""
        steps = []
        order = object.__getattribute__(self, '_order')
        for name in order:
            bvar = self.__dict__[name]
            step = _serialize_step(name, bvar)
            steps.append(step)
        return {"class_name": "ClusterBroadcastContext", "steps": steps}


def _serialize_step(name, bvar):
    """Serialize a single named BVar as a step dict for the ordered steps list."""
    # alias: variable reference to another named var (set by BroadcastContext.__setattr__)
    if (hasattr(bvar, '_alias_of') and bvar._alias_of is not None):
        return {"name": name, "ref": bvar._alias_of}

    # leaf: literal value (no op, value is the direct assignment)
    if (bvar._op is None):
        return {"name": name, "value": cluster_operand_serializer(bvar._value).to_json()}

    # has an op — serialize the tree
    tree = _serialize_op_tree(bvar)
    tree["name"] = name
    return tree


def hydra_udf(func):
    """Makes a function dual-mode: direct execution + Hydra serializable."""
    fqn = cluster_class_reflection.get_fully_qualified_name(func)

    class HydraUDFWrapper:
        def __init__(self):
            self.func = func
            self.fqn = fqn

        def __call__(self, *args, **kwargs):
            """Notebook mode: call the real function."""
            real_args = [a.value if (isinstance(a, BVar)) else a for a in args]
            real_kwargs = {k: v.value if (isinstance(v, BVar)) else v
                          for k, v in kwargs.items()}
            return self.func(*real_args, **real_kwargs)

        def as_bvar(self, *args, **kwargs):
            """Hydra mode: call AND record for serialization."""
            result = BVar.call(self.func, *args)
            return result

    return HydraUDFWrapper()
