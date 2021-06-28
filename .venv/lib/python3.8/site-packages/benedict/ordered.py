"""
OrderedBeneDict enables accessing dict values by attribute, just like Javascript's
dot notation. Supports JSON/YAML operations.
Builtin methods like "values()" and "items()" can be overriden by the data keys,
but their original version will always be protected with prefix builtin_

Adapted from: https://github.com/makinacorpus/EasyDict
"""
import benedict.data_format as df
from benedict.core import (
    BeneDict, benedict_to_dict, _Builtin
)
from collections import OrderedDict
import collections.abc as abc


class OrderedBeneDict(OrderedDict):
    """
    OrderedBeneDict enables accessing dict values by attribute,
    just like Javascript's dot notation. Supports JSON/YAML operations.

    Adapted from: https://github.com/makinacorpus/EasyDict

    Notes:
      Use `dict.items()` if you know there might be conflict in the keys
      or `builtin_` + method name

    Added methods: the version always prefixed by `builtin` is protected against
      changes. You can use the non-prefixed version if you know for sure that
      the name will never be overriden
    """
    def __new__(cls, *args, **kwargs):
        protected_methods = []
        # add builtin_ protection
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name)
            if (not attr_name.startswith('_')
                    and not _Builtin.is_(attr_name)
                    and callable(attr)):
                protected_name = _Builtin.convert(attr_name)
                setattr(cls, protected_name, attr)
                protected_methods.append(protected_name)
        cls._PROTECTED_METHODS = protected_methods
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__()
        d_items = {}
        if len(args) == 1:
            d_items = args[0]
            if isinstance(d_items, (OrderedBeneDict, BeneDict)):
                d_items = d_items.builtin_items()
            elif isinstance(d_items, abc.Mapping):
                d_items = d_items.items()
            else:
                assert isinstance(d_items, abc.Sequence), \
                    'positional argument should either be a Mapping or a Sequence of (key, value) tuples'
        elif len(args) > 1:
            raise ValueError('cannot have more than 1 positional arg')

        for k, v in d_items:
            self.__setattr__(k, v)
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __setattr__(self, name, value):
        cls = self.__class__  # carry over inherited class from BeneDict
        if name in cls._PROTECTED_METHODS:
            raise ValueError('Cannot override `{}()`: {} protected method'
                             .format(name, cls.__name__))
        if isinstance(value, (list, tuple)):
            value = type(value)(cls(x) if isinstance(x, abc.Mapping) else x
                                for x in value)
        elif isinstance(value, abc.Mapping):
            # implements deepcopy if OrderedBeneDict(OrderedBeneDict())
            # to make it shallow copy, add the following condition:
            # ...  and not isinstance(value, self.__class__)):
            value = cls(value)
        if isinstance(name, str):  # support non-string keys
            super().__setattr__(name, value)
        super().__setitem__(name, value)

    __setitem__ = __setattr__

    def to_dict(self):
        """
        Convert to raw dict
        """
        return benedict_to_ordereddict(self)

    def deepcopy(self):
        return self.__class__(self)

    @classmethod
    def load_json_file(cls, file_path, **loader_kwargs):
        return cls(df.ordered_load_json_file(file_path, **loader_kwargs))

    @classmethod
    def load_json_str(cls, string, **loader_kwargs):
        return cls(df.ordered_load_json_str(string, **loader_kwargs))

    @classmethod
    def load_yaml_file(cls, file_path, **loader_kwargs):
        return cls(df.ordered_load_yaml_file(file_path, **loader_kwargs))

    @classmethod
    def load_yaml_str(cls, string, **loader_kwargs):
        return cls(df.ordered_load_yaml_str(string, **loader_kwargs))

    @classmethod
    def load_file(cls, file_path, **loader_kwargs):
        """
        Args:
            file_path: JSON or YAML loader depends on the file extension

        Raises:
            IOError: if extension is not ".json", ".yml", or ".yaml"
        """
        return cls(df.ordered_load_file(file_path, **loader_kwargs))

    def dump_json_file(self, file_path, **dumper_kwargs):
        df.ordered_dump_json_file(
            benedict_to_ordereddict(self), file_path, **dumper_kwargs)

    def dump_json_str(self, **dumper_kwargs):
        "Returns: string"
        return df.ordered_dump_json_str(
            benedict_to_ordereddict(self), **dumper_kwargs)

    def dump_yaml_file(self, file_path, **dumper_kwargs):
        df.ordered_dump_yaml_file(
            benedict_to_ordereddict(self), file_path, **dumper_kwargs)

    def dump_yaml_str(self, **dumper_kwargs):
        "Returns: string"
        return df.ordered_dump_yaml_str(
            benedict_to_ordereddict(self), **dumper_kwargs)

    def dump_file(self, file_path, **dumper_kwargs):
        """
        Args:
            file_path: JSON or YAML loader depends on the file extension

        Raises:
            IOError: if extension is not ".json", ".yml", or ".yaml"
        """
        df.ordered_dump_file(
            benedict_to_ordereddict(self), file_path, **dumper_kwargs)

    def __getstate__(self):
        """
        Support pickling.
        Warning:
          if this BeneDict overrides dict builtin methods, like `items`,
          pickle will report error.
          don't know how to resolve yet
        """
        return benedict_to_ordereddict(self)

    def __setstate__(self, state):
        self.__init__(state)

    def __str__(self):
        return str(benedict_to_ordereddict(self))

    # we explicitly list them here so that IDEs like PyCharm can do auto-complete
    # call _print_protected_methods() to generate this code
    builtin_clear = OrderedDict.clear
    builtin_copy = OrderedDict.copy
    builtin_fromkeys = OrderedDict.fromkeys
    builtin_get = OrderedDict.get
    builtin_items = OrderedDict.items
    builtin_keys = OrderedDict.keys
    builtin_move_to_end = OrderedDict.move_to_end
    builtin_pop = OrderedDict.pop
    builtin_popitem = OrderedDict.popitem
    builtin_setdefault = OrderedDict.setdefault
    builtin_update = OrderedDict.update
    builtin_values = OrderedDict.values
    builtin_deepcopy = deepcopy
    builtin_dump_file = dump_file
    builtin_dump_json_file = dump_json_file
    builtin_dump_json_str = dump_json_str
    builtin_dump_yaml_file = dump_yaml_file
    builtin_dump_yaml_str = dump_yaml_str
    builtin_load_file = load_file
    builtin_load_json_file = load_json_file
    builtin_load_json_str = load_json_str
    builtin_load_yaml_file = load_yaml_file
    builtin_load_yaml_str = load_yaml_str
    builtin_to_dict = to_dict


def benedict_to_ordereddict(D):
    return benedict_to_dict(D, to_type=OrderedDict)


if __name__ == '__main__':
    _Builtin.print_protected(OrderedDict)
