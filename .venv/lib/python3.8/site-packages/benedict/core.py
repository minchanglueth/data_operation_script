"""
BeneDict enables accessing dict values by attribute, just like Javascript's
dot notation. Supports JSON/YAML operations.
Builtin methods like "values()" and "items()" can be overriden by the data keys,
but their original version will always be protected with prefix builtin_

Adapted from: https://github.com/makinacorpus/EasyDict
"""
import inspect
import collections.abc as abc
import benedict.data_format as df


class _Builtin:
    "staticmethods only, nothing but logical grouping of functions"

    @staticmethod
    def convert(method_name):
        return 'builtin_' + method_name

    @staticmethod
    def restore(builtin_name):
        return builtin_name[len('builtin_'):]

    @staticmethod
    def is_(method_name):
        "is it a builtin method?"
        return method_name.startswith('builtin_')

    @staticmethod
    def get_protected(d):
        """
        Can be applied to BeneDict itself or any class that inherits from BeneDict

        Args:
            d: a BeneDict or subclass object or class

        Returns:
            list of protected method names
        """
        if inspect.isclass(d):
            try:
                d = d()  # only applies to classes that can take no args
            except:
                raise ValueError('please pass in a concrete object of your class')
        return [
            attr_name for attr_name in dir(d)
            if _Builtin.is_(attr_name) and callable(getattr(d, attr_name))
        ]

    @staticmethod
    def print_protected(builtin_type):
        "paste the generated code into BeneDict class for PyCharm convenience"
        for method in [m for m in dir(builtin_type) if not m.startswith('_')]:
            print('{} = dict.{}'.format(_Builtin.convert(method), method))

        for protected in _Builtin.get_protected(BeneDict):
            original_name = _Builtin.restore(protected)
            if original_name not in dir(builtin_type):
                print('{} = {}'.format(protected, original_name))


class BeneDict(dict):
    """
    BeneDict enables accessing dict values by attribute, just like Javascript's
    dot notation. Supports JSON/YAML operations.

    Adapted from: https://github.com/makinacorpus/EasyDict

    Notes:
      Use `dict.items()` if you know there might be conflict in the keys
      or `builtin_` + method name

    Added methods: the version always prefixed by `builtin` is protected against
      changes. You can use the non-prefixed version if you know for sure that
      the name will never be overriden

    >>> d = BeneDict({'foo':3})
    >>> d['foo']
    3
    >>> d.foo
    3
    >>> d.bar
    Traceback (most recent call last):
    ...
    AttributeError: 'BeneDict' object has no attribute 'bar'

    Works recursively

    >>> d = BeneDict({'foo':3, 'bar':{'x':1, 'y':2}})
    >>> isinstance(d.bar, dict)
    True
    >>> d.bar.x
    1

    Bullet-proof

    >>> BeneDict({})
    {}
    >>> BeneDict(d={})
    {}
    >>> BeneDict(None)
    {}
    >>> d = {'a': 1}
    >>> BeneDict(**d)
    {'a': 1}

    Set attributes

    >>> d = BeneDict()
    >>> d.foo = 3
    >>> d.foo
    3
    >>> d.bar = {'prop': 'value'}
    >>> d.bar.prop
    'value'
    >>> d
    {'foo': 3, 'bar': {'prop': 'value'}}
    >>> d.bar.prop = 'newer'
    >>> d.bar.prop
    'newer'


    Values extraction

    >>> d = BeneDict({'foo':0, 'bar':[{'x':1, 'y':2}, {'x':3, 'y':4}]})
    >>> isinstance(d.bar, list)
    True
    >>> from operator import attrgetter
    >>> map(attrgetter('x'), d.bar)
    [1, 3]
    >>> map(attrgetter('y'), d.bar)
    [2, 4]
    >>> d = BeneDict()
    >>> d.keys()
    []
    >>> d = BeneDict(foo=3, bar=dict(x=1, y=2))
    >>> d.foo
    3
    >>> d.bar.x
    1

    Still like a dict though

    >>> o = BeneDict({'clean':True})
    >>> o.items()
    [('clean', True)]

    Can be inherited, subclass will be recursively applied to dict objects.

    Any new methods added in subclass will have a prefixed version "builtin_"
    that protected overwriting.
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
            # prevent cyclic import
            from benedict.ordered import OrderedBeneDict
            if isinstance(d_items, (BeneDict, OrderedBeneDict)):
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
        # cls = BeneDict
        if name in cls._PROTECTED_METHODS:
            raise ValueError('Cannot override `{}()`: {} protected method'
                             .format(name, cls.__name__))
        if isinstance(value, (list, tuple)):
            value = type(value)(cls(x) if isinstance(x, abc.Mapping) else x
                                for x in value)
        elif isinstance(value, abc.Mapping):
            # implements deepcopy if BeneDict(BeneDict())
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
        return benedict_to_dict(self)

    def deepcopy(self):
        return self.__class__(self)

    @classmethod
    def load_json_file(cls, file_path, **loader_kwargs):
        return cls(df.load_json_file(file_path, **loader_kwargs))

    @classmethod
    def load_json_str(cls, string, **loader_kwargs):
        return cls(df.load_json_str(string, **loader_kwargs))

    @classmethod
    def load_yaml_file(cls, file_path, **loader_kwargs):
        return cls(df.load_yaml_file(file_path, **loader_kwargs))

    @classmethod
    def load_yaml_str(cls, string, **loader_kwargs):
        return cls(df.load_yaml_str(string, **loader_kwargs))

    @classmethod
    def load_file(cls, file_path, **loader_kwargs):
        """
        Args:
            file_path: JSON or YAML loader depends on the file extension

        Raises:
            IOError: if extension is not ".json", ".yml", or ".yaml"
        """
        return cls(df.load_file(file_path, **loader_kwargs))

    def dump_json_file(self, file_path, **dumper_kwargs):
        df.dump_json_file(benedict_to_dict(self), file_path, **dumper_kwargs)

    def dump_json_str(self, **dumper_kwargs):
        "Returns: string"
        return df.dump_json_str(benedict_to_dict(self), **dumper_kwargs)

    def dump_yaml_file(self, file_path, **dumper_kwargs):
        df.dump_yaml_file(benedict_to_dict(self), file_path, **dumper_kwargs)

    def dump_yaml_str(self, **dumper_kwargs):
        "Returns: string"
        return df.dump_yaml_str(benedict_to_dict(self), **dumper_kwargs)

    def dump_file(self, file_path, **dumper_kwargs):
        """
        Args:
            file_path: JSON or YAML loader depends on the file extension

        Raises:
            IOError: if extension is not ".json", ".yml", or ".yaml"
        """
        df.dump_file(benedict_to_dict(self), file_path, **dumper_kwargs)

    def __getstate__(self):
        """
        Support pickling.
        Warning:
          if this BeneDict overrides dict builtin methods, like `items`,
          pickle will report error.
          don't know how to resolve yet
        """
        return benedict_to_dict(self)

    def __setstate__(self, state):
        self.__init__(state)

    def __str__(self):
        return str(benedict_to_dict(self))

    # we explicitly list them here so that IDEs like PyCharm can do auto-complete
    # call _print_protected_methods() to generate this code
    builtin_clear = dict.clear
    builtin_copy = dict.copy
    builtin_fromkeys = dict.fromkeys
    builtin_get = dict.get
    builtin_items = dict.items
    builtin_keys = dict.keys
    builtin_pop = dict.pop
    builtin_popitem = dict.popitem
    builtin_setdefault = dict.setdefault
    builtin_update = dict.update
    builtin_values = dict.values
    builtin_deepcopy = deepcopy
    builtin_dump_json_file = dump_json_file
    builtin_dump_json_str = dump_json_str
    builtin_dump_yaml_file = dump_yaml_file
    builtin_dump_yaml_str = dump_yaml_str
    builtin_dump_file = dump_file
    builtin_load_json_file = load_json_file
    builtin_load_json_str = load_json_str
    builtin_load_yaml_file = load_yaml_file
    builtin_load_yaml_str = load_yaml_str
    builtin_load_file = load_file
    builtin_to_dict = to_dict


def benedict_to_dict(D, to_type=dict):
    """
    Recursively convert back to builtin dict type
    """
    d = to_type()
    for k, value in to_type.items(D):
        if isinstance(value, abc.Mapping):
            d[k] = benedict_to_dict(value, to_type=to_type)
        elif isinstance(value, (list, tuple)):
            d[k] = type(value)(
                benedict_to_dict(v, to_type=to_type)
                if isinstance(v, BeneDict)
                else v for v in value
            )
        else:
            d[k] = value
    return d


if __name__ == '__main__':
    _Builtin.print_protected(dict)