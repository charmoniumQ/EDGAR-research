class Cache(object):
    def __init__(self, s3fs, bucket, suffix=''):
        '''
        Note this uses function.__name__ to determine the file name.
        If this is not unique within your program, define suffix'''
        self.dct = dct
        self.s3fs = s3fs
        self.suffix = suffix

    def __call__(self, *args, **kwargs):
        if hasattr(self, 'function'):
            return self.cached_function(*args, **kwargs)
        else:
            return self.set_function(*args, **kwargs)

    def set_function(self, function):
        '''Gets called when the decarotor is applied'''
        self.function = function
        self.name = function.__name__ + self.suffix
        self.load(self.name)
        return self

    def load(self):
        

    def cached_function(self, *args, **kwargs):
        '''gets called where the funciton would be called'''
        key = self.hashable_args(*args, **kwargs)
        if key in self.dct:
            res = self.dct[key]
            if self.hit_msg:
                print(self.hit_msg.replace('{key}', repr((args, kwargs))))
        else:
            res = self.function(*args, **kwargs)
            self.dct[key] = res
            if self.miss_msg:
                print(self.miss_msg.replace('{key}', repr((args, kwargs))))
        return res

    @classmethod
    def hashable_args(Cls, *args, **kwargs):
        return Cls.hashable((args, kwargs))

    @classmethod
    def hashable(Cls, obj):
        '''Converts args and kwargs into a hashable type (overridable)'''
        try:
            hash(obj)
        except:
            if hasattr(obj, 'items'):
                # turn dictionaries into frozenset((key, val))
                return frozenset((key, Cls.hashable(val))
                                 for key, val in obj.items())
            elif hasattr(obj, '__iter__'):
                # turn iterables into tuples
                return tuple(Cls.hashable(val) for val in obj)
            else:
                raise TypeError("I don't know how to hash {obj} ({type})"
                                .format(type=type(obj), obj=obj))
        else:
            return obj

    def clear(self):
        self.dct.clear()

    def clear_item(self, *args, **kwargs):
        key = self.hashable_args(*args, **kwargs)
        del self.dct[key]

    def __repr__(self):
        dct_type = type(self.dct).__name__
        if hasattr(self, 'function'):
            return 'Cache of {self.function!r} with {dct_type}' \
                .format(**locals())
        else:
            return 'Unapplied decorator with {dct_type}' \
                .format(**locals())
