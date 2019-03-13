import abc
from pathlib import Path
import logging
import pickle
from . import utils


class ProvisionedResource(abc.ABC):
    '''
A superclass for resources whose provisioning is expensive.
It allows them to be saved and loaded instead of created from fresh.
In order to do this transparently from the caller, call

    Subclass.create_orload(*args, save=False, **kwargs)

Use 'with' or call close explicitly to potentially save the resource.
Provision in Subclass.__init__(*args, **kwargs)
'''

    @classmethod
    def create_or_load(Cls, *args, should_save=False, **kwargs):
        '''
save: determines if the resource gets saved on close.
*args:
**kwargs: passed to Subclass.__init__
'''
        if Cls.present(*args, **kwargs):
            ret = Cls.load(*args, **kwargs)
            logging.info(f'Loaded {Cls.__name__}')
        else:
            ret = Cls(*args, **kwargs)
            logging.info(f'Created {Cls.__name__}')
        ret.opened = True
        ret.should_save = should_save
        return ret

    def close(self):
        if self.opened:
            if not self.should_save:
                logging.info(f'Deleted {type(self).__name__}')
                self.delete()
            else:
                logging.info(f'Saving {type(self).__name__}')
                self.save()
            self.opened = False
        else:
            logging.info(f'Already closed {type(self).__name__}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @classmethod
    def present(Cls, *args, **kwargs):
        return False

    @classmethod
    @abc.abstractmethod
    def load(Cls, *args, **kwargs):
        pass

    @abc.abstractmethod
    def save(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass


class FileProvisionedResource(ProvisionedResource):
    '''ProvisionedResource which is saved/loaded from a file.
version is important. Stored instances of the same class with a different version will not be loaded.
Override delete, but call super.
Subclass.__init__ should take cache_dir and name. It should call this __init__ with all kwargs.
'''

    version = '1.0.0'

    def __init__(self, *args, cache_dir=None, name=None, **kwargs):
        if cache_dir is None:
            raise ValueError('cache_dir is not optional')
        if name is None:
            raise ValueError('name is not optional')
        self.cache_path = self.get_cache_path(cache_dir, name)

    @classmethod
    def get_cache_path(Cls, cache_dir, name):
        return Path(cache_dir) / Cls.__name__ / Cls.version / f'{name}.pickle'

    @classmethod
    def present(Cls, cache_dir, name, *args, **kwargs):
        cache_path = Cls.get_cache_path(cache_dir, name)
        return cache_path.exists()

    @classmethod
    def load(Cls, cache_dir, name, *args, **kwargs):
        cache_path = Cls.get_cache_path(cache_dir, name)
        with open(cache_path, 'rb') as f:
            obj = pickle.load(f)
        obj.cache_path = cache_path
        return obj

    def save(self):
        '''default implementation uses pickle.
If only certain attributes need to be saved/loaded, write Subclass.__getstate__/Subclass.__setstate__ methods.
If pickle is not desired at all, write a custom Subclass.save/Subclass.load methods'''

        self.cache_path.parent.mkdir(exist_ok=True, parents=True)
        with open(self.cache_path, 'wb') as f:
            pickle.dump(self, f)

    def delete(self):
        if self.cache_path.exists():
            self.cache_path.unlink()

    @classmethod
    def list(Cls, cache_dir):
        name_dir = Path(cache_dir) / Cls.__name__ / Cls.version
        for fname in name_dir.iterdir():
            name = fname.relative_to(name_dir).name
            yield name

    @classmethod
    def delete_all(Cls):
        for name in Cls.list():
            Cls.unlink()


def test():
    class CoolResource(FileProvisionedResource):
        @classmethod
        def _create2(Cls, *args, **kwargs):
            return CoolResource()

    cache_dir = '/tmp/cache'

    with CoolResource.create_orload(
            cache_dir=cache_dir,
            name='resource_for_project',
            save=True
    ) as rec:
        pass

    for name in CoolResource.list(cache_dir):
        resource = CoolResource.create_or_load(cache_dir, name, should_save=False)
        print(f'{name} exists')
        resource.close()



if __name__ == '__main__':
    module_name = sys.argv[1]
    class_name = sys.argv[2]
    import importlib
    module = importlib.__import__(module_name)
    try:
        Cls = getattr(module, class_name)
    except AttributeError:
        raise RuntimeError(f'{class_name} not found in {module_name}')
    Cls.delete_all()
