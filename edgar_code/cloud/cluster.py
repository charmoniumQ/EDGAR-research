from haikunator import Haikunator
import logging

class ResourceNotFoundError(Exception):
    pass


class Cluster(object):
    def __init__(self, name=None, nodecount=3, save=False, load=False):
        if not name:
            name = Haikunator.haikunate(0, '-')
        self.name = name
        self.nodecount = nodecount
        self.load = load
        self.save = save

    def __enter__(self):
        if self.load:
            logging.info('Loading cluster')
            if self._cluster_load():
                pass
            else:
                logging.info('No cluster to load; creating cluster')
                self._cluster_create()
        else:
            logging.info('Creating cluster')
            self._cluster_create()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.save:
            logging.info('Deleting cluster')
            self._cluster_delete()
        else:
            logging.info('Not deleting cluster')

    def _cluster_load(self):
        # return True iff cluster could not be loaded
        raise NotImplementedError('over eyed me')

    def _cluster_create(self):
        raise NotImplementedError('over eyed me')

    def _cluster_delete(self):
        raise NotImplementedError('over eyed me')
