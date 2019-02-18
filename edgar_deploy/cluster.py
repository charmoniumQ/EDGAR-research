import logging


class Cluster(object):
    def __init__(self, name=None, nodecount=3, save=False, load=False, reconfigure=False):
        self.name = name
        self.nodecount = nodecount
        self.load = load
        self.save = save

    def __enter__(self):
        if self.load:
            logging.info('Loading cluster')
            if self._load():
                pass
            else:
                logging.info('No cluster to load; creating cluster')
                self._create()
        else:
            logging.info('Creating cluster')
            self._create()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.save:
            logging.info('Deleting cluster')
            self._delete()
        else:
            logging.info('Not deleting cluster')

    def _load(self):
        # return True iff cluster could be loaded
        raise NotImplementedError('over eyed me')

    def _create(self):
        raise NotImplementedError('over eyed me')

    def _delete(self):
        raise NotImplementedError('over eyed me')
