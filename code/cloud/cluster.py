import stat
import io
from haikunator import Haikunator
import contextlib


class Cluster(object):
    def __init__(self, provisioner, roles):
        self.provisioner = provisioner
        self.roles = roles

    @contextlib.contextmanager
    def provisioned(self, name=None, save=False, load=False):
        if not name:
            name = Haikunator.haikunate(0, '_')
        self.name = name
        self._provision_instances(load)
        self._configure_instances()
        yield
        self._deprovision_instances(save)

    def _provision_instances(self, load):
        if load:
            all_instances = self.provisioner.find_instances(self.name)
            for role, these_instances in all_instances.items():
                self.roles[role]['instances'] = these_instances
        else:
            for role_name, conf in self.roles.items():
                instances = self.provisioner.create_instances(
                    self.name,
                    role_name,
                    **conf['create_instances']
                )
                conf['instances'] = instances

        for role_name, conf in self.roles.items():
            conf['shells'] = self.provisioner.connect_to_instances(
                conf['instances'], **conf['connect_to_instances']
            )

    def _configure_instances(self):
        setup_script = b'./setup.sh'
        command = b'nohup ./setup.sh > out.txt 2> err.txt &'
        for role_name, role in self.roles.items():
            for shell in role['shells']:
                sftp = shell.open_sftp()
                new_setup = io.BytesIO(role['setup'])
                sftp.putfo(new_setup, setup_script)
                sftp.chmod(setup_script, stat.S_IRWXU)
                shell.exec_command(command)

    def _check_instances(self):
        for role_name, role in self.roles.items():
            for shell in role['shells']:
                sftp = shell.open_sftp()
                pass

    def _deprovision_instances(self, save):
        if not save:
            for role in self.roles:
                self.provisioner._deprovision(role['instances'])
