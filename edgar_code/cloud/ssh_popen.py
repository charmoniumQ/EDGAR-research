import shlex
import paramiko


class SSHClient(paramiko.client.SSHClient):
    def SSHPopen(self, args, stdin=None, input=None, stdout=None, stderr=None,
                 shell=False, cwd=None, timeout=None, check=False,
                 errors=None, bufsize=-1, env=None):
        if shell:
            command = args
        else:
            command = ' '.join(map(shlex.quote, args))
        channel = self.invoke_shell(environment=env)
        channel.send(
            command=command, bufsize=bufsize, timeout=timeout
        )
        return SSHPopen(channel, args)

    def close(self):
        self.close()


class SSHPopen(object):
    def __init__(self, channel, args):
        self.channel = channel
        self.args = args

    def poll(self):
        self.channel.exit_status_ready()

    def wait(self, timeout=None):
        self.channel.settimeout(timeout)
        self.poll()

    def communicate(self, input=None, timeout=None):
        self.channel.settimeout(timeout)
        self.channel.sendall(input)

        stdout = b''
        if self.channel.recv_ready():
            stdout += self.channel.recv(1000)

        stderr = b''
        if self.channel.recv_stderr_ready():
            stderr += self.channel.recv_stderr(1000)

        return stdout, stderr

    def send_signal(self):
        raise NotImplementedError()

    def terminate(self):
        raise NotImplementedError()

    def kill(self):
        raise NotImplementedError()

    @property
    def pid(self):
        raise NotImplementedError()

    @property
    def returncode(self):
        return self.channel.recv_exit_status()

    def close(self):
        self.channel.close()
