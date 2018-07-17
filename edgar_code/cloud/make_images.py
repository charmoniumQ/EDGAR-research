from . import config
import docker
import tempfile
import shutil

def main():
    client, repo, name = config.get_docker()
    version = 'worker'
    tag = f'{repo}/{name}:{version}'

    dockerfile = 'edgar_code/cloud/Dockerfile-worker'
    image = client.images.build(path='.', tag=tag, dockerfile=dockerfile)

    client.images.push(f'{repo}/{name}', version)

if __name__ == '__main__':
    main()
