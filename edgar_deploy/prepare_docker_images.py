import threading
import docker
import subprocess
from concurrent.futures import ThreadPoolExecutor
from . import utils
from .config import config


def prepare_docker_images():
    subprocess.run(
        ['gcloud', '--quiet', 'auth', 'configure-docker'],
        capture_output=True
    )
    dockerfolders = list((config.project_dir / 'edgar_cluster').iterdir())
    executor = ThreadPoolExecutor(max_workers=len(dockerfolders))
    compiled_images = {
        dockerfolder.name: executor.submit(prepare_docker_image, dockerfolder)
        for dockerfolder in dockerfolders
    }
    return compiled_images


import threading
def prepare_docker_image(dockerfolder):
    name = dockerfolder.name
    version = utils.rand_name(20, lowercase=True, digits=True)
    tag = f'gcr.io/{config.gcloud.project}/{name}:{version}'

    with utils.time_code(f'docker build/push {name}'):
        client = docker.from_env()
        try:
            output = list(client.images.build(
                path=str(dockerfolder),
                tag=tag,
                quiet=False,
                nocache=False,
                rm=False,
                # TODO: does this need cache_from=[tag]
            ))
        except Exception as e:
            print('\n'.join(map(str, output)))
            raise e
        client.images.push(tag)
    return tag
