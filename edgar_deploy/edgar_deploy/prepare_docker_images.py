import docker
import subprocess
import itertools
from concurrent.futures import ThreadPoolExecutor
from .time_code import time_code
from .config import config
from . import utils


def prepare_docker_images(cache_dir=None):
    subprocess.run(
        ['gcloud', '--quiet', 'auth', 'configure-docker'],
        capture_output=True
    )

    prepare_docker_image_cached(
        config.deploy_dir / 'dockerfiles/common',
        cache_dir, push=False, tag='common',
    )

    dockerfolders = [
        folder for folder in (config.deploy_dir / 'dockerfiles').iterdir()
        if folder.name != 'common'
    ]

    executor = ThreadPoolExecutor(max_workers=len(dockerfolders))
    compiled_images = {
        dockerfolder.name: executor.submit(
            prepare_docker_image_cached, dockerfolder, cache_dir, True
        )
        for dockerfolder in dockerfolders
    }
    return compiled_images


def prepare_docker_image(dockerfolder, push, tag):
    name = dockerfolder.name
    version = utils.rand_name(20, lowercase=True, digits=True)
    if tag is None:
        tag = f'gcr.io/{config.gcloud.project}/{name}:{version}'

    with time_code.ctx(f'docker build/push {tag}', print_start=True, print_time=True):
        client = docker.from_env()

        client.images.build(
            path=str(dockerfolder),
            tag=tag,
            quiet=True,
            nocache=False,
            rm=True, # should this be False?
            # TODO: does this need cache_from=[tag]
        )

        if push:
            client.images.push(tag)

        return tag


from .modtime import modtime, modtime_recursive
def prepare_docker_image_cached(dockerfolder, cache_dir, push=True, tag=None):
    name = dockerfolder.name

    if cache_dir is not None:
        cache_file = cache_dir / 'prepare_docker_image' / name
        if cache_file.exists():
            last_updated = modtime(cache_file)
            if modtime_recursive(dockerfolder) < last_updated:
                with open(cache_file, 'r') as f:
                    return f.read()
            else:
                pass
    else:
        cache_file = None

    tag = prepare_docker_image(dockerfolder, push, tag)

    if cache_file is not None:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as fil:
            fil.write(tag)

    return tag
