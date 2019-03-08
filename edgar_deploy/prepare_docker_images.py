import docker
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor
from . import utils
from .config import config


def prepare_docker_images(cache_dir=None):
    subprocess.run(
        ['gcloud', '--quiet', 'auth', 'configure-docker'],
        capture_output=True
    )
    dockerfolders = list((config.project_dir / 'edgar_cluster').iterdir())
    executor = ThreadPoolExecutor(max_workers=len(dockerfolders))
    compiled_images = {
        dockerfolder.name: executor.submit(
            prepare_docker_image_cached, dockerfolder, cache_dir
        )
        for dockerfolder in dockerfolders
    }
    return compiled_images


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


from .modtime import modtime, modtime_recursive
def prepare_docker_image_cached(dockerfolder, cache_dir):
    name = dockerfolder.name

    if cache_dir is not None:
        cache_file = cache_dir / 'prepare_docker_image' / name
        if cache_file.exists():
            last_updated = modtime(cache_file)
            if modtime_recursive(dockerfolder) < last_updated:
                with open(cache_file, 'r') as f:
                    return f.read()
            else:
                logging.info(f'{dockerfolder} new; building fresh image')
    else:
        cache_file = None

    tag = prepare_docker_image(dockerfolder)

    if cache_file is not None:
        with open(cache_file, 'w') as f:
            f.write(tag)

    return tag
