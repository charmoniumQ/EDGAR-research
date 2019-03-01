from .config import config
import logging
import base64
import contextlib
import docker
import subprocess
# https://github.com/kubernetes-client/python/blob/master/kubernetes/README.md
import kubernetes
from . import utils


def prepare_images():
    subprocess.run(['gcloud', '--quiet', 'auth', 'configure-docker'], capture_output=True)
    client = docker.from_env()

    cluster_dir = config.project_dir / 'edgar_cluster'
    with utils.time_code(f'docker'):
        for dockerfolder in cluster_dir.glob('*'):
            name = dockerfolder.name
            tag = f'gcr.io/{config.gcloud.project}/{name}:latest'

            # with utils.time_code(f'docker build {name}'):
            output_gen = client.images.build(
                path=str(dockerfolder),
                tag=tag,
                quiet=False,
                nocache=False,
                rm=False,
                # TODO: does this need cache_from=[tag]
            )
            for level, output in utils.flatten_gen(output_gen):
                print(f'docker: {level * " "}{output}')

            # with utils.time_code(f'docker push {name}'):
            client.images.push(tag)


@contextlib.contextmanager
def kubernetes_namespace(kube_api, namespace):
    kube_v1 = kubernetes.client.CoreV1Api(kube_api)
    kube_v1.create_namespace(
        kubernetes.client.V1Namespace(
            metadata=kubernetes.client.V1ObjectMeta(
                name=namespace,
            ),
        ),
    )
    try:
        yield
    finally:
        kube_v1.delete_namespace(
            namespace,
            body=kubernetes.client.V1DeleteOptions(
                propagation_policy='Foreground',
            ),
        )


def setup_kubernetes(kube_api, namespace, n_workers):
    ports = {
        'scheduler': 8786,
        'dashboard': 8787,
    }

    kube_v1 = kubernetes.client.CoreV1Api(kube_api)
    kube_v1beta = kubernetes.client.ExtensionsV1beta1Api(kube_api)
    kube_v1batch = kubernetes.client.BatchV1Api(kube_api)

    # TODO: async parallelism
    # TODO: support mulitple usages
    # TODO: include uninitialized

    with utils.time_code('kube deploy'):
        with open(config.gcloud.service_account_file, 'rb') as f:
            service_account_data = base64.encodebytes(f.read()).decode()

        kube_v1.create_namespaced_secret(
            namespace,
            kubernetes.client.V1Secret(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='service-account',
                ),                
                data={
                    'key.json': service_account_data,
                },
            ),
        )

        secret_volume_mount = kubernetes.client.V1VolumeMount(
            mount_path='/var/secrets/google',
            name='service-account',
        )
        secret_volume = kubernetes.client.V1Volume(
            name='service-account',
            secret=kubernetes.client.V1SecretVolumeSource(
                secret_name='service-account',
            ),
        )
        env_vars = [
            kubernetes.client.V1EnvVar(
                name=name,
                value=value,
            ) for name, value in {
                'GOOGLE_APPLICATION_CREDENTIALS': f'{secret_volume_mount.mount_path}/key.json',
                'gcloud_project': config.gcloud.project,
                'gcloud_topic': f'{namespace}-topic',
                'gcloud_subscription': f'{namespace}-subscription-2',
                'n_workers': str(n_workers),
                'dask_scheduler_address': f'tcp://scheduler:{ports["scheduler"]}',
            }.items()
        ]

        kube_v1beta.create_namespaced_deployment(
            namespace,
            kubernetes.client.ExtensionsV1beta1Deployment(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='scheduler',
                ),
                spec=kubernetes.client.ExtensionsV1beta1DeploymentSpec(
                    replicas=1,
                    selector=kubernetes.client.V1LabelSelector(
                        match_labels=dict(
                            deployment='scheduler',
                        )
                    ),
                    template=kubernetes.client.V1PodTemplateSpec(
                        metadata=kubernetes.client.V1ObjectMeta(
                            labels=dict(
                                deployment='scheduler',
                            ),
                        ),
                        spec=kubernetes.client.V1PodSpec(
                            # TODO: memory and cpu limits
                            containers=[
                                kubernetes.client.V1Container(
                                    name='scheduler',
                                    image=f'gcr.io/{config.gcloud.project}/scheduler:latest',
                                    ports=[
                                        kubernetes.client.V1ContainerPort(container_port=port, name=name)
                                        for name, port in ports.items()
                                    ],
                                    command=[
                                        '/bin/sh', '-c', f'unbuffer dask-scheduler --port {ports["scheduler"]} --no-bokeh | unbuffer -p /app/update_status.py',
                                    ],
                                    volume_mounts=[secret_volume_mount],
                                    env=env_vars,
                                ),
                            ],
                            volumes=[secret_volume],
                        ),
                    ),
                ),
            ),
        )

        kube_v1.create_namespaced_service(
            namespace,
            kubernetes.client.V1Service(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='scheduler',
                ),
                spec=kubernetes.client.V1ServiceSpec(
                    selector={'deployment': 'scheduler'},
                    type='ClusterIP',
                    ports=[
                        kubernetes.client.V1ServicePort(port=port, target_port=name, name=name)
                        for name, port in ports.items()
                    ],
                ),
            ),
        )

        kube_v1beta.create_namespaced_deployment(
            namespace,
            kubernetes.client.ExtensionsV1beta1Deployment(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='worker',
                ),
                spec=kubernetes.client.ExtensionsV1beta1DeploymentSpec(
                    replicas=n_workers,
                    selector=kubernetes.client.V1LabelSelector(
                        match_labels=dict(
                            deployment='scheduler',
                        )
                    ),
                    template=kubernetes.client.V1PodTemplateSpec(
                        metadata=kubernetes.client.V1ObjectMeta(
                            labels=dict(
                                deployment='scheduler',
                            ),
                        ),
                        spec=kubernetes.client.V1PodSpec(
                            # TODO: memory and cpu limits
                            containers=[
                                kubernetes.client.V1Container(
                                    name='worker',
                                    image=f'gcr.io/{config.gcloud.project}/worker:latest',
                                    command=[
                                        '/bin/sh', '-c', 'dask-worker ${dask_scheduler_address}'
                                        # TODO: memory management
                                        # TODO: nprocs
                                        # TODO: nthreads
                                    ],
                                    volume_mounts=[secret_volume_mount],
                                    env=env_vars,
                                ),
                            ],
                            volumes=[secret_volume],
                        ),
                    ),
                ),
            ),
        )

        kube_v1batch.create_namespaced_job(
            namespace,
            kubernetes.client.V1Job(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='job',
                ),
                spec=kubernetes.client.V1JobSpec(
                    completions=1,
                    backoff_limit=0,
                    template=kubernetes.client.V1PodTemplateSpec(
                        spec=kubernetes.client.V1PodSpec(
                            restart_policy='Never',
                            # TODO: memory and cpu limits
                            containers=[
                                kubernetes.client.V1Container(
                                    name='job',
                                    image=f'gcr.io/{config.gcloud.project}/job:latest',
                                    command=[
                                        '/bin/sh', '-c', '/work/wait_for_scheduler.py && /work/test.py',
                                    ],
                                    volume_mounts=[secret_volume_mount],
                                    env=env_vars,
                                ),
                            ],
                            volumes=[secret_volume],
                        ),
                    ),
                ),
                # TODO: name specific to this instance
            ),
        )

# kubectl -n ed logs $(kubectl -n ed get -o 'jsonpath={.items[].metadata.name}' pods -l deployment=scheduler)
# kubectl -n ed logs $(kubectl -n ed get -o 'jsonpath={.items[].metadata.name}' pods -l job-name=job)
# kubectl -n ed run -it --generator=run-pod/v1 --image gcr.io/edgar-research/worker test -- ed-worker tcp://scheduler.edgar-research.svc.cluster.local
