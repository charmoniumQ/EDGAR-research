from .config import config
import logging
import contextlib
import docker
import subprocess
import kubernetes
from . import utils


def prepare_images():
    subprocess.run(['gcloud', '--quiet', 'auth', 'configure-docker'])
    client = docker.from_env()

    cluster_dir = config.project_dir / 'edgar_cluster'
    for dockerfile in cluster_dir.glob('Dockerfile-*'):
        name = dockerfile.name.split('-')[-1]
        tag = f'gcr.io/{config.gcloud.project}/{name}:latest'

        with utils.time_code('docker build'):
            client.images.build(
                path=str(cluster_dir),
                dockerfile=dockerfile,
                tag=tag,
                quiet=False,
                nocache=False,
                rm=False,
            )

        with utils.time_code('docker push'):
            client.images.push(tag)


@contextlib.contextmanager
def deploy_kubernetes(cluster):
    ports = {
        'scheduler': 8786,
        # 'dashboard': 8787,
    }
    scheduler_address = f'tcp://scheduler:{ports["scheduler"]}'

    kube_v1 = kubernetes.client.CoreV1Api(cluster.kube_api)
    kube_v1beta = kubernetes.client.ExtensionsV1beta1Api(cluster.kube_api)
    kube_v1batch = kubernetes.client.BatchV1Api(cluster.kube_api)

    # TODO: async parallelism
    # TODO: support mulitple usages
    # TODO: include uninitialized

    with utils.time_code('kube deploy'):
        kube_v1beta.create_namespaced_deployment(
            cluster.namespace,
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
                                        'dask-scheduler',
                                        '--port', str(ports['scheduler']),
                                        # '--bokeh-port', str(ports['dashboard']),
                                        '--no-bokeh',
                                        '--host', '0.0.0.0',
                                    ],
                                ),
                            ],
                        ),
                    ),
                ),
            ),
        )

        kube_v1.create_namespaced_service(
            cluster.namespace,
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
            cluster.namespace,
            kubernetes.client.ExtensionsV1beta1Deployment(
                metadata=kubernetes.client.V1ObjectMeta(
                    name='worker',
                ),
                spec=kubernetes.client.ExtensionsV1beta1DeploymentSpec(
                    replicas=cluster.nodecount,
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
                                        'dask-worker', scheduler_address,
                                        # TODO: memory management
                                        # TODO: nprocs
                                        # TODO: nthreads
                                    ],
                                ),
                            ],
                        ),
                    ),
                ),
            ),
        )

        kube_v1batch.create_namespaced_job(
            cluster.namespace,
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
                                        'python3', '/work/test.py', scheduler_address,
                                        # TODO: memory management
                                        # TODO: nprocs
                                        # TODO: nthreads
                                    ],
                                ),
                            ],
                        ),
                    ),
                ),
                # TODO: name specific to this instance
            ),
        )
    yield
    with utils.time_code('kube delete'):
        delete_opts = kubernetes.client.V1DeleteOptions(
            propagation_policy='Foreground'
        )
        kube_v1beta.delete_namespaced_deployment('scheduler', cluster.namespace, body=delete_opts)
        kube_v1.delete_namespaced_service('scheduler', cluster.namespace, body=delete_opts)
        kube_v1beta.delete_namespaced_deployment('worker', cluster.namespace, body=delete_opts)
        kube_v1batch.delete_namespaced_job('job',cluster.namespace, body=delete_opts)

if __name__ == '__main__':
    # kubectl --namespace dask delete deployments,services,jobs -l use=temporary
    # prepare_images()
    from .gke_cluster import GKECluster
    g = GKECluster('test-cluster-1', load=True, save=True)
    with g:
        with deploy_kubernetes(g):
            print('done ish')
            input()
            # kubectl -n dask logs $(kubectl -n dask get -o 'jsonpath={.items[].metadata.name}' pods -l job-name=job)
    # kubectl -n dask run -it --generator=run-pod/v1 --image gcr.io/edgar-research/worker test -- dask-worker tcp://scheduler.edgar-research.svc.cluster.local
