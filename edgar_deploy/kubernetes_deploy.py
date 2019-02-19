from .config import config
import logging
import contextlib
import docker
import subprocess
import kubernetes
from . import utils


def prepare_images():
    subprocess.run(['gcloud', '--quiet', 'auth', 'configure-docker'], capture_output=True)
    client = docker.from_env()

    cluster_dir = config.project_dir / 'edgar_cluster'
    with utils.time_code(f'docker'):
        for dockerfile in cluster_dir.glob('Dockerfile-*'):
            name = dockerfile.name.split('-')[-1]
            tag = f'gcr.io/{config.gcloud.project}/{name}:latest'

            # with utils.time_code(f'docker build {name}'):
            client.images.build(
                path=str(cluster_dir),
                dockerfile=dockerfile,
                tag=tag,
                quiet=False,
                nocache=False,
                rm=False,
                # TODO: does this need cache_from=[tag]
            )

            # with utils.time_code(f'docker push {name}'):
            client.images.push(tag)


@contextlib.contextmanager
def deploy_kubernetes(kube_api, namespace, n_workers):
    ports = {
        'scheduler': 8786,
        # 'dashboard': 8787,
    }
    scheduler_address = f'tcp://scheduler:{ports["scheduler"]}'

    kube_v1 = kubernetes.client.CoreV1Api(kube_api)
    kube_v1beta = kubernetes.client.ExtensionsV1beta1Api(kube_api)
    kube_v1batch = kubernetes.client.BatchV1Api(kube_api)

    # TODO: async parallelism
    # TODO: support mulitple usages
    # TODO: include uninitialized

    with utils.time_code('kube deploy'):
        kube_v1.create_namespace(
            kubernetes.client.V1Namespace(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=namespace,
                ),
            ),
        )
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
                                        '/bin/sh', '-c', f'dask-scheduler --port {ports["scheduler"]} --no-bokeh | tee log', 
                                    ],
                                    # readiness_probe=kubernetes.client.V1Probe(
                                    #     _exec=kubernetes.client.V1ExecAction(
                                    #         command=[
                                    #             '/bin/sh', '-c', f"test {n_workers} -le $(grep 'Starting established connection' log | wc -l)",
                                    #         ],
                                    #     ),
                                    # ),
                                ),
                            ],
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
                                        '/bin/sh', '-c', f'dask-worker {scheduler_address} | tee log'
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
        kube_v1.delete_namespace(namespace, body=delete_opts)

if __name__ == '__main__':
    # kubectl --namespace dask delete deployments,services,jobs -l use=temporary
    # prepare_images()
    from .gke_cluster import GKECluster
    g = GKECluster('test-cluster-1', load=True, save=False)
    with g:
        with deploy_kubernetes(g.kube_api, g.managed_namespace, g.nodecount):
            print('done ish')
            input()
            # kubectl -n dask logs $(kubectl -n dask get -o 'jsonpath={.items[].metadata.name}' pods -l job-name=job)
    # kubectl -n dask run -it --generator=run-pod/v1 --image gcr.io/edgar-research/worker test -- dask-worker tcp://scheduler.edgar-research.svc.cluster.local
