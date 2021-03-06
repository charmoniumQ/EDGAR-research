import os
import base64
import contextlib
# https://github.com/kubernetes-client/python/blob/master/kubernetes/README.md
import kubernetes
from .config import config
from .time_code import time_code


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


@time_code.decor(print_start=False)
def setup_kubernetes(kube_api, namespace, n_workers, images, public_egg_path, run_module):
    ports = {
        'scheduler': 8786,
        'dashboard': 8787,
        'nameserver': 9090,
    }

    kube_v1 = kubernetes.client.CoreV1Api(kube_api)
    kube_v1beta = kubernetes.client.ExtensionsV1beta1Api(kube_api)
    kube_v1batch = kubernetes.client.BatchV1Api(kube_api)

    # TODO: async parallelism
    # TODO: support mulitple usages
    # TODO: include uninitialized

    with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'rb') as f:
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
            'N_WORKERS': str(n_workers),
            'DEPLOY_EGG': public_egg_path,
            'RUN_MODULE': run_module,
            # 'PYRO_SERIALIZERS_ACCEPTED': 'pickle',
            # 'PYRO_SERIALIZER': 'pickle',
            # 'PYRO_LOGLEVEL': 'DEBUG',
            # 'PYRO_THREADPOOL_SIZE': '80',
        }.items()
    ]

    memory = int(1e6) # in KiB

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
                                image=images['scheduler'].result(),
                                ports=[
                                    kubernetes.client.V1ContainerPort(
                                        container_port=port, name=name
                                    )
                                    for name, port in ports.items()
                                ],
                                command=[
                                    '/bin/sh', '-c',
                                    f'dask-scheduler --port ${{SCHEDULER_SERVICE_PORT_SCHEDULER}} --dashboard-address 0.0.0.0:${{SCHEDULER_SERVICE_PORT_DASHBOARD}}',
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
                    kubernetes.client.V1ServicePort(
                        port=port, target_port=name, name=name
                    )
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
                        deployment='worker',
                    )
                ),
                template=kubernetes.client.V1PodTemplateSpec(
                    metadata=kubernetes.client.V1ObjectMeta(
                        labels=dict(
                            deployment='worker',
                        ),
                    ),
                    spec=kubernetes.client.V1PodSpec(
                        containers=[
                            kubernetes.client.V1Container(
                                name='worker',
                                image=images['worker'].result(),
                                command=[
                                    '/bin/sh', '-c',
                                    'dask-worker ${SCHEDULER_PORT}',
                                    # --memory-limit {int(memory * 1024 * 0.95)}
                                ],
                                volume_mounts=[secret_volume_mount],
                                env=env_vars,
                                resources=kubernetes.client.V1ResourceRequirements(
                                    requests=dict(
                                        cpu='450m',
                                        memory=f'{memory}Ki',
                                    ),
                                ),
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
                                image=images['job'].result(),
                                command=[
                                    '/bin/sh', '-c',
                                    # can this deploy from an GS path?
                                    ' && '.join([
                                        'python3 -m easy_install ${DEPLOY_EGG} > /dev/null',
                                        'python3 -m ${RUN_MODULE}',
                                    ])
                                ],
                                volume_mounts=[secret_volume_mount],
                                env=env_vars,
                                resources=kubernetes.client.V1ResourceRequirements(
                                    requests=dict(
                                        cpu='450m',
                                        memory=f'{memory}Ki',
                                    ),
                                ),
                            ),
                        ],
                        volumes=[secret_volume],
                    ),
                ),
            ),
            # TODO: name specific to this instance
        ),
    )

# ns=$(kubectl get namespaces -o 'jsonpath={.items[*].metadata.name}' --field-selector 'status.phase==Active' | egrep 'edgar-[a-z]*' -o)
# kubectl -n ${ns} logs -f $(kubectl -n ${ns} get -o 'jsonpath={.items[].metadata.name}' pods -l job-name=job)
# kubectl -n ${ns} delete po $(kubectl -n ${ns} get po -o json | jq -r '.items[] | select(.status.podIP == "10.16.3.7") | .metadata.name')
