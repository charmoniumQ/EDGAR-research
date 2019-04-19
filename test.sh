#!/usr/bin/env zsh

while ! kubectl get namespaces -o 'jsonpath={.items[*].metadata.name}' --field-selector 'status.phase==Active' | egrep 'edgar-[a-z]*' -o
do
    echo waiting for ns
    sleep 5
done

ns=$(kubectl get namespaces -o 'jsonpath={.items[*].metadata.name}' --field-selector 'status.phase==Active' | egrep 'edgar-[a-z]*' -o)

while ! job_pod=$(kubectl -n ${ns} get -o 'jsonpath={.items[0].metadata.name}' pods -l job-name=job)
do
    echo Waiting for job pod
    sleep 5
done

while kube_logs=$(kubectl -n ${ns} get po | grep Pending)
do
    echo Pending $(echo ${kube_logs} | wc -l)
    sleep 5
done
while kube_logs=$(kubectl -n ${ns} get po | grep ContainerCreating)
do
    echo ContainerCreating $(echo ${kube_logs} | wc -l)
    sleep 5
done

kubectl -n ${ns} port-forward $(kubectl -n ${ns} get -o 'jsonpath={.items[].metadata.name}' pods -l deployment=scheduler) 8787:8787 > /dev/null &
port_forward_pid=$!

echo logging

kubectl -n ${ns} logs -f ${job_pod}
notify 'remote job done'
kill $!
