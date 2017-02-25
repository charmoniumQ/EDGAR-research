#!/bin/bash

cluster_name="cluster"
zone="us-east1-b"
main="PYTHONPATH=. ./cluster_config/test.py"
sources=("cluster_config" "mining" "setup.py")
apt_dependencies=(python3 python3-pip)
pip_dependencies=(bs4 six)
workers=10
ports=(7077 8888)

function provision() {
	result=($(gcloud dataproc clusters list | grep "^$1"))
	if [[ ! $? ]]
	then
		echo $result
		echo ^$1
		gcloud dataproc clusters create "$1" \
			   --project edgar-reserach --zone "$2" \
			   --master-machine-type n1-standard-1  --master-boot-disk-size 10 \
			   --num-workers $workers \
			   --worker-machine-type n1-standard-1 --worker-boot-disk-size 10 || exit 1
	else
		zone=${result[3]}
		echo "Already provisioned in $zone"
	fi
}

function get_nodes() {
	master_nodes=($(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.masterConfig.instanceNames)"))
	if [[ ! $? ]]; then exit 1; fi
	slave_nodes=( $(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.workerConfig.instanceNames)"))
	if [[ ! $? ]]; then exit 1; fi
	all_nodes=(${master_nodes[@]} ${slave_nodes[@]})
}

function configure_node() {
	gcloud compute ssh --zone "$zone" "$1" <<EOF
sudo apt-get install -y ${apt_dependencies[@]} && \
sudo pip3 install ${pip_dependencies[@]}
EOF
}

function configure_slave() {
	echo "No config necessary"
}

function configure_master() {
	for source in ${sources[@]}
	do
		gcloud compute copy-files --zone "$2" $source "${1}:${source}" || exit 1
		echo "Copied $source"
	done
	gcloud compute ssh --zone "$2" "$1" <<EOF
python3 setup.py bdist_egg
EOF
}

function run_spark2() {
	gcloud compute ssh --zone "$2" "$1" <<EOF
PYSPARK_PYTHON=python2 $main
EOF
}

function run_spark3() {
	gcloud compute ssh --zone "$2" "$1" <<EOF
PYSPARK_PYTHON=python3 $main
EOF
}

function deprovision() {
	gcloud dataproc clusters delete "$1"
}

function shell() {
	echo "gcloud compute ssh --zone \"$2\" \"$1\" -- $3"
	gcloud compute ssh --zone "$2" "$1" -- $3
}

function shell_with_ports() {
	L_flag=""
	ports=($(echo $3))
	for port in ${ports[@]}
	do
		L_flag="$L_flag -L ${port}:localhost:${port}"
	done
	echo $L_flag
	shell $1 $2 "$L_flag -N"
}
