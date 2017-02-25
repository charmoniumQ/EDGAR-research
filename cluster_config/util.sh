#!/bin/zsh

cluster_name="cluster"
zone="us-east1-b"
main_script="./cluster_config/test.py"
apt_dependencies=(python3 python3-pip)
pip_dependencies=(bs4 six pyhaikunator)
workers=10
ports=(7077 8888)

function provision() {
	result=($(gcloud dataproc clusters list | grep "^$1"))
	if [[ $? -ne 0 ]]
	then
		# https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/issues/25
		gcloud dataproc clusters create "$1" \
			   --project edgar-reserach --zone "$2" \
			   --master-machine-type n1-standard-1  --master-boot-disk-size 10 \
			   --num-workers $workers \
			   --properties spark:spark.executorEnv.PYTHONHASHSEED=0 \
			   --worker-machine-type n1-standard-1 --worker-boot-disk-size 10 || return 1
	else
		zone=${result[4]}
		echo "Already provisioned in $zone"
	fi
}

function get_nodes() {
	master_nodes=($(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.masterConfig.instanceNames)"))
	if [[ ! $? ]]; then return 1; fi
	master_node=${master_nodes[1]}
	slave_nodes=($(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.workerConfig.instanceNames)"))
	if [[ ! $? ]]; then return 1; fi
	all_nodes=(${master_nodes[@]} ${slave_nodes[@]})
}

function configure_node() {
	# https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/issues/25
	gcloud compute ssh --zone "$zone" "$1" <<EOF
echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
sudo apt-get install -y ${apt_dependencies[@]} && \
sudo pip3 install ${pip_dependencies[@]}
EOF
}

function configure_slave() {
	echo "No config necessary"
}

function configure_master() {
	echo "No config necessary"
}

function upload_code() {
	# TODO: clean directory
	python3 setup.py bdist_egg || return 1
	eggs=($(ls dist))
	egg=${eggs[1]}
	echo Transferring egg $egg
	gcloud compute copy-files --zone "$2" "dist/$egg" "${1}:${egg}" || return 1
	rm -rf dist *.egg-info build
	# for source in ${sources[@]}
	# do
	# 	gcloud compute copy-files --zone "$2" $source "${1}:${source}" || return 1
	# 	echo "Copied $source"
	# done
	gcloud compute ssh --zone "$2" "$1" <<EOF
sudo easy_install3 $egg
EOF
	gcloud compute copy-files --zone "$2" "$main_script" "${1}:$(basename $main_script)"
}

function run_spark3() {
	gcloud compute ssh --zone "$2" "$1" -- -L 4040:localhost:4040 <<EOF
export PYTHONHASHSEED=323
export PYSPARK_PYTHON=python3
./$(basename $main_script)
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
