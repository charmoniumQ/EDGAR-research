#!/bin/zsh

cluster="cluster"
zone="us-east1-b"
resources=("cluster_config" "mining")
main_script="./cluster_config/test.py"
apt_dependencies=(python3)
pip_dependencies=(bs4 six pyhaikunator)
workers=10
ports=(7077 8888)

function provision() {
	# upload_code NODE_NAME ZONE
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
	# get_nodes CLUSTER_NAME
	masters=($(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.masterConfig.instanceNames)"))
	if [[ ! $? ]]; then return 1; fi
	master=${masters[1]}
	slaves=($(gcloud dataproc clusters describe "$1" --format="value[delimiter=' '](config.workerConfig.instanceNames)"))
	if [[ ! $? ]]; then return 1; fi
	all_nodes=(${masters[@]} ${slaves[@]})
}

function configure_node() {
	# configure_node NODE_NAME ZONE
	gcloud compute ssh --zone "$2" "$1" <<EOF
# sudo echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
sudo apt-get install -y ${apt_dependencies[@]} && \
sudo pip3 install ${pip_dependencies[@]}
curl https://bootstrap.pypa.io/get-pip.py | sudo python3
EOF
}

function configure_slave() {
	echo "No config necessary"
}

function configure_master() {
	echo "No config necessary"
}

function upload_code() {
	# upload_code NODE_NAME ZONE
	archive="code.tar.gz"
	tar czf $archive $resources || return 1
	gcloud compute copy-files --zone "$2" $archive "$1:$archive" || return 1
	rm $archive
	gcloud compute ssh --zone "$2" "$1" <<EOF
tar xf $archive || exit 1
rm $archive
EOF
}

function run_spark3() {
	# run_spark4 NODE_NAME ZONE
	gcloud compute ssh --zone "$2" "$1" -- -L 4040:localhost:4040 <<EOF
# export PYTHONHASHSEED=0
export PYSPARK_PYTHON=python3
./$main_script
EOF
}

function deprovision() {
	# deprovision NODE_NAME
	gcloud dataproc clusters delete "$1"
}

function shell() {
	# shell NODE_NAME ZONE "EXTRA_SSH_ARGS"
	echo "gcloud compute ssh --zone \"$2\" \"$1\" -- $3"
	gcloud compute ssh --zone "$2" "$1" -- $3
}

function master_shell() {
	get_nodes $cluster $zone
	gcloud compute ssh --zone $zone $master
}

function master_jupyter() {
	PORT=8084
	get_nodes $cluster $zone
	gcloud compute ssh --zone $zone $master -- -L ${PORT}:localhost:$PORT <<EOF
# sudo pip3 install jupyter
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=$PORT"
/usr/lib/spark/bin/pyspark
EOF
}

function shell_with_ports() {
	# shell NODE_NAME ZONE "PORTS"
	L_flag=""
	ports=($(echo $3))
	for port in ${ports[@]}
	do
		L_flag="$L_flag -L ${port}:localhost:$port"
	done
	echo $L_flag
	shell $1 $2 "$L_flag -N"
}
