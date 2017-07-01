#!/bin/zsh

cluster="my-cluster4"
zone="us-east1-b"
main_script="cluster_config/frequency.py"
apt_dependencies=(python3 python3-numpy python3-nltk)
pip_dependencies=(bs4 six pyhaikunator)
workers=4
ports=(7077 8888)
egg="EDGAR_research-0.1-py3.5.egg"

function provision {
	# provision CLUSTER_NAME ZONE
	result=($(gcloud dataproc clusters list -q | grep "^$1"))
	if [[ $? -ne 0 ]]
	then
		# https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/issues/25
		gcloud -q dataproc clusters create "$1" \
		       --project edgar-reserach --zone "$2" \
		       --master-machine-type n1-standard-1  --master-boot-disk-size 10 \
		       --num-workers $workers \
		       --properties spark:spark.executorEnv.PYTHONHASHSEED=0 \
		       --worker-machine-type n1-standard-1 --worker-boot-disk-size 10 || return 1
		already_provisioned=0
	else
		zone=${result[4]}
		echo "Already provisioned in $zone"
		already_provisioned=1
	fi
}

function get_nodes {
	# get_nodes CLUSTER_NAME
	masters=($(gcloud dataproc clusters describe "$1" --format="value[delimiter='
'](config.masterConfig.instanceNames)" | sort))
	master=${masters[1]}
	slaves=($(gcloud dataproc clusters describe "$1" --format="value[delimiter='
'](config.workerConfig.instanceNames)" | sort))
	all_nodes=(${masters[@]} ${slaves[@]})
}

function configure_node {
	# configure_node NODE_NAME ZONE
	gcloud compute ssh --zone "$2" "$1" -q <<EOF
set -x -e
# sudo echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
sudo touch /etc/motd && sudo rm /etc/motd
sudo apt-get -qq install -y ${apt_dependencies[@]}
curl -s https://bootstrap.pypa.io/get-pip.py | sudo python3 > /dev/null
sudo pip3 -q install ${pip_dependencies[@]}
sudo python3 -m nltk.downloader -q -d /usr/local/lib/nltk_data punkt
EOF
}

function configure_slave {
	echo "No config necessary"
}

function configure_master {
	echo "No config necessary"
}

function upload_code {
	# upload_code NODE_NAME ZONE
	gcloud -q compute ssh --zone "$2" "$1" <<EOF
rm -rf *
EOF
	python3 setup.py -q bdist_egg
	gcloud -q compute copy-files --zone "$2" dist/$egg "${1}:${egg}" || return 1
	clean_build
	gcloud -q compute ssh --zone "$2" "$1" <<EOF
unzip -q $egg
chmod +x $main_script
EOF
}

function clean_build {
	/bin/rm -r dist build *.egg-info
}

function run_spark3 {
	# run_spark3 NODE_NAME ZONE
	gcloud -q compute copy-files --zone "$2" ./cluster_config/transfer "${1}:"
	gcloud -q compute ssh --zone "$2" "$1" -- -L 4040:localhost:4040 <<EOF
# export PYTHONHASHSEED=0
export PYSPARK_PYTHON=python3
export PYTHONPATH=.:$PYTHONPATH
/usr/lib/spark/bin/spark-submit $main_script | tee transfer/output.txt
EOF
	gcloud compute copy-files --zone "$2" "${1}:transfer/*" ./cluster_config/transfer
}

function deprovision {
	# deprovision NODE_NAME
	yes | gcloud -q dataproc clusters delete "$1"
}

function shell {
	# shell NODE_NAME ZONE "EXTRA_SSH_ARGS"
	echo "gcloud compute ssh --zone \"$2\" \"$1\" -- $3"
	gcloud compute ssh --zone "$2" "$1" -- $3
}

function master_shell {
	get_nodes $cluster $zone
	gcloud compute ssh --zone $zone $master -- -L 4040:localhost:4040 -L 18080:localhost:18080 -L 8080:localhost:8080
}

function master_jupyter {
	PORT=8084
	get_nodes $cluster $zone
	gcloud compute ssh --zone $zone $master -- -L ${PORT}:localhost:$PORT <<EOF
sudo pip3 install jupyter
pkill jupyter
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=$PORT"
/usr/lib/spark/bin/pyspark
EOF
}

function status {
	get_nodes $cluster $zone
	gcloud compute ssh --zone $zone $master -- -L 4044:localhost:4044 -N
}

function shell_with_ports {
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
