#!/bin/zsh

cd `dirname $0`/.. # cd to project root

. ./cluster_config/util.sh

provision $cluster_name $zone || exit 1
get_nodes $cluster_name $zone || exit 1

if [[ ! $@ =~ "--skip-conf" ]]
then
	for node in ${all_nodes[@]}; do
		echo configuring $node $zone
		configure_node $node $zone || exit 1
	done
fi

echo Running on $master_node $zone
upload_code $master_node $zone || exit 1
run_spark3 $master_node $zone || exit 1

if [[ ! $@ =~ "--persist" ]]
then
	deprovision $cluster_name $zone || exit 1
fi 
