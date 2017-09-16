#!/bin/zsh

cd `dirname $0`/.. # cd to project root

. ./cluster_config/util.sh

provision $cluster $zone || exit 1
get_nodes $cluster $zone || exit 1

if [[ ! $@ =~ "--skip-conf" ]]
then
	for node in ${all_nodes[@]}; do
		echo configuring $node $zone
		configure_node $node $zone || exit 1
	done
fi

echo Running on $master $zone
upload_code $master $zone || exit 1
run_spark3 $master $zone || exit 1

if [[ ! $@ =~ "--persist" ]]
then
	deprovision $cluster $zone || exit 1
fi 
