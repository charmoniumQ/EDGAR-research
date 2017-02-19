#!/bin/bash

cd `dirname $0`/.. # cd to project root

. ./cluster_config/util.sh

provision $cluster_name $zone || exit 1
get_nodes $cluster_name $zone || exit 1

if [[ ! $@ =~ "--skip-generic-conf" ]]
then
	for node in ${all_nodes[@]}; do
		echo configuring $node $zone
		configure_node $node $zone || exit 1
	done
fi

if [[ ! $@ =~ "--skip-master-conf" ]]
then
	
	for node in ${master_nodes[@]}; do
		echo configuring master $node $zone
		configure_master $node $zone || exit 1
	done
fi

if [[ ! $@ =~ "--skip-slave-conf" ]]
then
	for node in ${slave_nodes[@]}; do
		echo configuring slave $node $zone
		configure_slave $node $zone || exit 1
	done
fi

echo Running on ${master_nodes[1]} $zone
run_spark3 ${master_nodes[1]} $zone

if [[ ! $@ =~ "--persist-cluster" ]]
then
	deprovision $cluster_name $zone || exit 1
fi 
