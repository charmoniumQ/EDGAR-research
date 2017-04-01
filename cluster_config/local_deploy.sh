#!/bin/zsh

cd `dirname $0`/.. # cd to project root

. ./cluster_config/util.sh

if [[ $@ =~ "--pip" ]]
then
	sudo pip3 install ${pip_dependencies[@]}
fi

export PYSPARK_PYTHON=python3
export PYTHONPATH=.:$PYTHONPATH
spark-submit $main_script
