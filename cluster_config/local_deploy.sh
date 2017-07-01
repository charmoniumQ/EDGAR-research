#!/bin/zsh

cd `dirname $0`/.. # cd to project root

. ./cluster_config/util.sh

if [[ $@ =~ "--pip" ]]
then
	sudo pip3 install ${pip_dependencies[@]}
fi

export PYSPARK_PYTHON=python3
export PYTHONPATH=.:$PYTHONPATH
export SPARK_LOCAL_IP="0.0.0.0"
spark-submit --master "local[*]" "$main_script"

rm_files=(derby.log metastore_db)
# touch ${rm_files[@]}
# /bin/rm -rf ${rm_files=[@]}
