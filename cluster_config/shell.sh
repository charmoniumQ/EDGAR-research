#!/bin/bash

cd $(dirname $0)

. info.sh

ports="8888 7077"

L_string=""
for port in $ports
do
	L_string="$L_string -L $port:localhost:$port"
done


echo ssh -N $L_string -i $identity $user@$host
ssh -N $L_string -i $identity $user@$host

# PYSPARK_PYTHON=pthon3 PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" pyspark
